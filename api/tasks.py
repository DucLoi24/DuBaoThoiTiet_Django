# api/tasks.py
import requests
import json
import logging
from datetime import datetime, timedelta, date, timezone as dt_timezone # Import timezone từ datetime
from django.conf import settings
from django.db import transaction
from django.utils import timezone # Sử dụng timezone của Django
from concurrent.futures import ThreadPoolExecutor, as_completed # Import ThreadPoolExecutor

from .models import Location, WeatherData, ExtremeEvent

# Thiết lập logger riêng cho file tasks
# Level INFO sẽ ghi lại các bước chính, DEBUG sẽ ghi chi tiết hơn
logger = logging.getLogger(__name__)

# --- HÀM TIỆN ÍCH CHO TASKS ---

def call_weather_api_from_task(endpoint, params):
    """
    Hàm gọi API WeatherAPI dành riêng cho tasks, xử lý lỗi chi tiết hơn.
    Trả về tuple: (data, error_message). data là None nếu có lỗi.
    """
    if not settings.WEATHER_API_KEY:
        logger.error("WEATHER_API_KEY is not configured.")
        return None, "Weather API Key missing"

    params['key'] = settings.WEATHER_API_KEY
    params['lang'] = 'vi'
    full_url = f"{settings.BASE_WEATHER_URL}/{endpoint}.json"

    try:
        # Tăng timeout lên 30 giây cho các cuộc gọi API mạng
        response = requests.get(full_url, params=params, timeout=30)
        response.raise_for_status() # Ném lỗi HTTPError cho status >= 400
        return response.json(), None # Trả về dữ liệu JSON nếu thành công
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout calling WeatherAPI endpoint: {endpoint} for location: {params.get('q')}")
        return None, "API Timeout"
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        try:
            # Cố gắng lấy lỗi JSON nếu có
            error_data = e.response.json()
        except json.JSONDecodeError:
            # Nếu không phải JSON, lấy text thô
            error_data = {'message': e.response.text[:500]} # Giới hạn độ dài lỗi
        logger.error(f"HTTP Error calling WeatherAPI ({endpoint}) for {params.get('q')}: {status_code} - {error_data}")
        return None, f"API HTTP Error: {status_code}"
    except requests.exceptions.RequestException as e:
        logger.error(f"General Error calling WeatherAPI ({endpoint}) for {params.get('q')}: {e}")
        return None, f"API Request Error: {e}"
    except Exception as e:
        # Ghi lại lỗi không mong muốn kèm traceback
        logger.error(f"Unexpected error in call_weather_api_from_task: {e}", exc_info=True)
        return None, f"Unexpected Error: {e}"


def call_local_ai_for_analysis(time_series_data):
    """
    Hàm gọi API Ollama cục bộ để phân tích dữ liệu thời tiết.
    Sử dụng prompt "Chuyên gia Thận trọng" và trả về mảng cảnh báo.
    Đã cập nhật để xử lý response linh hoạt (list hoặc dict đơn).
    """
    prompt = f"""
        **VAI TRÒ:**
        Bạn là một chuyên gia khí tượng thủy văn thận trọng.

        **QUY TẮC VÀNG: HÃY HOÀI NGHI.** Câu trả lời mặc định là một mảng rỗng [].

        **DỮ LIỆU ĐẦU VÀO:**
        Chuỗi dữ liệu thời tiết 14 ngày (lịch sử + dự báo):
        {json.dumps(time_series_data, indent=2, default=str)}

        **CÁC NGƯỠNG KÍCH HOẠT CẢNH BÁO (Chỉ báo cáo nếu vượt ngưỡng):**
        - Cháy rừng (INFRASTRUCTURE - HIGH/CRITICAL): Nhiệt độ (avgtemp_c) > 37°C trong ÍT NHẤT 3 ngày VÀ độ ẩm (avghumidity) < 40%.
        - Sốc nhiệt (PUBLIC_HEALTH - HIGH): Nhiệt độ (avgtemp_c) > 38°C VÀ UV > 10 trong ÍT NHẤT 2 ngày.
        - Sâu bệnh (AGRICULTURE - MEDIUM): Độ ẩm (avghumidity) > 90% trong ÍT NHẤT 4 ngày VÀ nhiệt độ (avgtemp_c) > 25°C.

        **YÊU CẦU ĐẦU RA:**
        Chỉ trả lời bằng một MẢNG (array) các đối tượng JSON. Nếu không có rủi ro, trả về [].
        Cấu trúc của mỗi đối tượng:
        {{
          "severity": "Mức độ ('MEDIUM', 'HIGH', 'CRITICAL')",
          "impact_field": "Lĩnh vực ('AGRICULTURE', 'INFRASTRUCTURE', 'PUBLIC_HEALTH')",
          "forecast_details_vi": "Mô tả rủi ro và trích dẫn SỐ LIỆU bằng chứng.",
          "actionable_advice_vi": "Đưa ra một câu KHUYẾN NGHỊ hành động cụ thể."
        }}
    """
    try:
        # Log nhẹ nhàng hơn khi gọi AI
        logger.debug("[LOCAL AI] Sending analysis request to Ollama...")
        # Tăng timeout lên 5 phút (300 giây) vì AI có thể cần nhiều thời gian
        response = requests.post(settings.OLLAMA_API_URL, json={
            "model": "gemma3",
            "prompt": prompt,
            "format": "json",
            "stream": False
        }, timeout=300)
        response.raise_for_status()

        response_data = response.json()
        # Ollama trả về JSON string trong trường 'response'
        if 'response' in response_data:
            try:
                # Parse JSON string từ response của Ollama
                raw_result = json.loads(response_data['response'])

                # --- Xử lý Response Linh hoạt ---
                if isinstance(raw_result, list):
                    # Nếu AI trả về đúng là một list (kể cả list rỗng []), dùng nó luôn
                    return raw_result, None
                elif isinstance(raw_result, dict) and raw_result.get('severity', 'NONE').upper() != 'NONE':
                    # Nếu AI trả về một object và có severity khác NONE => cảnh báo đơn lẻ
                    logger.warning(f"[LOCAL AI] Ollama returned a single object, wrapping in list: {raw_result}")
                    return [raw_result], None # Gói vào list
                elif isinstance(raw_result, dict) and not raw_result:
                    # Nếu AI trả về object rỗng {}
                    logger.debug("[LOCAL AI] Ollama returned an empty object, treating as no alerts.")
                    return [], None # Coi như không có cảnh báo
                else:
                    # Các trường hợp khác (object có severity NONE, hoặc không phải list/dict)
                    logger.warning(f"[LOCAL AI] Ollama response was not a valid list/object: {raw_result}")
                    return [], "AI response invalid structure"
                # --- Kết thúc Xử lý Linh hoạt ---

            except json.JSONDecodeError as e:
                logger.error(f"[LOCAL AI] Error parsing JSON from Ollama response: {e}")
                logger.error(f"Ollama raw response string: {response_data.get('response', 'N/A')}")
                return [], "AI Response Parsing Error"
        else:
             logger.warning(f"[LOCAL AI] 'response' field missing in Ollama output: {response_data}")
             return [], "AI response field missing"

    except requests.exceptions.Timeout:
        logger.error("[LOCAL AI] Timeout calling local Ollama API (waited 300 seconds).")
        return [], "AI Timeout"
    except requests.exceptions.RequestException as e:
        logger.error(f"[LOCAL AI] Error calling Ollama API: {e}")
        logger.info("💡 Tip: Ensure Ollama is running and the 'gemma3' model is downloaded ('ollama run gemma3').")
        return [], f"AI Connection Error: {e}"
    except Exception as e:
        logger.error(f"[LOCAL AI] Unexpected error during AI analysis: {e}", exc_info=True)
        return [], f"Unexpected AI Error: {e}"

# --- CÁC HÀM TÁC VỤ NỀN (CRON JOBS) ---

@transaction.atomic # Đảm bảo tất cả các thao tác CSDL trong hàm này thành công hoặc thất bại cùng nhau
def trigger_data_ingestion():
    """ 
    Tác vụ thu thập dữ liệu lịch sử và dự báo (Cron job chạy hàng loạt) 
    Hàm này giờ chỉ gọi hàm con 'ingest_data_for_single_location'.
    """
    logger.info("--- [TASK START] Running Full Data Ingestion ---")
    active_locations = Location.objects.filter(is_active=True)
    if not active_locations.exists():
        logger.info("[DATA INGESTION] No active locations.")
        return {'success': True, 'message': 'No active locations.'}

    logger.info(f"[DATA INGESTION] Found {active_locations.count()} active location(s).")

    total_success = 0
    total_fail = 0

    # Vòng lặp này giờ đã sạch và đơn giản hơn rất nhiều
    for loc in active_locations:
        try:
            # Gọi hàm con cho từng địa điểm
            success = ingest_data_for_single_location(loc.location_id) 
            if success:
                total_success += 1
            else:
                total_fail += 1
                logger.warning(f"[DATA INGESTION] Failed to ingest data for loc {loc.location_id} during cron job.")
        except Exception as e:
            # Lỗi nghiêm trọng khi chạy hàm con
            logger.error(f"[DATA INGESTION] Critical error processing loc {loc.location_id}: {e}", exc_info=True)
            total_fail += 1

    errors_occurred = total_fail > 0
    logger.info(f"--- [TASK FINISH] Data Ingestion completed. Succeeded for {total_success} locations. Failed for {total_fail} locations. ---")
    return {'success': not errors_occurred, 'message': f'Data Ingestion completed. Success: {total_success}, Fail: {total_fail}.'}

# --- HÀM CON ĐỂ XỬ LÝ MỘT THÀNH PHỐ (ĐỊNH NGHĨA TRƯỚC) ---
def analyze_single_location(loc):
    """ Lấy dữ liệu, gọi AI và trả về kết quả cho một thành phố duy nhất """
    logger.debug(f"[LLM ANALYSIS] Analyzing location: {loc.name_en} (ID: {loc.location_id})")
    # Lấy 14 ngày dữ liệu gần nhất, sử dụng select_related để tối ưu
    time_series_qs = WeatherData.objects.select_related('location').filter(location=loc).order_by('-record_time')[:14]

    if len(time_series_qs) < 14:
        logger.warning(f"[LLM ANALYSIS] Not enough data for {loc.name_en} ({len(time_series_qs)}/14). Skipping.")
        # Trả về lỗi để hàm cha biết tác vụ con thất bại
        return loc, [], "Not enough data"

    # Chuyển đổi và sắp xếp cẩn thận
    try:
        time_series_data = sorted(
            list(time_series_qs.values('record_time', 'temp_c', 'humidity', 'wind_kph', 'data_type')),
            key=lambda x: x['record_time'] # Sắp xếp theo record_time
        )
    except Exception as e:
        logger.error(f"Error preparing time series data for {loc.name_en}: {e}", exc_info=True)
        return loc, [], f"Data preparation error: {e}"

    # Gọi AI
    alert_results, ai_err = call_local_ai_for_analysis(time_series_data)
    # Trả về kết quả, bao gồm cả lỗi AI nếu có
    return loc, alert_results, ai_err


# --- HÀM PHÂN TÍCH AI CHÍNH (XỬ LÝ ĐỒNG THỜI) ---
@transaction.atomic # Đảm bảo lưu CSDL an toàn khi chạy song song
def trigger_llm_analysis():
    """ Tác vụ phân tích AI - Chạy đồng thời cho nhiều thành phố """
    logger.info("--- [TASK START] Running CONCURRENT Local LLM Analysis ---")
    active_locations = Location.objects.filter(is_active=True)
    if not active_locations.exists():
        logger.info("[LLM ANALYSIS] No active locations.")
        return {'success': True, 'message': 'No active locations.'}

    logger.info(f"[LLM ANALYSIS] Found {active_locations.count()} active location(s) for concurrent analysis.")
    alerts_created_count = 0
    errors_occurred_ai = False # Cờ lỗi riêng cho việc gọi/parse AI response
    errors_occurred_db = False # Cờ lỗi riêng cho việc lưu CSDL
    errors_occurred_data = False # Cờ lỗi riêng cho việc chuẩn bị data

    # Sử dụng ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=3) as executor: # Giới hạn số luồng
        # Submit tasks
        future_to_loc_obj = {executor.submit(analyze_single_location, loc_obj): loc_obj for loc_obj in active_locations}

        # Xử lý kết quả khi hoàn thành
        for future in as_completed(future_to_loc_obj):
            loc_obj_from_future = future_to_loc_obj[future]
            try:
                analyzed_loc_obj, alert_results, task_err = future.result()

                # Kiểm tra lỗi trả về từ tác vụ con
                if task_err and task_err != "Not enough data":
                    errors_occurred_ai = True
                    logger.error(f"AI analysis task failed for {analyzed_loc_obj.name_en}: {task_err}")
                    continue
                elif task_err == "Not enough data":
                    errors_occurred_data = True # Ghi nhận lỗi thiếu data
                    continue

                # Lưu kết quả cảnh báo (nếu có)
                if alert_results:
                    logger.info(f"[LLM ANALYSIS] Storing {len(alert_results)} alert(s) for {analyzed_loc_obj.name_en}...")
                    for alert in alert_results:
                        required_keys = ['severity', 'impact_field', 'forecast_details_vi', 'actionable_advice_vi']
                        if isinstance(alert, dict) and all(k in alert and isinstance(alert[k], str) and alert[k] for k in required_keys):
                            try:
                                ExtremeEvent.objects.create(
                                    location=analyzed_loc_obj,
                                    severity=alert['severity'],
                                    impact_field=alert['impact_field'],
                                    forecast_details_vi=alert['forecast_details_vi'],
                                    actionable_advice_vi=alert['actionable_advice_vi'],
                                    raw_llm_json=alert
                                )
                                alerts_created_count += 1
                            except Exception as db_exc:
                                logger.error(f"Error saving alert for {analyzed_loc_obj.name_en}: {db_exc}", exc_info=True)
                                errors_occurred_db = True
                        else:
                            logger.warning(f"[LLM ANALYSIS] Invalid alert structure for {analyzed_loc_obj.name_en}: {alert}")
                            # Không bật cờ lỗi AI ở đây nếu call_local_ai_for_analysis đã xử lý
            except Exception as exc:
                logger.error(f"Error processing result for location {loc_obj_from_future.name_en}: {exc}", exc_info=True)
                errors_occurred_ai = True

    any_critical_errors = errors_occurred_ai or errors_occurred_db
    logger.info(f"--- [TASK FINISH] CONCURRENT LLM Analysis completed. Created {alerts_created_count} alerts. Critical errors: {any_critical_errors} (AI: {errors_occurred_ai}, DB: {errors_occurred_db}, Data: {errors_occurred_data}) ---")
    return {'success': not any_critical_errors, 'message': f'Concurrent analysis completed. Created {alerts_created_count} alerts.'}

def ingest_data_for_single_location(location_id):
    """ Tác vụ thu thập dữ liệu tức thì cho MỘT địa điểm mới. """
    try:
        loc = Location.objects.get(location_id=location_id)
        logger.info(f"[INSTANT INGEST] Running for new location: {loc.name_en} (ID: {loc.location_id})")
    except Location.DoesNotExist:
        logger.error(f"[INSTANT INGEST] Location ID {location_id} not found.")
        return False

    # Lấy 7 ngày lịch sử và 7 ngày dự báo (giống hệt logic trong hàm cron)
    today = timezone.now().date()
    end_date_hist = today - timedelta(days=1)
    start_date_hist = end_date_hist - timedelta(days=6)
    dt_str = start_date_hist.strftime('%Y-%m-%d')
    end_dt_str = end_date_hist.strftime('%Y-%m-%d')

    all_records_to_insert = []
    errors_occurred = False

    # --- Lấy lịch sử ---
    logger.debug(f"[INSTANT INGEST] Fetching history for {loc.name_en}")
    hist_data, hist_err = call_weather_api_from_task('history', {'q': loc.name_en, 'dt': dt_str, 'end_dt': end_dt_str})
    if hist_data and 'forecast' in hist_data and 'forecastday' in hist_data['forecast']:
        for day in hist_data['forecast']['forecastday']:
            try:
                record_dt_naive = datetime.strptime(day['date'], '%Y-%m-%d')
                record_dt_aware = timezone.make_aware(record_dt_naive, dt_timezone.utc)
                all_records_to_insert.append(WeatherData(
                    location=loc, record_time=record_dt_aware, data_type='HISTORY',
                    temp_c=day['day'].get('avgtemp_c'), humidity=day['day'].get('avghumidity'),
                    uv_index=day['day'].get('uv'), wind_kph=day['day'].get('maxwind_kph'), raw_json=day
                ))
            except (ValueError, KeyError, TypeError) as e:
                logger.warning(f"[INSTANT INGEST] Skipping invalid history record for {loc.name_en} on {day.get('date')}: {e}")
    elif hist_err:
        errors_occurred = True
        logger.error(f"[INSTANT INGEST] Failed to fetch history for {loc.name_en}: {hist_err}")

    # --- Lấy dự báo ---
    logger.debug(f"[INSTANT INGEST] Fetching 7-day forecast for {loc.name_en}")
    fc_data, fc_err = call_weather_api_from_task('forecast', {'q': loc.name_en, 'days': 7})
    if fc_data and 'forecast' in fc_data and 'forecastday' in fc_data['forecast']:
        for day in fc_data['forecast']['forecastday']:
             try:
                record_dt_naive = datetime.strptime(day['date'], '%Y-%m-%d')
                record_dt_aware = timezone.make_aware(record_dt_naive, dt_timezone.utc)
                all_records_to_insert.append(WeatherData(
                    location=loc, record_time=record_dt_aware, data_type='FORECAST',
                    temp_c=day['day'].get('avgtemp_c'), humidity=day['day'].get('avghumidity'),
                    uv_index=day['day'].get('uv'), wind_kph=day['day'].get('maxwind_kph'), raw_json=day
                ))
             except (ValueError, KeyError, TypeError) as e:
                logger.warning(f"[INSTANT INGEST] Skipping invalid forecast record for {loc.name_en} on {day.get('date')}: {e}")
    elif fc_err:
        errors_occurred = True
        logger.error(f"[INSTANT INGEST] Failed to fetch forecast for {loc.name_en}: {fc_err}")

    # --- Bulk insert ---
    if all_records_to_insert:
        try:
            created_records = WeatherData.objects.bulk_create(all_records_to_insert, ignore_conflicts=True)
            count = len(created_records)
            logger.info(f"[INSTANT INGEST] Stored {count} new unique records for {loc.name_en}.")
            return True # Báo hiệu thành công
        except Exception as e:
            logger.error(f"[INSTANT INGEST] Error bulk inserting weather data for {loc.name_en}: {e}", exc_info=True)
            return False # Báo hiệu thất bại
    
    return not errors_occurred

# @transaction.atomic
# def trigger_data_pruning():
#     """ Tác vụ dọn dẹp dữ liệu cũ - Đã comment out """
#     logger.info("--- [TASK START] Running Data Pruning ---")
#     ninety_days_ago = timezone.now() - timedelta(days=90)
#     try:
#         deleted_weather, _ = WeatherData.objects.filter(record_time__lt=ninety_days_ago).delete()
#         logger.info(f"[DATA PRUNING] Deleted {deleted_weather} old WeatherData records.")
#
#         thirty_days_ago = timezone.now() - timedelta(days=30)
#         deleted_events, _ = ExtremeEvent.objects.filter(analysis_time__lt=thirty_days_ago).delete()
#         logger.info(f"[DATA PRUNING] Deleted {deleted_events} old ExtremeEvent records.")
#
#         return {'success': True, 'message': f'Deleted {deleted_weather} weather records and {deleted_events} event records.'}
#     except Exception as e:
#         logger.error(f"--- [TASK ERROR] Data Pruning: {e}", exc_info=True)
#         return {'success': False, 'message': 'Error during Data Pruning.'}

