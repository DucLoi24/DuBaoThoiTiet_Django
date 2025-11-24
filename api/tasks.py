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

def call_local_ai_for_advice(hourly_time_series_data):
    """
    Hàm gọi API Ollama cục bộ để lấy lời khuyên hoặc cảnh báo THEO YÊU CẦU.
    Prompt khác biệt: Ưu tiên lời khuyên, nhưng sẽ cảnh báo nếu có dấu hiệu cực đoan.
    Trả về dict: {"type": "advice" | "warning", "message_vi": "..."} hoặc None nếu lỗi.
    """
    
    # === PRE-PROCESSING: Phân tích dữ liệu trước khi gửi AI ===
    if not hourly_time_series_data:
        return {"type": "error", "message_vi": "Không có dữ liệu thời tiết để phân tích."}
    
    # Tính toán các chỉ số quan trọng
    temps = [h.get('temp_c', 0) for h in hourly_time_series_data if h.get('temp_c') is not None]
    winds = [h.get('wind_kph', 0) for h in hourly_time_series_data if h.get('wind_kph') is not None]
    precips = [h.get('precip_mm', 0) for h in hourly_time_series_data if h.get('precip_mm') is not None]
    rain_chances = [h.get('chance_of_rain', 0) for h in hourly_time_series_data if h.get('chance_of_rain') is not None]
    
    # Tính min/max/avg
    temp_min = min(temps) if temps else 0
    temp_max = max(temps) if temps else 0
    temp_avg = sum(temps) / len(temps) if temps else 0
    wind_max = max(winds) if winds else 0
    wind_avg = sum(winds) / len(winds) if winds else 0
    precip_total = sum(precips) if precips else 0
    precip_max = max(precips) if precips else 0
    rain_chance_max = max(rain_chances) if rain_chances else 0
    
    # Đếm số giờ có mưa to
    heavy_rain_hours = sum(1 for p in precips if p > 10)
    high_rain_chance_hours = sum(1 for r in rain_chances if r > 70)
    
    # Đếm số giờ nắng nóng (temp > 35)
    hot_hours = sum(1 for t in temps if t > 35)
    
    # Đếm số giờ gió mạnh (wind > 40)
    strong_wind_hours = sum(1 for w in winds if w > 40)
    
    # Đếm số giờ rét (temp < 10)
    cold_hours = sum(1 for t in temps if t < 10)
    
    # Xác định loại cảnh báo dựa trên logic rõ ràng
    warning_type = None
    warning_details = {}
    
    # Cảnh báo lũ lụt (mưa to kéo dài)
    if precip_total > 100 or (heavy_rain_hours >= 6 and precip_total > 50):
        warning_type = "flood_risk"
        warning_details = {
            "precip_total": precip_total,
            "precip_max": precip_max,
            "heavy_rain_hours": heavy_rain_hours,
            "risk_level": "high" if precip_total > 150 else "moderate"
        }
    # Cảnh báo mưa to
    elif heavy_rain_hours >= 3 or (precip_max > 20 and high_rain_chance_hours >= 4):
        warning_type = "heavy_rain"
        warning_details = {
            "precip_max": precip_max,
            "precip_total": precip_total,
            "heavy_rain_hours": heavy_rain_hours,
            "rain_chance_max": rain_chance_max
        }
    # Cảnh báo nắng nóng nguy hiểm
    elif hot_hours >= 4:
        warning_type = "extreme_heat"
        warning_details = {
            "temp_max": temp_max,
            "hot_hours": hot_hours,
            "risk_level": "extreme" if temp_max > 39 else "high"
        }
    # Cảnh báo bão/gió mạnh
    elif strong_wind_hours >= 3 or wind_max > 50:
        warning_type = "strong_wind"
        warning_details = {
            "wind_max": wind_max,
            "wind_avg": wind_avg,
            "strong_wind_hours": strong_wind_hours,
            "risk_level": "storm" if wind_max > 60 else "strong_wind"
        }
    # Cảnh báo rét đậm
    elif cold_hours >= 6:
        warning_type = "extreme_cold"
        warning_details = {
            "temp_min": temp_min,
            "cold_hours": cold_hours,
            "risk_level": "extreme" if temp_min < 5 else "high"
        }
    
    # Tạo summary cho AI
    weather_summary = {
        "temp_range": f"{temp_min:.1f}°C - {temp_max:.1f}°C",
        "temp_avg": f"{temp_avg:.1f}°C",
        "wind_max": f"{wind_max:.1f} km/h",
        "wind_avg": f"{wind_avg:.1f} km/h",
        "precip_total": f"{precip_total:.1f} mm",
        "precip_max": f"{precip_max:.1f} mm/giờ",
        "rain_chance_max": f"{rain_chance_max}%",
        "warning_detected": warning_type,
        "warning_details": warning_details
    }
    
    # Lấy ngày hôm nay
    today_str = timezone.localdate().strftime('%d/%m/%Y')
    
    # --- PROMPT ĐƠN GIẢN HÓA (SỬ DỤNG SUMMARY) ---
    if warning_type:
        # Mapping tên cảnh báo sang tiếng Việt
        warning_names = {
            "flood_risk": "nguy cơ lũ lụt",
            "heavy_rain": "mưa to",
            "extreme_heat": "nắng nóng nguy hiểm",
            "strong_wind": "gió mạnh/bão",
            "extreme_cold": "rét đậm"
        }
        warning_vn = warning_names.get(warning_type, warning_type)
        
        # Nếu đã phát hiện cảnh báo, yêu cầu AI viết message cảnh báo
        prompt = f"""
**VAI TRÒ:** Chuyên gia thời tiết Việt Nam viết cảnh báo cho người dùng.

**NGÀY HÔM NAY:** {today_str}

**PHÂN TÍCH ĐÃ HOÀN TẤT:**
- Nhiệt độ: {weather_summary['temp_range']} (TB: {weather_summary['temp_avg']})
- Gió: Tối đa {weather_summary['wind_max']} (TB: {weather_summary['wind_avg']})
- Mưa: Tổng {weather_summary['precip_total']}, tối đa {weather_summary['precip_max']}/giờ
- Khả năng mưa: Tối đa {weather_summary['rain_chance_max']}

**CẢNH BÁO PHÁT HIỆN:** {warning_vn}
Chi tiết: {json.dumps(warning_details, ensure_ascii=False)}

**NHIỆM VỤ:** Viết cảnh báo ngắn gọn (2-3 câu) bằng tiếng Việt tự nhiên về {warning_vn} dựa trên số liệu trên. Bao gồm:
- Mô tả nguy cơ cụ thể
- Lời khuyên an toàn/phòng tránh

**ĐẦU RA (JSON):**
{{"type": "warning", "message_vi": "Cảnh báo cụ thể với số liệu + lời khuyên hành động"}}
"""
    else:
        # Không có cảnh báo, tạo lời khuyên
        prompt = f"""
**VAI TRÒ:** Chuyên gia thời tiết Việt Nam đưa lời khuyên cho người dùng.

**NGÀY HÔM NAY:** {today_str}

**PHÂN TÍCH THỜI TIẾT 2-3 NGÀY TỚI:**
- Nhiệt độ: {weather_summary['temp_range']} (TB: {weather_summary['temp_avg']})
- Gió: Tối đa {weather_summary['wind_max']} (TB: {weather_summary['wind_avg']})
- Mưa: Tổng {weather_summary['precip_total']}, khả năng tối đa {weather_summary['rain_chance_max']}

**NHIỆM VỤ:** Viết lời khuyên ngắn gọn (2-3 câu) bằng tiếng Việt tự nhiên về:
- Thời tiết chung (nắng/mát/mưa nhẹ...)
- Hoạt động phù hợp (dã ngoại, thể thao, mang ô...)

**ĐẦU RA (JSON):**
{{"type": "advice", "message_vi": "Lời khuyên cụ thể dựa trên thời tiết"}}
"""
    # --- KẾT THÚC PROMPT ---

    try:
        logger.debug("[LOCAL AI ADVICE] Sending advice request to Ollama...")
        # Timeout có thể ngắn hơn cho lời khuyên, ví dụ 2 phút (120 giây)
        response = requests.post(settings.OLLAMA_API_URL, json={
            "model": "gemma3:4b", # gemma3:4b
            "prompt": prompt,
            "format": "json",
            "stream": False,
            "keep_alive": "1h"
        }, timeout=300) 
        response.raise_for_status()

        response_data = response.json()
        if 'response' in response_data:
            try:
                # Parse JSON string từ response của Ollama
                result_json = json.loads(response_data['response'])

                # Kiểm tra cấu trúc cơ bản
                if isinstance(result_json, dict) and "type" in result_json and "message_vi" in result_json:
                    logger.info(f"[LOCAL AI ADVICE] Received: {result_json['type']}")
                    return result_json # Trả về dict đã parse
                else:
                    logger.warning(f"[LOCAL AI ADVICE] Invalid JSON structure received: {result_json}")
                    return None
            except json.JSONDecodeError as e:
                logger.error(f"[LOCAL AI ADVICE] Error parsing JSON response: {e}")
                logger.error(f"Ollama raw response string: {response_data.get('response', 'N/A')}")
                return None
        else:
            logger.warning(f"[LOCAL AI ADVICE] 'response' field missing in Ollama output: {response_data}")
            return None

    except requests.exceptions.Timeout:
        logger.error("[LOCAL AI ADVICE] Timeout calling local Ollama API for advice.")
        return None # Lỗi timeout trả về None
    except requests.exceptions.RequestException as e:
        logger.error(f"[LOCAL AI ADVICE] Error calling Ollama API: {e}")
        return None # Lỗi kết nối trả về None
    except Exception as e:
        logger.error(f"[LOCAL AI ADVICE] Unexpected error: {e}", exc_info=True)
        return None # Lỗi khác trả về None

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
            "model": "gemma3:4b",
            "prompt": prompt,
            "format": "json",
            "stream": False,
            "keep_alive": "1h"
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
    Hàm này gọi hàm con 'ingest_data_for_single_location' cho từng location.
    Hàm con sẽ tự động kiểm tra cảnh báo và gửi thông báo nếu cần.
    """
    logger.info("--- [TASK START] Running Full Data Ingestion with Weather Monitoring ---")
    active_locations = Location.objects.filter(is_active=True)
    if not active_locations.exists():
        logger.info("[DATA INGESTION] No active locations.")
        return {'success': True, 'message': 'No active locations.'}

    logger.info(f"[DATA INGESTION] Found {active_locations.count()} active location(s).")

    total_success = 0
    total_fail = 0
    total_alerts_detected = 0

    # Vòng lặp này giờ đã sạch và đơn giản hơn rất nhiều
    for loc in active_locations:
        try:
            # Gọi hàm con cho từng địa điểm (bao gồm cả weather monitoring)
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
    logger.info(f"--- [TASK FINISH] Data Ingestion with Weather Monitoring completed. Succeeded for {total_success} locations. Failed for {total_fail} locations. ---")
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
    """ 
    Tác vụ thu thập dữ liệu tức thì cho MỘT địa điểm mới.
    Sau khi thu thập, sẽ gọi WeatherConditionMonitor để phát hiện cảnh báo
    và NotificationService để gửi thông báo nếu cần.
    """
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
    current_weather_data = None  # Lưu dữ liệu thời tiết hiện tại để phân tích

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
        except Exception as e:
            logger.error(f"[INSTANT INGEST] Error bulk inserting weather data for {loc.name_en}: {e}", exc_info=True)
            return False # Báo hiệu thất bại
    
    # --- TÍCH HỢP GIÁM SÁT THỜI TIẾT ---
    # Lấy dữ liệu thời tiết hiện tại để phân tích cảnh báo
    logger.info(f"[INSTANT INGEST] Checking for weather alerts at {loc.name_en}")
    
    try:
        # Lấy dữ liệu thời tiết hiện tại
        current_data, current_err = call_weather_api_from_task('current', {'q': loc.name_en})
        
        if current_data and not current_err:
            current_weather_data = current_data
            
            # Khởi tạo WeatherConditionMonitor
            from .weather_monitor import WeatherConditionMonitor
            monitor = WeatherConditionMonitor()
            
            # Đánh giá dữ liệu thời tiết và phát hiện cảnh báo
            detected_alerts = monitor.evaluate_weather_data(current_weather_data, loc)
            
            if detected_alerts:
                logger.info(f"[INSTANT INGEST] Detected {len(detected_alerts)} alert(s) for {loc.name_en}")
                
                # Gửi thông báo cho các cảnh báo đã phát hiện
                from .notification_service import NotificationService
                notification_service = NotificationService()
                
                for alert in detected_alerts:
                    try:
                        # Gửi cảnh báo đến tất cả users theo dõi location này
                        send_result = notification_service.send_weather_alert(alert)
                        logger.info(f"[INSTANT INGEST] Alert notification sent: {send_result}")
                    except Exception as notify_err:
                        logger.error(f"[INSTANT INGEST] Error sending alert notification: {notify_err}", exc_info=True)
                        # Không fail toàn bộ task nếu gửi thông báo lỗi
            else:
                logger.debug(f"[INSTANT INGEST] No dangerous conditions detected for {loc.name_en}")
        else:
            logger.warning(f"[INSTANT INGEST] Could not fetch current weather data for alert monitoring: {current_err}")
            # Không fail task nếu không lấy được dữ liệu current weather
            
    except Exception as monitor_err:
        logger.error(f"[INSTANT INGEST] Error during weather monitoring: {monitor_err}", exc_info=True)
        # Không fail toàn bộ task nếu monitoring lỗi, vì dữ liệu đã được lưu thành công
    
    # Trả về kết quả dựa trên việc lưu dữ liệu
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



# --- TASK: KIỂM TRA THAY ĐỔI THỜI TIẾT VÀ GỬI THÔNG BÁO ---
def check_weather_changes_and_notify():
    """
    Task chạy mỗi phút để kiểm tra thay đổi thời tiết cho các vị trí được theo dõi
    và gửi push notification nếu có thay đổi
    """
    from .firebase_notifications import notify_weather_change, initialize_firebase
    
    logger.info("[WEATHER CHANGE CHECK] Starting weather change check...")
    
    # Khởi tạo Firebase (nếu chưa)
    initialize_firebase()
    
    # Lấy tất cả locations đang được theo dõi (có users)
    tracked_locations = Location.objects.filter(
        is_active=True,
        users__isnull=False
    ).exclude(users=[])
    
    if not tracked_locations.exists():
        logger.info("[WEATHER CHANGE CHECK] No tracked locations found")
        return
    
    logger.info(f"[WEATHER CHANGE CHECK] Checking {tracked_locations.count()} locations")
    
    for location in tracked_locations:
        try:
            # Gọi API để lấy thời tiết hiện tại
            weather_data, error = call_weather_api_from_task('current', {'q': location.name_en})
            
            if error or not weather_data:
                logger.warning(f"[WEATHER CHANGE CHECK] Failed to fetch weather for {location.name_en}: {error}")
                continue
            
            current_condition = weather_data.get('current', {}).get('condition', {}).get('text')
            
            if not current_condition:
                logger.warning(f"[WEATHER CHANGE CHECK] No condition data for {location.name_en}")
                continue
            
            # Log trạng thái hiện tại
            logger.info(f"[WEATHER CHECK] {location.name_en}: last='{location.last_weather_condition}' current='{current_condition}'")
            
            # So sánh với trạng thái cũ
            if location.last_weather_condition and location.last_weather_condition != current_condition:
                # Có thay đổi! Gửi thông báo
                logger.info(f"[WEATHER CHANGE] {location.name_en}: {location.last_weather_condition} → {current_condition}")
                
                # Lấy danh sách user IDs đang theo dõi location này
                user_ids = location.users if location.users else []
                
                if user_ids:
                    notify_weather_change(
                        location_name=weather_data.get('location', {}).get('name', location.name_en),
                        old_condition=location.last_weather_condition,
                        new_condition=current_condition,
                        user_ids=user_ids
                    )
            
            # Cập nhật trạng thái mới
            location.last_weather_condition = current_condition
            location.last_weather_check = timezone.now()
            location.save(update_fields=['last_weather_condition', 'last_weather_check'])
            
        except Exception as e:
            logger.error(f"[WEATHER CHANGE CHECK] Error checking {location.name_en}: {e}", exc_info=True)
    
    logger.info("[WEATHER CHANGE CHECK] Completed weather change check")


# --- SCHEDULED NOTIFICATION JOBS ---

def send_morning_summary_job():
    """
    Job gửi tóm tắt buổi sáng lúc 7:00 AM hàng ngày
    
    Yêu cầu:
    - 5.1: Gửi lúc 7:00 AM trong timezone của user
    - 5.3: Chỉ gửi cho users có bật morning_summary_enabled
    - 5.4: Chỉ gửi cho users có bật weekly_summary_enabled (cho weekly)
    
    Logic:
    - Lọc users theo preferences đã bật
    - Xử lý timezone cho thời gian gửi theo từng user
    - Gọi ScheduledNotificationService để gửi
    """
    from .scheduled_notifications import ScheduledNotificationService
    
    logger.info("--- [JOB START] Morning Summary Job at 7:00 AM ---")
    
    try:
        service = ScheduledNotificationService()
        result = service.send_morning_summary()
        
        logger.info(f"[MORNING SUMMARY JOB] Completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"[MORNING SUMMARY JOB] Error: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


def send_tomorrow_forecast_job():
    """
    Job gửi dự báo ngày mai lúc 8:00 PM hàng ngày
    
    Yêu cầu:
    - 6.1: Gửi lúc 8:00 PM trong timezone của user
    - 6.3: Chỉ gửi cho users có bật tomorrow_forecast_enabled
    - 6.4: Chỉ gửi cho users có bật weekly_summary_enabled (cho weekly)
    
    Logic:
    - Lọc users theo preferences đã bật
    - Xử lý timezone cho thời gian gửi theo từng user
    - Gọi ScheduledNotificationService để gửi
    """
    from .scheduled_notifications import ScheduledNotificationService
    
    logger.info("--- [JOB START] Tomorrow Forecast Job at 8:00 PM ---")
    
    try:
        service = ScheduledNotificationService()
        result = service.send_tomorrow_forecast()
        
        logger.info(f"[TOMORROW FORECAST JOB] Completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"[TOMORROW FORECAST JOB] Error: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


def send_weekly_summary_job():
    """
    Job gửi tóm tắt tuần lúc 8:00 PM mỗi Chủ nhật
    
    Yêu cầu:
    - 7.1: Gửi lúc 8:00 PM Chủ nhật trong timezone của user
    - 7.3: Chỉ gửi cho users có bật weekly_summary_enabled
    - 7.4: Chỉ gửi cho users có bật weekly_summary_enabled
    
    Logic:
    - Lọc users theo preferences đã bật
    - Xử lý timezone cho thời gian gửi theo từng user
    - Gọi ScheduledNotificationService để gửi
    """
    from .scheduled_notifications import ScheduledNotificationService
    
    logger.info("--- [JOB START] Weekly Summary Job at 8:00 PM Sunday ---")
    
    try:
        service = ScheduledNotificationService()
        result = service.send_weekly_summary()
        
        logger.info(f"[WEEKLY SUMMARY JOB] Completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"[WEEKLY SUMMARY JOB] Error: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


def cleanup_notification_history_job():
    """
    Job dọn dẹp lịch sử thông báo cũ hơn 90 ngày
    
    Yêu cầu:
    - 10.5: Lưu trữ lịch sử thông báo trong 90 ngày
    
    Logic:
    - Xóa các bản ghi NotificationRecord cũ hơn 90 ngày
    - Chạy lúc 2:00 AM hàng ngày để tránh ảnh hưởng đến hiệu suất
    """
    from .notification_service import NotificationService
    
    logger.info("--- [JOB START] Notification History Cleanup Job at 2:00 AM ---")
    
    try:
        service = NotificationService()
        result = service.cleanup_old_notification_records(retention_days=90)
        
        logger.info(f"[CLEANUP JOB] Completed: Deleted {result['deleted_count']} records older than {result['cutoff_date']}")
        return result
        
    except Exception as e:
        logger.error(f"[CLEANUP JOB] Error: {e}", exc_info=True)
        return {
            'deleted_count': 0,
            'error': str(e)
        }


def cleanup_device_tokens_job():
    """
    Job dọn dẹp device tokens không active cũ hơn 90 ngày
    
    Yêu cầu:
    - 16.4: Quản lý device tokens
    
    Logic:
    - Xóa các device tokens không active cũ hơn 90 ngày
    - Chạy lúc 3:00 AM mỗi Chủ nhật để tránh ảnh hưởng đến hiệu suất
    """
    from .models import DeviceToken
    from django.utils import timezone
    from datetime import timedelta
    
    logger.info("--- [JOB START] Device Tokens Cleanup Job at 3:00 AM Sunday ---")
    
    try:
        cutoff_date = timezone.now() - timedelta(days=90)
        
        # Tìm các tokens không active và quá cũ
        old_inactive_tokens = DeviceToken.objects.filter(
            is_active=False,
            updated_at__lt=cutoff_date
        )
        
        count = old_inactive_tokens.count()
        
        if count == 0:
            logger.info("[CLEANUP JOB] No device tokens to clean up")
            return {
                'deleted_count': 0,
                'cutoff_date': cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
            }
        
        # Xóa tokens
        deleted_count, _ = old_inactive_tokens.delete()
        
        logger.info(f"[CLEANUP JOB] Completed: Deleted {deleted_count} inactive device tokens older than 90 days")
        
        # Thống kê tokens hiện tại
        active_count = DeviceToken.objects.filter(is_active=True).count()
        inactive_count = DeviceToken.objects.filter(is_active=False).count()
        
        logger.info(f"[CLEANUP JOB] Current stats - Active: {active_count}, Inactive: {inactive_count}")
        
        return {
            'deleted_count': deleted_count,
            'cutoff_date': cutoff_date.strftime('%Y-%m-%d %H:%M:%S'),
            'active_tokens': active_count,
            'inactive_tokens': inactive_count
        }
        
    except Exception as e:
        logger.error(f"[CLEANUP JOB] Error: {e}", exc_info=True)
        return {
            'deleted_count': 0,
            'error': str(e)
        }


def monitor_all_locations_for_alerts():
    """
    Job chạy mỗi 30 phút để phát hiện cảnh báo thiên tai REAL-TIME.
    Phân tích tất cả locations đang active và gửi push notification ngay lập tức.
    """
    logger.info("--- [REAL-TIME ALERT MONITOR] Starting real-time weather alert monitoring ---")
    
    active_locations = Location.objects.filter(is_active=True)
    
    if not active_locations.exists():
        logger.info("[REAL-TIME ALERT] No active locations to monitor")
        return {
            'success': True,
            'monitored_count': 0,
            'alerts_detected': 0
        }
    
    logger.info(f"[REAL-TIME ALERT] Monitoring {active_locations.count()} locations")
    
    total_alerts = 0
    total_notifications_sent = 0
    
    for location in active_locations:
        try:
            # Phân tích location và gửi notification nếu có cảnh báo
            _, alerts, error = analyze_location_with_preprocessing(location)
            
            if alerts:
                total_alerts += len(alerts)
                logger.info(f"[REAL-TIME ALERT] Detected {len(alerts)} alert(s) for {location.name_en}")
            
        except Exception as e:
            logger.error(f"[REAL-TIME ALERT] Error monitoring {location.name_en}: {e}", exc_info=True)
    
    logger.info(f"--- [REAL-TIME ALERT MONITOR] Completed. Detected {total_alerts} alerts ---")
    
    return {
        'success': True,
        'monitored_count': active_locations.count(),
        'alerts_detected': total_alerts
    }


def analyze_location_with_preprocessing(location):
    """
    Phân tích location với logic pre-processing mới.
    Phát hiện: heavy_rain, flood_risk, extreme_heat, strong_wind, extreme_cold
    Trả về: (location, alerts_list, error_message)
    """
    from .firebase_notifications import send_weather_alert_notification
    from .models import DeviceToken, LocationNotificationPreferences
    
    try:
        # Lấy dữ liệu dự báo 3 ngày từ API
        forecast_data, err = call_weather_api_from_task('forecast', {'q': location.name_en, 'days': 3})
        
        if not forecast_data or 'forecast' not in forecast_data:
            logger.error(f"[PREPROCESSING] Failed to fetch forecast for {location.name_en}: {err}")
            return location, [], "API Error"
        
        # Chuyển đổi sang hourly data
        hourly_data = []
        for day in forecast_data['forecast']['forecastday']:
            hourly_data.extend(day.get('hour', []))
        
        if not hourly_data:
            return location, [], "No hourly data"
        
        # === LOGIC PRE-PROCESSING (GIỐNG TRONG call_local_ai_for_advice) ===
        temps = [h.get('temp_c', 0) for h in hourly_data if h.get('temp_c') is not None]
        winds = [h.get('wind_kph', 0) for h in hourly_data if h.get('wind_kph') is not None]
        precips = [h.get('precip_mm', 0) for h in hourly_data if h.get('precip_mm') is not None]
        rain_chances = [h.get('chance_of_rain', 0) for h in hourly_data if h.get('chance_of_rain') is not None]
        
        temp_min = min(temps) if temps else 0
        temp_max = max(temps) if temps else 0
        wind_max = max(winds) if winds else 0
        precip_total = sum(precips) if precips else 0
        precip_max = max(precips) if precips else 0
        
        heavy_rain_hours = sum(1 for p in precips if p > 10)
        high_rain_chance_hours = sum(1 for r in rain_chances if r > 70)
        hot_hours = sum(1 for t in temps if t > 35)
        strong_wind_hours = sum(1 for w in winds if w > 40)
        cold_hours = sum(1 for t in temps if t < 10)
        
        # Phát hiện cảnh báo
        alerts = []
        
        # Lũ lụt (ưu tiên cao nhất)
        if precip_total > 100 or (heavy_rain_hours >= 6 and precip_total > 50):
            alerts.append({
                'severity': 'EXTREME' if precip_total > 150 else 'HIGH',
                'impact_field': 'flood_risk',
                'forecast_details_vi': f'Nguy cơ lũ lụt cao. Tổng lượng mưa dự báo: {precip_total:.1f}mm trong 3 ngày tới. Mưa to kéo dài {heavy_rain_hours} giờ.',
                'actionable_advice_vi': 'Chuẩn bị sơ tán nếu ở vùng trũng. Tránh đi qua vùng ngập. Theo dõi tin tức địa phương.'
            })
        
        # Mưa to
        elif heavy_rain_hours >= 3 or (precip_max > 20 and high_rain_chance_hours >= 4):
            alerts.append({
                'severity': 'HIGH',
                'impact_field': 'heavy_rain',
                'forecast_details_vi': f'Mưa to dự báo. Lượng mưa tối đa: {precip_max:.1f}mm/giờ. Tổng: {precip_total:.1f}mm.',
                'actionable_advice_vi': 'Mang theo áo mưa, ô. Hạn chế di chuyển khi mưa lớn. Cẩn thận đường trơn.'
            })
        
        # Nắng nóng
        if hot_hours >= 4:
            alerts.append({
                'severity': 'EXTREME' if temp_max > 39 else 'HIGH',
                'impact_field': 'extreme_heat',
                'forecast_details_vi': f'Nắng nóng gay gắt. Nhiệt độ cao nhất: {temp_max:.1f}°C. Kéo dài {hot_hours} giờ.',
                'actionable_advice_vi': 'Hạn chế ra ngoài 11h-15h. Uống nhiều nước. Mặc quần áo thoáng mát. Cẩn thận say nắng.'
            })
        
        # Bão/Gió mạnh - Phân cấp chi tiết
        if wind_max >= 185:  # Siêu bão
            alerts.append({
                'severity': 'EXTREME',
                'impact_field': 'super_typhoon',
                'forecast_details_vi': f'⚠️ SIÊU BÃO CẤP 5 DỰ BÁO! Tốc độ gió tối đa: {wind_max:.1f} km/h. Cực kỳ nguy hiểm!',
                'actionable_advice_vi': 'KHẨN CẤP: Sơ tán ngay lập tức! Tìm nơi trú ẩn kiên cố. Tuyệt đối không ra ngoài.'
            })
        elif wind_max >= 118:  # Bão mạnh
            alerts.append({
                'severity': 'EXTREME',
                'impact_field': 'typhoon',
                'forecast_details_vi': f'🌀 BÃO MẠNH DỰ BÁO! Tốc độ gió tối đa: {wind_max:.1f} km/h. Rất nguy hiểm!',
                'actionable_advice_vi': 'KHẨN CẤP: Gia cố nhà cửa ngay. Chuẩn bị sơ tán. Tránh ra ngoài. Dự trữ lương thực, nước.'
            })
        elif wind_max >= 63:  # Bão nhiệt đới
            alerts.append({
                'severity': 'HIGH',
                'impact_field': 'tropical_storm',
                'forecast_details_vi': f'🌀 BÃO NHIỆT ĐỚI dự báo. Tốc độ gió tối đa: {wind_max:.1f} km/h. Kéo dài {strong_wind_hours} giờ.',
                'actionable_advice_vi': 'Gia cố nhà cửa. Hạn chế ra ngoài. Cẩn thận cây đổ, biển hiệu bay. Đóng cửa sổ chặt.'
            })
        elif wind_max >= 39:  # Áp thấp nhiệt đới
            alerts.append({
                'severity': 'HIGH',
                'impact_field': 'tropical_depression',
                'forecast_details_vi': f'🌪️ ÁP THẤP NHIỆT ĐỚI dự báo. Tốc độ gió tối đa: {wind_max:.1f} km/h. Kéo dài {strong_wind_hours} giờ.',
                'actionable_advice_vi': 'Cẩn thận khi ra ngoài. Gia cố vật dụng dễ bay. Đóng cửa sổ. Theo dõi tin tức.'
            })
        elif strong_wind_hours >= 3 or wind_max >= 50:  # Gió mạnh
            alerts.append({
                'severity': 'MEDIUM',
                'impact_field': 'strong_wind',
                'forecast_details_vi': f'💨 Gió mạnh dự báo. Tốc độ gió tối đa: {wind_max:.1f} km/h. Kéo dài {strong_wind_hours} giờ.',
                'actionable_advice_vi': 'Cẩn thận khi di chuyển. Tránh đứng dưới cây, biển hiệu. Gia cố vật dụng nhẹ.'
            })
        
        # Rét đậm
        if cold_hours >= 6:
            alerts.append({
                'severity': 'EXTREME' if temp_min < 5 else 'HIGH',
                'impact_field': 'extreme_cold',
                'forecast_details_vi': f'Rét đậm. Nhiệt độ thấp nhất: {temp_min:.1f}°C. Kéo dài {cold_hours} giờ.',
                'actionable_advice_vi': 'Mặc ấm. Cẩn thận với người già, trẻ em. Đề phòng bệnh đường hô hấp.'
            })
        
        # Nếu có cảnh báo, push notification ngay
        if alerts:
            logger.info(f"[PREPROCESSING] Detected {len(alerts)} alert(s) for {location.name_en}")
            
            # Lấy users theo dõi location này
            user_ids = location.users if location.users else []
            
            for alert in alerts:
                # Lưu vào database
                try:
                    event = ExtremeEvent.objects.create(
                        location=location,
                        severity=alert['severity'],
                        impact_field=alert['impact_field'],
                        forecast_details_vi=alert['forecast_details_vi'],
                        actionable_advice_vi=alert['actionable_advice_vi'],
                        raw_llm_json=alert,
                        is_notified=False
                    )
                    
                    # Push notification cho từng user
                    for user_id in user_ids:
                        try:
                            # Kiểm tra preferences
                            location_pref = LocationNotificationPreferences.objects.filter(
                                user_id=user_id,
                                location=location
                            ).first()
                            
                            # Nếu user tắt notification cho location này, skip
                            if location_pref and not location_pref.notifications_enabled:
                                continue
                            
                            # Lấy device tokens
                            tokens = list(DeviceToken.objects.filter(
                                user_id=user_id,
                                is_active=True
                            ).values_list('token', flat=True))
                            
                            if tokens:
                                # Push notification
                                send_weather_alert_notification(
                                    device_tokens=tokens,
                                    location_name=location.name_en,
                                    alert=event
                                )
                                logger.info(f"[PUSH] Sent {alert['impact_field']} alert to user {user_id}")
                        
                        except Exception as push_err:
                            logger.error(f"[PUSH] Error sending to user {user_id}: {push_err}")
                    
                    # Đánh dấu đã gửi
                    event.is_notified = True
                    event.save()
                    
                except Exception as db_err:
                    logger.error(f"[DB] Error saving alert for {location.name_en}: {db_err}")
        
        return location, alerts, None
        
    except Exception as e:
        logger.error(f"[PREPROCESSING] Error analyzing {location.name_en}: {e}", exc_info=True)
        return location, [], str(e)
