# api/views.py
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework import status
from django.core.cache import cache
from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings # Import settings
from django.db import transaction
import requests
import bcrypt
import json
import logging
from datetime import datetime, timedelta, date, timezone as dt_timezone
from .scheduler import scheduler
from django.utils import timezone
from .serializers import ExtremeEventSerializer
from .models import User, Location, WeatherData, ExtremeEvent, AdviceCache
from decimal import Decimal, InvalidOperation
from .tasks import trigger_data_ingestion, trigger_llm_analysis, ingest_data_for_single_location, analyze_single_location, call_local_ai_for_advice, call_weather_api_from_task
logger = logging.getLogger(__name__)

# --- Helper Functions ---
def call_weather_api(endpoint, params):
    # ... (Giữ nguyên như phiên bản trước) ...
    if not settings.WEATHER_API_KEY:
        raise Exception("Weather API Key missing")
    params['key'] = settings.WEATHER_API_KEY
    params['lang'] = 'vi'
    try:
        response = requests.get(f"{settings.BASE_WEATHER_URL}/{endpoint}.json", params=params, timeout=10) # Add timeout
        response.raise_for_status() # Ném lỗi nếu status code >= 400
        return response.status_code, response.json()
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout calling WeatherAPI endpoint: {endpoint} for params: {params.get('q')}")
        return 504, {'message': 'API Timeout'} # Gateway Timeout
    except requests.exceptions.RequestException as e:
        status_code = e.response.status_code if e.response is not None else 500
        error_data = e.response.json() if e.response is not None and e.response.headers.get('content-type') == 'application/json' else {'message': str(e)}
        logger.error(f"Error calling WeatherAPI ({endpoint}): {status_code} - {error_data}")
        return status_code, error_data

def admin_secret_required(view_func):
    """ Decorator để kiểm tra admin secret """
    def _wrapped_view(request, *args, **kwargs):
        # Đọc secret từ query params
        if request.query_params.get('secret') != settings.ADMIN_SECRET:
            return Response({"error": "Forbidden - Invalid Secret"}, status=status.HTTP_403_FORBIDDEN)
        return view_func(request, *args, **kwargs)
    return _wrapped_view

# --- Authentication Views ---
@api_view(['POST'])
@permission_classes([AllowAny])
def register_user(request):
    username = request.data.get('username')
    password = request.data.get('password')
    if not username or not password:
        return Response({'error': 'Username and password required'}, status=status.HTTP_400_BAD_REQUEST)
    try:
        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        user = User.objects.create(username=username, password_hash=hashed.decode('utf-8'))
        logger.info(f"[AUTH] New user registered: {username}")
        return Response({
            'message': 'User registered successfully',
            'user': {'user_id': user.user_id, 'username': user.username}
        }, status=status.HTTP_201_CREATED)
    except Exception as e:
         # Kiểm tra lỗi unique constraint một cách an toàn hơn
         if hasattr(e, 'pgcode') and e.pgcode == '23505': # Mã lỗi PostgreSQL cho unique violation
              return Response({'error': 'Username already exists.'}, status=status.HTTP_409_CONFLICT)
         logger.error(f"[DB ERROR] /api/register: {e}", exc_info=True)
         return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
@permission_classes([AllowAny])
def login_user(request):
    username = request.data.get('username')
    password = request.data.get('password')
    if not username or not password:
        return Response({'error': 'Username and password required'}, status=status.HTTP_400_BAD_REQUEST)
    try:
        user = User.objects.get(username=username)
        if bcrypt.checkpw(password.encode('utf-8'), user.password_hash.encode('utf-8')):
            logger.info(f"[AUTH] User logged in: {username}")
            return Response({
                'message': 'Login successful',
                'user': {'user_id': user.user_id, 'username': user.username}
                # Trả về JWT token ở đây trong ứng dụng thực tế
            })
        else:
            return Response({'error': 'Invalid username or password'}, status=status.HTTP_401_UNAUTHORIZED)
    except User.DoesNotExist:
        return Response({'error': 'Invalid username or password'}, status=status.HTTP_401_UNAUTHORIZED)
    except Exception as e:
        logger.error(f"[DB ERROR] /api/login: {e}", exc_info=True)
        return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# --- Public API Views ---
@api_view(['GET'])
@permission_classes([AllowAny])
def get_weather(request):
    q = request.query_params.get('q')
    days = request.query_params.get('days')
    if not q:
        return Response({'error': "'q' is required."}, status=status.HTTP_400_BAD_REQUEST)

    is_forecast = days and days.isdigit() and int(days) > 0
    endpoint = 'forecast' if is_forecast else 'current'
    cache_key = f"{endpoint}:{q.lower().strip()}{f':days{days}' if is_forecast else ''}"

    cached_data = cache.get(cache_key)
    if cached_data:
        logger.info(f"[DJANGO CACHE HIT] Key: {cache_key}")
        return Response(cached_data)

    logger.info(f"[DJANGO CACHE MISS] Key: {cache_key}")
    params = {'q': q, 'aqi': 'yes', 'alerts': 'yes'}
    if is_forecast:
        params['days'] = days

    status_code, data = call_weather_api(endpoint, params)
    if status_code == 200:
        cache.set(cache_key, data, timeout=settings.CACHE_TTL_SECONDS)
        logger.info(f"[DJANGO CACHE STORED] Key: {cache_key}")

    return Response(data, status=status_code)

@api_view(['POST'])
# Cần thêm @permission_classes([IsAuthenticated]) sau này
def track_location(request):
    name_en = request.data.get('name_en')
    latitude = request.data.get('latitude')
    longitude = request.data.get('longitude')
    user_id = request.data.get('user_id') # Lấy user_id từ request.user sau này

    if not all([name_en, latitude, longitude, user_id]):
        return Response({"error": "Missing required parameters."}, status=status.HTTP_400_BAD_REQUEST)

    try:
        new_location_created = False # Cờ để theo dõi location mới
        with transaction.atomic():
            location, created = Location.objects.get_or_create(
                name_en=name_en,
                defaults={'latitude': latitude, 'longitude': longitude, 'users': [user_id]}
            )
            if not created:
                # Nếu địa điểm đã tồn tại, cập nhật danh sách người theo dõi
                current_users = set(location.users) if location.users else set()
                current_users.add(user_id)
                location.users = list(current_users)
                location.is_active = True
                location.save(update_fields=['users', 'is_active'])
            else:
                # Nếu địa điểm LÀ MỚI, đặt cờ
                new_location_created = True

        # === PHẦN LOGIC MỚI ĐỂ KÍCH HOẠT AI TỨC THÌ ===
        if new_location_created:
            new_loc_id = location.location_id
            # Đặt lịch chạy nền (để không làm treo API)
            run_time_ingest = timezone.now() + timedelta(seconds=10) # Chạy thu thập sau 10 giây
            run_time_analyze = timezone.now() + timedelta(minutes=2) # Chạy AI sau 2 phút

            try:
                # Job 1: Thu thập dữ liệu
                scheduler.add_job(
                    ingest_data_for_single_location,
                    'date', # Kiểu: Chạy 1 lần vào ngày giờ cụ thể
                    run_date=run_time_ingest,
                    args=[new_loc_id], # Tham số truyền vào hàm
                    id=f'instant_ingest_{new_loc_id}', # ID duy nhất
                    replace_existing=True
                )
                
                # Job 2: Phân tích AI
                scheduler.add_job(
                    analyze_single_location, # Dùng hàm có sẵn trong tasks.py
                    'date', 
                    run_date=run_time_analyze,
                    args=[location], # Hàm này nhận nguyên đối tượng location
                    id=f'instant_analyze_{new_loc_id}',
                    replace_existing=True
                )
                logger.info(f"[INSTANT TASK] Đã lên lịch phân tích tức thì cho: {name_en}")
            except Exception as e:
                # Lỗi này không nên cản trở việc trả về 201, chỉ log lại
                logger.error(f"[INSTANT TASK] Lỗi khi lên lịch tác vụ cho {name_en}: {e}")
        # === KẾT THÚC PHẦN LOGIC MỚI ===

        logger.info(f"[DB] Tracked location: {name_en}")
        return Response({'message': f"Location '{name_en}' activated for tracking."}, status=status.HTTP_201_CREATED)
    
    except Exception as e:
        logger.error(f"[DB ERROR] /api/locations/track: {e}", exc_info=True)
        return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['GET'])
@permission_classes([AllowAny])
def get_ai_advice(request):
    """
    API endpoint để lấy lời khuyên/cảnh báo tức thời từ AI cho một địa điểm BẤT KỲ.
    Luôn lấy dữ liệu THEO GIỜ (-3 đến +3 ngày) trực tiếp từ WeatherAPI.
    Có cache kết quả AI trong 3 giờ (memory cache) VÀ LƯU vào bảng AdviceCache (tự tạo Location nếu cần).
    """
    location_name_en = request.query_params.get('q')
    if not location_name_en:
        return Response({'error': "'q' query parameter (location name_en) is required."}, status=status.HTTP_400_BAD_REQUEST)

    # --- 1. Kiểm tra Cache Memory ---
    today_date_str = timezone.now().strftime('%Y-%m-%d')
    # Key cache dùng tên địa điểm viết thường để đảm bảo tính nhất quán
    cache_key = f"ai_advice:{location_name_en.lower()}:{today_date_str}"
    cached_advice = cache.get(cache_key)

    if cached_advice:
        logger.info(f"[AI ADVICE CACHE HIT] Key: {cache_key}")
        # Cập nhật timestamp trong DB nếu lấy từ cache memory
        try:
            # Chỉ cập nhật DB nếu Location đã tồn tại
            location_obj_for_cache = Location.objects.filter(name_en__iexact=location_name_en).first()
            if location_obj_for_cache:
                advice_record, created = AdviceCache.objects.update_or_create(
                    location=location_obj_for_cache,
                    # Sử dụng location làm khóa chính để update_or_create hoạt động đúng
                    # Giả định bạn chỉ muốn giữ 1 bản ghi cache mới nhất cho mỗi location
                    # Nếu muốn tạo mới mỗi lần cache hit, dùng create() thay thế
                    defaults={
                       'generated_time': timezone.now(),
                       'advice_type': cached_advice.get('type', 'unknown'),
                       'message_vi': cached_advice.get('message_vi', '')
                    }
                )
                log_action = "created" if created else "updated"
                logger.info(f"[AI ADVICE DB] {log_action.capitalize()} AdviceCache record for {location_name_en} from memory cache hit.")
            # Không cần else vì nếu location chưa có, cache hit cũng không giúp tạo AdviceCache
        except Exception as e_db_update:
             logger.error(f"[AI ADVICE DB] Error during AdviceCache update/create from memory hit for {location_name_en}: {e_db_update}", exc_info=False)
        return Response(cached_advice, status=status.HTTP_200_OK)

    logger.info(f"[AI ADVICE CACHE MISS] Key: {cache_key}. Proceeding to fetch data and call AI.")

    # --- 2. Lấy dữ liệu theo giờ từ WeatherAPI ---
    hourly_data_list = []
    api_fetch_error = False
    location_lat_float = None # Lưu giá trị float gốc từ API
    location_lon_float = None
    location_name_actual = location_name_en # Tên thực tế từ API (có thể có dấu)
    hist_data = None # Giữ lại response cuối cùng để lấy lat/lon nếu forecast lỗi
    fc_data = None   # Giữ lại response forecast

    logger.info(f"[AI ADVICE API - HOURLY] Fetching hourly data directly from API for query: '{location_name_en}'")

    # Lấy lịch sử 3 ngày trước
    today = timezone.now().date()
    start_date_hist = today - timedelta(days=3)
    end_date_hist = today - timedelta(days=1)
    current_hist_date = start_date_hist
    while current_hist_date <= end_date_hist:
        date_str = current_hist_date.strftime('%Y-%m-%d')
        logger.debug(f"[AI ADVICE API - HOURLY] Fetching history for {date_str}")
        hist_data_day, hist_err = call_weather_api_from_task('history', {'q': location_name_en, 'dt': date_str})
        if hist_data_day and 'forecast' in hist_data_day and 'forecastday' in hist_data_day['forecast']:
            day_data = hist_data_day['forecast']['forecastday'][0]
            hourly_data_list.extend(day_data.get('hour', []))
            hist_data = hist_data_day # Lưu lại response cuối
        else:
            logger.warning(f"[AI ADVICE API - HOURLY] Failed/No data fetching history for {date_str}: {hist_err}")
        current_hist_date += timedelta(days=1)

    # Lấy dự báo 4 ngày
    logger.debug(f"[AI ADVICE API - HOURLY] Fetching forecast for 4 days")
    fc_data, fc_err = call_weather_api_from_task('forecast', {'q': location_name_en, 'days': 4})
    if fc_data and 'forecast' in fc_data and 'forecastday' in fc_data['forecast']:
        for day_data in fc_data['forecast']['forecastday']:
            hourly_data_list.extend(day_data.get('hour', []))
        # Ưu tiên lấy lat/lon từ forecast
        if 'location' in fc_data:
            location_lat_float = fc_data['location'].get('lat') # Lấy dạng float
            location_lon_float = fc_data['location'].get('lon')
            location_name_actual = fc_data['location'].get('name', location_name_en)
            logger.info(f"Location details from Forecast API: Name='{location_name_actual}', Lat={location_lat_float}, Lon={location_lon_float}")
    else:
        api_fetch_error = True # Lỗi dự báo là nghiêm trọng
        logger.error(f"[AI ADVICE API - HOURLY] Failed to fetch forecast from API: {fc_err}")
        # Thử lấy lat/lon từ history nếu forecast lỗi
        if hist_data and 'location' in hist_data:
            location_lat_float = hist_data['location'].get('lat')
            location_lon_float = hist_data['location'].get('lon')
            location_name_actual = hist_data['location'].get('name', location_name_en)
            logger.warning(f"Using location details from History API (fallback): Name='{location_name_actual}', Lat={location_lat_float}, Lon={location_lon_float}")

    if api_fetch_error or not hourly_data_list:
        logger.error(f"[AI ADVICE API - HOURLY] Failed to fetch sufficient hourly forecast data for {location_name_en}.")
        return Response({"type": "error", "message_vi": "Lỗi khi lấy dữ liệu thời tiết dự báo chi tiết. Vui lòng thử lại sau."}, status=status.HTTP_503_SERVICE_UNAVAILABLE)

    # --- CHUYỂN ĐỔI SANG DECIMAL VÀ KIỂM TRA ---
    lat_decimal = None
    lon_decimal = None
    if location_lat_float is not None and location_lon_float is not None:
        try:
            # Chuyển đổi float -> string -> Decimal để đảm bảo chính xác
            lat_decimal = Decimal(str(location_lat_float))
            lon_decimal = Decimal(str(location_lon_float))
        except InvalidOperation:
             logger.error(f"Invalid coordinate format received: lat={location_lat_float}, lon={location_lon_float}")
             # Để lat_decimal, lon_decimal là None

    if lat_decimal is None or lon_decimal is None:
         logger.error(f"[AI ADVICE API - HOURLY] Could not determine valid Decimal coordinates for {location_name_en}.")
         return Response({"type": "error", "message_vi": "Không thể xác định tọa độ hợp lệ cho địa điểm này."}, status=status.HTTP_404_NOT_FOUND)
    # --- KẾT THÚC CHUYỂN ĐỔI VÀ KIỂM TRA ---

    # --- 3. Chuẩn bị dữ liệu cho AI (CHỈ LẤY HÔM NAY + 2 NGÀY TỚI = 72 GIỜ) ---
    final_hourly_data_for_ai = []
    now = timezone.now()
    cutoff_time = now + timedelta(days=2, hours=12)  # Chỉ lấy đến 2.5 ngày tới
    
    for hour in hourly_data_list:
        try:
            hour_time_str = hour.get('time')
            hour_time = datetime.strptime(hour_time_str, '%Y-%m-%d %H:%M')
            hour_time = timezone.make_aware(hour_time) if timezone.is_naive(hour_time) else hour_time
            
            # CHỈ LẤY DỮ LIỆU TỪ HÔM NAY TRỞ ĐI (bỏ quá khứ)
            if hour_time >= now and hour_time <= cutoff_time:
                final_hourly_data_for_ai.append({
                    'time': hour_time_str,
                    'temp_c': hour.get('temp_c'),
                    'humidity': hour.get('humidity'),
                    'wind_kph': hour.get('wind_kph'),
                    'condition_text': hour.get('condition', {}).get('text'),
                    'uv': hour.get('uv'),
                    'precip_mm': hour.get('precip_mm', 0.0),
                    'chance_of_rain': hour.get('chance_of_rain', 0)
                })
        except (ValueError, KeyError, TypeError) as e:
           logger.warning(f"Skipping invalid hourly record parsing: {hour.get('time')} - {e}")

    try:
        final_hourly_data_for_ai.sort(key=lambda x: datetime.strptime(x['time'], '%Y-%m-%d %H:%M'))
    except ValueError:
        logger.error(f"[AI ADVICE API - HOURLY] Error sorting hourly data for {location_name_en}.")
        return Response({"type": "error", "message_vi": "Lỗi xử lý dữ liệu thời gian."}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    logger.info(f"[AI ADVICE API - HOURLY] Prepared {len(final_hourly_data_for_ai)} hourly records (next 2.5 days only) for AI for '{location_name_actual}'.")

    # --- 4. Gọi AI ---
    try:
        advice_result = call_local_ai_for_advice(final_hourly_data_for_ai) # Gọi AI
    except Exception as e:
        logger.error(f"[API ERROR] /api/advice during AI call (hourly) for {location_name_en}: {e}", exc_info=True)
        return Response({'error': 'Internal server error during AI call'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # --- 5. Xử lý kết quả AI: Lưu Cache và DB ---
    if advice_result and advice_result.get("type") != "error":
        # 5.1 Lưu vào Cache Memory
        cache.set(cache_key, advice_result, timeout=3 * 60 * 60)
        logger.info(f"[AI ADVICE CACHE STORED] Key: {cache_key}")

        # 5.2 Tìm hoặc Tạo Location trong DB (Dùng giá trị Decimal)
        location_obj = None
        try:
            # Dùng transaction.atomic để đảm bảo get_or_create và update (nếu có) là một khối
            with transaction.atomic():
                location_obj, created = Location.objects.get_or_create(
                    name_en__iexact=location_name_en, # Tìm bằng tên không dấu
                    defaults={
                        'name_en': location_name_en, # Lưu tên không dấu làm key
                        'latitude': lat_decimal,   # <-- Dùng Decimal
                        'longitude': lon_decimal, # <-- Dùng Decimal
                        # 'is_active': False # Mặc định không active khi tạo tự động
                    }
                )
                if created:
                    logger.info(f"[DB] Auto-created Location record for {location_name_en} (ID: {location_obj.location_id}).")
                # Chỉ cập nhật nếu tọa độ khác biệt đáng kể
                elif abs(location_obj.latitude - lat_decimal) > Decimal('0.001') or \
                     abs(location_obj.longitude - lon_decimal) > Decimal('0.001'):
                         location_obj.latitude = lat_decimal
                         location_obj.longitude = lon_decimal
                         location_obj.save(update_fields=['latitude', 'longitude'])
                         logger.info(f"[DB] Updated coordinates for existing Location {location_name_en} (ID: {location_obj.location_id}).")

        except Exception as loc_exc:
            logger.error(f"[DB ERROR] Failed to get or create Location for {location_name_en}: {loc_exc}", exc_info=True)
            location_obj = None # Đảm bảo là None nếu có lỗi

        # 5.3 Lưu vào AdviceCache DB nếu có location_obj
        if location_obj:
            try:
                # Tạo bản ghi mới mỗi lần AI chạy thành công
                AdviceCache.objects.create(
                    location=location_obj,
                    advice_type=advice_result['type'],
                    message_vi=advice_result['message_vi']
                )
                logger.info(f"[AI ADVICE DB] Stored new advice/warning in AdviceCache for {location_name_en} (Loc ID: {location_obj.location_id})")
            except Exception as db_exc:
                 logger.error(f"[AI ADVICE DB] Error storing advice in AdviceCache for {location_name_en}: {db_exc}", exc_info=True)
                 # Không trả lỗi về client nếu chỉ lỗi lưu DB cache
        else:
             logger.warning(f"[AI ADVICE DB] Could not save to AdviceCache because Location object for {location_name_en} was not obtained/created.")

        return Response(advice_result, status=status.HTTP_200_OK)
    else:
        # Xử lý khi AI lỗi hoặc trả về type "error"
        error_msg = advice_result.get("message_vi") if advice_result else "Không thể kết nối với trợ lý AI lúc này."
        logger.warning(f"[AI ADVICE] AI returned an error or no result for {location_name_en}. Message: {error_msg}")
        return Response({"type": "error", "message_vi": error_msg}, status=status.HTTP_503_SERVICE_UNAVAILABLE)
    
@api_view(['GET'])
@permission_classes([AllowAny])
def check_recent_advice(request):
    """
    API endpoint để kiểm tra xem có lời khuyên/cảnh báo nào gần đây
    (trong vòng 1 giờ) cho địa điểm này trong AdviceCache không.
    Trả về advice/warning nếu có, hoặc {"status": "stale"} nếu không có hoặc quá cũ.
    """
    location_name_en = request.query_params.get('q')
    if not location_name_en:
        return Response({'error': "'q' query parameter (location name_en) is required."}, status=status.HTTP_400_BAD_REQUEST)

    try:
        location = Location.objects.get(name_en__iexact=location_name_en)

        # Tính thời điểm 1 giờ trước
        one_hour_ago = timezone.now() - timedelta(hours=1)

        # Tìm bản ghi AdviceCache mới nhất cho location này
        latest_advice = AdviceCache.objects.filter(
            location=location
        ).order_by('-generated_time').first() # Lấy bản ghi đầu tiên (mới nhất)

        if latest_advice and latest_advice.generated_time >= one_hour_ago:
            # Nếu tìm thấy và còn mới (trong vòng 1 giờ)
            logger.info(f"[CHECK ADVICE] Found recent advice in DB for {location_name_en}")
            return Response({
                "type": latest_advice.advice_type,
                "message_vi": latest_advice.message_vi,
                "generated_time": latest_advice.generated_time # Trả thêm thời gian để debug
            }, status=status.HTTP_200_OK)
        else:
            # Nếu không tìm thấy hoặc đã quá 1 giờ
            logger.info(f"[CHECK ADVICE] No recent advice in DB for {location_name_en}. Status: stale.")
            return Response({"status": "stale"}, status=status.HTTP_200_OK) # Dùng 200 OK để app dễ xử lý

    except Location.DoesNotExist:
         logger.warning(f"[CHECK ADVICE] Location not found: {location_name_en}")
         # Trả về stale nếu không tìm thấy location (coi như chưa có advice)
         return Response({"status": "stale"}, status=status.HTTP_200_OK)
    except Exception as e:
        logger.error(f"[API ERROR] /api/check-advice: {e}", exc_info=True)
        return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# --- Admin API Views (Protected) ---
@api_view(['POST'])
@admin_secret_required
def run_admin_action(request, action):
    """ Endpoint chung để kích hoạt các tác vụ admin """
    logger.info(f"Admin action requested: {action}")
    result = {}
    success = False
    try:
        if action == 'run-ingestion':
            result = trigger_data_ingestion() # Gọi trực tiếp hàm task
            success = result.get('success', False)
        elif action == 'run-analysis':
            result = trigger_llm_analysis()
            success = result.get('success', False)
        elif action == 'check-alerts':
            # ENDPOINT MỚI: Kiểm tra cảnh báo thiên tai ngay lập tức
            from .tasks import monitor_all_locations_for_alerts
            result = monitor_all_locations_for_alerts()
            success = result.get('success', False)
        # elif action == 'run-pruning':
        #     result = trigger_data_pruning()
        #     success = result.get('success', False)
        else:
            return Response({"error": "Action not found."}, status=status.HTTP_404_NOT_FOUND)

        return Response(result, status=status.HTTP_200_OK if success else status.HTTP_500_INTERNAL_SERVER_ERROR)

    except Exception as e:
        logger.error(f"[ADMIN ACTION ERROR] Action '{action}': {e}", exc_info=True)
        return Response({'error': f'Failed to run {action}', 'details': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# --- Root View ---
@api_view(['GET'])
@permission_classes([AllowAny])
def root_view(request):
    """ Endpoint gốc để kiểm tra server """
    return Response({
        "message": "Weather API (Django) is running in LOCAL mode.",
        "status": "OK",
        "cache": "Django LocMemCache",
        "database": "Local PostgreSQL",
        "ai_model": "Ollama - gemma3",
        "scheduler": "APScheduler Running" # Thêm trạng thái scheduler
    })

@api_view(['GET'])
@permission_classes([AllowAny]) # Ai cũng có thể xem cảnh báo
def get_alerts_for_location(request):
    """
    API endpoint để lấy các cảnh báo ExtremeEvent gần đây cho một địa điểm.
    Cần query param 'q' (tên địa điểm tiếng Anh, ví dụ: ?q=Hanoi)
    """
    location_name_en = request.query_params.get('q')
    if not location_name_en:
        return Response({'error': "'q' query parameter (location name_en) is required."}, status=status.HTTP_400_BAD_REQUEST)

    try:
        # Tìm location_id dựa trên tên
        location = Location.objects.get(name_en__iexact=location_name_en) # iexact = không phân biệt hoa thường

        # Lọc các cảnh báo trong vòng 24h gần nhất và đang active
        one_day_ago = timezone.now() - timedelta(days=1)
        recent_alerts = ExtremeEvent.objects.filter(
            location=location,
            analysis_time__gte=one_day_ago, # Lấy từ 1 ngày trước đến giờ
            is_active=True # Chỉ lấy cảnh báo còn hiệu lực (nếu bạn có logic cập nhật is_active)
        ).order_by('-analysis_time') # Sắp xếp mới nhất lên đầu

        # Serialize dữ liệu
        serializer = ExtremeEventSerializer(recent_alerts, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    except Location.DoesNotExist:
        # Nếu không tìm thấy địa điểm trong DB (người dùng chưa theo dõi?)
        # Trả về mảng rỗng thay vì lỗi 404 để app không bị crash
        logger.warning(f"Alert API called for untracked/unknown location: {location_name_en}")
        return Response([], status=status.HTTP_200_OK)
    except Exception as e:
        logger.error(f"[API ERROR] /api/alerts: {e}", exc_info=True)
        return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(['GET'])
@permission_classes([AllowAny]) # Sau này nên đổi thành IsAuthenticated
def get_tracked_locations(request):
    user_id = request.query_params.get('user_id')
    if not user_id:
        return Response({'error': 'user_id is required'}, status=status.HTTP_400_BAD_REQUEST)

    try:
        # Chuyển user_id sang int để tìm trong JSONField (vì database lưu int)
        user_id_int = int(user_id)
        
        # Tìm các Location mà trong mảng 'users' có chứa user_id này
        # Lưu ý: Query này áp dụng cho PostgreSQL với JSONField
        locations = Location.objects.filter(users__contains=user_id_int)
        
        results = []
        
        for loc in locations:
            # Với mỗi địa điểm, lấy dữ liệu thời tiết (Ưu tiên Cache)
            # Gọi hàm get_weather logic hoặc gọi lại call_weather_api
            # Ở đây ta gọi API forecast 1 ngày để lấy đủ thông tin: Mưa (forecast), Gió (current)
            
            cache_key = f"tracked:{loc.name_en}"
            weather_data = cache.get(cache_key)
            
            if not weather_data:
                # Nếu không có cache, gọi WeatherAPI
                params = {'q': loc.name_en, 'days': 1, 'lang': 'vi'}
                status_code, api_data = call_weather_api('forecast', params)
                
                if status_code == 200:
                    weather_data = api_data
                    cache.set(cache_key, weather_data, timeout=300) # Cache 5 phút
            
            if weather_data:
                # Trích xuất dữ liệu cần thiết
                current = weather_data.get('current', {})
                forecast = weather_data.get('forecast', {}).get('forecastday', [{}])[0].get('day', {})
                
                results.append({
                    'id': loc.location_id,
                    'name': weather_data.get('location', {}).get('name', loc.name_en),
                    'temp_c': current.get('temp_c'),
                    'condition_text': current.get('condition', {}).get('text'),
                    'icon': current.get('condition', {}).get('icon'),
                    'wind_kph': current.get('wind_kph'),
                    'chance_of_rain': forecast.get('daily_chance_of_rain', 0), # Lấy tỉ lệ mưa từ dự báo ngày
                    'humidity': current.get('humidity')
                })

        return Response(results, status=status.HTTP_200_OK)

    except ValueError:
        return Response({'error': 'Invalid user_id format'}, status=status.HTTP_400_BAD_REQUEST)
    except Exception as e:
        logger.error(f"Error fetching tracked locations: {e}", exc_info=True)
        return Response({'error': 'Internal Server Error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST', 'DELETE'])
# Cần thêm @permission_classes([IsAuthenticated]) sau này
def register_device_token(request):
    """
    API endpoint để quản lý FCM device token
    
    POST: Đăng ký hoặc cập nhật device token
        Body: {"user_id": int, "token": str}
        
    DELETE: Xóa device token (đánh dấu không active)
        Body: {"user_id": int, "token": str}
    """
    from .models import DeviceToken, NotificationPreferences
    from .preference_manager import UserPreferenceManager
    
    user_id = request.data.get('user_id')
    token = request.data.get('token')
    
    if not user_id or not token:
        return Response({'error': 'user_id and token are required'}, status=status.HTTP_400_BAD_REQUEST)
    
    try:
        user_id = int(user_id)
        
        # Kiểm tra user có tồn tại không
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            return Response({'error': 'User not found'}, status=status.HTTP_404_NOT_FOUND)
        
        if request.method == 'POST':
            # Đăng ký hoặc cập nhật device token
            with transaction.atomic():
                # Tìm hoặc tạo device token
                device_token, created = DeviceToken.objects.update_or_create(
                    token=token,
                    defaults={
                        'user': user,
                        'is_active': True
                    }
                )
                
                # Nếu token đã tồn tại nhưng thuộc về user khác, cập nhật user mới
                if not created and device_token.user_id != user_id:
                    logger.warning(f"[DEVICE TOKEN] Token {token[:20]}... moved from user {device_token.user_id} to user {user_id}")
                    device_token.user = user
                    device_token.is_active = True
                    device_token.save()
                
                # Đảm bảo user có notification preferences
                # Nếu chưa có, tạo preferences mặc định
                preference_manager = UserPreferenceManager()
                preferences = preference_manager.get_user_preferences(user_id)
                
                if not preferences:
                    # Tạo preferences mặc định
                    default_preferences = {
                        'enabled_event_types': ['heavy_rain', 'storm', 'extreme_heat', 'extreme_cold'],
                        'notification_schedule': '24_7',
                        'morning_summary_enabled': True,
                        'tomorrow_forecast_enabled': True,
                        'weekly_summary_enabled': False,
                        'timezone': 'Asia/Ho_Chi_Minh'
                    }
                    preferences = preference_manager.update_preferences(user_id, default_preferences)
                    logger.info(f"[DEVICE TOKEN] Created default notification preferences for user {user_id}")
                
                # Dọn dẹp các token cũ không active của user này (giữ tối đa 5 tokens active)
                active_tokens = DeviceToken.objects.filter(
                    user=user,
                    is_active=True
                ).order_by('-updated_at')
                
                if active_tokens.count() > 5:
                    # Giữ 5 tokens mới nhất, đánh dấu các tokens cũ là inactive
                    old_tokens = active_tokens[5:]
                    old_token_ids = [t.token_id for t in old_tokens]
                    DeviceToken.objects.filter(token_id__in=old_token_ids).update(is_active=False)
                    logger.info(f"[DEVICE TOKEN] Deactivated {len(old_token_ids)} old tokens for user {user_id}")
            
            action = "registered" if created else "updated"
            logger.info(f"[DEVICE TOKEN] {action} for user {user_id}: {token[:20]}...")
            
            return Response({
                'message': f'Device token {action} successfully',
                'token_id': device_token.token_id,
                'has_preferences': preferences is not None
            }, status=status.HTTP_201_CREATED if created else status.HTTP_200_OK)
        
        elif request.method == 'DELETE':
            # Xóa (deactivate) device token
            try:
                device_token = DeviceToken.objects.get(
                    token=token,
                    user=user
                )
                device_token.is_active = False
                device_token.save()
                
                logger.info(f"[DEVICE TOKEN] Deactivated token for user {user_id}: {token[:20]}...")
                
                return Response({
                    'message': 'Device token deactivated successfully'
                }, status=status.HTTP_200_OK)
                
            except DeviceToken.DoesNotExist:
                return Response({
                    'error': 'Device token not found for this user'
                }, status=status.HTTP_404_NOT_FOUND)
    
    except ValueError:
        return Response({'error': 'Invalid user_id format'}, status=status.HTTP_400_BAD_REQUEST)
    except Exception as e:
        logger.error(f"[DEVICE TOKEN] Error: {e}", exc_info=True)
        return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@permission_classes([AllowAny])
def test_notification(request):
    """
    Test endpoint để gửi notification thử
    Body: {"user_id": int, "title": str, "body": str}
    """
    from .firebase_notifications import send_fcm_notification
    from .models import DeviceToken
    
    user_id = request.data.get('user_id')
    title = request.data.get('title', 'Test Notification')
    body = request.data.get('body', 'This is a test notification')
    
    if not user_id:
        return Response({'error': 'user_id is required'}, status=status.HTTP_400_BAD_REQUEST)
    
    try:
        # Lấy device tokens của user
        tokens = list(DeviceToken.objects.filter(
            user_id=user_id,
            is_active=True
        ).values_list('token', flat=True))
        
        if not tokens:
            return Response({
                'error': 'No device tokens found for this user',
                'user_id': user_id
            }, status=status.HTTP_404_NOT_FOUND)
        
        # Gửi notification
        result = send_fcm_notification(
            device_tokens=tokens,
            title=title,
            body=body,
            data={'type': 'test'}
        )
        
        return Response({
            'message': 'Test notification sent',
            'result': result,
            'tokens_count': len(tokens)
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        logger.error(f"[TEST NOTIFICATION] Error: {e}", exc_info=True)
        return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# --- Notification Preferences API Views ---

@api_view(['GET', 'POST'])
# Cần thêm @permission_classes([IsAuthenticated]) sau này
def notification_preferences(request):
    """
    GET: Lấy notification preferences của user
    POST: Cập nhật notification preferences của user
    Query params: user_id (int)
    """
    from .serializers import NotificationPreferencesSerializer
    from .models import NotificationPreferences
    from .preference_manager import UserPreferenceManager
    
    # Lấy user_id từ query params cho cả GET và POST
    user_id = request.query_params.get('user_id')
    
    logger.info(f"[PREFERENCES API] Request method: {request.method}, user_id: {user_id}")
    
    if not user_id:
        logger.warning(f"[PREFERENCES API] Missing user_id in request")
        return Response({'error': 'user_id is required'}, status=status.HTTP_400_BAD_REQUEST)
    
    try:
        user_id = int(user_id)
        
        # Kiểm tra user có tồn tại không
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            return Response({'error': 'User not found'}, status=status.HTTP_404_NOT_FOUND)
        
        if request.method == 'GET':
            # Lấy preferences của user
            preference_manager = UserPreferenceManager()
            preferences = preference_manager.get_user_preferences(user_id)
            
            if preferences:
                serializer = NotificationPreferencesSerializer(preferences)
                logger.info(f"[PREFERENCES API] Retrieved preferences for user {user_id}")
                return Response(serializer.data, status=status.HTTP_200_OK)
            else:
                # Nếu chưa có preferences, trả về empty với status 200
                return Response({
                    'message': 'No preferences found. Default preferences will be created on first update.',
                    'user_id': user_id
                }, status=status.HTTP_200_OK)
        
        elif request.method == 'POST':
            # Cập nhật preferences
            # Log request data để debug
            logger.info(f"[PREFERENCES API] Received data: {request.data}")
            logger.info(f"[PREFERENCES API] User ID: {user_id}")
            
            preference_data = {}
            
            # Xử lý từng field riêng để tránh filter nhầm boolean False
            if 'notifications_enabled' in request.data:
                preference_data['notifications_enabled'] = request.data.get('notifications_enabled')
            
            if 'enabled_event_types' in request.data:
                preference_data['enabled_event_types'] = request.data.get('enabled_event_types')
            
            if 'notification_schedule' in request.data:
                preference_data['notification_schedule'] = request.data.get('notification_schedule')
            
            if 'morning_summary_enabled' in request.data:
                preference_data['morning_summary_enabled'] = request.data.get('morning_summary_enabled')
            
            if 'tomorrow_forecast_enabled' in request.data:
                preference_data['tomorrow_forecast_enabled'] = request.data.get('tomorrow_forecast_enabled')
            
            if 'weekly_summary_enabled' in request.data:
                preference_data['weekly_summary_enabled'] = request.data.get('weekly_summary_enabled')
            
            if 'timezone' in request.data:
                preference_data['timezone'] = request.data.get('timezone')
            else:
                preference_data['timezone'] = 'Asia/Ho_Chi_Minh'
            
            logger.info(f"[PREFERENCES API] Processed preference_data: {preference_data}")
            
            if not preference_data:
                logger.warning(f"[PREFERENCES API] No preference data provided for user {user_id}")
                return Response({'error': 'No preference data provided'}, status=status.HTTP_400_BAD_REQUEST)
            
            # Sử dụng UserPreferenceManager để cập nhật (truyền request để audit logging)
            preference_manager = UserPreferenceManager()
            
            try:
                updated_preferences = preference_manager.update_preferences(
                    user_id, 
                    preference_data,
                    request=request  # Truyền request để audit logging
                )
                serializer = NotificationPreferencesSerializer(updated_preferences)
                
                logger.info(f"[PREFERENCES API] Updated preferences for user {user_id}")
                return Response({
                    'message': 'Preferences updated successfully',
                    'preferences': serializer.data
                }, status=status.HTTP_200_OK)
                
            except Exception as validation_error:
                logger.error(f"[PREFERENCES API] Validation error for user {user_id}: {validation_error}")
                return Response({
                    'error': 'Invalid preference data',
                    'details': str(validation_error)
                }, status=status.HTTP_400_BAD_REQUEST)
    
    except ValueError:
        return Response({'error': 'Invalid user_id format'}, status=status.HTTP_400_BAD_REQUEST)
    except Exception as e:
        logger.error(f"[PREFERENCES API] Error: {e}", exc_info=True)
        return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET', 'POST'])
# Cần thêm @permission_classes([IsAuthenticated]) sau này
def location_notification_preferences(request, location_id):
    """
    GET: Lấy notification preferences cho một location cụ thể
    POST: Cập nhật notification preferences cho một location cụ thể
    URL param: location_id (int)
    Query/Body param: user_id (int)
    """
    from .serializers import LocationNotificationPreferencesSerializer
    from .models import LocationNotificationPreferences, Location
    from .preference_manager import UserPreferenceManager
    
    # Lấy user_id từ query params cho cả GET và POST
    user_id = request.query_params.get('user_id')
    
    if not user_id:
        return Response({'error': 'user_id is required'}, status=status.HTTP_400_BAD_REQUEST)
    
    try:
        user_id = int(user_id)
        location_id = int(location_id)
        
        # Kiểm tra user có tồn tại không
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            return Response({'error': 'User not found'}, status=status.HTTP_404_NOT_FOUND)
        
        # Kiểm tra location có tồn tại không
        try:
            location = Location.objects.get(location_id=location_id)
        except Location.DoesNotExist:
            return Response({'error': 'Location not found'}, status=status.HTTP_404_NOT_FOUND)
        
        if request.method == 'GET':
            # Lấy preferences cho location
            try:
                location_pref = LocationNotificationPreferences.objects.get(
                    user=user,
                    location=location
                )
                serializer = LocationNotificationPreferencesSerializer(location_pref)
                logger.info(f"[LOCATION PREFS API] Retrieved preferences for user {user_id}, location {location_id}")
                return Response(serializer.data, status=status.HTTP_200_OK)
            except LocationNotificationPreferences.DoesNotExist:
                # Nếu chưa có preferences cho location này, trả về default
                return Response({
                    'message': 'No preferences found for this location. Default will be created on first update.',
                    'user_id': user_id,
                    'location_id': location_id,
                    'notifications_enabled': True  # Default value
                }, status=status.HTTP_200_OK)
        
        elif request.method == 'POST':
            # Cập nhật preferences cho location
            logger.info(f"[LOCATION PREFS API] POST request data: {request.data}")
            logger.info(f"[LOCATION PREFS API] Content-Type: {request.content_type}")
            
            notifications_enabled = request.data.get('notifications_enabled')
            
            if notifications_enabled is None:
                logger.error(f"[LOCATION PREFS API] notifications_enabled is None. Full request.data: {request.data}")
                return Response({'error': 'notifications_enabled is required'}, status=status.HTTP_400_BAD_REQUEST)
            
            # Validate boolean
            if not isinstance(notifications_enabled, bool):
                return Response({'error': 'notifications_enabled must be a boolean'}, status=status.HTTP_400_BAD_REQUEST)
            
            # Sử dụng UserPreferenceManager để cập nhật (truyền request để audit logging)
            preference_manager = UserPreferenceManager()
            result = preference_manager.update_location_preferences(
                user_id=user_id,
                location_id=location_id,
                notifications_enabled=notifications_enabled,
                request=request  # Truyền request để audit logging
            )
            
            # Lấy lại object để serialize
            location_pref = LocationNotificationPreferences.objects.get(
                user=user,
                location=location
            )
            serializer = LocationNotificationPreferencesSerializer(location_pref)
            
            logger.info(f"[LOCATION PREFS API] Updated preferences for user {user_id}, location {location_id}")
            return Response({
                'message': 'Location preferences updated successfully',
                'preferences': serializer.data
            }, status=status.HTTP_200_OK)
    
    except ValueError:
        return Response({'error': 'Invalid user_id or location_id format'}, status=status.HTTP_400_BAD_REQUEST)
    except Exception as e:
        logger.error(f"[LOCATION PREFS API] Error: {e}", exc_info=True)
        return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# --- Notification History API Views ---

@api_view(['GET'])
# Cần thêm @permission_classes([IsAuthenticated]) sau này
def notification_history(request):
    """
    GET: Lấy lịch sử thông báo của user với filtering và pagination
    Query params:
        - user_id (int, required): ID của user
        - notification_type (str, optional): Lọc theo loại thông báo
        - start_date (str, optional): Ngày bắt đầu (YYYY-MM-DD)
        - end_date (str, optional): Ngày kết thúc (YYYY-MM-DD)
        - page (int, optional): Số trang (default: 1)
        - page_size (int, optional): Số items mỗi trang (default: 20, max: 100)
    """
    from .serializers import NotificationRecordSerializer
    from .models import NotificationRecord
    from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
    
    user_id = request.query_params.get('user_id')
    
    if not user_id:
        return Response({'error': 'user_id is required'}, status=status.HTTP_400_BAD_REQUEST)
    
    try:
        user_id = int(user_id)
        
        # Kiểm tra user có tồn tại không
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            return Response({'error': 'User not found'}, status=status.HTTP_404_NOT_FOUND)
        
        # Bắt đầu với queryset cơ bản
        queryset = NotificationRecord.objects.filter(user=user).select_related('location', 'alert')
        
        # Lọc theo notification_type nếu có
        notification_type = request.query_params.get('notification_type')
        if notification_type:
            queryset = queryset.filter(notification_type=notification_type)
        
        # Lọc theo khoảng thời gian
        start_date = request.query_params.get('start_date')
        end_date = request.query_params.get('end_date')
        
        if start_date:
            try:
                start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
                # Chuyển sang timezone-aware datetime
                start_datetime = timezone.make_aware(start_datetime.replace(hour=0, minute=0, second=0))
                queryset = queryset.filter(sent_at__gte=start_datetime)
            except ValueError:
                return Response({'error': 'Invalid start_date format. Use YYYY-MM-DD'}, status=status.HTTP_400_BAD_REQUEST)
        
        if end_date:
            try:
                end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
                # Chuyển sang timezone-aware datetime, set to end of day
                end_datetime = timezone.make_aware(end_datetime.replace(hour=23, minute=59, second=59))
                queryset = queryset.filter(sent_at__lte=end_datetime)
            except ValueError:
                return Response({'error': 'Invalid end_date format. Use YYYY-MM-DD'}, status=status.HTTP_400_BAD_REQUEST)
        
        # Sắp xếp theo thời gian gửi (mới nhất trước)
        queryset = queryset.order_by('-sent_at')
        
        # Pagination
        page = request.query_params.get('page', 1)
        page_size = request.query_params.get('page_size', 20)
        
        try:
            page = int(page)
            page_size = int(page_size)
            
            # Giới hạn page_size
            if page_size > 100:
                page_size = 100
            if page_size < 1:
                page_size = 20
                
        except ValueError:
            return Response({'error': 'Invalid page or page_size format'}, status=status.HTTP_400_BAD_REQUEST)
        
        paginator = Paginator(queryset, page_size)
        
        try:
            notifications_page = paginator.page(page)
        except PageNotAnInteger:
            notifications_page = paginator.page(1)
        except EmptyPage:
            notifications_page = paginator.page(paginator.num_pages)
        
        serializer = NotificationRecordSerializer(notifications_page, many=True)
        
        logger.info(f"[HISTORY API] Retrieved {len(serializer.data)} notifications for user {user_id} (page {page})")
        
        return Response({
            'count': paginator.count,
            'num_pages': paginator.num_pages,
            'current_page': notifications_page.number,
            'page_size': page_size,
            'has_next': notifications_page.has_next(),
            'has_previous': notifications_page.has_previous(),
            'results': serializer.data
        }, status=status.HTTP_200_OK)
    
    except ValueError:
        return Response({'error': 'Invalid user_id format'}, status=status.HTTP_400_BAD_REQUEST)
    except Exception as e:
        logger.error(f"[HISTORY API] Error: {e}", exc_info=True)
        return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
# Cần thêm @permission_classes([IsAuthenticated]) sau này
def notification_history_detail(request, record_id):
    """
    GET: Lấy chi tiết một notification record cụ thể
    URL param: record_id (int)
    Query param: user_id (int, required)
    """
    from .serializers import NotificationRecordSerializer
    from .models import NotificationRecord
    
    user_id = request.query_params.get('user_id')
    
    if not user_id:
        return Response({'error': 'user_id is required'}, status=status.HTTP_400_BAD_REQUEST)
    
    try:
        user_id = int(user_id)
        record_id = int(record_id)
        
        # Kiểm tra user có tồn tại không
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            return Response({'error': 'User not found'}, status=status.HTTP_404_NOT_FOUND)
        
        # Lấy notification record
        try:
            notification = NotificationRecord.objects.select_related('location', 'alert').get(
                record_id=record_id,
                user=user
            )
        except NotificationRecord.DoesNotExist:
            return Response({'error': 'Notification record not found'}, status=status.HTTP_404_NOT_FOUND)
        
        serializer = NotificationRecordSerializer(notification)
        
        logger.info(f"[HISTORY API] Retrieved notification detail {record_id} for user {user_id}")
        
        return Response(serializer.data, status=status.HTTP_200_OK)
    
    except ValueError:
        return Response({'error': 'Invalid user_id or record_id format'}, status=status.HTTP_400_BAD_REQUEST)
    except Exception as e:
        logger.error(f"[HISTORY API] Error: {e}", exc_info=True)
        return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# --- Preference Audit Log API Views ---

@api_view(['GET'])
# Cần thêm @permission_classes([IsAuthenticated]) sau này
def preference_audit_logs(request):
    """
    GET: Lấy audit logs của preference changes cho user
    Query params:
        - user_id (int, required): ID của user
        - preference_type (str, optional): Lọc theo loại ('global' hoặc 'location')
        - location_id (int, optional): Lọc theo location cụ thể
        - start_date (str, optional): Ngày bắt đầu (YYYY-MM-DD)
        - end_date (str, optional): Ngày kết thúc (YYYY-MM-DD)
        - page (int, optional): Số trang (default: 1)
        - page_size (int, optional): Số items mỗi trang (default: 20, max: 100)
    """
    from .models import PreferenceAuditLog
    from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
    
    user_id = request.query_params.get('user_id')
    
    if not user_id:
        return Response({'error': 'user_id is required'}, status=status.HTTP_400_BAD_REQUEST)
    
    try:
        user_id = int(user_id)
        
        # Kiểm tra user có tồn tại không
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            return Response({'error': 'User not found'}, status=status.HTTP_404_NOT_FOUND)
        
        # Bắt đầu với queryset cơ bản
        queryset = PreferenceAuditLog.objects.filter(user=user).select_related('location')
        
        # Lọc theo preference_type nếu có
        preference_type = request.query_params.get('preference_type')
        if preference_type:
            queryset = queryset.filter(preference_type=preference_type)
        
        # Lọc theo location_id nếu có
        location_id = request.query_params.get('location_id')
        if location_id:
            try:
                location_id = int(location_id)
                queryset = queryset.filter(location_id=location_id)
            except ValueError:
                return Response({'error': 'Invalid location_id format'}, status=status.HTTP_400_BAD_REQUEST)
        
        # Lọc theo khoảng thời gian
        start_date = request.query_params.get('start_date')
        end_date = request.query_params.get('end_date')
        
        if start_date:
            try:
                start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
                start_datetime = timezone.make_aware(start_datetime.replace(hour=0, minute=0, second=0))
                queryset = queryset.filter(changed_at__gte=start_datetime)
            except ValueError:
                return Response({'error': 'Invalid start_date format. Use YYYY-MM-DD'}, status=status.HTTP_400_BAD_REQUEST)
        
        if end_date:
            try:
                end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
                end_datetime = timezone.make_aware(end_datetime.replace(hour=23, minute=59, second=59))
                queryset = queryset.filter(changed_at__lte=end_datetime)
            except ValueError:
                return Response({'error': 'Invalid end_date format. Use YYYY-MM-DD'}, status=status.HTTP_400_BAD_REQUEST)
        
        # Sắp xếp theo thời gian thay đổi (mới nhất trước)
        queryset = queryset.order_by('-changed_at')
        
        # Pagination
        page = request.query_params.get('page', 1)
        page_size = request.query_params.get('page_size', 20)
        
        try:
            page = int(page)
            page_size = int(page_size)
            
            if page_size > 100:
                page_size = 100
            if page_size < 1:
                page_size = 20
                
        except ValueError:
            return Response({'error': 'Invalid page or page_size format'}, status=status.HTTP_400_BAD_REQUEST)
        
        paginator = Paginator(queryset, page_size)
        
        try:
            logs_page = paginator.page(page)
        except PageNotAnInteger:
            logs_page = paginator.page(1)
        except EmptyPage:
            logs_page = paginator.page(paginator.num_pages)
        
        # Serialize data
        results = []
        for log in logs_page:
            results.append({
                'log_id': log.log_id,
                'preference_type': log.preference_type,
                'location_id': log.location.location_id if log.location else None,
                'location_name': log.location.name_en if log.location else None,
                'field_name': log.field_name,
                'old_value': log.old_value,
                'new_value': log.new_value,
                'changed_at': log.changed_at,
                'ip_address': log.ip_address,
                'user_agent': log.user_agent
            })
        
        logger.info(f"[AUDIT LOG API] Retrieved {len(results)} audit logs for user {user_id} (page {page})")
        
        return Response({
            'count': paginator.count,
            'num_pages': paginator.num_pages,
            'current_page': logs_page.number,
            'page_size': page_size,
            'has_next': logs_page.has_next(),
            'has_previous': logs_page.has_previous(),
            'results': results
        }, status=status.HTTP_200_OK)
    
    except ValueError:
        return Response({'error': 'Invalid user_id format'}, status=status.HTTP_400_BAD_REQUEST)
    except Exception as e:
        logger.error(f"[AUDIT LOG API] Error: {e}", exc_info=True)
        return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST', 'DELETE'])
@permission_classes([AllowAny])
def delete_tracked_location(request):
    """
    Xóa một vị trí đã theo dõi
    POST /api/locations/delete/?user_id=X&location_id=Y
    DELETE /api/locations/delete/?user_id=X&location_id=Y
    """
    user_id = request.query_params.get('user_id') or request.data.get('user_id')
    location_id = request.query_params.get('location_id') or request.data.get('location_id')
    
    if not user_id or not location_id:
        return Response(
            {'error': 'user_id and location_id are required'}, 
            status=status.HTTP_400_BAD_REQUEST
        )
    
    try:
        user_id = int(user_id)
        location_id = int(location_id)
        
        # Tìm location
        try:
            location = Location.objects.get(location_id=location_id)
        except Location.DoesNotExist:
            return Response(
                {'error': 'Location not found'},
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Kiểm tra user có trong danh sách users không
        users_list = location.users or []
        if user_id not in users_list:
            return Response(
                {'error': 'User is not tracking this location'},
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Xóa user khỏi danh sách
        users_list.remove(user_id)
        location.users = users_list
        location.save()
        
        logger.info(f"[TRACK] User {user_id} untracked location {location_id}")
        
        return Response(
            {'message': 'Location untracked successfully'},
            status=status.HTTP_200_OK
        )
        
    except ValueError:
        return Response(
            {'error': 'Invalid user_id or location_id'},
            status=status.HTTP_400_BAD_REQUEST
        )
    except Exception as e:
        logger.error(f"[ERROR] delete_tracked_location: {e}", exc_info=True)
        return Response(
            {'error': 'Internal server error'},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
