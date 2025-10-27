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
from .models import User, Location, WeatherData, ExtremeEvent
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
@permission_classes([AllowAny]) # Tạm thời cho phép mọi người gọi
def get_ai_advice(request):
    """
    API endpoint để lấy lời khuyên/cảnh báo tức thời từ AI cho một địa điểm.
    Lấy dữ liệu THEO GIỜ (-3 đến +3 ngày) từ WeatherAPI.
    Có cache kết quả AI trong 3 giờ.
    """
    location_name_en = request.query_params.get('q')
    if not location_name_en:
        return Response({'error': "'q' query parameter (location name_en) is required."}, status=status.HTTP_400_BAD_REQUEST)

    # --- Logic Cache giữ nguyên ---
    today_date_str = timezone.now().strftime('%Y-%m-%d')
    cache_key = f"ai_advice:{location_name_en.lower()}:{today_date_str}"
    cached_advice = cache.get(cache_key)
    if cached_advice:
        logger.info(f"[AI ADVICE CACHE HIT] Key: {cache_key}")
        return Response(cached_advice, status=status.HTTP_200_OK)
    logger.info(f"[AI ADVICE CACHE MISS] Key: {cache_key}.")
    # --- Kết thúc Cache ---

    hourly_data_list = []
    api_fetch_error = False
    
    # --- LẤY DỮ LIỆU THEO GIỜ TỪ WEATHERAPI ---
    logger.info(f"[AI ADVICE API - HOURLY] Fetching hourly data for {location_name_en}")
    
    # 1. Lấy dữ liệu lịch sử theo giờ (3 ngày trước)
    today = timezone.now().date()
    start_date_hist = today - timedelta(days=3)
    end_date_hist = today - timedelta(days=1) # Lấy đến hết hôm qua

    # API history chỉ cho lấy 1 ngày 1 lần, nên cần gọi 3 lần
    current_hist_date = start_date_hist
    while current_hist_date <= end_date_hist:
        date_str = current_hist_date.strftime('%Y-%m-%d')
        logger.debug(f"[AI ADVICE API - HOURLY] Fetching history for {date_str}")
        hist_data, hist_err = call_weather_api_from_task('history', {'q': location_name_en, 'dt': date_str})
        if hist_data and 'forecast' in hist_data and 'forecastday' in hist_data['forecast']:
            day_data = hist_data['forecast']['forecastday'][0] # Chỉ có 1 ngày trong response
            hourly_data_list.extend(day_data.get('hour', [])) # Thêm list các giờ vào
        else:
            logger.warning(f"[AI ADVICE API - HOURLY] Failed to fetch history for {date_str}: {hist_err}")
            # Có thể chấp nhận thiếu vài ngày lịch sử, không đặt cờ lỗi nặng
        current_hist_date += timedelta(days=1)

    # 2. Lấy dữ liệu dự báo theo giờ (Hôm nay + 3 ngày tới = 4 ngày)
    logger.debug(f"[AI ADVICE API - HOURLY] Fetching forecast for 4 days")
    fc_data, fc_err = call_weather_api_from_task('forecast', {'q': location_name_en, 'days': 4}) # Lấy 4 ngày dự báo
    if fc_data and 'forecast' in fc_data and 'forecastday' in fc_data['forecast']:
        for day_data in fc_data['forecast']['forecastday']:
            hourly_data_list.extend(day_data.get('hour', [])) # Thêm list các giờ vào
    else:
        api_fetch_error = True # Lỗi dự báo là nghiêm trọng hơn
        logger.error(f"[AI ADVICE API - HOURLY] Failed to fetch forecast from API: {fc_err}")
    
    # --- KẾT THÚC LẤY DỮ LIỆU GIỜ ---
    
    if api_fetch_error or not hourly_data_list:
        logger.error(f"[AI ADVICE API - HOURLY] Failed to fetch sufficient hourly data for {location_name_en}.")
        return Response({"type": "error", "message_vi": "Lỗi khi lấy dữ liệu thời tiết chi tiết. Vui lòng thử lại sau."}, status=status.HTTP_503_SERVICE_UNAVAILABLE)

    # --- Chuẩn bị dữ liệu cuối cùng cho AI (chỉ lấy các trường cần thiết) ---
    final_hourly_data_for_ai = []
    for hour in hourly_data_list:
        try:
           # Chuyển đổi 'time' thành đối tượng datetime để sort (nếu cần) và lấy thông tin
           # API trả về dạng "YYYY-MM-DD HH:MM"
           record_time_naive = datetime.strptime(hour.get('time'), '%Y-%m-%d %H:%M')
           record_time_aware = timezone.make_aware(record_time_naive) # Giả định là giờ địa phương theo TIME_ZONE

           final_hourly_data_for_ai.append({
               'time': hour.get('time'),
               'temp_c': hour.get('temp_c'),
               'humidity': hour.get('humidity'),
               'wind_kph': hour.get('wind_kph'),
               'condition_text': hour.get('condition', {}).get('text'),
               'uv': hour.get('uv'),
               'precip_mm': hour.get('precip_mm', 0.0),
               'chance_of_rain': hour.get('chance_of_rain', 0)
               # Thêm các trường khác nếu AI cần: feelslike_c, chance_of_rain, etc.
           })
        except (ValueError, KeyError, TypeError) as e:
           logger.warning(f"Skipping invalid hourly record: {hour.get('time')} - {e}")
           
    # Sắp xếp lại lần cuối theo thời gian để đảm bảo đúng thứ tự
    final_hourly_data_for_ai.sort(key=lambda x: datetime.strptime(x['time'], '%Y-%m-%d %H:%M'))

    logger.info(f"[AI ADVICE API - HOURLY] Prepared {len(final_hourly_data_for_ai)} hourly records for AI.")

    # --- Gọi AI và Cache (Giữ nguyên) ---
    try:
        advice_result = call_local_ai_for_advice(final_hourly_data_for_ai) # Gửi dữ liệu giờ cho AI
        if advice_result:
            cache.set(cache_key, advice_result, timeout=3 * 60 * 60)
            logger.info(f"[AI ADVICE CACHE STORED] Key: {cache_key}")
            return Response(advice_result, status=status.HTTP_200_OK)
        else:
            return Response({"type": "error", "message_vi": "Không thể kết nối với trợ lý AI lúc này."}, status=status.HTTP_503_SERVICE_UNAVAILABLE)
    except Exception as e:
        logger.error(f"[API ERROR] /api/advice during AI call (hourly) for {location_name_en}: {e}", exc_info=True)
        return Response({'error': 'Internal server error during AI call'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

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