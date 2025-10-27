# api/views.py
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework import status
from django.core.cache import cache
from django.conf import settings # Import settings
from django.db import transaction
import requests
import bcrypt
import json
import logging
from datetime import datetime, timedelta
from .scheduler import scheduler
from django.utils import timezone

from .models import User, Location, WeatherData, ExtremeEvent
from .tasks import trigger_data_ingestion, trigger_llm_analysis
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
