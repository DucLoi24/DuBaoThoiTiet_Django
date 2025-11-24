# api/firebase_notifications.py
import logging
from typing import List, Dict, Any
import os
from pathlib import Path
from django.conf import settings
import firebase_admin
from firebase_admin import credentials, messaging

logger = logging.getLogger(__name__)

# Khởi tạo Firebase Admin SDK (chỉ 1 lần)
_firebase_initialized = False

def initialize_firebase():
    """Khởi tạo Firebase Admin SDK"""
    global _firebase_initialized
    
    if _firebase_initialized:
        return True
    
    try:
        # Tìm file service account JSON
        service_account_path = getattr(settings, 'FIREBASE_SERVICE_ACCOUNT_PATH', None)
        
        if not service_account_path:
            # Thử tìm trong thư mục project
            base_dir = Path(settings.BASE_DIR)
            possible_paths = [
                base_dir / 'firebase-service-account.json',
                base_dir / 'serviceAccountKey.json',
            ]
            
            for path in possible_paths:
                if path.exists():
                    service_account_path = str(path)
                    break
        
        if not service_account_path or not os.path.exists(service_account_path):
            logger.error("Firebase service account JSON file not found")
            return False
        
        # Khởi tạo Firebase Admin
        cred = credentials.Certificate(service_account_path)
        firebase_admin.initialize_app(cred)
        
        _firebase_initialized = True
        logger.info("Firebase Admin SDK initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing Firebase Admin SDK: {e}", exc_info=True)
        return False

def send_fcm_notification(
    device_tokens: List[str],
    title: str,
    body: str,
    data: Dict[str, Any] = None
) -> Dict[str, int]:
    """
    Gửi push notification qua Firebase Cloud Messaging (HTTP v1 API)
    
    Args:
        device_tokens: Danh sách FCM tokens
        title: Tiêu đề notification
        body: Nội dung notification
        data: Dữ liệu bổ sung (optional)
    
    Returns:
        Dict với success_count và failure_count
    """
    if not device_tokens:
        logger.warning("No device tokens provided")
        return {"success_count": 0, "failure_count": 0}
    
    # Khởi tạo Firebase nếu chưa
    if not initialize_firebase():
        logger.error("Firebase not initialized, cannot send notifications")
        return {"success_count": 0, "failure_count": len(device_tokens)}
    
    success_count = 0
    failure_count = 0
    
    for token in device_tokens:
        try:
            # Chuẩn bị data payload (bao gồm title và body)
            data_payload = data.copy() if data else {}
            data_payload['title'] = title
            data_payload['body'] = body
            
            # Tạo message
            message = messaging.Message(
                notification=messaging.Notification(
                    title=title,
                    body=body,
                ),
                data=data_payload,  # Gửi title và body trong data
                token=token,
                android=messaging.AndroidConfig(
                    priority='high',
                    notification=messaging.AndroidNotification(
                        sound='default',
                    ),
                ),
            )
            
            # Gửi message
            response = messaging.send(message)
            success_count += 1
            logger.info(f"FCM notification sent successfully: {response}")
            
        except messaging.UnregisteredError:
            failure_count += 1
            logger.warning(f"Token is invalid or unregistered: {token[:20]}...")
            # TODO: Có thể xóa token này khỏi database
            
        except Exception as e:
            failure_count += 1
            logger.error(f"Error sending FCM notification to {token[:20]}...: {e}")
    
    return {
        "success_count": success_count,
        "failure_count": failure_count
    }


def notify_weather_change(location_name: str, old_condition: str, new_condition: str, user_ids: List[int]):
    """
    Gửi thông báo thay đổi thời tiết cho users
    
    Args:
        location_name: Tên địa điểm
        old_condition: Trạng thái thời tiết cũ
        new_condition: Trạng thái thời tiết mới
        user_ids: Danh sách user IDs cần thông báo
    """
    from .models import DeviceToken
    
    # Lấy tất cả device tokens của users
    tokens = DeviceToken.objects.filter(
        user_id__in=user_ids,
        is_active=True
    ).values_list('token', flat=True)
    
    if not tokens:
        logger.info(f"No active device tokens found for users: {user_ids}")
        return
    
    title = f"🌤️ Thời tiết {location_name} thay đổi"
    body = f"{old_condition} → {new_condition}"
    
    data = {
        "type": "weather_change",
        "location": location_name,
        "old_condition": old_condition,
        "new_condition": new_condition
    }
    
    result = send_fcm_notification(list(tokens), title, body, data)
    logger.info(f"Weather change notification sent: {result}")


def send_weather_alert_notification(
    device_tokens: List[str],
    location_name: str,
    alert
) -> Dict[str, Any]:
    """
    Gửi push notification cho weather alert với độ ưu tiên cao.
    Được gọi từ tasks.py trong analyze_location_with_preprocessing.
    
    Args:
        device_tokens: List FCM tokens
        location_name: Tên địa điểm
        alert: ExtremeEvent object
    
    Returns:
        Dict với kết quả gửi
    """
    try:
        # Mapping impact_field sang emoji và title (phân cấp bão chi tiết)
        alert_config = {
            'flood_risk': {'emoji': '🌊', 'title': 'CẢNH BÁO LŨ LỤT'},
            'heavy_rain': {'emoji': '🌧️', 'title': 'Cảnh báo mưa to'},
            'extreme_heat': {'emoji': '🔥', 'title': 'Cảnh báo nắng nóng'},
            'strong_wind': {'emoji': '💨', 'title': 'Cảnh báo gió mạnh'},
            # Phân cấp bão
            'super_typhoon': {'emoji': '🔴🌀', 'title': '⚠️ SIÊU BÃO CẤP 5'},
            'typhoon': {'emoji': '🌀', 'title': 'CẢNH BÁO BÃO MẠNH'},
            'tropical_storm': {'emoji': '🌀', 'title': 'Cảnh báo bão nhiệt đới'},
            'tropical_depression': {'emoji': '🌪️', 'title': 'Cảnh báo áp thấp nhiệt đới'},
            'extreme_cold': {'emoji': '❄️', 'title': 'Cảnh báo rét đậm'},
            # UV Index
            'extreme_uv': {'emoji': '☀️🔴', 'title': 'CẢNH BÁO UV CỰC KỲ NGUY HIỂM'},
            'very_high_uv': {'emoji': '☀️', 'title': 'Cảnh báo UV rất cao'},
            'high_uv': {'emoji': '☀️', 'title': 'Cảnh báo UV cao'},
            # Air Quality
            'hazardous_aqi': {'emoji': '🔴😷', 'title': 'CẢNH BÁO KHÔNG KHÍ NGUY HẠI'},
            'very_unhealthy_aqi': {'emoji': '🟣😷', 'title': 'Cảnh báo không khí rất không tốt'},
            'unhealthy_aqi': {'emoji': '🔴😷', 'title': 'Cảnh báo không khí không tốt'},
            'unhealthy_sensitive_aqi': {'emoji': '🟠😷', 'title': 'Cảnh báo không khí cho nhóm nhạy cảm'}
        }
        
        config = alert_config.get(alert.impact_field, {
            'emoji': '⚠️',
            'title': 'Cảnh báo thời tiết'
        })
        
        title = f"{config['emoji']} {config['title']} - {location_name}"
        body = alert.forecast_details_vi
        
        # Data payload với độ ưu tiên cao
        data = {
            'type': 'weather_alert',
            'alert_id': str(alert.event_id),
            'location_name': location_name,
            'severity': alert.severity,
            'impact_field': alert.impact_field,
            'advice': alert.actionable_advice_vi,
            'priority': 'high' if alert.severity in ['HIGH', 'EXTREME'] else 'medium'
        }
        
        # Gửi FCM notification với priority cao
        result = send_fcm_notification(
            device_tokens=device_tokens,
            title=title,
            body=body,
            data=data
        )
        
        logger.info(f"Weather alert notification sent: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error in send_weather_alert_notification: {e}", exc_info=True)
        return {
            'success_count': 0,
            'failure_count': len(device_tokens)
        }
