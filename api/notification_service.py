# api/notification_service.py
"""
Service xử lý gửi push notifications cho weather alerts và events.
Tích hợp với Firebase Cloud Messaging.
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from django.utils import timezone
from django.db import transaction

from .models import (
    ExtremeEvent, 
    DeviceToken, 
    NotificationRecord,
    LocationNotificationPreferences,
    Location,
    User
)
from .firebase_notifications import send_fcm_notification, initialize_firebase

logger = logging.getLogger(__name__)


class NotificationService:
    """
    Service chính để gửi các loại push notifications.
    Xử lý weather alerts, scheduled notifications, và notification history.
    """
    
    # Mapping severity sang priority
    SEVERITY_PRIORITY_MAP = {
        'EXTREME': 'critical',
        'HIGH': 'high',
        'MEDIUM': 'medium',
        'LOW': 'low'
    }
    
    # Mapping impact_field sang emoji và title
    ALERT_TYPE_CONFIG = {
        'flood_risk': {
            'emoji': '🌊',
            'title_template': 'Cảnh báo lũ lụt - {location}'
        },
        'heavy_rain': {
            'emoji': '🌧️',
            'title_template': 'Cảnh báo mưa to - {location}'
        },
        'extreme_heat': {
            'emoji': '🔥',
            'title_template': 'Cảnh báo nắng nóng - {location}'
        },
        'strong_wind': {
            'emoji': '💨',
            'title_template': 'Cảnh báo gió mạnh - {location}'
        },
        # Phân cấp bão chi tiết
        'super_typhoon': {
            'emoji': '🔴🌀',
            'title_template': '⚠️ SIÊU BÃO CẤP 5 - {location}'
        },
        'typhoon': {
            'emoji': '🌀',
            'title_template': 'CẢNH BÁO BÃO MẠNH - {location}'
        },
        'tropical_storm': {
            'emoji': '🌀',
            'title_template': 'Cảnh báo bão nhiệt đới - {location}'
        },
        'tropical_depression': {
            'emoji': '🌪️',
            'title_template': 'Cảnh báo áp thấp nhiệt đới - {location}'
        },
        'extreme_cold': {
            'emoji': '❄️',
            'title_template': 'Cảnh báo rét đậm - {location}'
        },
        # UV Index
        'extreme_uv': {
            'emoji': '☀️🔴',
            'title_template': 'CẢNH BÁO UV CỰC KỲ NGUY HIỂM - {location}'
        },
        'very_high_uv': {
            'emoji': '☀️',
            'title_template': 'Cảnh báo UV rất cao - {location}'
        },
        'high_uv': {
            'emoji': '☀️',
            'title_template': 'Cảnh báo UV cao - {location}'
        },
        # Air Quality Index (AQI)
        'hazardous_aqi': {
            'emoji': '🔴😷',
            'title_template': 'CẢNH BÁO KHÔNG KHÍ NGUY HẠI - {location}'
        },
        'very_unhealthy_aqi': {
            'emoji': '🟣😷',
            'title_template': 'Cảnh báo không khí rất không tốt - {location}'
        },
        'unhealthy_aqi': {
            'emoji': '🔴😷',
            'title_template': 'Cảnh báo không khí không tốt - {location}'
        },
        'unhealthy_sensitive_aqi': {
            'emoji': '🟠😷',
            'title_template': 'Cảnh báo không khí cho nhóm nhạy cảm - {location}'
        }
    }
    
    def __init__(self):
        """Khởi tạo Notification Service"""
        self.logger = logger
        # Khởi tạo Firebase
        initialize_firebase()
    
    def send_weather_alert(self, alert: ExtremeEvent) -> Dict[str, Any]:
        """
        Gửi push notification cho weather alert.
        
        Args:
            alert: ExtremeEvent object
        
        Returns:
            Dict với kết quả gửi notification
        """
        try:
            location = alert.location
            
            # Lấy danh sách users theo dõi location này
            user_ids = location.users if location.users else []
            
            if not user_ids:
                self.logger.info(f"No users tracking location {location.name_en}")
                return {
                    'success': True,
                    'sent_count': 0,
                    'message': 'No users to notify'
                }
            
            total_sent = 0
            total_failed = 0
            
            # Gửi cho từng user
            for user_id in user_ids:
                result = self._send_alert_to_user(alert, user_id)
                total_sent += result['sent_count']
                total_failed += result['failed_count']
            
            # Đánh dấu alert đã được gửi
            alert.is_notified = True
            alert.save(update_fields=['is_notified'])
            
            return {
                'success': True,
                'sent_count': total_sent,
                'failed_count': total_failed,
                'alert_id': alert.event_id
            }
            
        except Exception as e:
            self.logger.error(f"Error sending weather alert: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
    
    def _send_alert_to_user(
        self, 
        alert: ExtremeEvent, 
        user_id: int
    ) -> Dict[str, int]:
        """Gửi alert notification cho một user cụ thể"""
        try:
            # Kiểm tra preferences
            location_pref = LocationNotificationPreferences.objects.filter(
                user_id=user_id,
                location=alert.location
            ).first()
            
            # Nếu user tắt notification cho location này, skip
            if location_pref and not location_pref.notifications_enabled:
                self.logger.info(f"User {user_id} disabled notifications for {alert.location.name_en}")
                return {'sent_count': 0, 'failed_count': 0}
            
            # Lấy device tokens
            tokens = list(DeviceToken.objects.filter(
                user_id=user_id,
                is_active=True
            ).values_list('token', flat=True))
            
            if not tokens:
                self.logger.info(f"No active tokens for user {user_id}")
                return {'sent_count': 0, 'failed_count': 0}
            
            # Chuẩn bị notification content
            config = self.ALERT_TYPE_CONFIG.get(alert.impact_field, {
                'emoji': '⚠️',
                'title_template': 'Cảnh báo thời tiết - {location}'
            })
            
            title = config['emoji'] + ' ' + config['title_template'].format(
                location=alert.location.name_en
            )
            body = alert.forecast_details_vi
            
            # Data payload
            data = {
                'type': 'weather_alert',
                'alert_id': str(alert.event_id),
                'location_id': str(alert.location.location_id),
                'location_name': alert.location.name_en,
                'severity': alert.severity,
                'impact_field': alert.impact_field,
                'advice': alert.actionable_advice_vi
            }
            
            # Gửi FCM notification
            result = send_fcm_notification(
                device_tokens=tokens,
                title=title,
                body=body,
                data=data
            )
            
            # Lưu notification record
            self._save_notification_record(
                user_id=user_id,
                notification_type='weather_alert',
                title=title,
                body=body,
                data=data,
                success=result['success_count'] > 0
            )
            
            return {
                'sent_count': result['success_count'],
                'failed_count': result['failure_count']
            }
            
        except Exception as e:
            self.logger.error(f"Error sending alert to user {user_id}: {e}", exc_info=True)
            return {'sent_count': 0, 'failed_count': 1}
    
    def _save_notification_record(
        self,
        user_id: int,
        notification_type: str,
        title: str,
        body: str,
        data: Dict[str, Any],
        success: bool
    ):
        """Lưu lịch sử notification vào database"""
        try:
            # Lấy priority từ data hoặc mặc định là 'medium'
            priority = data.get('priority', 'medium')
            
            NotificationRecord.objects.create(
                user_id=user_id,
                notification_type=notification_type,
                title=title,
                body=body,
                priority=priority,
                delivered=success,
                fcm_message_id=None  # Có thể cập nhật sau nếu FCM trả về message_id
            )
        except Exception as e:
            self.logger.error(f"Error saving notification record: {e}")
    
    def cleanup_old_notification_records(self, retention_days: int = 90) -> Dict[str, Any]:
        """
        Dọn dẹp lịch sử notification cũ hơn retention_days.
        
        Args:
            retention_days: Số ngày lưu trữ (mặc định 90)
        
        Returns:
            Dict với số lượng records đã xóa
        """
        try:
            cutoff_date = timezone.now() - timedelta(days=retention_days)
            
            deleted_count, _ = NotificationRecord.objects.filter(
                sent_at__lt=cutoff_date
            ).delete()
            
            self.logger.info(f"Cleaned up {deleted_count} notification records older than {retention_days} days")
            
            return {
                'deleted_count': deleted_count,
                'cutoff_date': cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            self.logger.error(f"Error cleaning up notification records: {e}", exc_info=True)
            return {
                'deleted_count': 0,
                'error': str(e)
            }
    
    def send_test_notification(
        self, 
        user_id: int, 
        title: str = "Test Notification",
        body: str = "This is a test notification"
    ) -> Dict[str, Any]:
        """Gửi test notification cho user"""
        try:
            tokens = list(DeviceToken.objects.filter(
                user_id=user_id,
                is_active=True
            ).values_list('token', flat=True))
            
            if not tokens:
                return {
                    'success': False,
                    'error': 'No active tokens found'
                }
            
            result = send_fcm_notification(
                device_tokens=tokens,
                title=title,
                body=body,
                data={'type': 'test'}
            )
            
            return {
                'success': result['success_count'] > 0,
                'sent_count': result['success_count'],
                'failed_count': result['failure_count']
            }
            
        except Exception as e:
            self.logger.error(f"Error sending test notification: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }


def send_weather_alert_notification(
    device_tokens: List[str],
    location_name: str,
    alert: ExtremeEvent
) -> Dict[str, Any]:
    """
    Helper function để gửi weather alert notification.
    Được gọi từ tasks.py trong analyze_location_with_preprocessing.
    
    Args:
        device_tokens: List FCM tokens
        location_name: Tên địa điểm
        alert: ExtremeEvent object
    
    Returns:
        Dict với kết quả gửi
    """
    try:
        # Chuẩn bị notification content
        config = NotificationService.ALERT_TYPE_CONFIG.get(alert.impact_field, {
            'emoji': '⚠️',
            'title_template': 'Cảnh báo thời tiết - {location}'
        })
        
        title = config['emoji'] + ' ' + config['title_template'].format(
            location=location_name
        )
        body = alert.forecast_details_vi
        
        # Data payload
        data = {
            'type': 'weather_alert',
            'alert_id': str(alert.event_id),
            'location_id': str(alert.location.location_id),
            'location_name': location_name,
            'severity': alert.severity,
            'impact_field': alert.impact_field,
            'advice': alert.actionable_advice_vi
        }
        
        # Gửi FCM notification
        result = send_fcm_notification(
            device_tokens=device_tokens,
            title=title,
            body=body,
            data=data
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error in send_weather_alert_notification: {e}", exc_info=True)
        return {
            'success_count': 0,
            'failure_count': len(device_tokens)
        }
