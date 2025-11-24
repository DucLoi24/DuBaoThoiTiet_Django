# api/scheduled_notifications.py
"""
Service xử lý các scheduled notifications:
- Morning summary (7:00 AM)
- Tomorrow forecast (8:00 PM)
- Weekly summary (8:00 PM Sunday)
"""
import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta
from django.utils import timezone
from django.db.models import Q

from .models import (
    User,
    Location,
    DeviceToken,
    NotificationPreferences,
    LocationNotificationPreferences
)
from .firebase_notifications import send_fcm_notification
from .tasks import call_weather_api_from_task

logger = logging.getLogger(__name__)


class ScheduledNotificationService:
    """
    Service xử lý các scheduled notifications theo lịch trình.
    """
    
    def __init__(self):
        """Khởi tạo Scheduled Notification Service"""
        self.logger = logger
    
    def send_morning_summary(self) -> Dict[str, Any]:
        """
        Gửi tóm tắt thời tiết buổi sáng lúc 7:00 AM.
        
        Yêu cầu:
        - Gửi cho users có bật morning_summary_enabled
        - Nội dung: Thời tiết hôm nay + lời khuyên
        
        Returns:
            Dict với kết quả gửi
        """
        try:
            # Lấy users có bật morning summary
            users_with_prefs = NotificationPreferences.objects.filter(
                morning_summary_enabled=True
            ).select_related('user')
            
            if not users_with_prefs.exists():
                self.logger.info("No users with morning summary enabled")
                return {
                    'success': True,
                    'sent_count': 0,
                    'message': 'No users to notify'
                }
            
            total_sent = 0
            total_failed = 0
            
            for pref in users_with_prefs:
                try:
                    result = self._send_morning_summary_to_user(pref.user)
                    total_sent += result['sent_count']
                    total_failed += result['failed_count']
                except Exception as e:
                    self.logger.error(f"Error sending morning summary to user {pref.user.user_id}: {e}")
                    total_failed += 1
            
            return {
                'success': True,
                'sent_count': total_sent,
                'failed_count': total_failed
            }
            
        except Exception as e:
            self.logger.error(f"Error in send_morning_summary: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
    
    def _send_morning_summary_to_user(self, user: User) -> Dict[str, int]:
        """Gửi morning summary cho một user"""
        try:
            # Lấy locations user đang theo dõi
            tracked_locations = Location.objects.filter(
                users__contains=user.user_id,
                is_active=True
            )
            
            if not tracked_locations.exists():
                return {'sent_count': 0, 'failed_count': 0}
            
            # Lấy device tokens
            tokens = list(DeviceToken.objects.filter(
                user_id=user.user_id,
                is_active=True
            ).values_list('token', flat=True))
            
            if not tokens:
                return {'sent_count': 0, 'failed_count': 0}
            
            # Tạo nội dung summary
            summary_content = self._generate_morning_summary(tracked_locations)
            
            if not summary_content:
                return {'sent_count': 0, 'failed_count': 0}
            
            # Gửi notification
            result = send_fcm_notification(
                device_tokens=tokens,
                title="☀️ Chào buổi sáng! Thời tiết hôm nay",
                body=summary_content,
                data={
                    'type': 'morning_summary',
                    'timestamp': timezone.now().isoformat()
                }
            )
            
            return {
                'sent_count': result['success_count'],
                'failed_count': result['failure_count']
            }
            
        except Exception as e:
            self.logger.error(f"Error sending morning summary to user: {e}")
            return {'sent_count': 0, 'failed_count': 1}
    
    def _generate_morning_summary(self, locations: List[Location]) -> str:
        """Tạo nội dung morning summary"""
        try:
            summaries = []
            
            for loc in locations[:3]:  # Giới hạn 3 locations
                # Lấy thời tiết hôm nay
                weather_data, err = call_weather_api_from_task('forecast', {
                    'q': loc.name_en,
                    'days': 1
                })
                
                if not weather_data or err:
                    continue
                
                current = weather_data.get('current', {})
                forecast_day = weather_data.get('forecast', {}).get('forecastday', [{}])[0].get('day', {})
                
                temp = current.get('temp_c', 0)
                condition = current.get('condition', {}).get('text', '')
                max_temp = forecast_day.get('maxtemp_c', 0)
                min_temp = forecast_day.get('mintemp_c', 0)
                rain_chance = forecast_day.get('daily_chance_of_rain', 0)
                
                summary = f"{loc.name_en}: {temp}°C, {condition}. "
                summary += f"Cao/Thấp: {max_temp}°C/{min_temp}°C. "
                
                if rain_chance > 50:
                    summary += f"Khả năng mưa {rain_chance}%, nhớ mang ô!"
                
                summaries.append(summary)
            
            return " | ".join(summaries) if summaries else ""
            
        except Exception as e:
            self.logger.error(f"Error generating morning summary: {e}")
            return ""
    
    def send_tomorrow_forecast(self) -> Dict[str, Any]:
        """
        Gửi dự báo thời tiết ngày mai lúc 8:00 PM.
        
        Yêu cầu:
        - Gửi cho users có bật tomorrow_forecast_enabled
        - Nội dung: Dự báo chi tiết ngày mai
        
        Returns:
            Dict với kết quả gửi
        """
        try:
            # Lấy users có bật tomorrow forecast
            users_with_prefs = NotificationPreferences.objects.filter(
                tomorrow_forecast_enabled=True
            ).select_related('user')
            
            if not users_with_prefs.exists():
                self.logger.info("No users with tomorrow forecast enabled")
                return {
                    'success': True,
                    'sent_count': 0,
                    'message': 'No users to notify'
                }
            
            total_sent = 0
            total_failed = 0
            
            for pref in users_with_prefs:
                try:
                    result = self._send_tomorrow_forecast_to_user(pref.user)
                    total_sent += result['sent_count']
                    total_failed += result['failed_count']
                except Exception as e:
                    self.logger.error(f"Error sending tomorrow forecast to user {pref.user.user_id}: {e}")
                    total_failed += 1
            
            return {
                'success': True,
                'sent_count': total_sent,
                'failed_count': total_failed
            }
            
        except Exception as e:
            self.logger.error(f"Error in send_tomorrow_forecast: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
    
    def _send_tomorrow_forecast_to_user(self, user: User) -> Dict[str, int]:
        """Gửi tomorrow forecast cho một user"""
        try:
            # Lấy locations user đang theo dõi
            tracked_locations = Location.objects.filter(
                users__contains=user.user_id,
                is_active=True
            )
            
            if not tracked_locations.exists():
                return {'sent_count': 0, 'failed_count': 0}
            
            # Lấy device tokens
            tokens = list(DeviceToken.objects.filter(
                user_id=user.user_id,
                is_active=True
            ).values_list('token', flat=True))
            
            if not tokens:
                return {'sent_count': 0, 'failed_count': 0}
            
            # Tạo nội dung forecast
            forecast_content = self._generate_tomorrow_forecast(tracked_locations)
            
            if not forecast_content:
                return {'sent_count': 0, 'failed_count': 0}
            
            # Gửi notification
            result = send_fcm_notification(
                device_tokens=tokens,
                title="🌙 Dự báo thời tiết ngày mai",
                body=forecast_content,
                data={
                    'type': 'tomorrow_forecast',
                    'timestamp': timezone.now().isoformat()
                }
            )
            
            return {
                'sent_count': result['success_count'],
                'failed_count': result['failure_count']
            }
            
        except Exception as e:
            self.logger.error(f"Error sending tomorrow forecast to user: {e}")
            return {'sent_count': 0, 'failed_count': 1}
    
    def _generate_tomorrow_forecast(self, locations: List[Location]) -> str:
        """Tạo nội dung tomorrow forecast"""
        try:
            forecasts = []
            
            for loc in locations[:3]:  # Giới hạn 3 locations
                # Lấy dự báo 2 ngày
                weather_data, err = call_weather_api_from_task('forecast', {
                    'q': loc.name_en,
                    'days': 2
                })
                
                if not weather_data or err:
                    continue
                
                # Lấy dự báo ngày mai (index 1)
                forecast_days = weather_data.get('forecast', {}).get('forecastday', [])
                if len(forecast_days) < 2:
                    continue
                
                tomorrow = forecast_days[1].get('day', {})
                
                max_temp = tomorrow.get('maxtemp_c', 0)
                min_temp = tomorrow.get('mintemp_c', 0)
                condition = tomorrow.get('condition', {}).get('text', '')
                rain_chance = tomorrow.get('daily_chance_of_rain', 0)
                
                forecast = f"{loc.name_en}: {condition}, {max_temp}°C/{min_temp}°C"
                
                if rain_chance > 50:
                    forecast += f", mưa {rain_chance}%"
                
                forecasts.append(forecast)
            
            return " | ".join(forecasts) if forecasts else ""
            
        except Exception as e:
            self.logger.error(f"Error generating tomorrow forecast: {e}")
            return ""
    
    def send_weekly_summary(self) -> Dict[str, Any]:
        """
        Gửi tóm tắt thời tiết tuần lúc 8:00 PM Chủ nhật.
        
        Yêu cầu:
        - Gửi cho users có bật weekly_summary_enabled
        - Nội dung: Tóm tắt thời tiết 7 ngày tới
        
        Returns:
            Dict với kết quả gửi
        """
        try:
            # Lấy users có bật weekly summary
            users_with_prefs = NotificationPreferences.objects.filter(
                weekly_summary_enabled=True
            ).select_related('user')
            
            if not users_with_prefs.exists():
                self.logger.info("No users with weekly summary enabled")
                return {
                    'success': True,
                    'sent_count': 0,
                    'message': 'No users to notify'
                }
            
            total_sent = 0
            total_failed = 0
            
            for pref in users_with_prefs:
                try:
                    result = self._send_weekly_summary_to_user(pref.user)
                    total_sent += result['sent_count']
                    total_failed += result['failed_count']
                except Exception as e:
                    self.logger.error(f"Error sending weekly summary to user {pref.user.user_id}: {e}")
                    total_failed += 1
            
            return {
                'success': True,
                'sent_count': total_sent,
                'failed_count': total_failed
            }
            
        except Exception as e:
            self.logger.error(f"Error in send_weekly_summary: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
    
    def _send_weekly_summary_to_user(self, user: User) -> Dict[str, int]:
        """Gửi weekly summary cho một user"""
        try:
            # Lấy locations user đang theo dõi
            tracked_locations = Location.objects.filter(
                users__contains=user.user_id,
                is_active=True
            )
            
            if not tracked_locations.exists():
                return {'sent_count': 0, 'failed_count': 0}
            
            # Lấy device tokens
            tokens = list(DeviceToken.objects.filter(
                user_id=user.user_id,
                is_active=True
            ).values_list('token', flat=True))
            
            if not tokens:
                return {'sent_count': 0, 'failed_count': 0}
            
            # Tạo nội dung summary
            summary_content = self._generate_weekly_summary(tracked_locations)
            
            if not summary_content:
                return {'sent_count': 0, 'failed_count': 0}
            
            # Gửi notification
            result = send_fcm_notification(
                device_tokens=tokens,
                title="📅 Tóm tắt thời tiết tuần tới",
                body=summary_content,
                data={
                    'type': 'weekly_summary',
                    'timestamp': timezone.now().isoformat()
                }
            )
            
            return {
                'sent_count': result['success_count'],
                'failed_count': result['failure_count']
            }
            
        except Exception as e:
            self.logger.error(f"Error sending weekly summary to user: {e}")
            return {'sent_count': 0, 'failed_count': 1}
    
    def _generate_weekly_summary(self, locations: List[Location]) -> str:
        """Tạo nội dung weekly summary"""
        try:
            summaries = []
            
            for loc in locations[:2]:  # Giới hạn 2 locations cho weekly
                # Lấy dự báo 7 ngày
                weather_data, err = call_weather_api_from_task('forecast', {
                    'q': loc.name_en,
                    'days': 7
                })
                
                if not weather_data or err:
                    continue
                
                forecast_days = weather_data.get('forecast', {}).get('forecastday', [])
                
                if not forecast_days:
                    continue
                
                # Tính toán thống kê tuần
                temps = [day['day'].get('avgtemp_c', 0) for day in forecast_days]
                rain_days = sum(1 for day in forecast_days if day['day'].get('daily_chance_of_rain', 0) > 50)
                
                avg_temp = sum(temps) / len(temps) if temps else 0
                max_temp = max([day['day'].get('maxtemp_c', 0) for day in forecast_days])
                min_temp = min([day['day'].get('mintemp_c', 0) for day in forecast_days])
                
                summary = f"{loc.name_en}: TB {avg_temp:.0f}°C ({min_temp:.0f}-{max_temp:.0f}°C)"
                
                if rain_days > 0:
                    summary += f", {rain_days} ngày mưa"
                
                summaries.append(summary)
            
            return " | ".join(summaries) if summaries else ""
            
        except Exception as e:
            self.logger.error(f"Error generating weekly summary: {e}")
            return ""
