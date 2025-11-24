# api/preference_manager.py
"""
User Preference Manager - Quản lý preferences thông báo của người dùng
"""
from typing import List, Dict, Optional
from django.db import transaction
from django.core.exceptions import ObjectDoesNotExist
from .models import (
    User, 
    Location, 
    NotificationPreferences, 
    LocationNotificationPreferences
)
from .audit_logger import PreferenceAuditLogger


class UserPreferenceManager:
    """
    Trình quản lý preferences thông báo của người dùng.
    Xử lý việc lấy, cập nhật và khởi tạo preferences.
    """
    
    # Default values cho preferences mới
    DEFAULT_ENABLED_EVENT_TYPES = [
        'heavy_rain',
        'storm', 
        'extreme_heat',
        'extreme_cold',
        'moderate_rain',
        'sunny'
    ]
    DEFAULT_NOTIFICATION_SCHEDULE = '24_7'
    DEFAULT_TIMEZONE = 'Asia/Ho_Chi_Minh'
    
    def get_user_preferences(self, user_id: int) -> NotificationPreferences:
        """
        Lấy preferences của user. Nếu chưa có, tạo mới với giá trị mặc định.
        
        Args:
            user_id: ID của user
            
        Returns:
            NotificationPreferences object
            
        Raises:
            ObjectDoesNotExist: Nếu user không tồn tại
        """
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            raise ObjectDoesNotExist(f"User with ID {user_id} does not exist")
        
        # Lấy hoặc tạo preferences với giá trị mặc định
        preferences, created = NotificationPreferences.objects.get_or_create(
            user=user,
            defaults={
                'enabled_event_types': self.DEFAULT_ENABLED_EVENT_TYPES.copy(),
                'notification_schedule': self.DEFAULT_NOTIFICATION_SCHEDULE,
                'morning_summary_enabled': True,
                'tomorrow_forecast_enabled': True,
                'weekly_summary_enabled': False,
                'timezone': self.DEFAULT_TIMEZONE
            }
        )
        
        return preferences
    
    def update_preferences(self, user_id: int, preferences_data: Dict, 
                          request=None) -> NotificationPreferences:
        """
        Cập nhật preferences của user với audit logging.
        
        Args:
            user_id: ID của user
            preferences_data: Dictionary chứa các giá trị cần cập nhật
                Có thể bao gồm:
                - enabled_event_types: List[str]
                - notification_schedule: str ('24_7' hoặc 'daytime_only')
                - morning_summary_enabled: bool
                - tomorrow_forecast_enabled: bool
                - weekly_summary_enabled: bool
                - timezone: str
            request: Django request object (optional, để lấy IP và user agent)
                
        Returns:
            NotificationPreferences object đã được cập nhật
            
        Raises:
            ObjectDoesNotExist: Nếu user không tồn tại
        """
        # Lấy preferences hiện tại (hoặc tạo mới nếu chưa có)
        preferences = self.get_user_preferences(user_id)
        
        # Lưu giá trị cũ để audit logging
        changes = {}
        
        # Lấy IP và user agent từ request nếu có
        ip_address = None
        user_agent = None
        if request:
            ip_address = PreferenceAuditLogger.get_client_ip(request)
            user_agent = PreferenceAuditLogger.get_user_agent(request)
        
        # Cập nhật các trường được cung cấp và track changes
        if 'notifications_enabled' in preferences_data:
            old_value = preferences.notifications_enabled
            new_value = preferences_data['notifications_enabled']
            if old_value != new_value:
                changes['notifications_enabled'] = (old_value, new_value)
                preferences.notifications_enabled = new_value
        
        if 'enabled_event_types' in preferences_data:
            old_value = preferences.enabled_event_types.copy() if preferences.enabled_event_types else []
            new_value = preferences_data['enabled_event_types']
            if old_value != new_value:
                changes['enabled_event_types'] = (old_value, new_value)
                preferences.enabled_event_types = new_value
        
        if 'notification_schedule' in preferences_data:
            old_value = preferences.notification_schedule
            new_value = preferences_data['notification_schedule']
            if old_value != new_value:
                changes['notification_schedule'] = (old_value, new_value)
                preferences.notification_schedule = new_value
        
        if 'morning_summary_enabled' in preferences_data:
            old_value = preferences.morning_summary_enabled
            new_value = preferences_data['morning_summary_enabled']
            if old_value != new_value:
                changes['morning_summary_enabled'] = (old_value, new_value)
                preferences.morning_summary_enabled = new_value
        
        if 'tomorrow_forecast_enabled' in preferences_data:
            old_value = preferences.tomorrow_forecast_enabled
            new_value = preferences_data['tomorrow_forecast_enabled']
            if old_value != new_value:
                changes['tomorrow_forecast_enabled'] = (old_value, new_value)
                preferences.tomorrow_forecast_enabled = new_value
        
        if 'weekly_summary_enabled' in preferences_data:
            old_value = preferences.weekly_summary_enabled
            new_value = preferences_data['weekly_summary_enabled']
            if old_value != new_value:
                changes['weekly_summary_enabled'] = (old_value, new_value)
                preferences.weekly_summary_enabled = new_value
        
        if 'timezone' in preferences_data:
            old_value = preferences.timezone
            new_value = preferences_data['timezone']
            if old_value != new_value:
                changes['timezone'] = (old_value, new_value)
                preferences.timezone = new_value
        
        # Lưu vào database
        preferences.save()
        
        # Log tất cả các thay đổi
        if changes:
            PreferenceAuditLogger.log_multiple_changes(
                user_id=user_id,
                changes=changes,
                preference_type='global',
                ip_address=ip_address,
                user_agent=user_agent
            )
        
        return preferences
    
    def get_enabled_event_types(self, user_id: int) -> List[str]:
        """
        Lấy danh sách các loại sự kiện thời tiết được bật cho user.
        
        Args:
            user_id: ID của user
            
        Returns:
            List các loại sự kiện được bật (ví dụ: ['heavy_rain', 'storm'])
        """
        preferences = self.get_user_preferences(user_id)
        return preferences.enabled_event_types if preferences.enabled_event_types else []
    
    def get_notification_schedule(self, user_id: int) -> str:
        """
        Lấy lịch trình thông báo của user.
        
        Args:
            user_id: ID của user
            
        Returns:
            Lịch trình thông báo: '24_7' hoặc 'daytime_only'
        """
        preferences = self.get_user_preferences(user_id)
        return preferences.notification_schedule
    
    def get_location_preferences(self, user_id: int, location_id: int) -> Dict:
        """
        Lấy preferences thông báo cho một vị trí cụ thể.
        Nếu chưa có, tạo mới với giá trị mặc định (notifications_enabled=True).
        
        Args:
            user_id: ID của user
            location_id: ID của location
            
        Returns:
            Dictionary chứa thông tin preferences cho location:
            {
                'location_id': int,
                'notifications_enabled': bool,
                'created_at': datetime,
                'updated_at': datetime
            }
            
        Raises:
            ObjectDoesNotExist: Nếu user hoặc location không tồn tại
        """
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            raise ObjectDoesNotExist(f"User with ID {user_id} does not exist")
        
        try:
            location = Location.objects.get(location_id=location_id)
        except Location.DoesNotExist:
            raise ObjectDoesNotExist(f"Location with ID {location_id} does not exist")
        
        # Lấy hoặc tạo location preferences với giá trị mặc định
        location_pref, created = LocationNotificationPreferences.objects.get_or_create(
            user=user,
            location=location,
            defaults={
                'notifications_enabled': True
            }
        )
        
        return {
            'location_id': location_pref.location.location_id,
            'notifications_enabled': location_pref.notifications_enabled,
            'created_at': location_pref.created_at,
            'updated_at': location_pref.updated_at
        }
    
    def update_location_preferences(self, user_id: int, location_id: int, 
                                   notifications_enabled: bool, request=None) -> Dict:
        """
        Cập nhật preferences thông báo cho một vị trí cụ thể với audit logging.
        
        Args:
            user_id: ID của user
            location_id: ID của location
            notifications_enabled: Bật/tắt thông báo cho location này
            request: Django request object (optional, để lấy IP và user agent)
            
        Returns:
            Dictionary chứa thông tin preferences đã cập nhật
            
        Raises:
            ObjectDoesNotExist: Nếu user hoặc location không tồn tại
        """
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            raise ObjectDoesNotExist(f"User with ID {user_id} does not exist")
        
        try:
            location = Location.objects.get(location_id=location_id)
        except Location.DoesNotExist:
            raise ObjectDoesNotExist(f"Location with ID {location_id} does not exist")
        
        # Lấy IP và user agent từ request nếu có
        ip_address = None
        user_agent = None
        if request:
            ip_address = PreferenceAuditLogger.get_client_ip(request)
            user_agent = PreferenceAuditLogger.get_user_agent(request)
        
        # Lấy hoặc tạo location preferences
        location_pref, created = LocationNotificationPreferences.objects.get_or_create(
            user=user,
            location=location,
            defaults={
                'notifications_enabled': notifications_enabled
            }
        )
        
        # Nếu đã tồn tại, cập nhật giá trị và log thay đổi
        if not created:
            old_value = location_pref.notifications_enabled
            if old_value != notifications_enabled:
                # Log thay đổi
                PreferenceAuditLogger.log_preference_change(
                    user_id=user_id,
                    field_name='notifications_enabled',
                    old_value=old_value,
                    new_value=notifications_enabled,
                    preference_type='location',
                    location_id=location_id,
                    ip_address=ip_address,
                    user_agent=user_agent
                )
            location_pref.notifications_enabled = notifications_enabled
            location_pref.save()
        else:
            # Nếu mới tạo, cũng log (old_value = None)
            PreferenceAuditLogger.log_preference_change(
                user_id=user_id,
                field_name='notifications_enabled',
                old_value=None,
                new_value=notifications_enabled,
                preference_type='location',
                location_id=location_id,
                ip_address=ip_address,
                user_agent=user_agent
            )
        
        return {
            'location_id': location_pref.location.location_id,
            'notifications_enabled': location_pref.notifications_enabled,
            'created_at': location_pref.created_at,
            'updated_at': location_pref.updated_at
        }
    
    def get_all_location_preferences(self, user_id: int) -> List[Dict]:
        """
        Lấy tất cả location preferences của user.
        
        Args:
            user_id: ID của user
            
        Returns:
            List các dictionary chứa thông tin preferences cho từng location
        """
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            raise ObjectDoesNotExist(f"User with ID {user_id} does not exist")
        
        location_prefs = LocationNotificationPreferences.objects.filter(user=user)
        
        return [
            {
                'location_id': pref.location.location_id,
                'location_name': pref.location.name_en,
                'notifications_enabled': pref.notifications_enabled,
                'created_at': pref.created_at,
                'updated_at': pref.updated_at
            }
            for pref in location_prefs
        ]
    
    def delete_location_preferences(self, user_id: int, location_id: int) -> bool:
        """
        Xóa preferences cho một location cụ thể.
        
        Args:
            user_id: ID của user
            location_id: ID của location
            
        Returns:
            True nếu xóa thành công, False nếu không tìm thấy
        """
        try:
            user = User.objects.get(user_id=user_id)
            location = Location.objects.get(location_id=location_id)
            
            deleted_count, _ = LocationNotificationPreferences.objects.filter(
                user=user,
                location=location
            ).delete()
            
            return deleted_count > 0
        except (User.DoesNotExist, Location.DoesNotExist):
            return False
