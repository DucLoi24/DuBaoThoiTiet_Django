from rest_framework import serializers
from .models import ExtremeEvent, NotificationPreferences, LocationNotificationPreferences, NotificationRecord

class ExtremeEventSerializer(serializers.ModelSerializer):
    class Meta:
        model = ExtremeEvent
        # Chọn các trường bạn muốn hiển thị trên app
        fields = [
            'event_id',
            'analysis_time',
            'severity',
            'impact_field',
            'forecast_details_vi',
            'actionable_advice_vi',
            # Bạn có thể bỏ 'location' vì API sẽ lọc theo location rồi
        ]
        read_only_fields = fields # Đảm bảo API chỉ đọc, không ghi


class NotificationPreferencesSerializer(serializers.ModelSerializer):
    """Serializer cho NotificationPreferences"""
    class Meta:
        model = NotificationPreferences
        fields = [
            'preference_id',
            'user',
            'notifications_enabled',
            'enabled_event_types',
            'notification_schedule',
            'morning_summary_enabled',
            'tomorrow_forecast_enabled',
            'weekly_summary_enabled',
            'timezone',
            'created_at',
            'updated_at'
        ]
        read_only_fields = ['preference_id', 'created_at', 'updated_at']
    
    def validate_enabled_event_types(self, value):
        """Validate enabled_event_types là một list"""
        if not isinstance(value, list):
            raise serializers.ValidationError("enabled_event_types phải là một mảng")
        
        # Validate các giá trị hợp lệ
        valid_types = ['heavy_rain', 'storm', 'extreme_heat', 'extreme_cold', 'moderate_rain', 'sunny']
        for event_type in value:
            if event_type not in valid_types:
                raise serializers.ValidationError(f"Loại sự kiện không hợp lệ: {event_type}")
        
        return value
    
    def validate_notification_schedule(self, value):
        """Validate notification_schedule"""
        valid_schedules = ['24_7', 'daytime_only']
        if value not in valid_schedules:
            raise serializers.ValidationError(f"Lịch trình không hợp lệ. Chỉ chấp nhận: {', '.join(valid_schedules)}")
        return value


class LocationNotificationPreferencesSerializer(serializers.ModelSerializer):
    """Serializer cho LocationNotificationPreferences"""
    location_name = serializers.CharField(source='location.name_en', read_only=True)
    
    class Meta:
        model = LocationNotificationPreferences
        fields = [
            'id',
            'user',
            'location',
            'location_name',
            'notifications_enabled',
            'created_at',
            'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at', 'location_name']


class NotificationRecordSerializer(serializers.ModelSerializer):
    """Serializer cho NotificationRecord"""
    location_name = serializers.CharField(source='location.name_en', read_only=True, allow_null=True)
    alert_type = serializers.CharField(source='alert.alert_type', read_only=True, allow_null=True)
    
    class Meta:
        model = NotificationRecord
        fields = [
            'record_id',
            'user',
            'location',
            'location_name',
            'notification_type',
            'alert',
            'alert_type',
            'title',
            'body',
            'priority',
            'sent_at',
            'delivered',
            'fcm_message_id'
        ]
        read_only_fields = fields  # Tất cả các trường đều read-only