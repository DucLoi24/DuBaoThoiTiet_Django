# api/models.py
from django.db import models
from django.utils import timezone # Sử dụng timezone của Django

# Đảm bảo tương thích JSONField
try:
    from django.db.models import JSONField
except ImportError:
    from django.contrib.postgres.fields import JSONField

class User(models.Model):
    user_id = models.BigAutoField(primary_key=True)
    username = models.CharField(max_length=50, unique=True, null=False)
    password_hash = models.TextField(null=False)
    # Thay auto_now_add=True bằng default=timezone.now để hoạt động tốt hơn với tests
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = '"Users"' # Giữ nguyên dấu ngoặc kép cho PostgreSQL

class Location(models.Model):
    location_id = models.BigAutoField(primary_key=True)
    name_en = models.CharField(max_length=100, unique=True, null=False)
    latitude = models.DecimalField(max_digits=10, decimal_places=6, null=False)
    longitude = models.DecimalField(max_digits=10, decimal_places=6, null=False)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(default=timezone.now)
    # Cho phép users là null hoặc rỗng
    users = JSONField(default=list, blank=True, null=True)
    # Lưu trạng thái thời tiết hiện tại để so sánh
    last_weather_condition = models.CharField(max_length=100, null=True, blank=True)
    last_weather_check = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = '"Locations"'

class DeviceToken(models.Model):
    """Lưu FCM device tokens của users"""
    token_id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='device_tokens')
    token = models.CharField(max_length=255, unique=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = '"DeviceTokens"'
        indexes = [
            models.Index(fields=['user', 'is_active']),
        ]

class WeatherData(models.Model):
    weather_data_id = models.BigAutoField(primary_key=True)
    # Thêm related_name để truy vấn ngược dễ dàng
    location = models.ForeignKey(Location, on_delete=models.CASCADE, related_name='weather_data')
    # Sử dụng DateField nếu chỉ lưu ngày, DateTimeField nếu lưu cả giờ
    record_time = models.DateTimeField(null=False)
    data_type = models.CharField(max_length=20, null=False) # 'HISTORY' or 'FORECAST'
    temp_c = models.DecimalField(max_digits=4, decimal_places=2, null=True)
    humidity = models.IntegerField(null=True)
    uv_index = models.DecimalField(max_digits=3, decimal_places=1, null=True)
    wind_kph = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    raw_json = JSONField(null=True)

    class Meta:
        db_table = '"WeatherData"'
        unique_together = ('location', 'record_time')
        indexes = [ models.Index(fields=['record_time']), ] # Index cho record_time

class ExtremeEvent(models.Model):
    event_id = models.BigAutoField(primary_key=True)
    location = models.ForeignKey(Location, on_delete=models.CASCADE, related_name='extreme_events')
    analysis_time = models.DateTimeField(default=timezone.now, null=False)
    severity = models.CharField(max_length=20, null=False)
    impact_field = models.CharField(max_length=50, null=False)
    forecast_details_vi = models.TextField(null=False)
    actionable_advice_vi = models.TextField(null=True) # Cột lời khuyên
    is_active = models.BooleanField(default=True)
    is_notified = models.BooleanField(default=False)
    raw_llm_json = JSONField(null=True)

    class Meta:
        db_table = '"ExtremeEvents"'
        indexes = [ models.Index(fields=['location']), ] # Index cho location

class AdviceCache(models.Model):
    advice_id = models.BigAutoField(primary_key=True)
    location = models.ForeignKey(Location, on_delete=models.CASCADE, related_name='advice_cache')
    generated_time = models.DateTimeField(default=timezone.now)
    advice_type = models.CharField(max_length=10) # 'advice' or 'warning'
    message_vi = models.TextField()
    # Thêm unique constraint để đảm bảo mỗi location chỉ có 1 bản ghi cache gần nhất?
    # Hoặc đơn giản là luôn lấy bản ghi mới nhất theo generated_time

    class Meta:
        db_table = '"AdviceCache"'
        indexes = [
            models.Index(fields=['location', '-generated_time']), # Index để lấy bản ghi mới nhất nhanh
        ]
        ordering = ['-generated_time']


class NotificationPreferences(models.Model):
    """Lưu trữ preferences thông báo của user"""
    preference_id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='notification_preferences')
    
    # Global notification toggle
    notifications_enabled = models.BooleanField(default=True)
    
    # Event type preferences (JSON array of enabled types)
    enabled_event_types = JSONField(default=list)  # ['heavy_rain', 'storm', 'extreme_heat', ...]
    
    # Schedule preferences
    notification_schedule = models.CharField(max_length=20, default='24_7')  # '24_7' or 'daytime_only'
    
    # Scheduled notification preferences
    morning_summary_enabled = models.BooleanField(default=True)
    tomorrow_forecast_enabled = models.BooleanField(default=True)
    weekly_summary_enabled = models.BooleanField(default=False)
    
    # Timezone
    timezone = models.CharField(max_length=50, default='Asia/Ho_Chi_Minh')
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'NotificationPreferences'


class LocationNotificationPreferences(models.Model):
    """Preferences thông báo cho từng location cụ thể"""
    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='location_notification_preferences')
    location = models.ForeignKey(Location, on_delete=models.CASCADE, related_name='notification_preferences')
    
    notifications_enabled = models.BooleanField(default=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'LocationNotificationPreferences'
        unique_together = ('user', 'location')


class WeatherAlert(models.Model):
    """Lưu trữ các cảnh báo thời tiết được phát hiện"""
    alert_id = models.BigAutoField(primary_key=True)
    location = models.ForeignKey(Location, on_delete=models.CASCADE, related_name='weather_alerts')
    
    alert_type = models.CharField(max_length=50)  # 'heavy_rain', 'storm', 'extreme_heat', 'extreme_cold'
    severity = models.CharField(max_length=20)  # 'high', 'medium', 'low'
    
    detected_at = models.DateTimeField(auto_now_add=True)
    resolved_at = models.DateTimeField(null=True, blank=True)
    is_active = models.BooleanField(default=True)
    
    # Alert details
    title_vi = models.CharField(max_length=200)
    message_vi = models.TextField()
    recommended_actions = models.TextField(null=True, blank=True)
    
    # Weather data at time of alert
    weather_data = JSONField()
    
    class Meta:
        db_table = 'WeatherAlerts'
        indexes = [
            models.Index(fields=['location', '-detected_at']),
            models.Index(fields=['is_active']),
        ]


class NotificationRecord(models.Model):
    """Lịch sử các thông báo đã gửi"""
    record_id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='notification_records')
    location = models.ForeignKey(Location, on_delete=models.SET_NULL, null=True, blank=True, related_name='notification_records')
    
    notification_type = models.CharField(max_length=50)  # 'alert', 'morning_summary', 'tomorrow_forecast', 'weekly_summary'
    alert = models.ForeignKey(WeatherAlert, on_delete=models.SET_NULL, null=True, blank=True, related_name='notification_records')
    
    title = models.CharField(max_length=200)
    body = models.TextField()
    priority = models.CharField(max_length=20)  # 'high', 'medium', 'low'
    
    sent_at = models.DateTimeField(auto_now_add=True)
    delivered = models.BooleanField(default=False)
    
    # FCM response
    fcm_message_id = models.CharField(max_length=255, null=True, blank=True)
    
    class Meta:
        db_table = 'NotificationRecords'
        indexes = [
            models.Index(fields=['user', '-sent_at']),
            models.Index(fields=['notification_type']),
        ]


class QueuedNotification(models.Model):
    """Thông báo được xếp hàng để gửi sau"""
    queue_id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='queued_notifications')
    location = models.ForeignKey(Location, on_delete=models.CASCADE, null=True, blank=True, related_name='queued_notifications')
    
    notification_type = models.CharField(max_length=50)
    title = models.CharField(max_length=200)
    body = models.TextField()
    priority = models.CharField(max_length=20)
    data = JSONField(default=dict)
    
    scheduled_for = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    sent = models.BooleanField(default=False)
    
    class Meta:
        db_table = 'QueuedNotifications'
        indexes = [
            models.Index(fields=['scheduled_for', 'sent']),
        ]


class PreferenceAuditLog(models.Model):
    """Audit log cho các thay đổi notification preferences"""
    log_id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='preference_audit_logs')
    
    # Loại preference được thay đổi
    preference_type = models.CharField(max_length=50)  # 'global' hoặc 'location'
    location = models.ForeignKey(Location, on_delete=models.SET_NULL, null=True, blank=True, related_name='preference_audit_logs')
    
    # Thông tin thay đổi
    field_name = models.CharField(max_length=100)  # Tên field được thay đổi
    old_value = JSONField(null=True, blank=True)  # Giá trị cũ
    new_value = JSONField(null=True, blank=True)  # Giá trị mới
    
    # Metadata
    changed_at = models.DateTimeField(auto_now_add=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    user_agent = models.TextField(null=True, blank=True)
    
    class Meta:
        db_table = 'PreferenceAuditLogs'
        indexes = [
            models.Index(fields=['user', '-changed_at']),
            models.Index(fields=['preference_type']),
        ]
        ordering = ['-changed_at']