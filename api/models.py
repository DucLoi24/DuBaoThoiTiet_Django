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

    class Meta:
        db_table = '"Locations"'

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