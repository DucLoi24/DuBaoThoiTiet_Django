# api/urls.py
from django.urls import path
from . import views
from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView

urlpatterns = [
    # Root (optional, good for testing)
    path('', views.root_view, name='api_root'),

    # Auth
    path('register/', views.register_user, name='register_user'),
    path('login/', views.login_user, name='login_user'),

    # Weather Data
    path('weather/', views.get_weather, name='get_weather'),
    # Thêm path cho search, history, alerts nếu bạn cần API công khai cho chúng
    # path('search/', views.search_location, name='search_location'),
    # path('history/', views.get_history, name='get_history'),
    # path('alerts/', views.get_alerts, name='get_alerts'),

    # Alerts for a specific location
    path('alerts/', views.get_alerts_for_location, name='get_alerts_for_location'),
    path('advice/', views.get_ai_advice, name='get_ai_advice'),
    path('check-advice/', views.check_recent_advice, name='check_recent_advice'),

    # Tracking
    path('locations/track/', views.track_location, name='track_location'),
    path('locations/tracked/', views.get_tracked_locations, name='get_tracked_locations'),
    path('locations/delete/', views.delete_tracked_location, name='delete_tracked_location'),
    
    # Device Token
    path('device-token/register/', views.register_device_token, name='register_device_token'),
    
    # Notification Preferences
    path('notifications/preferences/', views.notification_preferences, name='notification_preferences'),
    path('notifications/preferences/location/<int:location_id>/', views.location_notification_preferences, name='location_notification_preferences'),
    
    # Notification History
    path('notifications/history/', views.notification_history, name='notification_history'),
    path('notifications/history/<int:record_id>/', views.notification_history_detail, name='notification_history_detail'),
    
    # Preference Audit Logs
    path('notifications/preferences/audit-logs/', views.preference_audit_logs, name='preference_audit_logs'),
    
    # Test
    path('test-notification/', views.test_notification, name='test_notification'),

    # Admin Actions
    path('admin/<str:action>/', views.run_admin_action, name='run_admin_action'),

    path('schema/', SpectacularAPIView.as_view(), name='api_schema'),
    path('docs/', SpectacularSwaggerView.as_view(url_name='api_schema'), name='api_docs'),
]
