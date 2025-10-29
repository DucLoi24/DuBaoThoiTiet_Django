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

    # Admin Actions
    path('admin/<str:action>/', views.run_admin_action, name='run_admin_action'),

    path('schema/', SpectacularAPIView.as_view(), name='api_schema'),
    path('docs/', SpectacularSwaggerView.as_view(url_name='api_schema'), name='api_docs'),
]
