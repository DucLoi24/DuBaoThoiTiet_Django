# weather_project/urls.py
from django.contrib import admin
from django.urls import path, include
from api import views # <-- THÊM DÒNG NÀY

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include('api.urls')),
    path('', views.root_view, name='project_root'), # <-- THÊM DÒNG NÀY
]