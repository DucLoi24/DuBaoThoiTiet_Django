# api/weather_monitor.py
"""
Module giám sát điều kiện thời tiết và phát hiện cảnh báo thiên tai.
Tích hợp với logic pre-processing đã có trong tasks.py.
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from django.utils import timezone
from .models import Location, ExtremeEvent

logger = logging.getLogger(__name__)


class WeatherConditionMonitor:
    """
    Class giám sát điều kiện thời tiết và phát hiện các cảnh báo nguy hiểm.
    Sử dụng logic pre-processing để phân tích dữ liệu thời tiết.
    """
    
    # Ngưỡng cảnh báo
    THRESHOLDS = {
        'flood_risk': {
            'precip_total_extreme': 150,  # mm trong 3 ngày
            'precip_total_high': 100,
            'precip_total_moderate': 50,
            'heavy_rain_hours': 6
        },
        'heavy_rain': {
            'precip_max': 20,  # mm/giờ
            'heavy_rain_hours': 3,
            'high_rain_chance_hours': 4,
            'rain_chance_threshold': 70
        },
        'extreme_heat': {
            'temp_extreme': 39,  # °C
            'temp_high': 35,
            'hot_hours': 4
        },
        'storm': {
            # Phân cấp bão theo chuẩn quốc tế (Saffir-Simpson)
            'super_typhoon': 185,      # >= 185 km/h - Siêu bão (Category 5)
            'typhoon': 118,            # 118-184 km/h - Bão mạnh (Category 3-4)
            'tropical_storm': 63,      # 63-117 km/h - Bão nhiệt đới (Category 1-2)
            'tropical_depression': 39, # 39-62 km/h - Áp thấp nhiệt đới
            'strong_wind': 50,         # 50+ km/h - Gió mạnh
            'moderate_wind': 40,       # 40+ km/h - Gió vừa
            'strong_wind_hours': 3
        },
        'extreme_cold': {
            'temp_extreme': 5,  # °C
            'temp_high': 10,
            'cold_hours': 6
        },
        'uv_index': {
            # Phân cấp UV Index theo WHO
            'extreme': 11,      # 11+ - Cực kỳ nguy hiểm
            'very_high': 8,     # 8-10 - Rất cao
            'high': 6,          # 6-7 - Cao
            'moderate': 3,      # 3-5 - Trung bình
            'low': 0,           # 0-2 - Thấp
            'dangerous_hours': 3  # Số giờ liên tục UV cao
        },
        'air_quality': {
            # Phân cấp AQI theo US EPA
            'hazardous': 301,        # 301+ - Nguy hại (Maroon)
            'very_unhealthy': 201,   # 201-300 - Rất không tốt (Purple)
            'unhealthy': 151,        # 151-200 - Không tốt (Red)
            'unhealthy_sensitive': 101,  # 101-150 - Không tốt cho nhóm nhạy cảm (Orange)
            'moderate': 51,          # 51-100 - Trung bình (Yellow)
            'good': 0                # 0-50 - Tốt (Green)
        }
    }
    
    def __init__(self):
        """Khởi tạo Weather Monitor"""
        self.logger = logger
    
    def evaluate_weather_data(
        self, 
        weather_data: Dict[str, Any], 
        location: Location
    ) -> List[ExtremeEvent]:
        """
        Đánh giá dữ liệu thời tiết và phát hiện các cảnh báo.
        
        Args:
            weather_data: Dữ liệu thời tiết từ API (forecast 3 ngày)
            location: Location object
        
        Returns:
            List các ExtremeEvent đã được tạo trong database
        """
        try:
            # Trích xuất hourly data
            hourly_data = self._extract_hourly_data(weather_data)
            
            if not hourly_data:
                self.logger.warning(f"No hourly data for {location.name_en}")
                return []
            
            # Phân tích và phát hiện cảnh báo
            alerts = self._analyze_conditions(hourly_data, location)
            
            # Lưu vào database
            saved_events = self._save_alerts_to_db(alerts, location)
            
            return saved_events
            
        except Exception as e:
            self.logger.error(f"Error evaluating weather data for {location.name_en}: {e}", exc_info=True)
            return []
    
    def _extract_hourly_data(self, weather_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Trích xuất dữ liệu theo giờ từ forecast data"""
        hourly_data = []
        
        try:
            forecast_days = weather_data.get('forecast', {}).get('forecastday', [])
            
            for day in forecast_days:
                hours = day.get('hour', [])
                hourly_data.extend(hours)
            
            return hourly_data
            
        except Exception as e:
            self.logger.error(f"Error extracting hourly data: {e}")
            return []
    
    def _analyze_conditions(
        self, 
        hourly_data: List[Dict[str, Any]], 
        location: Location
    ) -> List[Dict[str, Any]]:
        """
        Phân tích điều kiện thời tiết và phát hiện cảnh báo.
        Logic giống với pre-processing trong tasks.py.
        """
        alerts = []
        
        try:
            # Thu thập dữ liệu
            temps = [h.get('temp_c', 0) for h in hourly_data if h.get('temp_c') is not None]
            winds = [h.get('wind_kph', 0) for h in hourly_data if h.get('wind_kph') is not None]
            precips = [h.get('precip_mm', 0) for h in hourly_data if h.get('precip_mm') is not None]
            rain_chances = [h.get('chance_of_rain', 0) for h in hourly_data if h.get('chance_of_rain') is not None]
            
            if not temps or not winds or not precips:
                return []
            
            # Tính toán các chỉ số
            temp_min = min(temps)
            temp_max = max(temps)
            wind_max = max(winds)
            precip_total = sum(precips)
            precip_max = max(precips)
            
            heavy_rain_hours = sum(1 for p in precips if p > 10)
            high_rain_chance_hours = sum(1 for r in rain_chances if r > self.THRESHOLDS['heavy_rain']['rain_chance_threshold'])
            hot_hours = sum(1 for t in temps if t > self.THRESHOLDS['extreme_heat']['temp_high'])
            strong_wind_hours = sum(1 for w in winds if w > self.THRESHOLDS['strong_wind']['wind_moderate'])
            cold_hours = sum(1 for t in temps if t < self.THRESHOLDS['extreme_cold']['temp_high'])
            
            # Phát hiện lũ lụt (ưu tiên cao nhất)
            flood_alert = self._check_flood_risk(precip_total, precip_max, heavy_rain_hours)
            if flood_alert:
                alerts.append(flood_alert)
            
            # Phát hiện mưa to (nếu chưa có cảnh báo lũ)
            elif heavy_rain_hours >= self.THRESHOLDS['heavy_rain']['heavy_rain_hours'] or \
                 (precip_max > self.THRESHOLDS['heavy_rain']['precip_max'] and 
                  high_rain_chance_hours >= self.THRESHOLDS['heavy_rain']['high_rain_chance_hours']):
                alerts.append({
                    'severity': 'HIGH',
                    'impact_field': 'heavy_rain',
                    'forecast_details_vi': f'Mưa to dự báo. Lượng mưa tối đa: {precip_max:.1f}mm/giờ. Tổng: {precip_total:.1f}mm.',
                    'actionable_advice_vi': 'Mang theo áo mưa, ô. Hạn chế di chuyển khi mưa lớn. Cẩn thận đường trơn.'
                })
            
            # Phát hiện nắng nóng
            if hot_hours >= self.THRESHOLDS['extreme_heat']['hot_hours']:
                severity = 'EXTREME' if temp_max > self.THRESHOLDS['extreme_heat']['temp_extreme'] else 'HIGH'
                alerts.append({
                    'severity': severity,
                    'impact_field': 'extreme_heat',
                    'forecast_details_vi': f'Nắng nóng gay gắt. Nhiệt độ cao nhất: {temp_max:.1f}°C. Kéo dài {hot_hours} giờ.',
                    'actionable_advice_vi': 'Hạn chế ra ngoài 11h-15h. Uống nhiều nước. Mặc quần áo thoáng mát. Cẩn thận say nắng.'
                })
            
            # Phát hiện bão/gió mạnh với phân cấp chi tiết
            storm_alert = self._check_storm_level(wind_max, strong_wind_hours)
            if storm_alert:
                alerts.append(storm_alert)
            
            # Phát hiện rét đậm
            if cold_hours >= self.THRESHOLDS['extreme_cold']['cold_hours']:
                severity = 'EXTREME' if temp_min < self.THRESHOLDS['extreme_cold']['temp_extreme'] else 'HIGH'
                alerts.append({
                    'severity': severity,
                    'impact_field': 'extreme_cold',
                    'forecast_details_vi': f'Rét đậm. Nhiệt độ thấp nhất: {temp_min:.1f}°C. Kéo dài {cold_hours} giờ.',
                    'actionable_advice_vi': 'Mặc ấm. Cẩn thận với người già, trẻ em. Đề phòng bệnh đường hô hấp.'
                })
            
            # Phát hiện UV Index nguy hiểm
            uv_alert = self._check_uv_index(hourly_data)
            if uv_alert:
                alerts.append(uv_alert)
            
            # Phát hiện chất lượng không khí xấu
            aqi_alert = self._check_air_quality(hourly_data)
            if aqi_alert:
                alerts.append(aqi_alert)
            
            return alerts
            
        except Exception as e:
            self.logger.error(f"Error analyzing conditions: {e}", exc_info=True)
            return []
    
    def _check_flood_risk(
        self, 
        precip_total: float, 
        precip_max: float, 
        heavy_rain_hours: int
    ) -> Optional[Dict[str, Any]]:
        """Kiểm tra nguy cơ lũ lụt"""
        thresholds = self.THRESHOLDS['flood_risk']
        
        if precip_total > thresholds['precip_total_extreme']:
            return {
                'severity': 'EXTREME',
                'impact_field': 'flood_risk',
                'forecast_details_vi': f'NGUY CƠ LŨ LỤT CỰC KỲ CAO! Tổng lượng mưa dự báo: {precip_total:.1f}mm trong 3 ngày tới. Mưa to kéo dài {heavy_rain_hours} giờ.',
                'actionable_advice_vi': 'KHẨN CẤP: Sơ tán ngay nếu ở vùng trũng. Tránh xa sông suối. Theo dõi tin tức địa phương. Chuẩn bị lương thực, nước uống.'
            }
        elif precip_total > thresholds['precip_total_high'] or \
             (heavy_rain_hours >= thresholds['heavy_rain_hours'] and precip_total > thresholds['precip_total_moderate']):
            return {
                'severity': 'HIGH',
                'impact_field': 'flood_risk',
                'forecast_details_vi': f'Nguy cơ lũ lụt cao. Tổng lượng mưa dự báo: {precip_total:.1f}mm trong 3 ngày tới. Mưa to kéo dài {heavy_rain_hours} giờ.',
                'actionable_advice_vi': 'Chuẩn bị sơ tán nếu ở vùng trũng. Tránh đi qua vùng ngập. Theo dõi tin tức địa phương. Di chuyển tài sản lên cao.'
            }
        
        return None
    
    def _check_storm_level(
        self, 
        wind_max: float, 
        strong_wind_hours: int
    ) -> Optional[Dict[str, Any]]:
        """
        Kiểm tra cấp độ bão/gió mạnh theo phân cấp quốc tế.
        
        Phân cấp:
        - Siêu bão (Super Typhoon): >= 185 km/h
        - Bão mạnh (Typhoon): 118-184 km/h
        - Bão nhiệt đới (Tropical Storm): 63-117 km/h
        - Áp thấp nhiệt đới (Tropical Depression): 39-62 km/h
        - Gió mạnh: 50+ km/h
        """
        thresholds = self.THRESHOLDS['storm']
        
        # Siêu bão (Category 5)
        if wind_max >= thresholds['super_typhoon']:
            return {
                'severity': 'EXTREME',
                'impact_field': 'super_typhoon',
                'forecast_details_vi': f'⚠️ SIÊU BÃO CẤP 5 DỰ BÁO! Tốc độ gió tối đa: {wind_max:.1f} km/h. Cực kỳ nguy hiểm!',
                'actionable_advice_vi': 'KHẨN CẤP: Sơ tán ngay lập tức! Tìm nơi trú ẩn kiên cố. Tuyệt đối không ra ngoài. Chuẩn bị lương thực, nước uống, thuốc men. Theo dõi tin tức liên tục.'
            }
        
        # Bão mạnh (Category 3-4)
        elif wind_max >= thresholds['typhoon']:
            return {
                'severity': 'EXTREME',
                'impact_field': 'typhoon',
                'forecast_details_vi': f'🌀 BÃO MẠNH DỰ BÁO! Tốc độ gió tối đa: {wind_max:.1f} km/h. Rất nguy hiểm!',
                'actionable_advice_vi': 'KHẨN CẤP: Gia cố nhà cửa ngay. Chuẩn bị sơ tán. Tránh ra ngoài. Dự trữ lương thực, nước. Cẩn thận cây đổ, mái tôn bay.'
            }
        
        # Bão nhiệt đới (Category 1-2)
        elif wind_max >= thresholds['tropical_storm']:
            return {
                'severity': 'HIGH',
                'impact_field': 'tropical_storm',
                'forecast_details_vi': f'🌀 BÃO NHIỆT ĐỚI dự báo. Tốc độ gió tối đa: {wind_max:.1f} km/h. Kéo dài {strong_wind_hours} giờ.',
                'actionable_advice_vi': 'Gia cố nhà cửa. Hạn chế ra ngoài. Cẩn thận cây đổ, biển hiệu bay. Đóng cửa sổ chặt. Chuẩn bị đèn pin, nước uống.'
            }
        
        # Áp thấp nhiệt đới
        elif wind_max >= thresholds['tropical_depression']:
            return {
                'severity': 'HIGH',
                'impact_field': 'tropical_depression',
                'forecast_details_vi': f'🌪️ ÁP THẤP NHIỆT ĐỚI dự báo. Tốc độ gió tối đa: {wind_max:.1f} km/h. Kéo dài {strong_wind_hours} giờ.',
                'actionable_advice_vi': 'Cẩn thận khi ra ngoài. Gia cố vật dụng dễ bay. Đóng cửa sổ. Chuẩn bị ô, áo mưa. Theo dõi tin tức.'
            }
        
        # Gió mạnh
        elif wind_max >= thresholds['strong_wind'] or \
             strong_wind_hours >= thresholds['strong_wind_hours']:
            return {
                'severity': 'MEDIUM',
                'impact_field': 'strong_wind',
                'forecast_details_vi': f'💨 Gió mạnh dự báo. Tốc độ gió tối đa: {wind_max:.1f} km/h. Kéo dài {strong_wind_hours} giờ.',
                'actionable_advice_vi': 'Cẩn thận khi di chuyển. Tránh đứng dưới cây, biển hiệu. Gia cố vật dụng nhẹ. Đóng cửa sổ.'
            }
        
        return None
    
    def _check_uv_index(self, hourly_data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Kiểm tra chỉ số UV nguy hiểm.
        
        Phân cấp UV Index theo WHO:
        - 11+: Cực kỳ nguy hiểm (Extreme)
        - 8-10: Rất cao (Very High)
        - 6-7: Cao (High)
        - 3-5: Trung bình (Moderate)
        - 0-2: Thấp (Low)
        """
        try:
            # Lấy UV index từ hourly data
            uv_values = [h.get('uv', 0) for h in hourly_data if h.get('uv') is not None]
            
            if not uv_values:
                return None
            
            uv_max = max(uv_values)
            
            # Đếm số giờ có UV nguy hiểm (>= 8)
            dangerous_uv_hours = sum(1 for uv in uv_values if uv >= self.THRESHOLDS['uv_index']['very_high'])
            
            thresholds = self.THRESHOLDS['uv_index']
            
            # UV cực kỳ nguy hiểm (11+)
            if uv_max >= thresholds['extreme']:
                return {
                    'severity': 'HIGH',
                    'impact_field': 'extreme_uv',
                    'forecast_details_vi': f'☀️ CHỈ SỐ UV CỰC KỲ NGUY HIỂM! UV Index: {uv_max:.0f}. Kéo dài {dangerous_uv_hours} giờ.',
                    'actionable_advice_vi': 'CẢNH BÁO: Tránh ra ngoài từ 10h-16h. Bắt buộc dùng kem chống nắng SPF 50+. Mặc áo dài tay, đội mũ rộng vành, đeo kính UV. Nguy cơ cháy nắng cao.'
                }
            
            # UV rất cao (8-10)
            elif uv_max >= thresholds['very_high']:
                return {
                    'severity': 'HIGH',
                    'impact_field': 'very_high_uv',
                    'forecast_details_vi': f'☀️ Chỉ số UV rất cao. UV Index: {uv_max:.0f}. Kéo dài {dangerous_uv_hours} giờ.',
                    'actionable_advice_vi': 'Hạn chế ra ngoài 10h-16h. Dùng kem chống nắng SPF 30+. Mặc áo dài tay, đội mũ, đeo kính. Tìm bóng mát khi ra ngoài.'
                }
            
            # UV cao (6-7)
            elif uv_max >= thresholds['high'] and dangerous_uv_hours >= thresholds['dangerous_hours']:
                return {
                    'severity': 'MEDIUM',
                    'impact_field': 'high_uv',
                    'forecast_details_vi': f'☀️ Chỉ số UV cao. UV Index: {uv_max:.0f}. Cần bảo vệ da khi ra ngoài.',
                    'actionable_advice_vi': 'Dùng kem chống nắng SPF 30. Đội mũ và đeo kính khi ra ngoài lâu. Tìm bóng mát vào giữa trưa.'
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error checking UV index: {e}", exc_info=True)
            return None
    
    def _check_air_quality(self, hourly_data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Kiểm tra chất lượng không khí (AQI).
        
        Phân cấp AQI theo US EPA:
        - 301+: Nguy hại (Hazardous) - Maroon
        - 201-300: Rất không tốt (Very Unhealthy) - Purple
        - 151-200: Không tốt (Unhealthy) - Red
        - 101-150: Không tốt cho nhóm nhạy cảm (Unhealthy for Sensitive Groups) - Orange
        - 51-100: Trung bình (Moderate) - Yellow
        - 0-50: Tốt (Good) - Green
        """
        try:
            # Lấy AQI từ hourly data (WeatherAPI trả về trong air_quality)
            aqi_values = []
            for h in hourly_data:
                air_quality = h.get('air_quality', {})
                # WeatherAPI có thể trả về us-epa-index hoặc gb-defra-index
                aqi = air_quality.get('us-epa-index') or air_quality.get('gb-defra-index')
                if aqi:
                    aqi_values.append(aqi)
            
            if not aqi_values:
                return None
            
            aqi_max = max(aqi_values)
            aqi_avg = sum(aqi_values) / len(aqi_values)
            
            # Đếm số giờ có AQI xấu (>= 101)
            unhealthy_hours = sum(1 for aqi in aqi_values if aqi >= self.THRESHOLDS['air_quality']['unhealthy_sensitive'])
            
            thresholds = self.THRESHOLDS['air_quality']
            
            # AQI nguy hại (301+)
            if aqi_max >= thresholds['hazardous']:
                return {
                    'severity': 'EXTREME',
                    'impact_field': 'hazardous_aqi',
                    'forecast_details_vi': f'🔴 CHẤT LƯỢNG KHÔNG KHÍ NGUY HẠI! AQI: {aqi_max:.0f} (Nguy hại). Kéo dài {unhealthy_hours} giờ.',
                    'actionable_advice_vi': 'KHẨN CẤP: Ở trong nhà, đóng cửa sổ. Dùng máy lọc không khí. Tránh mọi hoạt động ngoài trời. Đeo khẩu trang N95 nếu bắt buộc ra ngoài. Nguy hiểm cho mọi người.'
                }
            
            # AQI rất không tốt (201-300)
            elif aqi_max >= thresholds['very_unhealthy']:
                return {
                    'severity': 'HIGH',
                    'impact_field': 'very_unhealthy_aqi',
                    'forecast_details_vi': f'🟣 Chất lượng không khí rất không tốt. AQI: {aqi_max:.0f} (Rất không tốt). Kéo dài {unhealthy_hours} giờ.',
                    'actionable_advice_vi': 'Hạn chế ra ngoài. Đóng cửa sổ. Dùng máy lọc không khí. Đeo khẩu trang N95 khi ra ngoài. Tránh vận động mạnh. Nguy hiểm cho mọi người.'
                }
            
            # AQI không tốt (151-200)
            elif aqi_max >= thresholds['unhealthy']:
                return {
                    'severity': 'HIGH',
                    'impact_field': 'unhealthy_aqi',
                    'forecast_details_vi': f'🔴 Chất lượng không khí không tốt. AQI: {aqi_max:.0f} (Không tốt). Kéo dài {unhealthy_hours} giờ.',
                    'actionable_advice_vi': 'Hạn chế hoạt động ngoài trời. Đeo khẩu trang khi ra ngoài. Người già, trẻ em, bệnh hô hấp nên ở trong nhà. Đóng cửa sổ.'
                }
            
            # AQI không tốt cho nhóm nhạy cảm (101-150)
            elif aqi_max >= thresholds['unhealthy_sensitive'] and unhealthy_hours >= 3:
                return {
                    'severity': 'MEDIUM',
                    'impact_field': 'unhealthy_sensitive_aqi',
                    'forecast_details_vi': f'🟠 Chất lượng không khí không tốt cho nhóm nhạy cảm. AQI: {aqi_max:.0f}. Trung bình: {aqi_avg:.0f}.',
                    'actionable_advice_vi': 'Người già, trẻ em, bệnh hô hấp/tim mạch nên hạn chế ra ngoài. Đeo khẩu trang khi cần. Tránh vận động mạnh ngoài trời.'
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error checking air quality: {e}", exc_info=True)
            return None
    
    def _save_alerts_to_db(
        self, 
        alerts: List[Dict[str, Any]], 
        location: Location
    ) -> List[ExtremeEvent]:
        """Lưu các cảnh báo vào database"""
        saved_events = []
        
        for alert in alerts:
            try:
                # Kiểm tra xem đã có cảnh báo tương tự trong 6 giờ qua chưa
                six_hours_ago = timezone.now() - timedelta(hours=6)
                existing = ExtremeEvent.objects.filter(
                    location=location,
                    impact_field=alert['impact_field'],
                    analysis_time__gte=six_hours_ago,
                    is_active=True
                ).first()
                
                if existing:
                    # Cập nhật cảnh báo hiện có
                    existing.severity = alert['severity']
                    existing.forecast_details_vi = alert['forecast_details_vi']
                    existing.actionable_advice_vi = alert['actionable_advice_vi']
                    existing.analysis_time = timezone.now()
                    existing.save()
                    saved_events.append(existing)
                    self.logger.info(f"Updated existing alert: {alert['impact_field']} for {location.name_en}")
                else:
                    # Tạo cảnh báo mới
                    event = ExtremeEvent.objects.create(
                        location=location,
                        severity=alert['severity'],
                        impact_field=alert['impact_field'],
                        forecast_details_vi=alert['forecast_details_vi'],
                        actionable_advice_vi=alert['actionable_advice_vi'],
                        raw_llm_json=alert,
                        is_notified=False,
                        is_active=True
                    )
                    saved_events.append(event)
                    self.logger.info(f"Created new alert: {alert['impact_field']} for {location.name_en}")
                    
            except Exception as e:
                self.logger.error(f"Error saving alert to DB: {e}", exc_info=True)
        
        return saved_events
