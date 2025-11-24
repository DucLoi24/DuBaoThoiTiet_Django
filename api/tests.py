from django.test import TestCase
from django.utils import timezone
from decimal import Decimal

from .models import Location, WeatherAlert
from .weather_monitor import WeatherConditionMonitor


class WeatherConditionMonitorTestCase(TestCase):
    """Test cases for WeatherConditionMonitor"""
    
    def setUp(self):
        """Set up test data"""
        self.monitor = WeatherConditionMonitor()
        self.location = Location.objects.create(
            name_en='Test City',
            latitude=Decimal('10.762622'),
            longitude=Decimal('106.660172'),
            is_active=True
        )
    
    def test_heavy_rain_detection(self):
        """Test that heavy rain > 50mm/h is detected"""
        weather_data = {
            'current': {
                'temp_c': 25.0,
                'wind_kph': 20.0,
                'precip_mm': 55.0  # Above threshold
            }
        }
        
        alerts = self.monitor.evaluate_weather_data(weather_data, self.location)
        
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, 'heavy_rain')
        self.assertEqual(alerts[0].severity, 'high')
        self.assertTrue(alerts[0].is_active)
    
    def test_storm_detection(self):
        """Test that storm with wind > 60km/h is detected"""
        weather_data = {
            'current': {
                'temp_c': 25.0,
                'wind_kph': 65.0,  # Above threshold
                'precip_mm': 10.0
            }
        }
        
        alerts = self.monitor.evaluate_weather_data(weather_data, self.location)
        
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, 'storm')
        self.assertEqual(alerts[0].severity, 'high')
    
    def test_extreme_heat_detection(self):
        """Test that extreme heat > 38°C is detected"""
        weather_data = {
            'current': {
                'temp_c': 40.0,  # Above threshold
                'wind_kph': 20.0,
                'precip_mm': 0.0
            }
        }
        
        alerts = self.monitor.evaluate_weather_data(weather_data, self.location)
        
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, 'extreme_heat')
        self.assertEqual(alerts[0].severity, 'high')
    
    def test_extreme_cold_detection(self):
        """Test that extreme cold < 5°C is detected"""
        weather_data = {
            'current': {
                'temp_c': 3.0,  # Below threshold
                'wind_kph': 20.0,
                'precip_mm': 0.0
            }
        }
        
        alerts = self.monitor.evaluate_weather_data(weather_data, self.location)
        
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, 'extreme_cold')
        self.assertEqual(alerts[0].severity, 'high')
    
    def test_multiple_conditions_aggregation(self):
        """Test that multiple dangerous conditions are aggregated into one comprehensive alert"""
        weather_data = {
            'current': {
                'temp_c': 40.0,  # Extreme heat
                'wind_kph': 65.0,  # Storm
                'precip_mm': 55.0  # Heavy rain
            }
        }
        
        alerts = self.monitor.evaluate_weather_data(weather_data, self.location)
        
        # Should return one aggregated alert
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, 'multiple_conditions')
        self.assertEqual(alerts[0].severity, 'high')
        self.assertIn('Đa điều kiện', alerts[0].title_vi)
        
        # Verify individual alerts are marked as resolved
        individual_alerts = WeatherAlert.objects.filter(
            location=self.location,
            alert_type__in=['heavy_rain', 'storm', 'extreme_heat'],
            is_active=False
        )
        self.assertEqual(individual_alerts.count(), 3)
    
    def test_no_dangerous_conditions(self):
        """Test that normal weather doesn't trigger alerts"""
        weather_data = {
            'current': {
                'temp_c': 25.0,  # Normal
                'wind_kph': 20.0,  # Normal
                'precip_mm': 10.0  # Normal
            }
        }
        
        alerts = self.monitor.evaluate_weather_data(weather_data, self.location)
        
        self.assertEqual(len(alerts), 0)
    
    def test_alert_record_creation(self):
        """Test that alert records are properly created in database"""
        weather_data = {
            'current': {
                'temp_c': 40.0,
                'wind_kph': 20.0,
                'precip_mm': 0.0
            }
        }
        
        alerts = self.monitor.evaluate_weather_data(weather_data, self.location)
        
        # Verify alert is in database
        db_alert = WeatherAlert.objects.filter(
            location=self.location,
            alert_type='extreme_heat'
        ).first()
        
        self.assertIsNotNone(db_alert)
        self.assertEqual(db_alert.severity, 'high')
        self.assertTrue(db_alert.is_active)
        self.assertIn('Nắng Nóng', db_alert.title_vi)
    
    def test_duplicate_alert_handling(self):
        """Test that duplicate alerts are handled correctly"""
        weather_data = {
            'current': {
                'temp_c': 40.0,
                'wind_kph': 20.0,
                'precip_mm': 0.0
            }
        }
        
        # Create first alert
        alerts1 = self.monitor.evaluate_weather_data(weather_data, self.location)
        self.assertEqual(len(alerts1), 1)
        
        # Try to create same alert again
        alerts2 = self.monitor.evaluate_weather_data(weather_data, self.location)
        self.assertEqual(len(alerts2), 1)
        
        # Should only have one alert in database
        alert_count = WeatherAlert.objects.filter(
            location=self.location,
            alert_type='extreme_heat',
            is_active=True
        ).count()
        
        self.assertEqual(alert_count, 1)

    def test_alert_resolution_when_condition_ends(self):
        """Test that alerts are resolved when weather conditions return to normal"""
        # First, create an alert with extreme heat
        weather_data_hot = {
            'current': {
                'temp_c': 40.0,  # Extreme heat
                'wind_kph': 20.0,
                'precip_mm': 0.0
            }
        }
        
        alerts = self.monitor.evaluate_weather_data(weather_data_hot, self.location)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, 'extreme_heat')
        self.assertTrue(alerts[0].is_active)
        
        # Now weather returns to normal
        weather_data_normal = {
            'current': {
                'temp_c': 25.0,  # Normal temperature
                'wind_kph': 20.0,
                'precip_mm': 0.0
            }
        }
        
        alerts_normal = self.monitor.evaluate_weather_data(weather_data_normal, self.location)
        self.assertEqual(len(alerts_normal), 0)
        
        # Verify the alert is marked as resolved
        resolved_alert = WeatherAlert.objects.filter(
            location=self.location,
            alert_type='extreme_heat'
        ).first()
        
        self.assertIsNotNone(resolved_alert)
        self.assertFalse(resolved_alert.is_active)
        self.assertIsNotNone(resolved_alert.resolved_at)
    
    def test_aggregated_alert_resolution(self):
        """Test that aggregated alerts are resolved when conditions reduce"""
        # Create multiple conditions
        weather_data_multiple = {
            'current': {
                'temp_c': 40.0,  # Extreme heat
                'wind_kph': 65.0,  # Storm
                'precip_mm': 0.0
            }
        }
        
        alerts = self.monitor.evaluate_weather_data(weather_data_multiple, self.location)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].alert_type, 'multiple_conditions')
        
        # Now only one condition remains
        weather_data_single = {
            'current': {
                'temp_c': 40.0,  # Still extreme heat
                'wind_kph': 20.0,  # Normal wind
                'precip_mm': 0.0
            }
        }
        
        alerts_single = self.monitor.evaluate_weather_data(weather_data_single, self.location)
        self.assertEqual(len(alerts_single), 1)
        self.assertEqual(alerts_single[0].alert_type, 'extreme_heat')
        
        # Verify aggregated alert is resolved
        aggregated_alert = WeatherAlert.objects.filter(
            location=self.location,
            alert_type='multiple_conditions'
        ).first()
        
        self.assertIsNotNone(aggregated_alert)
        self.assertFalse(aggregated_alert.is_active)
        self.assertIsNotNone(aggregated_alert.resolved_at)
    
    def test_alert_resolution_notification_data(self):
        """Test that resolved alerts contain proper resolution timestamp"""
        # Create alert
        weather_data_alert = {
            'current': {
                'temp_c': 3.0,  # Extreme cold
                'wind_kph': 20.0,
                'precip_mm': 0.0
            }
        }
        
        alerts = self.monitor.evaluate_weather_data(weather_data_alert, self.location)
        self.assertEqual(len(alerts), 1)
        
        alert_id = alerts[0].alert_id
        
        # Resolve alert
        weather_data_normal = {
            'current': {
                'temp_c': 25.0,
                'wind_kph': 20.0,
                'precip_mm': 0.0
            }
        }
        
        self.monitor.evaluate_weather_data(weather_data_normal, self.location)
        
        # Check resolved alert has timestamp
        resolved_alert = WeatherAlert.objects.get(alert_id=alert_id)
        self.assertFalse(resolved_alert.is_active)
        self.assertIsNotNone(resolved_alert.resolved_at)
        self.assertIsNotNone(resolved_alert.detected_at)
        # Resolved time should be after detected time
        self.assertGreater(resolved_alert.resolved_at, resolved_alert.detected_at)
