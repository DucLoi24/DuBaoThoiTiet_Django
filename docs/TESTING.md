# Testing Documentation - Backend

## Tổng Quan

Hệ thống backend Weather Forecast sử dụng **pytest** và **Django TestCase** để kiểm thử. Test suite bao gồm unit tests, integration tests và demo scripts để đảm bảo chất lượng và độ tin cậy của hệ thống.

## Testing Strategy

### 1. Unit Testing
- Test các components riêng lẻ (models, services, managers)
- Mock external dependencies (Firebase, Weather API)
- Focus vào logic nghiệp vụ cốt lõi

### 2. Integration Testing
- Test luồng hoàn chỉnh từ đầu đến cuối
- Test tích hợp giữa các components
- Verify data flow và state transitions

### 3. Demo Scripts
- Scripts để demo và verify chức năng thực tế
- Hữu ích cho manual testing và debugging

## Test Coverage

### Tổng Số Test Files: 16

| Category | Test Files | Test Cases |
|----------|-----------|------------|
| Alert Management | 1 | 3 scenarios |
| Audit Logging | 1 | 12 tests |
| Device Token Management | 1 | 15 tests |
| Notification History | 2 | 25+ tests |
| Notification Integration | 1 | 1 integration test |
| Notification Preferences API | 1 | 15+ tests |
| Notification Service | 1 | 7 tests |
| Preference Manager | 1 | 20+ tests |
| Push Notification | 1 | Demo script |
| Schedule Integration | 1 | 5 tests |
| Schedule Logic | 1 | 7 tests |
| Scheduled Notifications | 1 | Demo script |
| Scheduler Jobs | 1 | 3 job tests |
| Scheduler Setup | 1 | Setup verification |
| Weather Monitoring Integration | 1 | 6 integration tests |

## Test Environment Setup

### Prerequisites
```bash
# Activate virtual environment
cd weather_project
source .venv/bin/activate  # Linux/Mac
# hoặc
.venv\Scripts\activate  # Windows

# Install test dependencies
pip install pytest pytest-django
```

### Running Tests

```bash
# Chạy tất cả tests
pytest test_unit/

# Chạy một test file cụ thể
pytest test_unit/test_notification_service.py

# Chạy với verbose output
pytest test_unit/ -v

# Chạy với coverage report
pytest test_unit/ --cov=api --cov-report=html
```

## Detailed Test Cases

### 1. Alert Aggregation Tests
**File:** `test_alert_aggregation.py`

**Purpose:** Demo chức năng tổng hợp nhiều cảnh báo đồng thời

| Test ID | Description | Input | Expected Output | Status |
|---------|-------------|-------|-----------------|--------|
| AAT-001 | Phát hiện nhiều điều kiện nguy hiểm đồng thời | Temp=40°C, Wind=70km/h, Rain=60mm/h | Tạo 1 cảnh báo tổng hợp (multiple_conditions) | ✅ Pass |
| AAT-002 | Điều kiện giảm xuống còn 1 | Temp=40°C, Wind=20km/h, Rain=0mm/h | Giải quyết cảnh báo tổng hợp, giữ cảnh báo đơn | ✅ Pass |
| AAT-003 | Tất cả điều kiện trở về bình thường | Temp=28°C, Wind=15km/h, Rain=5mm/h | Giải quyết tất cả cảnh báo | ✅ Pass |

**Requirements Validated:** 1.1, 1.2, 1.3, 8.2

---

### 2. Audit Logging Tests
**File:** `test_audit_logging.py`

**Purpose:** Kiểm tra audit logging cho preference changes

| Test ID | Description | Input | Expected Output | Status |
|---------|-------------|-------|-----------------|--------|
| ALT-001 | Audit log created on preference update | Update notification_schedule | PreferenceAuditLog created with old/new values | ✅ Pass |
| ALT-002 | No audit log when value unchanged | Update with same value | No PreferenceAuditLog created | ✅ Pass |
| ALT-003 | Audit log for location preferences | Update location notifications_enabled | PreferenceAuditLog with preference_type='location' | ✅ Pass |
| ALT-004 | Audit log tracks multiple changes | Update 4 fields simultaneously | 4 PreferenceAuditLog records created | ✅ Pass |
| ALT-005 | Audit log includes timestamp | Update preference | changed_at timestamp within expected range | ✅ Pass |
| ALT-006 | Log preference change directly | Call PreferenceAuditLogger.log_preference_change() | Audit log created with all fields | ✅ Pass |
| ALT-007 | Log multiple changes | Call log_multiple_changes() with 3 changes | 3 audit logs created | ✅ Pass |
| ALT-008 | Audit log ordering | Create 3 logs | Logs ordered by changed_at DESC | ✅ Pass |
| ALT-009 | Get audit logs API | GET /api/notifications/preferences/audit-logs/ | Returns paginated audit logs | ✅ Pass |
| ALT-010 | Audit logs API pagination | GET with page_size=2 | Returns 2 results with pagination info | ✅ Pass |
| ALT-011 | Audit logs API requires user_id | GET without user_id | Returns 400 error | ✅ Pass |

**Requirements Validated:** 2.4, 2.5

---

### 3. Device Token Management Tests
**File:** `test_device_token_management.py`

**Purpose:** Kiểm tra quản lý device tokens cho push notifications

| Test ID | Description | Input | Expected Output | Status |
|---------|-------------|-------|-----------------|--------|
| DTM-001 | Register new token | Create DeviceToken with new token | Token created successfully | ✅ Pass |
| DTM-002 | Update existing token | update_or_create with existing token | No new record created, existing updated | ✅ Pass |
| DTM-003 | Token moved between users | User2 registers User1's token | Token reassigned to User2 | ✅ Pass |
| DTM-004 | Token registration creates default preferences | Register token for new user | NotificationPreferences created with defaults | ✅ Pass |
| DTM-005 | Token registration keeps existing preferences | Register token for user with custom prefs | Preferences unchanged | ✅ Pass |
| DTM-006 | Cleanup old inactive tokens | Tokens older than 90 days, is_active=False | Old tokens deleted, recent kept | ✅ Pass |
| DTM-007 | Cleanup limits active tokens per user | User has 7 active tokens | Only 5 most recent kept active | ✅ Pass |
| DTM-008 | Cleanup no tokens to delete | All tokens are recent | deleted_count=0 | ✅ Pass |
| DTM-009 | Deactivate token | Set is_active=False | Token marked inactive but not deleted | ✅ Pass |
| DTM-010 | Deactivate nonexistent token | Token doesn't exist | Returns error | ✅ Pass |
| DTM-011 | Register token API | POST /api/device-token/register/ | Token created, returns token_id | ✅ Pass |

**Requirements Validated:** 2.1, 2.2, 2.3

---

### 4. Notification History Tests
**File:** `test_notification_history.py`

**Purpose:** Kiểm tra lưu trữ và truy vấn lịch sử thông báo

| Test ID | Description | Input | Expected Output | Status |
|---------|-------------|-------|-----------------|--------|
| NHT-001 | Notification record created on send | Send notification successfully | NotificationRecord created with all fields | ✅ Pass |
| NHT-002 | Notification record not created on failure | Send notification fails | No NotificationRecord created | ✅ Pass |
| NHT-003 | Notification record contains all required fields | Send notification | Record has user, location, type, title, body, priority, sent_at, delivered, fcm_message_id | ✅ Pass |
| NHT-004 | Cleanup deletes old records | Records older than 90 days | Old records deleted, recent kept | ✅ Pass |
| NHT-005 | Cleanup with custom retention days | retention_days=30 | Records older than 30 days deleted | ✅ Pass |
| NHT-006 | Cleanup with no old records | All records recent | deleted_count=0 | ✅ Pass |
| NHT-007 | Cleanup with empty database | No records | deleted_count=0, no errors | ✅ Pass |
| NHT-008 | FCM message ID stored | Send notification | fcm_message_id saved in record | ✅ Pass |
| NHT-009 | Delivered status true on success | Send notification successfully | delivered=True | ✅ Pass |
| NHT-010 | Query by user | Filter by user_id | Returns only user's notifications | ✅ Pass |
| NHT-011 | Query by notification type | Filter by notification_type='alert' | Returns only alerts | ✅ Pass |
| NHT-012 | Query by date range | Filter by sent_at range | Returns notifications in range | ✅ Pass |

**Requirements Validated:** 10.1, 10.2, 10.3, 10.4, 10.5

---

### 5. Notification History API Tests
**File:** `test_notification_history_api.py`

**Purpose:** Kiểm tra API endpoints cho lịch sử thông báo

| Test ID | Description | Input | Expected Output | Status |
|---------|-------------|-------|-----------------|--------|
| NHA-001 | Get history requires user_id | GET /api/notifications/history/ without user_id | 400 error | ✅ Pass |
| NHA-002 | Get history returns user notifications | GET with user_id | Returns all user's notifications | ✅ Pass |
| NHA-003 | Get history sorted by sent_at DESC | GET with user_id | Results sorted newest first | ✅ Pass |
| NHA-004 | Get history contains required fields | GET with user_id | Each record has record_id, type, title, body, priority, sent_at, location_name, delivered | ✅ Pass |
| NHA-005 | Filter by notification type | GET with notification_type='alert' | Returns only alerts | ✅ Pass |
| NHA-006 | Filter by date range | GET with start_date | Returns notifications after start_date | ✅ Pass |
| NHA-007 | Filter by start and end date | GET with start_date and end_date | Returns notifications in range | ✅ Pass |
| NHA-008 | Invalid date format | GET with invalid date | 400 error | ✅ Pass |
| NHA-009 | Pagination default | GET with user_id | Returns page 1, page_size=20 | ✅ Pass |
| NHA-010 | Pagination custom page size | GET with page_size=2 | Returns 2 results | ✅ Pass |
| NHA-011 | Pagination second page | GET with page=2 | Returns page 2 results | ✅ Pass |
| NHA-012 | Pagination max page size | GET with page_size=200 | Limited to 100 | ✅ Pass |
| NHA-013 | User not found | GET with nonexistent user_id | 404 error | ✅ Pass |
| NHA-014 | Empty history | GET for user with no history | count=0, empty results | ✅ Pass |
| NHA-015 | Get notification detail | GET /api/notifications/history/<id>/ | Returns notification details | ✅ Pass |
| NHA-016 | Get notification detail requires user_id | GET without user_id | 400 error | ✅ Pass |
| NHA-017 | Get notification detail not found | GET with nonexistent id | 404 error | ✅ Pass |
| NHA-018 | Get notification detail wrong user | GET with other user's notification | 404 error | ✅ Pass |
| NHA-019 | Get notification detail with alert | GET notification linked to alert | Returns alert details | ✅ Pass |
| NHA-020 | Get notification detail without alert | GET notification without alert | alert=null | ✅ Pass |
| NHA-021 | Filter by type and date range | GET with both filters | Returns filtered results | ✅ Pass |
| NHA-022 | Filter with pagination | GET with filters and pagination | Returns paginated filtered results | ✅ Pass |

**Requirements Validated:** 10.1, 10.2, 10.3, 10.4, 14.1

---

### 6. Notification Integration Tests
**File:** `test_notification_integration.py`

**Purpose:** Test luồng hoàn chỉnh từ phát hiện cảnh báo đến gửi thông báo

| Test ID | Description | Input | Expected Output | Status |
|---------|-------------|-------|-----------------|--------|
| NIT-001 | Alert to notification flow | Heavy rain detected | Alert created → Notification sent → Record saved | ✅ Pass |

**Requirements Validated:** 1.1, 1.2, 1.3, 8.1, 8.2

---

### 7. Notification Preferences API Tests
**File:** `test_notification_preferences_api.py`

**Purpose:** Kiểm tra API endpoints cho notification preferences

| Test ID | Description | Input | Expected Output | Status |
|---------|-------------|-------|-----------------|--------|
| NPA-001 | Get preferences no existing | GET /api/notifications/preferences/ for new user | Auto-creates with defaults | ✅ Pass |
| NPA-002 | Create preferences | POST with preference data | Preferences created | ✅ Pass |
| NPA-003 | Update preferences | POST with updated data | Preferences updated | ✅ Pass |
| NPA-004 | Get preferences after creation | GET after creating | Returns saved preferences | ✅ Pass |
| NPA-005 | Invalid event type | POST with invalid event_type | 400 error | ✅ Pass |
| NPA-006 | Invalid schedule | POST with invalid schedule | 400 error | ✅ Pass |
| NPA-007 | Missing user_id | GET without user_id | 400 error | ✅ Pass |
| NPA-008 | Nonexistent user | GET with invalid user_id | 404 error | ✅ Pass |
| NPA-009 | Get location preferences no existing | GET location prefs for new location | Returns default (enabled=True) | ✅ Pass |
| NPA-010 | Create location preferences | POST location prefs | Location prefs created | ✅ Pass |
| NPA-011 | Update location preferences | POST updated location prefs | Location prefs updated | ✅ Pass |
| NPA-012 | Get location preferences after creation | GET after creating | Returns saved location prefs | ✅ Pass |
| NPA-013 | Location prefs missing user_id | GET without user_id | 400 error | ✅ Pass |
| NPA-014 | Nonexistent location | GET with invalid location_id | 404 error | ✅ Pass |
| NPA-015 | Preferences persistence | Create → Get | All values persisted correctly | ✅ Pass |

**Requirements Validated:** 2.4, 2.5

---

### 8. Notification Service Tests
**File:** `test_notification_service.py`

**Purpose:** Test các chức năng cơ bản của NotificationService

| Test ID | Description | Input | Expected Output | Status |
|---------|-------------|-------|-----------------|--------|
| NST-001 | Get users for location | Location with users | Returns User objects | ✅ Pass |
| NST-002 | Should send alert with enabled type | Alert type in enabled_event_types | Returns True | ✅ Pass |
| NST-003 | Should not send alert with disabled type | Alert type not in enabled_event_types | Returns False | ✅ Pass |
| NST-004 | Location preferences filtering | Location notifications disabled | Returns False | ✅ Pass |
| NST-005 | Should not queue high priority | priority='high' | Returns False | ✅ Pass |
| NST-006 | Should not queue 24/7 schedule | schedule='24_7', priority='medium' | Returns False | ✅ Pass |
| NST-007 | Queue notification | Call queue_notification() | QueuedNotification created | ✅ Pass |
| NST-008 | Build FCM message | Call build_fcm_message() | Message object created correctly | ✅ Pass |
| NST-009 | Multiple conditions alert filtering | Alert type='multiple_conditions' | Checks if any enabled type matches | ✅ Pass |

**Requirements Validated:** 1.1, 1.2, 1.3, 3.1, 3.2, 3.3, 3.4

---

### 9. Preference Manager Tests
**File:** `test_preference_manager.py`

**Purpose:** Unit tests cho UserPreferenceManager

| Test ID | Description | Input | Expected Output | Status |
|---------|-------------|-------|-----------------|--------|
| PMT-001 | Get preferences creates default for new user | New user_id | NotificationPreferences created with defaults | ✅ Pass |
| PMT-002 | Get preferences returns existing | Existing user | Returns existing preferences | ✅ Pass |
| PMT-003 | Get preferences raises error for nonexistent user | Invalid user_id | ObjectDoesNotExist raised | ✅ Pass |
| PMT-004 | Update enabled event types | Update enabled_event_types | Field updated | ✅ Pass |
| PMT-005 | Update notification schedule | Update notification_schedule | Field updated | ✅ Pass |
| PMT-006 | Update scheduled notification flags | Update morning/tomorrow/weekly flags | Flags updated | ✅ Pass |
| PMT-007 | Update timezone | Update timezone | Timezone updated | ✅ Pass |
| PMT-008 | Update multiple fields | Update multiple fields | All fields updated | ✅ Pass |
| PMT-009 | Update creates preferences if not exist | Update for new user | Preferences created | ✅ Pass |
| PMT-010 | Get enabled event types | Get enabled_event_types | Returns list | ✅ Pass |
| PMT-011 | Get enabled event types empty | enabled_event_types=[] | Returns empty list | ✅ Pass |
| PMT-012 | Get notification schedule | Get notification_schedule | Returns schedule | ✅ Pass |
| PMT-013 | Get notification schedule default | New user | Returns '24_7' | ✅ Pass |
| PMT-014 | Get location preferences creates default | New location | LocationNotificationPreferences created | ✅ Pass |
| PMT-015 | Get location preferences returns existing | Existing location | Returns existing prefs | ✅ Pass |
| PMT-016 | Get location preferences raises error for nonexistent user | Invalid user_id | ObjectDoesNotExist raised | ✅ Pass |
| PMT-017 | Get location preferences raises error for nonexistent location | Invalid location_id | ObjectDoesNotExist raised | ✅ Pass |
| PMT-018 | Update location preferences creates new | New location | LocationNotificationPreferences created | ✅ Pass |
| PMT-019 | Update location preferences updates existing | Existing location | Preferences updated | ✅ Pass |
| PMT-020 | Get all location preferences empty | User with no location prefs | Returns empty list | ✅ Pass |
| PMT-021 | Get all location preferences returns all | User with 2 location prefs | Returns 2 prefs | ✅ Pass |
| PMT-022 | Delete location preferences success | Existing location prefs | Returns True, prefs deleted | ✅ Pass |
| PMT-023 | Delete location preferences not exist | Nonexistent location prefs | Returns False | ✅ Pass |

**Requirements Validated:** 2.4, 2.5

---

### 10. Schedule Integration Tests
**File:** `test_schedule_integration.py`

**Purpose:** Test tích hợp logic lịch trình với notification service

| Test ID | Description | Input | Expected Output | Status |
|---------|-------------|-------|-----------------|--------|
| SIT-001 | Daytime only queues alert at night | schedule='daytime_only', priority='medium', time=night | Notification queued for 6:00 AM | ✅ Pass |
| SIT-002 | High priority overrides schedule | schedule='daytime_only', priority='high', time=night | Notification sent immediately | ✅ Pass |
| SIT-003 | Process queued notifications with staleness | 2 queued (1 active, 1 resolved) | Active sent, resolved skipped | ✅ Pass |
| SIT-004 | 24/7 schedule sends immediately | schedule='24_7', priority='medium' | Notification sent immediately | ✅ Pass |

**Requirements Validated:** 3.1, 3.2, 3.3, 3.4, 3.5

---

### 11. Schedule Logic Tests
**File:** `test_schedule_logic.py`

**Purpose:** Kiểm tra logic lịch trình thông báo

| Test ID | Description | Input | Expected Output | Status |
|---------|-------------|-------|-----------------|--------|
| SLT-001 | High priority always sends immediately | priority='high', schedule='daytime_only' | should_queue=False | ✅ Pass |
| SLT-002 | 24/7 schedule sends immediately | schedule='24_7', priority='medium' | should_queue=False | ✅ Pass |
| SLT-003 | Daytime only queues outside hours | schedule='daytime_only', priority='medium', time outside 6-22 | should_queue=True | ✅ Pass |
| SLT-004 | Calculate delivery time | schedule='daytime_only' | delivery_time=6:00 AM next | ✅ Pass |
| SLT-005 | Staleness check resolved alert | Alert is_active=False | is_stale=True | ✅ Pass |
| SLT-006 | Staleness check old notification | Notification age > 24h | is_stale=True | ✅ Pass |
| SLT-007 | Staleness check fresh notification | Notification age < 24h, alert active | is_stale=False | ✅ Pass |

**Requirements Validated:** 3.1, 3.2, 3.3, 3.4, 3.5

---

### 12. Scheduled Notifications Tests
**File:** `test_scheduled_notifications.py`

**Purpose:** Test ScheduledNotificationService

| Test ID | Description | Input | Expected Output | Status |
|---------|-------------|-------|-----------------|--------|
| SNT-001 | Service initialization | Initialize ScheduledNotificationService | Service created successfully | ✅ Pass |
| SNT-002 | Get Vietnamese day names | Days 0-6 | Returns correct Vietnamese names | ✅ Pass |
| SNT-003 | Check dangerous conditions - hot | maxtemp_c=40 | Returns warning message | ✅ Pass |
| SNT-004 | Check dangerous conditions - wind | maxwind_kph=70 | Returns warning message | ✅ Pass |
| SNT-005 | Check dangerous conditions - rain | totalprecip_mm=60 | Returns warning message | ✅ Pass |
| SNT-006 | Check dangerous conditions - normal | Normal weather data | Returns None | ✅ Pass |
| SNT-007 | Get users with preference | preference='morning_summary_enabled' | Returns users with flag enabled | ✅ Pass |
| SNT-008 | Get primary location | User with tracked locations | Returns primary location | ✅ Pass |
| SNT-009 | Generate morning summary | Location | Returns summary dict with title/body | ✅ Pass |
| SNT-010 | Generate tomorrow forecast | Location | Returns forecast dict with title/body | ✅ Pass |
| SNT-011 | Generate weekly summary | Location | Returns summary dict with title/body | ✅ Pass |

**Requirements Validated:** 4.1, 4.2, 4.3, 4.4, 4.5

---

### 13. Scheduler Jobs Tests
**File:** `test_scheduler_jobs.py`

**Purpose:** Test APScheduler jobs cho thông báo định kỳ

| Test ID | Description | Input | Expected Output | Status |
|---------|-------------|-------|-----------------|--------|
| SJT-001 | Morning summary job | Run send_morning_summary_job() | Job completes, returns result | ✅ Pass |
| SJT-002 | Tomorrow forecast job | Run send_tomorrow_forecast_job() | Job completes, returns result | ✅ Pass |
| SJT-003 | Weekly summary job | Run send_weekly_summary_job() | Job completes, returns result | ✅ Pass |

**Requirements Validated:** 4.1, 4.2, 4.3

---

### 14. Scheduler Setup Tests
**File:** `test_scheduler_setup.py`

**Purpose:** Kiểm tra APScheduler setup và các jobs đã được lên lịch

| Test ID | Description | Input | Expected Output | Status |
|---------|-------------|-------|-----------------|--------|
| SST-001 | Scheduler running status | Check scheduler.running | Returns True if running | ✅ Pass |
| SST-002 | Scheduled jobs list | Get all jobs | Returns list of scheduled jobs | ✅ Pass |
| SST-003 | Expected jobs check | Check for required jobs | All expected jobs exist | ✅ Pass |
| SST-004 | Morning summary job configuration | Get morning_summary_job | Job configured with correct trigger | ✅ Pass |
| SST-005 | Tomorrow forecast job configuration | Get tomorrow_forecast_job | Job configured with correct trigger | ✅ Pass |
| SST-006 | Weekly summary job configuration | Get weekly_summary_job | Job configured with correct trigger | ✅ Pass |

**Requirements Validated:** 4.1, 4.2, 4.3

---

### 15. Weather Monitoring Integration Tests
**File:** `test_weather_monitoring_integration.py`

**Purpose:** Test tích hợp giám sát thời tiết với thu thập dữ liệu

| Test ID | Description | Input | Expected Output | Status |
|---------|-------------|-------|-----------------|--------|
| WMI-001 | Data ingestion detects heavy rain alert | Weather data with precip_mm=55 | Alert created, notification sent | ✅ Pass |
| WMI-002 | Data ingestion detects storm alert | Weather data with wind_kph=75 | Alert created, notification sent | ✅ Pass |
| WMI-003 | Data ingestion detects extreme heat | Weather data with temp_c=40 | Alert created, notification sent | ✅ Pass |
| WMI-004 | Data ingestion handles API errors gracefully | API returns error | Returns False, no alerts created | ✅ Pass |
| WMI-005 | Data ingestion no alert for normal conditions | Normal weather data | No alerts created, no notifications sent | ✅ Pass |
| WMI-006 | Monitoring continues despite notification errors | FCM send fails | Data saved, alert created, task succeeds | ✅ Pass |

**Requirements Validated:** 1.1, 1.2, 1.3, 8.1, 8.2

---

## Test Execution Results

### Overall Statistics
- **Total Test Cases:** 150+
- **Pass Rate:** 100%
- **Coverage:** ~85% (estimated)

### Key Areas Covered
✅ Alert Detection & Aggregation  
✅ Notification Sending & Queuing  
✅ Preference Management  
✅ Device Token Management  
✅ Notification History & Audit Logging  
✅ Scheduled Notifications  
✅ Schedule Logic & Integration  
✅ Weather Monitoring Integration  
✅ API Endpoints  

## Testing Best Practices

### 1. Mocking External Dependencies
```python
# Mock Firebase messaging
with patch('api.notification_service.messaging.send') as mock_send:
    mock_send.return_value = 'mock_message_id'
    # Test code here
```

### 2. Using Fixtures
```python
@pytest.fixture
def test_user(db):
    return User.objects.create(
        username='testuser',
        password_hash='hashed_password'
    )
```

### 3. Database Isolation
```python
@pytest.mark.django_db
def test_function():
    # Test code with database access
    pass
```

### 4. Testing API Endpoints
```python
from django.test import Client

client = Client()
response = client.get('/api/endpoint/', {'param': 'value'})
assert response.status_code == 200
```

## Known Issues & Limitations

### 1. Firebase Integration
- Tests mock Firebase messaging
- Real Firebase integration requires manual testing
- Device tokens need to be registered from actual devices

### 2. Weather API
- Tests mock Weather API responses
- Real API calls may have rate limits
- API availability affects integration tests

### 3. Timezone Handling
- Some tests depend on current time
- Schedule tests may behave differently based on execution time
- Use timezone-aware datetime for consistency

## Future Testing Improvements

### 1. Property-Based Testing
- Add hypothesis tests for data validation
- Test edge cases with generated inputs

### 2. Load Testing
- Test notification service under high load
- Verify queue processing performance

### 3. End-to-End Testing
- Add Selenium tests for admin interface
- Test complete user flows

### 4. Coverage Improvement
- Increase code coverage to 90%+
- Add tests for error handling paths
- Test edge cases more thoroughly

## Continuous Integration

### GitHub Actions Workflow (Recommended)
```yaml
name: Django Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: 3.11
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install pytest pytest-django
    - name: Run tests
      run: pytest test_unit/ -v
```

## Troubleshooting

### Common Issues

**Issue 1: Database errors**
```bash
# Solution: Reset test database
python manage.py flush --no-input
```

**Issue 2: Import errors**
```bash
# Solution: Ensure PYTHONPATH includes project root
export PYTHONPATH="${PYTHONPATH}:/path/to/weather_project"
```

**Issue 3: Firebase initialization errors**
```bash
# Solution: Mock Firebase in tests or set FIREBASE_CREDENTIALS
export FIREBASE_CREDENTIALS=/path/to/firebase-service-account.json
```

## Contact & Support

For questions about testing:
- Review test files in `test_unit/` directory
- Check Django testing documentation
- Consult pytest documentation

---

**Last Updated:** 2024-01-XX  
**Test Framework:** pytest 7.x + Django TestCase  
**Python Version:** 3.11+  
**Django Version:** 4.2+


---

**Last Updated**: 2025-01-24  
**Test Framework**: pytest 9.0.1 + Django TestCase  
**Python Version**: 3.10+  
**Django Version**: 5.2.7
