# API Documentation - Weather Forecast Backend

## 📋 Mục lục

- [Tổng quan](#tổng-quan)
- [Base URL](#base-url)
- [Authentication](#authentication)
- [Response Format](#response-format)
- [HTTP Status Codes](#http-status-codes)
- [1. Authentication Endpoints](#1-authentication-endpoints)
- [2. Weather Data Endpoints](#2-weather-data-endpoints)
- [3. Location Tracking Endpoints](#3-location-tracking-endpoints)
- [4. Alerts & AI Advice Endpoints](#4-alerts--ai-advice-endpoints)
- [5. Device Token Management](#5-device-token-management)
- [6. Notification Preferences](#6-notification-preferences)
- [7. Notification History](#7-notification-history)
- [8. Preference Audit Logs](#8-preference-audit-logs)
- [9. Testing & Admin Endpoints](#9-testing--admin-endpoints)
- [10. Root & Documentation Endpoints](#10-root--documentation-endpoints)
- [11. Error Handling](#11-error-handling)
- [12. Rate Limiting & Caching](#12-rate-limiting--caching)

## Tổng quan

Backend API cung cấp các dịch vụ dự báo thời tiết, quản lý người dùng, theo dõi địa điểm, cảnh báo thời tiết, và quản lý thông báo push notification. API được xây dựng bằng Django REST Framework với authentication và caching.

## Base URL

```
http://localhost:8000/api/
```

## Authentication

Hiện tại API sử dụng `AllowAny` permission cho hầu hết endpoints (development mode). Trong production, cần implement JWT authentication và thay đổi permission classes thành `IsAuthenticated`.

### Planned Authentication Flow
- User đăng ký/đăng nhập → Nhận JWT token
- Gửi token trong header: `Authorization: Bearer <token>`
- Token được validate ở mỗi request

## Response Format

### Success Response
```json
{
  "data": {},
  "message": "Success message"
}
```

### Error Response
```json
{
  "error": "Error message",
  "details": "Additional error details (optional)"
}
```

## HTTP Status Codes

- `200 OK` - Request thành công
- `201 Created` - Tạo resource thành công
- `400 Bad Request` - Request không hợp lệ
- `401 Unauthorized` - Chưa xác thực
- `403 Forbidden` - Không có quyền truy cập
- `404 Not Found` - Resource không tồn tại
- `409 Conflict` - Conflict với resource hiện tại
- `500 Internal Server Error` - Lỗi server
- `503 Service Unavailable` - Service tạm thời không khả dụng

---


## 1. Authentication Endpoints

### 1.1 Đăng ký người dùng

**Endpoint:** `POST /api/register/`

**Description:** Tạo tài khoản người dùng mới

**Authentication:** Không yêu cầu

**Request Body:**
```json
{
  "username": "string (required)",
  "password": "string (required)"
}
```

**Success Response (201 Created):**
```json
{
  "message": "User registered successfully",
  "user": {
    "user_id": 1,
    "username": "john_doe"
  }
}
```

**Error Responses:**
- `400 Bad Request` - Thiếu username hoặc password
- `409 Conflict` - Username đã tồn tại
- `500 Internal Server Error` - Lỗi database

**Example:**
```bash
curl -X POST http://localhost:8000/api/register/ \
  -H "Content-Type: application/json" \
  -d '{"username": "john_doe", "password": "secure_password"}'
```

---

### 1.2 Đăng nhập

**Endpoint:** `POST /api/login/`

**Description:** Xác thực người dùng và đăng nhập

**Authentication:** Không yêu cầu

**Request Body:**
```json
{
  "username": "string (required)",
  "password": "string (required)"
}
```

**Success Response (200 OK):**
```json
{
  "message": "Login successful",
  "user": {
    "user_id": 1,
    "username": "john_doe"
  }
}
```

**Error Responses:**
- `400 Bad Request` - Thiếu username hoặc password
- `401 Unauthorized` - Username hoặc password không đúng
- `500 Internal Server Error` - Lỗi database

**Example:**
```bash
curl -X POST http://localhost:8000/api/login/ \
  -H "Content-Type: application/json" \
  -d '{"username": "john_doe", "password": "secure_password"}'
```

**Note:** Trong production, endpoint này sẽ trả về JWT token thay vì chỉ user info.

---


## 2. Weather Data Endpoints

### 2.1 Lấy dữ liệu thời tiết

**Endpoint:** `GET /api/weather/`

**Description:** Lấy dữ liệu thời tiết hiện tại hoặc dự báo cho một địa điểm

**Authentication:** Không yêu cầu

**Query Parameters:**
- `q` (string, required) - Tên địa điểm (ví dụ: "Hanoi", "Ho Chi Minh")
- `days` (integer, optional) - Số ngày dự báo (1-10). Nếu không có, trả về thời tiết hiện tại

**Success Response (200 OK):**
```json
{
  "location": {
    "name": "Hanoi",
    "region": "Ha Noi",
    "country": "Vietnam",
    "lat": 21.03,
    "lon": 105.85,
    "tz_id": "Asia/Ho_Chi_Minh",
    "localtime": "2024-01-15 14:30"
  },
  "current": {
    "temp_c": 25.0,
    "condition": {
      "text": "Partly cloudy",
      "icon": "//cdn.weatherapi.com/weather/64x64/day/116.png"
    },
    "wind_kph": 15.0,
    "humidity": 70,
    "feelslike_c": 26.0,
    "uv": 5.0
  },
  "forecast": {
    "forecastday": [
      {
        "date": "2024-01-15",
        "day": {
          "maxtemp_c": 28.0,
          "mintemp_c": 20.0,
          "avgtemp_c": 24.0,
          "daily_chance_of_rain": 30,
          "condition": {
            "text": "Partly cloudy"
          }
        },
        "hour": [
          {
            "time": "2024-01-15 00:00",
            "temp_c": 22.0,
            "condition": {"text": "Clear"},
            "wind_kph": 10.0,
            "humidity": 75,
            "chance_of_rain": 10
          }
        ]
      }
    ]
  }
}
```

**Error Responses:**
- `400 Bad Request` - Thiếu parameter 'q'
- `504 Gateway Timeout` - API timeout
- `500 Internal Server Error` - Lỗi khi gọi WeatherAPI

**Caching:** Response được cache trong 5 phút (300 seconds)

**Examples:**
```bash
# Lấy thời tiết hiện tại
curl "http://localhost:8000/api/weather/?q=Hanoi"

# Lấy dự báo 3 ngày
curl "http://localhost:8000/api/weather/?q=Hanoi&days=3"
```

---


## 3. Location Tracking Endpoints

### 3.1 Theo dõi địa điểm

**Endpoint:** `POST /api/locations/track/`

**Description:** Thêm địa điểm vào danh sách theo dõi của user. Nếu là địa điểm mới, hệ thống sẽ tự động lên lịch thu thập dữ liệu và phân tích AI.

**Authentication:** Cần user_id (sẽ dùng IsAuthenticated trong production)

**Request Body:**
```json
{
  "name_en": "string (required) - Tên địa điểm tiếng Anh",
  "latitude": "decimal (required) - Vĩ độ",
  "longitude": "decimal (required) - Kinh độ",
  "user_id": "integer (required) - ID của user"
}
```

**Success Response (201 Created):**
```json
{
  "message": "Location 'Hanoi' activated for tracking."
}
```

**Error Responses:**
- `400 Bad Request` - Thiếu parameters bắt buộc
- `500 Internal Server Error` - Lỗi database

**Background Tasks:**
- Nếu là địa điểm mới, hệ thống sẽ:
  - Thu thập dữ liệu thời tiết sau 10 giây
  - Chạy phân tích AI sau 2 phút

**Example:**
```bash
curl -X POST http://localhost:8000/api/locations/track/ \
  -H "Content-Type: application/json" \
  -d '{
    "name_en": "Hanoi",
    "latitude": 21.0285,
    "longitude": 105.8542,
    "user_id": 1
  }'
```

---

### 3.2 Lấy danh sách địa điểm đang theo dõi

**Endpoint:** `GET /api/locations/tracked/`

**Description:** Lấy danh sách tất cả địa điểm mà user đang theo dõi kèm thông tin thời tiết

**Authentication:** Cần user_id

**Query Parameters:**
- `user_id` (integer, required) - ID của user

**Success Response (200 OK):**
```json
[
  {
    "id": 1,
    "name": "Hà Nội",
    "temp_c": 25.0,
    "condition_text": "Partly cloudy",
    "icon": "//cdn.weatherapi.com/weather/64x64/day/116.png",
    "wind_kph": 15.0,
    "chance_of_rain": 30,
    "humidity": 70
  },
  {
    "id": 2,
    "name": "Hồ Chí Minh",
    "temp_c": 32.0,
    "condition_text": "Sunny",
    "icon": "//cdn.weatherapi.com/weather/64x64/day/113.png",
    "wind_kph": 10.0,
    "chance_of_rain": 10,
    "humidity": 65
  }
]
```

**Error Responses:**
- `400 Bad Request` - Thiếu user_id hoặc format không hợp lệ
- `500 Internal Server Error` - Lỗi server

**Caching:** Mỗi location được cache 5 phút

**Example:**
```bash
curl "http://localhost:8000/api/locations/tracked/?user_id=1"
```

---

### 3.3 Xóa địa điểm khỏi danh sách theo dõi

**Endpoint:** `POST /api/locations/delete/` hoặc `DELETE /api/locations/delete/`

**Description:** Xóa địa điểm khỏi danh sách theo dõi của user

**Authentication:** Cần user_id

**Query Parameters hoặc Request Body:**
- `user_id` (integer, required) - ID của user
- `location_id` (integer, required) - ID của location cần xóa

**Success Response (200 OK):**
```json
{
  "message": "Location untracked successfully"
}
```

**Error Responses:**
- `400 Bad Request` - Thiếu parameters hoặc format không hợp lệ
- `404 Not Found` - Location không tồn tại hoặc user không theo dõi location này
- `500 Internal Server Error` - Lỗi server

**Examples:**
```bash
# Dùng POST với query params
curl -X POST "http://localhost:8000/api/locations/delete/?user_id=1&location_id=5"

# Dùng DELETE với request body
curl -X DELETE http://localhost:8000/api/locations/delete/ \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1, "location_id": 5}'
```

---


## 4. Alerts & AI Advice Endpoints

### 4.1 Lấy cảnh báo thời tiết cho địa điểm

**Endpoint:** `GET /api/alerts/`

**Description:** Lấy các cảnh báo thời tiết cực đoan (ExtremeEvent) trong 24 giờ gần nhất cho một địa điểm

**Authentication:** Không yêu cầu

**Query Parameters:**
- `q` (string, required) - Tên địa điểm tiếng Anh (ví dụ: "Hanoi")

**Success Response (200 OK):**
```json
[
  {
    "event_id": 1,
    "analysis_time": "2024-01-15T10:30:00Z",
    "severity": "high",
    "impact_field": "heavy_rain",
    "forecast_details_vi": "Mưa lớn dự kiến từ 14:00 đến 18:00 với lượng mưa 50-80mm",
    "actionable_advice_vi": "Nên mang theo ô, tránh di chuyển không cần thiết"
  },
  {
    "event_id": 2,
    "analysis_time": "2024-01-15T08:00:00Z",
    "severity": "medium",
    "impact_field": "extreme_heat",
    "forecast_details_vi": "Nhiệt độ cao nhất 38°C vào buổi trưa",
    "actionable_advice_vi": "Hạn chế hoạt động ngoài trời, uống nhiều nước"
  }
]
```

**Empty Response (200 OK):**
```json
[]
```
*Trả về mảng rỗng nếu không có cảnh báo hoặc địa điểm chưa được theo dõi*

**Error Responses:**
- `400 Bad Request` - Thiếu parameter 'q'
- `500 Internal Server Error` - Lỗi server

**Example:**
```bash
curl "http://localhost:8000/api/alerts/?q=Hanoi"
```

---

### 4.2 Lấy lời khuyên AI tức thì

**Endpoint:** `GET /api/advice/`

**Description:** Lấy lời khuyên/cảnh báo từ AI cho bất kỳ địa điểm nào. Hệ thống sẽ lấy dữ liệu theo giờ từ WeatherAPI (-3 đến +3 ngày) và phân tích bằng AI.

**Authentication:** Không yêu cầu

**Query Parameters:**
- `q` (string, required) - Tên địa điểm (ví dụ: "Hanoi")

**Success Response (200 OK):**
```json
{
  "type": "warning",
  "message_vi": "Cảnh báo: Mưa lớn dự kiến vào chiều nay (14:00-18:00) với lượng mưa 50-80mm. Nên mang theo ô và tránh di chuyển không cần thiết."
}
```

hoặc

```json
{
  "type": "advice",
  "message_vi": "Thời tiết thuận lợi trong 2 ngày tới. Nhiệt độ 22-28°C, ít mưa. Thích hợp cho các hoạt động ngoài trời."
}
```

**Error Response (503 Service Unavailable):**
```json
{
  "type": "error",
  "message_vi": "Lỗi khi lấy dữ liệu thời tiết dự báo chi tiết. Vui lòng thử lại sau."
}
```

**Caching:** 
- Memory cache: 3 giờ
- Database cache (AdviceCache): Lưu vĩnh viễn để tracking

**Background Processing:**
- Nếu địa điểm chưa có trong database, hệ thống sẽ tự động tạo Location record
- Tọa độ được lấy từ WeatherAPI response

**Example:**
```bash
curl "http://localhost:8000/api/advice/?q=Hanoi"
```

---

### 4.3 Kiểm tra lời khuyên gần đây

**Endpoint:** `GET /api/check-advice/`

**Description:** Kiểm tra xem có lời khuyên/cảnh báo nào trong vòng 1 giờ gần đây không (từ AdviceCache)

**Authentication:** Không yêu cầu

**Query Parameters:**
- `q` (string, required) - Tên địa điểm tiếng Anh

**Success Response - Có advice gần đây (200 OK):**
```json
{
  "type": "warning",
  "message_vi": "Cảnh báo mưa lớn...",
  "generated_time": "2024-01-15T10:30:00Z"
}
```

**Success Response - Không có advice gần đây (200 OK):**
```json
{
  "status": "stale"
}
```

**Error Responses:**
- `400 Bad Request` - Thiếu parameter 'q'
- `500 Internal Server Error` - Lỗi server

**Use Case:** App có thể gọi endpoint này trước để kiểm tra, nếu trả về "stale" thì mới gọi `/api/advice/` để tạo advice mới.

**Example:**
```bash
curl "http://localhost:8000/api/check-advice/?q=Hanoi"
```

---


## 5. Device Token Management

### 5.1 Đăng ký/Cập nhật Device Token

**Endpoint:** `POST /api/device-token/register/`

**Description:** Đăng ký hoặc cập nhật FCM device token cho push notifications

**Authentication:** Cần user_id

**Request Body:**
```json
{
  "user_id": "integer (required)",
  "token": "string (required) - FCM device token"
}
```

**Success Response (201 Created hoặc 200 OK):**
```json
{
  "message": "Device token registered successfully",
  "token_id": 123,
  "has_preferences": true
}
```

**Behavior:**
- Nếu token đã tồn tại cho user khác, sẽ chuyển token sang user mới
- Tự động tạo NotificationPreferences mặc định nếu user chưa có
- Giữ tối đa 5 active tokens cho mỗi user, các token cũ hơn sẽ bị deactivate

**Error Responses:**
- `400 Bad Request` - Thiếu user_id hoặc token
- `404 Not Found` - User không tồn tại
- `500 Internal Server Error` - Lỗi server

**Example:**
```bash
curl -X POST http://localhost:8000/api/device-token/register/ \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "token": "fK7x9mN2pQ8..."
  }'
```

---

### 5.2 Xóa Device Token

**Endpoint:** `DELETE /api/device-token/register/`

**Description:** Deactivate device token (đánh dấu không active)

**Authentication:** Cần user_id

**Request Body:**
```json
{
  "user_id": "integer (required)",
  "token": "string (required)"
}
```

**Success Response (200 OK):**
```json
{
  "message": "Device token deactivated successfully"
}
```

**Error Responses:**
- `400 Bad Request` - Thiếu user_id hoặc token
- `404 Not Found` - User hoặc token không tồn tại
- `500 Internal Server Error` - Lỗi server

**Example:**
```bash
curl -X DELETE http://localhost:8000/api/device-token/register/ \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "token": "fK7x9mN2pQ8..."
  }'
```

---


## 6. Notification Preferences

### 6.1 Lấy Notification Preferences

**Endpoint:** `GET /api/notifications/preferences/`

**Description:** Lấy cài đặt thông báo toàn cục của user

**Authentication:** Cần user_id

**Query Parameters:**
- `user_id` (integer, required)

**Success Response (200 OK):**
```json
{
  "preference_id": 1,
  "user": 1,
  "notifications_enabled": true,
  "enabled_event_types": ["heavy_rain", "storm", "extreme_heat", "extreme_cold"],
  "notification_schedule": "24_7",
  "morning_summary_enabled": true,
  "tomorrow_forecast_enabled": true,
  "weekly_summary_enabled": false,
  "timezone": "Asia/Ho_Chi_Minh",
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:00:00Z"
}
```

**Response khi chưa có preferences (200 OK):**
```json
{
  "message": "No preferences found. Default preferences will be created on first update.",
  "user_id": 1
}
```

**Error Responses:**
- `400 Bad Request` - Thiếu user_id
- `404 Not Found` - User không tồn tại
- `500 Internal Server Error` - Lỗi server

**Example:**
```bash
curl "http://localhost:8000/api/notifications/preferences/?user_id=1"
```

---

### 6.2 Cập nhật Notification Preferences

**Endpoint:** `POST /api/notifications/preferences/`

**Description:** Cập nhật cài đặt thông báo toàn cục của user

**Authentication:** Cần user_id

**Query Parameters:**
- `user_id` (integer, required)

**Request Body:**
```json
{
  "notifications_enabled": "boolean (optional)",
  "enabled_event_types": "array (optional) - ['heavy_rain', 'storm', 'extreme_heat', 'extreme_cold', 'moderate_rain', 'sunny']",
  "notification_schedule": "string (optional) - '24_7' hoặc 'daytime_only'",
  "morning_summary_enabled": "boolean (optional)",
  "tomorrow_forecast_enabled": "boolean (optional)",
  "weekly_summary_enabled": "boolean (optional)",
  "timezone": "string (optional) - Default: 'Asia/Ho_Chi_Minh'"
}
```

**Success Response (200 OK):**
```json
{
  "message": "Preferences updated successfully",
  "preferences": {
    "preference_id": 1,
    "user": 1,
    "notifications_enabled": false,
    "enabled_event_types": ["heavy_rain", "storm"],
    "notification_schedule": "daytime_only",
    "morning_summary_enabled": true,
    "tomorrow_forecast_enabled": false,
    "weekly_summary_enabled": false,
    "timezone": "Asia/Ho_Chi_Minh",
    "created_at": "2024-01-15T10:00:00Z",
    "updated_at": "2024-01-15T14:30:00Z"
  }
}
```

**Error Responses:**
- `400 Bad Request` - Thiếu user_id, không có data, hoặc data không hợp lệ
- `404 Not Found` - User không tồn tại
- `500 Internal Server Error` - Lỗi server

**Audit Logging:** Mọi thay đổi được ghi vào PreferenceAuditLog với IP address và user agent

**Example:**
```bash
curl -X POST "http://localhost:8000/api/notifications/preferences/?user_id=1" \
  -H "Content-Type: application/json" \
  -d '{
    "notifications_enabled": false,
    "enabled_event_types": ["heavy_rain", "storm"],
    "notification_schedule": "daytime_only"
  }'
```

---

### 6.3 Lấy Location-Specific Preferences

**Endpoint:** `GET /api/notifications/preferences/location/<location_id>/`

**Description:** Lấy cài đặt thông báo cho một địa điểm cụ thể

**Authentication:** Cần user_id

**URL Parameters:**
- `location_id` (integer, required)

**Query Parameters:**
- `user_id` (integer, required)

**Success Response (200 OK):**
```json
{
  "id": 1,
  "user": 1,
  "location": 5,
  "location_name": "Hanoi",
  "notifications_enabled": true,
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:00:00Z"
}
```

**Response khi chưa có preferences (200 OK):**
```json
{
  "message": "No preferences found for this location. Default will be created on first update.",
  "user_id": 1,
  "location_id": 5,
  "notifications_enabled": true
}
```

**Error Responses:**
- `400 Bad Request` - Thiếu user_id hoặc format không hợp lệ
- `404 Not Found` - User hoặc Location không tồn tại
- `500 Internal Server Error` - Lỗi server

**Example:**
```bash
curl "http://localhost:8000/api/notifications/preferences/location/5/?user_id=1"
```

---

### 6.4 Cập nhật Location-Specific Preferences

**Endpoint:** `POST /api/notifications/preferences/location/<location_id>/`

**Description:** Cập nhật cài đặt thông báo cho một địa điểm cụ thể

**Authentication:** Cần user_id

**URL Parameters:**
- `location_id` (integer, required)

**Query Parameters:**
- `user_id` (integer, required)

**Request Body:**
```json
{
  "notifications_enabled": "boolean (required)"
}
```

**Success Response (200 OK):**
```json
{
  "message": "Location preferences updated successfully",
  "preferences": {
    "id": 1,
    "user": 1,
    "location": 5,
    "location_name": "Hanoi",
    "notifications_enabled": false,
    "created_at": "2024-01-15T10:00:00Z",
    "updated_at": "2024-01-15T14:30:00Z"
  }
}
```

**Error Responses:**
- `400 Bad Request` - Thiếu parameters hoặc format không hợp lệ
- `404 Not Found` - User hoặc Location không tồn tại
- `500 Internal Server Error` - Lỗi server

**Audit Logging:** Thay đổi được ghi vào PreferenceAuditLog

**Example:**
```bash
curl -X POST "http://localhost:8000/api/notifications/preferences/location/5/?user_id=1" \
  -H "Content-Type: application/json" \
  -d '{"notifications_enabled": false}'
```

---


## 7. Notification History

### 7.1 Lấy lịch sử thông báo

**Endpoint:** `GET /api/notifications/history/`

**Description:** Lấy lịch sử thông báo của user với filtering và pagination

**Authentication:** Cần user_id

**Query Parameters:**
- `user_id` (integer, required) - ID của user
- `notification_type` (string, optional) - Lọc theo loại: 'alert', 'morning_summary', 'tomorrow_forecast', 'weekly_summary'
- `start_date` (string, optional) - Ngày bắt đầu (YYYY-MM-DD)
- `end_date` (string, optional) - Ngày kết thúc (YYYY-MM-DD)
- `page` (integer, optional) - Số trang (default: 1)
- `page_size` (integer, optional) - Số items mỗi trang (default: 20, max: 100)

**Success Response (200 OK):**
```json
{
  "count": 45,
  "num_pages": 3,
  "current_page": 1,
  "page_size": 20,
  "has_next": true,
  "has_previous": false,
  "results": [
    {
      "record_id": 123,
      "user": 1,
      "location": 5,
      "location_name": "Hanoi",
      "notification_type": "alert",
      "alert": 10,
      "alert_type": "heavy_rain",
      "title": "Cảnh báo mưa lớn",
      "body": "Mưa lớn dự kiến từ 14:00 đến 18:00...",
      "priority": "high",
      "sent_at": "2024-01-15T10:30:00Z",
      "delivered": true,
      "fcm_message_id": "projects/123/messages/456"
    },
    {
      "record_id": 122,
      "user": 1,
      "location": null,
      "location_name": null,
      "notification_type": "morning_summary",
      "alert": null,
      "alert_type": null,
      "title": "Tóm tắt thời tiết buổi sáng",
      "body": "Hôm nay: Nhiệt độ 22-28°C, có mưa nhẹ...",
      "priority": "normal",
      "sent_at": "2024-01-15T07:00:00Z",
      "delivered": true,
      "fcm_message_id": "projects/123/messages/455"
    }
  ]
}
```

**Error Responses:**
- `400 Bad Request` - Thiếu user_id hoặc format không hợp lệ
- `404 Not Found` - User không tồn tại
- `500 Internal Server Error` - Lỗi server

**Examples:**
```bash
# Lấy tất cả thông báo (trang 1)
curl "http://localhost:8000/api/notifications/history/?user_id=1"

# Lọc theo loại và thời gian
curl "http://localhost:8000/api/notifications/history/?user_id=1&notification_type=alert&start_date=2024-01-01&end_date=2024-01-31"

# Pagination
curl "http://localhost:8000/api/notifications/history/?user_id=1&page=2&page_size=50"
```

---

### 7.2 Lấy chi tiết một thông báo

**Endpoint:** `GET /api/notifications/history/<record_id>/`

**Description:** Lấy chi tiết một notification record cụ thể

**Authentication:** Cần user_id

**URL Parameters:**
- `record_id` (integer, required)

**Query Parameters:**
- `user_id` (integer, required)

**Success Response (200 OK):**
```json
{
  "record_id": 123,
  "user": 1,
  "location": 5,
  "location_name": "Hanoi",
  "notification_type": "alert",
  "alert": 10,
  "alert_type": "heavy_rain",
  "title": "Cảnh báo mưa lớn",
  "body": "Mưa lớn dự kiến từ 14:00 đến 18:00 với lượng mưa 50-80mm. Nên mang theo ô và tránh di chuyển không cần thiết.",
  "priority": "high",
  "sent_at": "2024-01-15T10:30:00Z",
  "delivered": true,
  "fcm_message_id": "projects/123/messages/456"
}
```

**Error Responses:**
- `400 Bad Request` - Thiếu user_id hoặc format không hợp lệ
- `404 Not Found` - User hoặc notification record không tồn tại
- `500 Internal Server Error` - Lỗi server

**Example:**
```bash
curl "http://localhost:8000/api/notifications/history/123/?user_id=1"
```

---


## 8. Preference Audit Logs

### 8.1 Lấy audit logs của preference changes

**Endpoint:** `GET /api/notifications/preferences/audit-logs/`

**Description:** Lấy lịch sử thay đổi preferences của user với filtering và pagination

**Authentication:** Cần user_id

**Query Parameters:**
- `user_id` (integer, required) - ID của user
- `preference_type` (string, optional) - Lọc theo loại: 'global' hoặc 'location'
- `location_id` (integer, optional) - Lọc theo location cụ thể
- `start_date` (string, optional) - Ngày bắt đầu (YYYY-MM-DD)
- `end_date` (string, optional) - Ngày kết thúc (YYYY-MM-DD)
- `page` (integer, optional) - Số trang (default: 1)
- `page_size` (integer, optional) - Số items mỗi trang (default: 20, max: 100)

**Success Response (200 OK):**
```json
{
  "count": 15,
  "num_pages": 1,
  "current_page": 1,
  "page_size": 20,
  "has_next": false,
  "has_previous": false,
  "results": [
    {
      "log_id": 45,
      "preference_type": "global",
      "location_id": null,
      "location_name": null,
      "field_name": "notifications_enabled",
      "old_value": "true",
      "new_value": "false",
      "changed_at": "2024-01-15T14:30:00Z",
      "ip_address": "192.168.1.100",
      "user_agent": "Mozilla/5.0 (Android 12; Mobile)"
    },
    {
      "log_id": 44,
      "preference_type": "location",
      "location_id": 5,
      "location_name": "Hanoi",
      "field_name": "notifications_enabled",
      "old_value": "true",
      "new_value": "false",
      "changed_at": "2024-01-15T14:25:00Z",
      "ip_address": "192.168.1.100",
      "user_agent": "Mozilla/5.0 (Android 12; Mobile)"
    }
  ]
}
```

**Error Responses:**
- `400 Bad Request` - Thiếu user_id hoặc format không hợp lệ
- `404 Not Found` - User không tồn tại
- `500 Internal Server Error` - Lỗi server

**Examples:**
```bash
# Lấy tất cả audit logs
curl "http://localhost:8000/api/notifications/preferences/audit-logs/?user_id=1"

# Lọc theo loại preference
curl "http://localhost:8000/api/notifications/preferences/audit-logs/?user_id=1&preference_type=global"

# Lọc theo location và thời gian
curl "http://localhost:8000/api/notifications/preferences/audit-logs/?user_id=1&location_id=5&start_date=2024-01-01"
```

---


## 9. Testing & Admin Endpoints

### 9.1 Test Notification

**Endpoint:** `POST /api/test-notification/`

**Description:** Gửi test notification đến device của user

**Authentication:** Không yêu cầu (development only)

**Request Body:**
```json
{
  "user_id": "integer (required)",
  "title": "string (optional) - Default: 'Test Notification'",
  "body": "string (optional) - Default: 'This is a test notification'"
}
```

**Success Response (200 OK):**
```json
{
  "message": "Test notification sent",
  "result": {
    "success": true,
    "message_id": "projects/123/messages/789"
  },
  "tokens_count": 2
}
```

**Error Responses:**
- `400 Bad Request` - Thiếu user_id
- `404 Not Found` - Không tìm thấy device tokens cho user
- `500 Internal Server Error` - Lỗi khi gửi notification

**Example:**
```bash
curl -X POST http://localhost:8000/api/test-notification/ \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1,
    "title": "Test",
    "body": "This is a test message"
  }'
```

---

### 9.2 Admin Actions

**Endpoint:** `POST /api/admin/<action>/`

**Description:** Kích hoạt các tác vụ admin (protected bằng admin secret)

**Authentication:** Yêu cầu admin secret

**URL Parameters:**
- `action` (string, required) - Tên action: 'run-ingestion', 'run-analysis', 'check-alerts'

**Query Parameters:**
- `secret` (string, required) - Admin secret key

**Available Actions:**

#### 9.2.1 Run Data Ingestion
**Action:** `run-ingestion`

Kích hoạt thu thập dữ liệu thời tiết cho tất cả locations đang active.

**Success Response (200 OK):**
```json
{
  "success": true,
  "message": "Data ingestion completed",
  "locations_processed": 5,
  "records_created": 120
}
```

#### 9.2.2 Run AI Analysis
**Action:** `run-analysis`

Kích hoạt phân tích AI cho tất cả locations đang active.

**Success Response (200 OK):**
```json
{
  "success": true,
  "message": "AI analysis completed",
  "locations_analyzed": 5,
  "alerts_created": 3
}
```

#### 9.2.3 Check Alerts
**Action:** `check-alerts`

Kiểm tra cảnh báo thiên tai ngay lập tức cho tất cả locations.

**Success Response (200 OK):**
```json
{
  "success": true,
  "message": "Alert monitoring completed",
  "locations_checked": 5,
  "alerts_sent": 2
}
```

**Error Responses:**
- `403 Forbidden` - Invalid admin secret
- `404 Not Found` - Action không tồn tại
- `500 Internal Server Error` - Lỗi khi thực thi action

**Examples:**
```bash
# Run data ingestion
curl -X POST "http://localhost:8000/api/admin/run-ingestion/?secret=YOUR_ADMIN_SECRET"

# Run AI analysis
curl -X POST "http://localhost:8000/api/admin/run-analysis/?secret=YOUR_ADMIN_SECRET"

# Check alerts
curl -X POST "http://localhost:8000/api/admin/check-alerts/?secret=YOUR_ADMIN_SECRET"
```

**Security Note:** Admin secret được cấu hình trong `settings.ADMIN_SECRET`. Không share secret này trong production.

---


## 10. Root & Documentation Endpoints

### 10.1 API Root

**Endpoint:** `GET /api/`

**Description:** Kiểm tra trạng thái server và thông tin cấu hình

**Authentication:** Không yêu cầu

**Success Response (200 OK):**
```json
{
  "message": "Weather API (Django) is running in LOCAL mode.",
  "status": "OK",
  "cache": "Django LocMemCache",
  "database": "Local PostgreSQL",
  "ai_model": "Ollama - gemma3",
  "scheduler": "APScheduler Running"
}
```

**Example:**
```bash
curl http://localhost:8000/api/
```

---

### 10.2 API Schema

**Endpoint:** `GET /api/schema/`

**Description:** Lấy OpenAPI schema của API (drf-spectacular)

**Authentication:** Không yêu cầu

**Response:** OpenAPI 3.0 JSON schema

**Example:**
```bash
curl http://localhost:8000/api/schema/
```

---

### 10.3 API Documentation UI

**Endpoint:** `GET /api/docs/`

**Description:** Swagger UI để explore và test API endpoints

**Authentication:** Không yêu cầu

**Access:** Mở trong browser tại `http://localhost:8000/api/docs/`

---

## 11. Error Handling

### Common Error Patterns

#### 11.1 Validation Errors
```json
{
  "error": "Invalid preference data",
  "details": "enabled_event_types must be an array"
}
```

#### 11.2 Not Found Errors
```json
{
  "error": "User not found"
}
```

#### 11.3 Authentication Errors
```json
{
  "error": "Invalid username or password"
}
```

#### 11.4 Server Errors
```json
{
  "error": "Internal server error"
}
```

### Error Logging

Tất cả errors được log với các mức độ:
- `WARNING` - Validation errors, not found errors
- `ERROR` - Database errors, API errors
- `CRITICAL` - System failures

---

## 12. Rate Limiting & Caching

### 12.1 Caching Strategy

**Weather Data:**
- Cache key format: `{endpoint}:{location}:{days}`
- TTL: 5 phút (300 seconds)
- Cache backend: Django LocMemCache

### 12.2 Rate Limiting

**Current Implementation:**
- No rate limiting implemented
- Relies on external API rate limits (WeatherAPI.com)

**Recommendations for Production:**
- Implement per-user rate limiting
- Throttle requests per IP address
- Cache aggressively to reduce API calls

---

**Last Updated**: 2025-01-24  
**API Version**: 1.0  
**Maintained By**: Weather Forecast Teamjango LocMemCache

**Tracked Locations:**
- Cache key format: `tracked:{location_name}`
- TTL: 5 phút (300 seconds)

**AI Advice:**
- Memory cache: 3 giờ (10800 seconds)
- Database cache: Permanent (AdviceCache table)
- Cache key format: `ai_advice:{location}:{date}`

### 12.2 Rate Limiting

Hiện tại chưa implement rate limiting. Trong production nên thêm:
- Django REST Framework throttling
- Redis-based rate limiting
- Per-user và per-IP limits

---

## 13. Data Models Reference

### 13.1 Event Types (enabled_event_types)
- `heavy_rain` - Mưa lớn
- `storm` - Bão
- `extreme_heat` - Nắng nóng cực đoan
- `extreme_cold` - Rét đậm
- `moderate_rain` - Mưa vừa
- `sunny` - Nắng đẹp

### 13.2 Notification Types
- `alert` - Cảnh báo thời tiết cực đoan
- `morning_summary` - Tóm tắt buổi sáng
- `tomorrow_forecast` - Dự báo ngày mai
- `weekly_summary` - Tóm tắt tuần

### 13.3 Notification Schedules
- `24_7` - Nhận thông báo 24/7
- `daytime_only` - Chỉ nhận trong giờ ban ngày (7:00-22:00)

### 13.4 Priority Levels
- `high` - Ưu tiên cao (cảnh báo khẩn cấp)
- `normal` - Ưu tiên thường (thông báo thông thường)
- `low` - Ưu tiên thấp (thông tin tham khảo)

### 13.5 Severity Levels
- `high` - Mức độ cao (nguy hiểm)
- `medium` - Mức độ trung bình
- `low` - Mức độ thấp

---

## 14. Best Practices

### 14.1 API Usage

1. **Caching:** Tận dụng cache để giảm số lượng requests
2. **Pagination:** Sử dụng pagination cho endpoints trả về danh sách lớn
3. **Error Handling:** Luôn kiểm tra status code và xử lý errors
4. **Filtering:** Sử dụng query parameters để filter data thay vì lấy tất cả

### 14.2 Security

1. **Authentication:** Implement JWT authentication trong production
2. **HTTPS:** Luôn sử dụng HTTPS trong production
3. **Secrets:** Không hardcode API keys hoặc secrets
4. **Validation:** Validate tất cả user inputs

### 14.3 Performance

1. **Batch Requests:** Gộp nhiều requests thành một nếu có thể
2. **Selective Fields:** Chỉ request fields cần thiết
3. **Compression:** Enable gzip compression
4. **CDN:** Sử dụng CDN cho static assets

---

## 15. Migration Notes

### From Development to Production

1. **Authentication:**
   - Implement JWT authentication
   - Update all endpoints với `IsAuthenticated` permission
   - Remove `AllowAny` permissions

2. **Environment Variables:**
   - Set `DEBUG=False`
   - Configure production database
   - Set secure `SECRET_KEY`
   - Configure `ALLOWED_HOSTS`

3. **Caching:**
   - Migrate từ LocMemCache sang Redis
   - Configure cache timeouts phù hợp

4. **Rate Limiting:**
   - Implement throttling
   - Configure per-user limits

5. **Monitoring:**
   - Setup error tracking (Sentry)
   - Configure logging
   - Setup performance monitoring

---

## 16. Support & Contact

Để báo cáo bugs hoặc request features, vui lòng tạo issue trên repository hoặc liên hệ development team.

**API Version:** 1.0  
**Last Updated:** 2024-01-15  
**Documentation Generated:** Automatically from code analysis

