# Weather Forecast Backend API

Hệ thống backend API dự báo thời tiết với tích hợp AI (Ollama), cảnh báo thiên tai thời gian thực, và hệ thống thông báo đẩy (Push Notifications) qua Firebase Cloud Messaging.

## 📋 Mục lục

- [Tổng quan](#-tổng-quan)
- [Tính năng chính](#-tính-năng-chính)
- [Công nghệ sử dụng](#-công-nghệ-sử-dụng)
- [Yêu cầu hệ thống](#-yêu-cầu-hệ-thống)
- [Cài đặt nhanh](#-cài-đặt-nhanh)
- [Cấu hình](#-cấu-hình)
- [Chạy ứng dụng](#-chạy-ứng-dụng)
- [API Documentation](#-api-documentation)
- [Tài liệu kỹ thuật](#-tài-liệu-kỹ-thuật)
- [Testing](#-testing)
- [Deployment](#-deployment)
- [Troubleshooting](#-troubleshooting)

## 🌟 Tổng quan

Weather Forecast Backend là một REST API được xây dựng bằng Django, cung cấp dịch vụ dự báo thời tiết thông minh với các tính năng:

- **Thu thập dữ liệu tự động**: Lấy dữ liệu thời tiết từ WeatherAPI.com mỗi 6 giờ
- **Phân tích AI**: Sử dụng Ollama (gemma3) để phát hiện rủi ro thời tiết và đưa ra lời khuyên
- **Cảnh báo thời gian thực**: Giám sát và cảnh báo các điều kiện nguy hiểm (mưa to, bão, nắng nóng, rét đậm)
- **Push Notifications**: Gửi thông báo đẩy qua Firebase Cloud Messaging
- **Quản lý thông báo thông minh**: Người dùng có thể tùy chỉnh loại và thời gian nhận thông báo
- **Lịch sử đầy đủ**: Lưu trữ và truy vấn lịch sử thông báo với filtering và pagination

## ✨ Tính năng chính

### 1. Quản lý người dùng
- Đăng ký và đăng nhập
- Quản lý device tokens cho push notifications
- Theo dõi nhiều địa điểm

### 2. Dữ liệu thời tiết
- Lấy thời tiết hiện tại và dự báo (1-10 ngày)
- Cache thông minh (TTL: 5 phút)
- Lưu trữ lịch sử và dự báo trong database
- Thu thập dữ liệu tự động mỗi 6 giờ

### 3. Phân tích AI
- Phân tích 14 ngày dữ liệu thời tiết
- Phát hiện rủi ro: cháy rừng, sốc nhiệt, sâu bệnh
- Đưa ra lời khuyên hành động cụ thể
- Chạy tự động mỗi 12 giờ

### 4. Cảnh báo thời tiết
- Giám sát theo thời gian thực
- Phát hiện: mưa to (>10mm/h), bão (>50km/h), nắng nóng (>37°C), rét đậm (<10°C)
- Gửi thông báo khẩn cấp tức thì
- Lưu lịch sử cảnh báo

### 5. Push Notifications
- Cảnh báo khẩn cấp (alert)
- Tóm tắt buổi sáng (7:00 AM)
- Dự báo ngày mai (8:00 PM)
- Tóm tắt tuần (Chủ nhật 6:00 PM)

### 6. Tùy chỉnh thông báo
- Bật/tắt thông báo toàn cục
- Chọn loại sự kiện nhận thông báo
- Lịch thông báo: 24/7 hoặc chỉ ban ngày
- Tùy chỉnh cho từng địa điểm
- Audit log đầy đủ

## 🛠 Công nghệ sử dụng

### Core Framework
- **Django 5.2.7** - Web framework
- **Django REST Framework 3.16.1** - REST API
- **PostgreSQL** - Database chính
- **psycopg2-binary 2.9.11** - PostgreSQL adapter

### Authentication & Security
- **PyJWT 2.10.1** - JWT token
- **bcrypt 5.0.0** - Password hashing
- **cryptography 46.0.3** - Cryptographic operations

### Background Jobs
- **APScheduler 3.11.0** - Job scheduling
- **django-apscheduler 0.7.0** - Django integration

### External Services
- **requests 2.32.5** - HTTP client
- **httpx 0.28.1** - Async HTTP client
- **firebase-admin 7.1.0** - Firebase Cloud Messaging

### API Documentation
- **drf-spectacular 0.28.0** - OpenAPI 3.0 schema

### Testing
- **pytest 9.0.1** - Testing framework
- **pytest-django 4.11.1** - Django integration

### Utilities
- **python-dotenv 1.1.1** - Environment variables
- **colorama 0.4.6** - Colored terminal output

## 📦 Yêu cầu hệ thống

- **Python**: 3.10 hoặc cao hơn
- **PostgreSQL**: 12 hoặc cao hơn
- **Ollama**: Cài đặt và chạy local với model gemma3:4b
- **Firebase**: Project với FCM enabled và service account key

## 🚀 Cài đặt nhanh

### 1. Clone repository

```bash
git clone <repository-url>
cd weather_project
```

### 2. Tạo virtual environment

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# hoặc
.venv\Scripts\activate  # Windows
```

### 3. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

### 4. Cài đặt PostgreSQL

Tạo database:
```sql
CREATE DATABASE weather_db;
CREATE USER weather_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE weather_db TO weather_user;
```

### 5. Cài đặt Ollama

Xem hướng dẫn chi tiết trong [OLLAMA_SETUP_GUIDE.md](OLLAMA_SETUP_GUIDE.md)

```bash
# Cài đặt Ollama
curl -fsSL https://ollama.com/install.sh | sh

# Pull model gemma3:4b
ollama pull gemma3:4b

# Chạy Ollama server
ollama serve
```

### 6. Cài đặt Firebase

Xem hướng dẫn chi tiết trong [FIREBASE_SETUP.md](FIREBASE_SETUP.md)

1. Tạo Firebase project
2. Enable Firebase Cloud Messaging
3. Tạo service account và download key
4. Đặt file key vào `firebase-service-account.json`

### 7. Cấu hình environment variables

Tạo file `.env` từ `.env.example`:
```bash
cp .env.example .env
```

Chỉnh sửa `.env` với thông tin của bạn:
```env
DJANGO_SECRET_KEY=your-secret-key-here
DEBUG=True

DB_NAME=weather_db
DB_USER=weather_user
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432

WEATHER_API_KEY=your-weatherapi-key
FIREBASE_PROJECT_ID=your-firebase-project-id
```

### 8. Chạy migrations

```bash
python manage.py migrate
```

### 9. Tạo superuser (optional)

```bash
python manage.py createsuperuser
```

## ⚙️ Cấu hình

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `DJANGO_SECRET_KEY` | Django secret key | Yes | - |
| `DEBUG` | Debug mode | No | False |
| `DB_NAME` | Database name | Yes | - |
| `DB_USER` | Database user | Yes | - |
| `DB_PASSWORD` | Database password | Yes | - |
| `DB_HOST` | Database host | No | localhost |
| `DB_PORT` | Database port | No | 5432 |
| `WEATHER_API_KEY` | WeatherAPI.com API key | Yes | - |
| `FIREBASE_PROJECT_ID` | Firebase project ID | Yes | - |
| `SCHEDULER_TIMEZONE` | Timezone cho scheduler | No | Asia/Ho_Chi_Minh |
| `NOTIFICATION_HISTORY_RETENTION_DAYS` | Số ngày giữ lịch sử thông báo | No | 90 |
| `MAX_DEVICE_TOKENS_PER_USER` | Số token tối đa mỗi user | No | 5 |

### Firebase Configuration

Đặt file `firebase-service-account.json` ở thư mục gốc project. File này chứa credentials để gửi push notifications.

**⚠️ Lưu ý**: File này đã được thêm vào `.gitignore` và không được commit lên git.

### Ollama Configuration

Ollama phải chạy ở `http://localhost:11434` với model `gemma3:4b` đã được pull.

Kiểm tra Ollama:
```bash
curl http://localhost:11434/api/tags
```

## 🏃 Chạy ứng dụng

### Development Mode

```bash
python manage.py runserver
```

Server sẽ chạy tại `http://localhost:8000`

### Với Scheduler

Scheduler sẽ tự động chạy khi start server. Các jobs được lập lịch:

- **Data Ingestion**: Mỗi 6 giờ
- **AI Analysis**: Mỗi 12 giờ
- **Morning Summary**: 7:00 AM
- **Tomorrow Forecast**: 8:00 PM
- **Weekly Summary**: Chủ nhật 6:00 PM

### Chạy manual jobs

```bash
# Trigger data ingestion
curl -X POST "http://localhost:8000/api/admin/run-ingestion/?secret=YOUR_ADMIN_SECRET"

# Trigger AI analysis
curl -X POST "http://localhost:8000/api/admin/run-analysis/?secret=YOUR_ADMIN_SECRET"

# Check alerts
curl -X POST "http://localhost:8000/api/admin/check-alerts/?secret=YOUR_ADMIN_SECRET"
```

## 📚 API Documentation

### Interactive Documentation

- **Swagger UI**: http://localhost:8000/api/docs/
- **OpenAPI Schema**: http://localhost:8000/api/schema/

### API Endpoints Overview

#### Authentication
- `POST /api/register/` - Đăng ký user mới
- `POST /api/login/` - Đăng nhập

#### Weather Data
- `GET /api/weather/` - Lấy dữ liệu thời tiết
- `GET /api/alerts/` - Lấy cảnh báo cực đoan
- `GET /api/advice/` - Lấy lời khuyên AI
- `GET /api/check-advice/` - Kiểm tra lời khuyên gần đây

#### Location Tracking
- `POST /api/locations/track/` - Theo dõi địa điểm
- `GET /api/locations/tracked/` - Lấy danh sách địa điểm
- `DELETE /api/locations/delete/` - Xóa địa điểm

#### Notifications
- `POST /api/device-token/register/` - Đăng ký device token
- `GET/PUT /api/notifications/preferences/` - Quản lý preferences
- `GET /api/notifications/history/` - Lịch sử thông báo
- `GET /api/notifications/preferences/audit-logs/` - Audit logs

#### Admin
- `POST /api/admin/<action>/` - Admin actions

Xem chi tiết trong [docs/API_DOCUMENTATION.md](docs/API_DOCUMENTATION.md)

## 📖 Tài liệu kỹ thuật

- **[System Design](docs/SYSTEM_DESIGN.md)** - Kiến trúc hệ thống, components, data flow
- **[Database Schema](docs/DATABASE.md)** - ERD, tables, relationships, indexes
- **[API Documentation](docs/API_DOCUMENTATION.md)** - Chi tiết tất cả endpoints
- **[Flows](docs/FLOWS.md)** - Sequence diagrams cho các flows chính
- **[Algorithms](docs/ALGORITHMS.md)** - Thuật toán weather monitoring, notification scheduling
- **[Project Structure](docs/PROJECT_STRUCTURE.md)** - Cấu trúc thư mục và modules
- **[Testing](docs/TESTING.md)** - Test cases và testing strategy

## 🧪 Testing

### Chạy tests

```bash
# Chạy tất cả tests
pytest

# Chạy với coverage
pytest --cov=api --cov-report=html

# Chạy specific test file
pytest test_unit/test_models.py

# Chạy với verbose output
pytest -v
```

### Test Structure

```
test_unit/
├── test_models.py          # Model tests
├── test_views.py           # API endpoint tests
├── test_serializers.py     # Serializer tests
├── test_tasks.py           # Background task tests
├── test_weather_monitor.py # Weather monitoring tests
└── test_notifications.py   # Notification tests
```

Xem chi tiết trong [docs/TESTING.md](docs/TESTING.md)

## 🚢 Deployment

### Production Checklist

- [ ] Set `DEBUG=False`
- [ ] Configure proper `SECRET_KEY`
- [ ] Set up production database (PostgreSQL)
- [ ] Configure `ALLOWED_HOSTS`
- [ ] Set up proper logging
- [ ] Configure static files serving
- [ ] Set up HTTPS
- [ ] Configure Firebase service account
- [ ] Set up Ollama service
- [ ] Configure APScheduler for production
- [ ] Set up monitoring and alerting
- [ ] Configure backup strategy

### Environment Setup

```bash
# Production environment variables
DEBUG=False
DJANGO_SECRET_KEY=<strong-secret-key>
ALLOWED_HOSTS=yourdomain.com,www.yourdomain.com

# Database
DB_NAME=weather_db_prod
DB_USER=weather_user_prod
DB_PASSWORD=<strong-password>
DB_HOST=your-db-host
DB_PORT=5432

# External services
WEATHER_API_KEY=<your-key>
FIREBASE_PROJECT_ID=<your-project-id>
```

### Collect Static Files

```bash
python manage.py collectstatic --noinput
```

### Run with Gunicorn

```bash
pip install gunicorn
gunicorn weather_project.wsgi:application --bind 0.0.0.0:8000 --workers 4
```

## 🔧 Troubleshooting

### Database Connection Issues

```bash
# Kiểm tra PostgreSQL đang chạy
sudo systemctl status postgresql

# Kiểm tra connection
psql -U weather_user -d weather_db -h localhost
```

### Ollama Issues

```bash
# Kiểm tra Ollama đang chạy
curl http://localhost:11434/api/tags

# Restart Ollama
pkill ollama
ollama serve

# Pull model lại
ollama pull gemma3:4b
```

### Firebase Issues

```bash
# Kiểm tra file service account
ls -la firebase-service-account.json

# Validate JSON
python -m json.tool firebase-service-account.json
```

### Scheduler Issues

```bash
# Xem logs của scheduler
python manage.py shell
>>> from django_apscheduler.models import DjangoJob
>>> DjangoJob.objects.all()
```

### Cache Issues

```bash
# Clear cache
python manage.py shell
>>> from django.core.cache import cache
>>> cache.clear()
```

## 📝 License

Private Project

## 👥 Contributors

- Development Team

## 📞 Support

Nếu gặp vấn đề, vui lòng:
1. Kiểm tra [Troubleshooting](#-troubleshooting)
2. Xem [Documentation](docs/)
3. Liên hệ team

---

**Last Updated**: 2025-01-24
