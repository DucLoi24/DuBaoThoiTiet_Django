a) Kiểm tra Server:
Method: GET
URL: http://127.0.0.1:8000/
Kết quả: JSON xác nhận server đang chạy.
b) Đăng ký User:
Method: POST
URL: http://127.0.0.1:8000/api/register/
Body: Chọn raw, JSON. Nhập:
{
    "username": "testuser",
    "password": "yourpassword"
}


Kết quả: 201 Created. Kiểm tra bảng "Users" trong CSDL.
c) Đăng nhập User:
Method: POST
URL: http://127.0.0.1:8000/api/login/
Body: Chọn raw, JSON. Nhập:
{
    "username": "testuser",
    "password": "yourpassword"
}


Kết quả: 200 OK với thông tin user (không có mật khẩu).
d) Lấy Thời tiết Hiện tại:
Method: GET
URL: http://127.0.0.1:8000/api/weather/?q=Hanoi
Kết quả: JSON dữ liệu thời tiết. Kiểm tra log server để xem CACHE MISS (lần đầu) hoặc CACHE HIT (các lần sau trong 5 phút).
e) Theo dõi Vị trí (Test SQL):
Method: POST
URL: http://127.0.0.1:8000/api/locations/track/
Body: Chọn raw, JSON. Nhập (thay user_id bằng ID thực tế bạn có sau khi đăng ký):
{
    "name_en": "Hanoi",
    "latitude": 21.0285,
    "longitude": 105.8542,
    "user_id": 1
}


Kết quả: 201 Created. Kiểm tra bảng "Locations" trong CSDL.
f) Kích hoạt Thu thập Dữ liệu (Test Cron Job 1):
Method: POST
URL: http://127.0.0.1:8000/api/admin/run-ingestion/?secret=your_admin_secret_value (Thay your_admin_secret_value bằng giá trị ADMIN_SECRET trong .env)
Kết quả: 200 OK. Kiểm tra log server và bảng "WeatherData" trong CSDL.
g) Kích hoạt Phân tích AI (Test Cron Job 2):
Method: POST
URL: http://127.0.0.1:8000/api/admin/run-analysis/?secret=your_admin_secret_value
Kết quả: 200 OK. Kiểm tra log server (xem có gọi Ollama không) và bảng "ExtremeEvents" trong CSDL.