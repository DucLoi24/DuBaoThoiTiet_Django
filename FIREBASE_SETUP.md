# Hướng dẫn Setup Firebase Cloud Messaging

## Bước 1: Tạo Firebase Project

1. Truy cập https://console.firebase.google.com/
2. Click "Add project" (Thêm dự án)
3. Đặt tên project: "DuBaoThoiTiet"
4. Tắt Google Analytics (không cần thiết)
5. Click "Create project"

## Bước 2: Thêm Android App vào Firebase

1. Trong Firebase Console, click icon Android
2. Nhập package name: `com.example.dubaothoitiet`
3. Nhập App nickname: "Du Bao Thoi Tiet"
4. Click "Register app"
5. **TẢI FILE `google-services.json`** và lưu lại

## Bước 3: Tạo Service Account Key

1. Trong Firebase Console, click icon ⚙️ (Settings) → Project settings
2. Chọn tab "Service accounts"
3. Click "Generate new private key"
4. Click "Generate key" để tải file JSON
5. **Đổi tên file thành `firebase-service-account.json`**
6. **Copy file vào thư mục `weather_project/`** (cùng cấp với manage.py)

## Bước 4: Cài đặt Django packages (ĐÃ XONG)

✅ Firebase Admin SDK đã được cài đặt

## Bước 5: Chạy migrations (ĐÃ XONG)

✅ Migrations đã được chạy

## Bước 6: Restart Django server

```bash
python manage.py makemigrations
python manage.py migrate
```

Server sẽ tự động:
- Khởi tạo Firebase Admin SDK
- Bắt đầu check thời tiết mỗi phút
- Gửi push notifications khi có thay đổi!

---

## Kiểm tra hoạt động

Xem log Django để kiểm tra:
```
[WEATHER CHANGE CHECK] Starting weather change check...
[WEATHER CHANGE] Hanoi: Partly cloudy → Sunny
```

Nếu thấy log này nghĩa là hệ thống đang hoạt động!
