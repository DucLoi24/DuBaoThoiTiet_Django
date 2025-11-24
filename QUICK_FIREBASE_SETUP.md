# 🚀 Hướng dẫn Setup Firebase NHANH (10 phút)

## Bước 1: Tạo Firebase Project (3 phút)

1. Vào https://console.firebase.google.com/
2. Click "Add project" → Tên: "DuBaoThoiTiet" → Create

## Bước 2: Thêm Android App (3 phút)

1. Click icon Android
2. Package name: `com.example.dubaothoitiet`
3. Download `google-services.json`
4. **Copy vào: `DuBaoThoiTiet/app/google-services.json`**

## Bước 3: Lấy Service Account Key (4 phút)

1. Click icon ⚙️ → Project settings
2. Tab "Service accounts"
3. Click "Generate new private key" → "Generate key"
4. File JSON sẽ được tải xuống
5. **Đổi tên thành: `firebase-service-account.json`**
6. **Copy vào: `weather_project/firebase-service-account.json`** (cùng cấp manage.py)

## Bước 4: Cập nhật Android Gradle (5 phút)

### File `build.gradle.kts` (Project level):
```kotlin
plugins {
    // ... các plugins khác
    id("com.google.gms.google-services") version "4.4.0" apply false
}
```

### File `app/build.gradle.kts`:
```kotlin
plugins {
    // ... các plugins khác
    id("com.google.gms.google-services")
}

dependencies {
    // ... các dependencies khác
    
    // Firebase
    implementation(platform("com.google.firebase:firebase-bom:32.7.0"))
    implementation("com.google.firebase:firebase-messaging-ktx")
}
```

### Sync Gradle:
Click "Sync Now" trong Android Studio

## Bước 5: Test! (2 phút)

1. **Restart Django server**:
   ```bash
   cd weather_project
   .venv\Scripts\Activate.ps1
   python manage.py runserver
   ```

2. **Build & Run Android app**

3. **Test**:
   - Đăng nhập
   - Theo dõi một vị trí
   - Đợi 1-2 phút
   - Nhận notification! 🎉

---

## ✅ Kiểm tra thành công:

**Django log:**
```
[WEATHER CHANGE CHECK] Starting weather change check...
Firebase Admin SDK initialized successfully
[WEATHER CHANGE] Hanoi: Partly cloudy → Sunny
FCM notification sent successfully
```

**Android Logcat:**
```
D/FCMService: Message received
D/FCMService: Notification title: 🌤️ Thời tiết Hanoi thay đổi
```

---

## ⚠️ Lưu ý quan trọng:

1. **File `firebase-service-account.json` PHẢI ở đúng vị trí**: `weather_project/firebase-service-account.json`
2. **File `google-services.json` PHẢI ở đúng vị trí**: `DuBaoThoiTiet/app/google-services.json`
3. **KHÔNG commit 2 file này lên Git** (đã có trong .gitignore)

---

## 🔧 Troubleshooting:

**Django báo "Firebase service account JSON file not found"?**
→ Kiểm tra file `firebase-service-account.json` đã đúng vị trí chưa

**Android không build được?**
→ Kiểm tra `google-services.json` và Sync Gradle lại

**Không nhận notification?**
→ Xem Django log có lỗi gì không

---

**Xong! Hệ thống sẽ tự động gửi notification mỗi khi thời tiết thay đổi!** 🎉
