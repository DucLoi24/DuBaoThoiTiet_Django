# Hướng dẫn Cài đặt Ollama cho Phân tích Thời tiết AI

## Giới thiệu

Ollama là công cụ chạy các mô hình AI ngôn ngữ lớn (LLM) trên máy tính cá nhân. Dự án này sử dụng Ollama để phân tích dữ liệu thời tiết và đưa ra lời khuyên thông minh.

## Yêu cầu Hệ thống

### Windows
- Windows 10/11 (64-bit)
- RAM: Tối thiểu 8GB (khuyến nghị 16GB)
- Dung lượng: ~4-10GB tùy model

### macOS
- macOS 11 Big Sur trở lên
- Apple Silicon (M1/M2/M3) hoặc Intel
- RAM: Tối thiểu 8GB

### Linux
- Ubuntu 20.04+, Debian 11+, hoặc các distro tương đương
- RAM: Tối thiểu 8GB

## Bước 1: Tải và Cài đặt Ollama

### Windows

1. Truy cập: https://ollama.com/download/windows
2. Tải file `OllamaSetup.exe`
3. Chạy file cài đặt và làm theo hướng dẫn
4. Sau khi cài đặt, Ollama sẽ tự động chạy ở background

### macOS

```bash
# Cách 1: Tải từ website
# Truy cập: https://ollama.com/download/mac
# Tải file .dmg và cài đặt

# Cách 2: Dùng Homebrew
brew install ollama
```

### Linux

```bash
curl -fsSL https://ollama.com/install.sh | sh
```

## Bước 2: Kiểm tra Cài đặt

Mở Terminal/Command Prompt và chạy:

```bash
ollama --version
```

Nếu hiển thị version (ví dụ: `ollama version is 0.1.x`) thì đã cài đặt thành công.

## Bước 3: Tải Model AI

Dự án này sử dụng model **Gemma3** (4B parameters) - nhẹ và nhanh.

```bash
# Tải model Gemma 3 4B
ollama pull gemma3:4b
```

**Lưu ý:** 
- Model này có dung lượng ~3GB
- Lần đầu tải sẽ mất vài phút tùy tốc độ mạng
- Model sẽ được lưu tại:
  - Windows: `C:\Users\<username>\.ollama\models`
  - macOS/Linux: `~/.ollama/models`

### Các Model Khác (Tùy chọn)

Nếu máy bạn mạnh hơn, có thể dùng model lớn hơn:

```bash
# Llama 3.2 1B - Rất nhẹ, phù hợp máy yếu
ollama pull llama3.2:1b

# Llama 3.1 8B - Chính xác hơn nhưng nặng hơn
ollama pull llama3.1:8b

# Gemma 2 2B - Model của Google, nhẹ và nhanh
ollama pull gemma2:2b
```

## Bước 4: Kiểm tra Model

```bash
# Liệt kê các model đã tải
ollama list

# Test model
ollama run gemma3:4b "Xin chào, bạn là ai?"
```

Nếu model trả lời được thì đã hoạt động tốt.

## Bước 5: Chạy Ollama Server

### Windows
Ollama tự động chạy ở background sau khi cài đặt. Kiểm tra:

```bash
# Kiểm tra service đang chạy
curl http://localhost:11434
```

Nếu trả về `Ollama is running` thì OK.

### macOS/Linux

```bash
# Chạy Ollama server
ollama serve
```

**Lưu ý:** Để server chạy liên tục, mở terminal riêng cho lệnh này.

## Bước 6: Cấu hình Django Backend

Backend đã được cấu hình sẵn để kết nối với Ollama tại `http://localhost:11434`.

Kiểm tra file `weather_project/settings.py`:

```python
OLLAMA_API_URL = 'http://localhost:11434/api/generate'
```

## Bước 7: Test Tích hợp

1. Đảm bảo Ollama đang chạy:
```bash
curl http://localhost:11434
```

2. Chạy Django server:
```bash
cd weather_project
python manage.py runserver
```

3. Test API phân tích AI:
```bash
# Gọi API để lấy lời khuyên AI
curl http://localhost:8000/api/advice/?q=Hanoi
```

Nếu trả về lời khuyên thời tiết bằng tiếng Việt thì đã thành công!

## Xử lý Sự cố

### Lỗi: "Connection refused" khi gọi Ollama

**Nguyên nhân:** Ollama server chưa chạy

**Giải pháp:**
```bash
# Windows: Khởi động lại Ollama từ Start Menu
# macOS/Linux:
ollama serve
```

### Lỗi: Model không tìm thấy

**Nguyên nhân:** Chưa tải model

**Giải pháp:**
```bash
ollama pull gemma3:4b
```

### Lỗi: Out of memory

**Nguyên nhân:** RAM không đủ

**Giải pháp:**
- Dùng model nhỏ hơn: `ollama pull llama3.2:1b`
- Đóng các ứng dụng khác
- Nâng cấp RAM

### Ollama chạy chậm

**Giải pháp:**
- Dùng model nhỏ hơn (1B thay vì 3B)
- Đảm bảo không có nhiều ứng dụng chạy cùng lúc
- Nếu dùng laptop, cắm sạc để CPU chạy full power

## Tùy chỉnh Model

Nếu muốn thay đổi model, sửa trong `weather_project/api/tasks.py`:

```python
# Tìm dòng này
"model": "gemma3:4b",

# Đổi thành model khác
"model": "gemma3:4b",  # Hoặc model khác
```

## Tắt Ollama

### Windows
- Tìm icon Ollama ở System Tray (góc dưới phải)
- Right-click → Quit

### macOS/Linux
- Nhấn `Ctrl+C` ở terminal đang chạy `ollama serve`

## Gỡ cài đặt

### Windows
- Settings → Apps → Ollama → Uninstall
- Xóa thư mục: `C:\Users\<username>\.ollama`

### macOS
```bash
brew uninstall ollama
rm -rf ~/.ollama
```

### Linux
```bash
sudo systemctl stop ollama
sudo systemctl disable ollama
sudo rm /usr/local/bin/ollama
sudo rm -rf /usr/share/ollama
rm -rf ~/.ollama
```

## Tài liệu Tham khảo

- Website chính thức: https://ollama.com
- GitHub: https://github.com/ollama/ollama
- Danh sách models: https://ollama.com/library
- Discord community: https://discord.gg/ollama

## Lưu ý Quan trọng

1. **Ollama phải chạy liên tục** khi backend Django hoạt động
2. **Model đã tải sẽ được cache**, không cần tải lại
3. **Lần đầu chạy model** sẽ hơi chậm (load vào RAM), sau đó sẽ nhanh hơn
4. **Không cần GPU** - Ollama chạy tốt trên CPU
5. **Dữ liệu an toàn** - Tất cả xử lý local, không gửi lên cloud

## Hỗ trợ

Nếu gặp vấn đề, kiểm tra:
1. Ollama có đang chạy không: `curl http://localhost:11434`
2. Model đã tải chưa: `ollama list`
3. Log của Django: Xem terminal đang chạy `runserver`
4. Log của Ollama: Xem terminal đang chạy `ollama serve`
