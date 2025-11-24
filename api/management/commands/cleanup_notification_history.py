# api/management/commands/cleanup_notification_history.py
"""
Django management command để dọn dẹp lịch sử thông báo cũ

Sử dụng:
    python manage.py cleanup_notification_history
    python manage.py cleanup_notification_history --days 60
"""
from django.core.management.base import BaseCommand
from api.notification_service import NotificationService


class Command(BaseCommand):
    help = 'Dọn dẹp các bản ghi thông báo cũ hơn số ngày được chỉ định (mặc định 90 ngày)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--days',
            type=int,
            default=90,
            help='Số ngày lưu trữ lịch sử thông báo (mặc định: 90)'
        )

    def handle(self, *args, **options):
        retention_days = options['days']
        
        self.stdout.write(
            self.style.WARNING(
                f'Bắt đầu dọn dẹp lịch sử thông báo cũ hơn {retention_days} ngày...'
            )
        )
        
        # Khởi tạo NotificationService và chạy cleanup
        service = NotificationService()
        result = service.cleanup_old_notification_records(retention_days=retention_days)
        
        # Hiển thị kết quả
        self.stdout.write(
            self.style.SUCCESS(
                f'✓ Đã xóa {result["deleted_count"]} bản ghi thông báo'
            )
        )
        self.stdout.write(
            self.style.SUCCESS(
                f'✓ Ngày cutoff: {result["cutoff_date"]}'
            )
        )
        self.stdout.write(
            self.style.SUCCESS(
                f'✓ Hoàn thành dọn dẹp lịch sử thông báo'
            )
        )
