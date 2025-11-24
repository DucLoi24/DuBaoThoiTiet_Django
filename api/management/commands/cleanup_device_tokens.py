# api/management/commands/cleanup_device_tokens.py
from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from api.models import DeviceToken
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Dọn dẹp các device tokens không active hoặc quá cũ'

    def add_arguments(self, parser):
        parser.add_argument(
            '--days',
            type=int,
            default=90,
            help='Xóa tokens không active quá X ngày (mặc định: 90)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Chỉ hiển thị tokens sẽ bị xóa mà không thực sự xóa'
        )

    def handle(self, *args, **options):
        days = options['days']
        dry_run = options['dry_run']
        
        cutoff_date = timezone.now() - timedelta(days=days)
        
        self.stdout.write(self.style.WARNING(
            f'Đang tìm device tokens không active từ trước {cutoff_date.strftime("%Y-%m-%d %H:%M:%S")}...'
        ))
        
        # Tìm các tokens không active và quá cũ
        old_inactive_tokens = DeviceToken.objects.filter(
            is_active=False,
            updated_at__lt=cutoff_date
        )
        
        count = old_inactive_tokens.count()
        
        if count == 0:
            self.stdout.write(self.style.SUCCESS('✓ Không có token nào cần dọn dẹp'))
            return
        
        self.stdout.write(f'Tìm thấy {count} tokens không active cần xóa:')
        
        # Hiển thị thông tin tokens sẽ bị xóa
        for token in old_inactive_tokens[:10]:  # Hiển thị tối đa 10 tokens
            self.stdout.write(
                f'  - Token ID: {token.token_id}, User: {token.user_id}, '
                f'Updated: {token.updated_at.strftime("%Y-%m-%d")}'
            )
        
        if count > 10:
            self.stdout.write(f'  ... và {count - 10} tokens khác')
        
        if dry_run:
            self.stdout.write(self.style.WARNING(
                '\n[DRY RUN] Không xóa tokens. Chạy lại không có --dry-run để xóa thực sự.'
            ))
        else:
            # Xóa tokens
            deleted_count, _ = old_inactive_tokens.delete()
            
            self.stdout.write(self.style.SUCCESS(
                f'\n✓ Đã xóa {deleted_count} device tokens không active'
            ))
            
            logger.info(f'[CLEANUP] Deleted {deleted_count} inactive device tokens older than {days} days')
        
        # Thống kê tokens hiện tại
        active_count = DeviceToken.objects.filter(is_active=True).count()
        inactive_count = DeviceToken.objects.filter(is_active=False).count()
        
        self.stdout.write('\nThống kê device tokens:')
        self.stdout.write(f'  - Active: {active_count}')
        self.stdout.write(f'  - Inactive: {inactive_count}')
        self.stdout.write(f'  - Tổng: {active_count + inactive_count}')
