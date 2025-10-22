# api/apps.py
from django.apps import AppConfig
import os
import logging

logger = logging.getLogger(__name__)

class ApiConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'api'

    def ready(self):
        """ Được gọi khi app sẵn sàng """
        # Kiểm tra biến môi trường RUN_MAIN để tránh chạy scheduler nhiều lần
        # (Ví dụ: khi chạy lệnh manage.py hoặc trong quá trình reload)
        run_once = os.environ.get('APPSCHEDULER_RUN_ONCE', None)
        if run_once is None:
            os.environ['APPSCHEDULER_RUN_ONCE'] = 'true' # Đặt cờ
            logger.info("Attempting to start scheduler...")
            from . import scheduler # Import và chạy scheduler
            scheduler.start()
        else:
             logger.info("Scheduler start skipped (already started or not main process).")
