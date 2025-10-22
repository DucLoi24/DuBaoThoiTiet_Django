# api/scheduler.py
from apscheduler.schedulers.background import BackgroundScheduler
from django.conf import settings
import logging
import atexit

logger = logging.getLogger(__name__)

# Khởi tạo scheduler
scheduler = BackgroundScheduler(timezone=settings.TIME_ZONE)

def start():
    """ Khởi động và lập lịch các tác vụ """
    # Import tasks ở đây để tránh lỗi circular import
    from .tasks import trigger_data_ingestion, trigger_llm_analysis # , trigger_data_pruning

    if scheduler.running:
        logger.info("APScheduler is already running.")
        return

    # Lập lịch các job
    try:
        scheduler.add_job(trigger_data_ingestion, 'cron', hour=0, minute=1, id='data_ingestion_job', replace_existing=True)
        scheduler.add_job(trigger_llm_analysis, 'cron', hour=3, minute=1, id='llm_analysis_job', replace_existing=True)
        # scheduler.add_job(trigger_data_pruning, 'cron', day_of_week='mon', hour=1, minute=1, id='data_pruning_job', replace_existing=True)

        scheduler.start()
        logger.info("⏰ APScheduler started and jobs scheduled successfully.")

        # Đảm bảo scheduler tắt khi ứng dụng dừng
        atexit.register(lambda: shutdown_scheduler())

    except Exception as e:
        logger.error(f"Error starting APScheduler or scheduling jobs: {e}")

def shutdown_scheduler():
    """ Hàm tắt scheduler một cách an toàn """
    if scheduler.running:
        logger.info("Shutting down APScheduler...")
        scheduler.shutdown()
        logger.info("APScheduler shut down.")
