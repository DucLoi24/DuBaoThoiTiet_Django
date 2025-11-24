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
    from .tasks import (
        trigger_data_ingestion, 
        trigger_llm_analysis, 
        check_weather_changes_and_notify,
        monitor_all_locations_for_alerts,
        send_morning_summary_job,
        send_tomorrow_forecast_job,
        send_weekly_summary_job,
        cleanup_notification_history_job,
        cleanup_device_tokens_job
    )

    if scheduler.running:
        logger.info("APScheduler is already running.")
        return

    # Lập lịch các job
    try:
        scheduler.add_job(trigger_data_ingestion, 'cron', hour=0, minute=1, id='data_ingestion_job', replace_existing=True)
        scheduler.add_job(trigger_llm_analysis, 'cron', hour=3, minute=1, id='llm_analysis_job', replace_existing=True)
        
        # Job mới: Kiểm tra thay đổi thời tiết mỗi phút
        scheduler.add_job(check_weather_changes_and_notify, 'interval', minutes=1, id='weather_change_check_job', replace_existing=True)
        
        # Job QUAN TRỌNG: Phát hiện cảnh báo thiên tai REAL-TIME mỗi 30 phút
        scheduler.add_job(
            monitor_all_locations_for_alerts, 
            'interval', 
            minutes=30, 
            id='realtime_alert_monitor_job', 
            replace_existing=True
        )
        logger.info("⚠️ Scheduled REAL-TIME weather alert monitoring every 30 minutes")
        
        # Job thông báo định kỳ - Tóm tắt buổi sáng lúc 7:00 AM hàng ngày
        scheduler.add_job(
            send_morning_summary_job, 
            'cron', 
            hour=7, 
            minute=0, 
            id='morning_summary_job', 
            replace_existing=True
        )
        logger.info("⏰ Scheduled morning summary job at 7:00 AM daily")
        
        # Job thông báo định kỳ - Dự báo ngày mai lúc 8:00 PM hàng ngày
        scheduler.add_job(
            send_tomorrow_forecast_job, 
            'cron', 
            hour=20, 
            minute=0, 
            id='tomorrow_forecast_job', 
            replace_existing=True
        )
        logger.info("⏰ Scheduled tomorrow forecast job at 8:00 PM daily")
        
        # Job thông báo định kỳ - Tóm tắt tuần lúc 8:00 PM mỗi Chủ nhật
        scheduler.add_job(
            send_weekly_summary_job, 
            'cron', 
            day_of_week='sun',  # Chủ nhật
            hour=20, 
            minute=0, 
            id='weekly_summary_job', 
            replace_existing=True
        )
        logger.info("⏰ Scheduled weekly summary job at 8:00 PM every Sunday")
        
        # Job dọn dẹp lịch sử thông báo - Chạy lúc 2:00 AM hàng ngày
        scheduler.add_job(
            cleanup_notification_history_job,
            'cron',
            hour=2,
            minute=0,
            id='cleanup_notification_history_job',
            replace_existing=True
        )
        logger.info("⏰ Scheduled notification history cleanup job at 2:00 AM daily")
        
        # Job dọn dẹp device tokens không active - Chạy lúc 3:00 AM hàng tuần (Chủ nhật)
        scheduler.add_job(
            cleanup_device_tokens_job,
            'cron',
            day_of_week='sun',
            hour=3,
            minute=0,
            id='cleanup_device_tokens_job',
            replace_existing=True
        )
        logger.info("⏰ Scheduled device tokens cleanup job at 3:00 AM every Sunday")
        
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
