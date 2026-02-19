import time
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from backend.etl_pipeline import run_etl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def job():
    logger.info("Scheduler triggering ETL job...")
    try:
        run_etl()
    except Exception as e:
        logger.error(f"ETL job failed: {e}")

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    # Schedule to run daily at 12:30 UTC (18:00 IST)
    scheduler.add_job(job, 'cron', hour=12, minute=30)
    
    logger.info("Scheduler started. Running ETL daily at 12:30 UTC (18:00 IST). Press Ctrl+C to exit.")
    
    # Run immediately on startup for verification
    logger.info("Running initial ETL job...")
    job()
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
