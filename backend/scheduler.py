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
    # Schedule to run every 1 hour
    scheduler.add_job(job, 'interval', hours=1)
    
    logger.info("Scheduler started. Running ETL every 1 hour. Press Ctrl+C to exit.")
    
    # Run immediately on startup? 
    # User might want to ensure data is fresh on start, but let's stick to interval.
    # Actually, often good to run once on start.
    logger.info("Running initial ETL job...")
    job()
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
