import os
import random
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY]):
    logger.error("Missing Supabase credentials.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_or_create_time_id(timestamp_dt):
    time_id = int(timestamp_dt.strftime("%Y%m%d%H"))
    record = {
        "id": time_id,
        "timestamp": timestamp_dt.isoformat(),
        "date": timestamp_dt.date().isoformat(),
        "day": timestamp_dt.day,
        "month": timestamp_dt.month,
        "year": timestamp_dt.year,
        "hour": timestamp_dt.hour
    }
    try:
        supabase.table("dim_time").upsert(record).execute()
        return time_id
    except Exception as e:
        logger.error(f"Error creating time {time_id}: {e}")
        return None

def seed_history():
    logger.info("Seeding historical data...")
    
    # Fetch existing IDs
    metals_resp = supabase.table("dim_metal").select("id, metal_name").execute()
    markets_resp = supabase.table("dim_market").select("id, market_name").execute()
    
    if not metals_resp.data or not markets_resp.data:
        logger.error("Dimensions not populated. Run ETL first.")
        return

    metals = {m['metal_name']: m['id'] for m in metals_resp.data}
    markets = {m['market_name']: m['id'] for m in markets_resp.data}
    
    # Base prices
    base_prices = {
        'Gold': 2000.0,
        'Silver': 23.0,
        'Platinum': 900.0,
        'Palladium': 1100.0
    }
    
    # Generate 30 days of hourly data
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=30)
    current_date = start_date
    
    records = []
    
    while current_date <= end_date:
        time_id = get_or_create_time_id(current_date)
        if not time_id:
            current_date += timedelta(hours=1)
            continue
            
        # Random walk for prices
        for metal_name, metal_id in metals.items():
            base = base_prices.get(metal_name, 100.0)
            
            # Create a localized trend (random walk)
            volatility = 0.02 if metal_name == 'Silver' else 0.01
            change = random.uniform(-volatility, volatility)
            price = base * (1 + change)
            
            # Update base for next iteration to simulate trend
            base_prices[metal_name] = price 
            
            for market_name, market_id in markets.items():
                # Slight variation per market
                market_price = price * random.uniform(0.998, 1.002)
                
                records.append({
                    "metal_id": metal_id,
                    "market_id": market_id,
                    "time_id": time_id,
                    "price": round(market_price, 2),
                    "currency": "USD",
                    "unit": "toz"
                })
        
        # Batch insert every 24 hours (approx 24 * 4 * 3 = 288 records)
        if len(records) >= 100:
            try:
                supabase.table("fact_metal_prices").upsert(records, on_conflict="metal_id,market_id,time_id").execute()
                logger.info(f"Seeded data for {current_date}")
                records = []
            except Exception as e:
                logger.error(f"Batch insert failed: {e}")
        
        current_date += timedelta(hours=4) # 4-hour intervals to save time/API calls
        
    # Final batch
    if records:
        supabase.table("fact_metal_prices").upsert(records, on_conflict="metal_id,market_id,time_id").execute()

    logger.info("Seeding complete.")

if __name__ == "__main__":
    seed_history()
