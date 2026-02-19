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
    logger.info("Seeding Silver INR historical data...")
    
    # Fetch existing IDs
    metals_resp = supabase.table("dim_metal").select("id, metal_name").execute()
    markets_resp = supabase.table("dim_market").select("id, market_name").execute()
    
    if not metals_resp.data or not markets_resp.data:
        logger.error("Dimensions not populated. Run ETL first.")
        return

    # Filter strictly for Silver and Markets
    silver_id = next((m['id'] for m in metals_resp.data if 'Silver' in m['metal_name']), None)
    
    spot_id = next((m['id'] for m in markets_resp.data if 'Spot' in m['market_name']), None)
    mcx_id = next((m['id'] for m in markets_resp.data if 'MCX' in m['market_name']), None)
    
    if not silver_id:
        logger.error("Silver ID not found.")
        return

    # Base prices (INR per toz approx)
    # 1 toz = 31.1035 grams
    # Silver price approx ₹75,000 / kg -> ₹75 / gram
    # 1 toz = 31.1 * 75 = ₹2332 approx.
    # Let's start around ₹2300 INR
    current_price_spot = 2300.0
    current_price_mcx = 2350.0  # slight premium/difference
    
    # Generate 60 days of Daily data (since we moved to daily)
    # Actually, keep hourly points to show charts better, or daily?
    # User asked for "update daily".
    # I will generate 4 points per day for 90 days to give a good chart.
    
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=90)
    current_date = start_date
    
    records = []
    
    while current_date <= end_date:
        time_id = get_or_create_time_id(current_date)
        if not time_id:
            current_date += timedelta(hours=6)
            continue
            
        # Random walk
        change_pct = random.uniform(-0.02, 0.02) # +/- 2%
        current_price_spot *= (1 + change_pct)
        current_price_mcx = current_price_spot * random.uniform(1.01, 1.05) # MCX premium
        
        # Spot Record
        records.append({
            "metal_id": silver_id,
            "market_id": spot_id,
            "time_id": time_id,
            "price": round(current_price_spot, 2),
            "currency": "INR",
            "unit": "toz"
        })
        
        # MCX Record
        if mcx_id:
            records.append({
                "metal_id": silver_id,
                "market_id": mcx_id,
                "time_id": time_id,
                "price": round(current_price_mcx, 2),
                "currency": "INR",
                "unit": "toz"
            })
        
        if len(records) >= 100:
            try:
                # Note: We now have currency in unique constraint hopefully
                supabase.table("fact_metal_prices").upsert(
                    records, 
                    on_conflict="metal_id,market_id,time_id,currency"
                ).execute()
                records = []
            except Exception as e:
                logger.error(f"Batch insert failed: {e}")
        
        current_date += timedelta(hours=6)
        
    if records:
        supabase.table("fact_metal_prices").upsert(records, on_conflict="metal_id,market_id,time_id,currency").execute()

    logger.info("Seeding complete.")

if __name__ == "__main__":
    seed_history()
