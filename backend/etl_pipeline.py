import os
import requests
import logging
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
METALS_API_KEY = os.getenv("METALS_DEV_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
API_URL = "https://api.metals.dev/v1/latest"

if not all([METALS_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
    logger.error("Missing environment variables. Please check .env file.")
    exit(1)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_metals_data():
    """Fetch latest metal prices from metals.dev API."""
    try:
        response = requests.get(API_URL, params={"api_key": METALS_API_KEY, "currency": "USD", "unit": "toz"})
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching data from API: {e}")
        return None

def get_or_create_dimension(table, column, value, additional_data=None):
    """Get ID from dimension table or create if not exists."""
    try:
        # Try to find existing
        response = supabase.table(table).select("id").eq(column, value).execute()
        if response.data:
            return response.data[0]['id']
        
        # Create new
        data = {column: value}
        if additional_data:
            data.update(additional_data)
            
        response = supabase.table(table).insert(data).execute()
        if response.data:
            logger.info(f"Created new entry in {table}: {value}")
            return response.data[0]['id']
        return None
    except Exception as e:
        logger.error(f"Error in dimension lookup/creation for {table}: {e}")
        return None

def get_or_create_time_id(timestamp_dt):
    """Generate time_id and ensure entry exists in dim_time."""
    # Create integer ID: YYYYMMDDHH
    time_id = int(timestamp_dt.strftime("%Y%m%d%H"))
    
    try:
        # Check if exists (optimization: check mostly not needed if we trust our logic, but safer)
        # Using Supabase upsert for dim_time might be cleaner if we didn't have other columns to calculate
        
        # We need to act carefully with big integers and Supabase JS/Py clients sometimes. 
        # But here we are just sending it.
        
        # Let's try to insert, on conflict do nothing? 
        # Supabase-py 'upsert' works.
        
        record = {
            "id": time_id,
            "timestamp": timestamp_dt.isoformat(),
            "date": timestamp_dt.date().isoformat(),
            "day": timestamp_dt.day,
            "month": timestamp_dt.month,
            "year": timestamp_dt.year,
            "hour": timestamp_dt.hour
        }
        
        # Upsert: if id exists, update (or ignore if we want). usage: upsert(data, on_conflict='id')
        response = supabase.table("dim_time").upsert(record).execute()
        return time_id
        
    except Exception as e:
        logger.error(f"Error managing dim_time: {e}")
        return None

def run_etl():
    logger.info("Starting ETL process...")
    
    # 1. Extract
    data = fetch_metals_data()
    if not data or 'metals' not in data:
        logger.error("No valid data received from API.")
        return

    timestamp_str = data.get('timestamps', {}).get('metal', datetime.utcnow().isoformat())
    # Handle API timestamp or fallback to now
    try:
        timestamp_dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except:
        timestamp_dt = datetime.utcnow()

    time_id = get_or_create_time_id(timestamp_dt)
    if not time_id:
        logger.error("Failed to generate Time ID.")
        return

    metals_map = data.get('metals', {})
    
    # 2. Transform & Load
    
    # Pre-fetch Market IDs
    market_id_spot = get_or_create_dimension("dim_market", "market_name", "Spot")
    market_id_mcx = get_or_create_dimension("dim_market", "market_name", "MCX")
    market_id_lbma = get_or_create_dimension("dim_market", "market_name", "LBMA")
    
    # Define categorization logic (Customize based on actual API keys if they differ)
    # Standard metals.dev 'latest' returns spot prices in 'metals'.
    # If using a plan that includes MCX/LBMA, they might appear as specific keys or separate objects.
    # We will assume 'metals' contains Spot prices primarily.
    
    for metal_key, price in metals_map.items():
        # logic to distinguish markets if keys indicate it (e.g. 'gold_mcx')
        # Otherwise default to Spot
        
        market_id = market_id_spot
        clean_metal_name = metal_key
        
        if '_mcx' in metal_key:
            market_id = market_id_mcx
            clean_metal_name = metal_key.replace('_mcx', '')
        elif '_lbma' in metal_key or '_am' in metal_key or '_pm' in metal_key:
            market_id = market_id_lbma
            clean_metal_name = metal_key.replace('_lbma', '').replace('_am', '').replace('_pm', '')
        
        clean_metal_name = clean_metal_name.capitalize()
        
        # Get Metal ID
        metal_id = get_or_create_dimension("dim_metal", "metal_name", clean_metal_name)
        
        if metal_id and market_id and time_id:
            fact_record = {
                "metal_id": metal_id,
                "market_id": market_id,
                "time_id": time_id,
                "price": price,
                "currency": "USD",
                "unit": "toz"
            }
            
            try:
                # Upsert is safer for idempotency
                # We use the unique constraint on (metal_id, market_id, time_id)
                supabase.table("fact_metal_prices").upsert(
                    fact_record, 
                    on_conflict="metal_id,market_id,time_id"
                ).execute()
                logger.debug(f"Processed {clean_metal_name} ({market_id}) : {price}")
            except Exception as e:
                logger.error(f"Error inserting {clean_metal_name}: {e}")

    logger.info("ETL process completed successfully.")

if __name__ == "__main__":
    run_etl()
