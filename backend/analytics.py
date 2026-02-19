import os
import pandas as pd
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY]):
    logger.error("Missing Supabase credentials.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_data_for_analytics(currency="INR"):
    """
    Fetch price data for Silver in specific currency.
    Returns a pandas DataFrame.
    """
    try:
        response = supabase.table("fact_metal_prices").select(
            "price, currency, metal_id, market_id, time_id, dim_metal(metal_name), dim_market(market_name), dim_time(timestamp, date)"
        ).eq("currency", currency).execute()
        
        data = response.data
        if not data:
            return pd.DataFrame()
            
        flattened_data = []
        for row in data:
            flattened_data.append({
                'price': float(row['price']),
                'currency': row['currency'],
                'metal': row['dim_metal']['metal_name'],
                'market': row['dim_market']['market_name'],
                'timestamp': row['dim_time']['timestamp'],
                'date': row['dim_time']['date']
            })
            
        df = pd.DataFrame(flattened_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        return df
        
    except Exception as e:
        logger.error(f"Error fetching data for analytics: {e}")
        return pd.DataFrame()

def calculate_daily_change(df, metal_name="Silver", market_name="Spot"):
    """Calculate daily percentage change."""
    subset = df[(df['metal'] == metal_name) & (df['market'] == market_name)].copy()
    if subset.empty:
        return 0.0
        
    subset.set_index('timestamp', inplace=True)
    daily = subset['price'].resample('D').last()
    
    dummy_change = daily.pct_change() * 100
    return dummy_change.dropna().tail(1).values[0] if not dummy_change.dropna().empty else 0.0

def calculate_moving_averages(df, windows=[7, 30]):
    """Calculate Simple Moving Averages."""
    if df.empty:
        return {}
    
    mas = {}
    for window in windows:
        mas[f'SMA_{window}'] = df['price'].rolling(window=window).mean()
    return mas

def detect_breakouts(df, threshold_pct=2.0):
    """
    Identify days where daily change exceeds threshold.
    Returns DataFrame of breakout events.
    """
    if df.empty:
        return pd.DataFrame()
        
    # Resample to daily max/close to find volatile days
    # Or just use raw hourly data? user said "Daily % change", let's check daily volatility.
    daily_df = df.set_index('timestamp')['price'].resample('D').last().to_frame()
    daily_df['pct_change'] = daily_df['price'].pct_change() * 100
    
    breakouts = daily_df[abs(daily_df['pct_change']) >= threshold_pct].copy()
    return breakouts

def calculate_premium_series(spot_df, mcx_df):
    """
    Calculate Premium % series: (MCX - Spot) / Spot * 100
    Aligns timestamps.
    """
    if spot_df.empty or mcx_df.empty:
        return pd.Series()
        
    # Merge on timestamp
    merged = pd.merge_asof(
        mcx_df.sort_values('timestamp'), 
        spot_df.sort_values('timestamp'), 
        on='timestamp', 
        suffixes=('_mcx', '_spot'),
        direction='nearest',
        tolerance=pd.Timedelta('1h') # Allow small drift
    )
    
    merged['premium_pct'] = ((merged['price_mcx'] - merged['price_spot']) / merged['price_spot']) * 100
    return merged.set_index('timestamp')['premium_pct']
