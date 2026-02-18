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

def get_data_for_analytics():
    """
    Fetch all relevant price data joined with dimensions.
    Returns a pandas DataFrame.
    """
    try:
        # We need metal name, market name, price, and timestamp
        # Supabase-py format for nested joins:
        response = supabase.table("fact_metal_prices").select(
            "price, metal_id, market_id, time_id, dim_metal(metal_name), dim_market(market_name), dim_time(timestamp, date)"
        ).execute()
        
        data = response.data
        if not data:
            return pd.DataFrame()
            
        # Flatten the data structure
        flattened_data = []
        for row in data:
            flattened_data.append({
                'price': float(row['price']),
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

def calculate_daily_change(df, metal_name, market_name="Spot"):
    """Calculate daily percentage change for a specific metal/market."""
    subset = df[(df['metal'] == metal_name) & (df['market'] == market_name)].copy()
    if subset.empty:
        return None
        
    # Resample to daily if multiple entries per day, take the last close
    subset.set_index('timestamp', inplace=True)
    daily = subset['price'].resample('D').last() # simple daily aggregation
    
    # Calculate % change
    dummy_change = daily.pct_change() * 100
    return dummy_change.dropna().tail(1).values[0] if not dummy_change.dropna().empty else 0.0

def calculate_moving_averages(df, metal_name, market_name="Spot"):
    """Calculate 7-day and 30-day moving averages."""
    subset = df[(df['metal'] == metal_name) & (df['market'] == market_name)].copy()
    if subset.empty:
        return None, None
        
    subset.set_index('timestamp', inplace=True)
    daily = subset['price'].resample('D').last().dropna()
    
    sma_7 = daily.rolling(window=7).mean()
    sma_30 = daily.rolling(window=30).mean()
    
    return sma_7, sma_30

def get_correlation_matrix(df, market_name="Spot"):
    """Calculate correlation matrix between metals in a specific market."""
    subset = df[df['market'] == market_name]
    if subset.empty:
        return pd.DataFrame()
        
    pivot_df = subset.pivot_table(index='timestamp', columns='metal', values='price', aggfunc='last')
    correlation_matrix = pivot_df.corr()
    return correlation_matrix

def detect_volatility(df, threshold_pct=3.0):
    """Identify metals with recent price spikes > threshold %."""
    spikes = []
    
    metals = df['metal'].unique()
    markets = df['market'].unique()
    
    for metal in metals:
        for market in markets:
            subset = df[(df['metal'] == metal) & (df['market'] == market)].copy()
            if len(subset) < 2:
                continue
                
            subset.set_index('timestamp', inplace=True)
            subset['pct_change'] = subset['price'].pct_change() * 100
            
            # Check recent data (e.g. last record)
            last_change = subset['pct_change'].iloc[-1]
            if abs(last_change) > threshold_pct:
                spikes.append({
                    'metal': metal,
                    'market': market,
                    'change': last_change,
                    'date': subset.index[-1]
                })
                
    return spikes

def identify_most_volatile(df):
    """Identify metal with highest standard deviation of returns."""
    volatility_scores = {}
    
    metals = df['metal'].unique()
    for metal in metals:
        # Aggregate across markets or pick Spot? Let's pick Spot for consistency
        subset = df[(df['metal'] == metal) & (df['market'] == 'Spot')].copy()
        if len(subset) < 3:
            continue
            
        subset.set_index('timestamp', inplace=True)
        # Daily returns
        daily_returns = subset['price'].resample('D').last().pct_change()
        std_dev = daily_returns.std()
        
        volatility_scores[metal] = std_dev
        
    if not volatility_scores:
        return None
        
    return max(volatility_scores, key=volatility_scores.get)

if __name__ == "__main__":
    logger.info("Running Analytics Test...")
    df = get_data_for_analytics()
    if not df.empty:
        logger.info(f"Loaded {len(df)} records.")
        logger.info(f"Columns: {df.columns.tolist()}")
        logger.info(f"Metals: {df['metal'].unique()}")
        
        # Test Gold Spot
        change = calculate_daily_change(df, "Gold")
        logger.info(f"Gold Daily Change: {change}%")
        
        sma7, sma30 = calculate_moving_averages(df, "Gold")
        if sma7 is not None and not sma7.empty:
            logger.info(f"Gold SMA7 (last): {sma7.iloc[-1]}")
            
        corr = get_correlation_matrix(df)
        logger.info("Correlation Matrix:\n" + str(corr))
        
        spikes = detect_volatility(df)
        logger.info(f"Detected Spikes: {spikes}")
        
        most_volatile = identify_most_volatile(df)
        logger.info(f"Most Volatile Metal: {most_volatile}")
        
    else:
        logger.warning("No data found to analyze.")
