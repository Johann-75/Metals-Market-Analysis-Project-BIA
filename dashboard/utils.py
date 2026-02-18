import streamlit as st
import pandas as pd
from backend.analytics import (
    get_data_for_analytics,
    calculate_daily_change,
    calculate_moving_averages,
    get_correlation_matrix,
    detect_volatility,
    identify_most_volatile
)

@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data():
    """Fetch analytics data with caching."""
    df = get_data_for_analytics()
    return df

def get_kpi_metrics(df, metal_name, market_name="Spot"):
    """Get current price and daily change."""
    subset = df[(df['metal'] == metal_name) & (df['market'] == market_name)]
    if subset.empty:
        return 0.0, 0.0
        
    current_price = subset.iloc[-1]['price']
    
    # Calculate change
    change = calculate_daily_change(df, metal_name, market_name)
    
    return current_price, change
