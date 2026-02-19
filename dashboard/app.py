import streamlit as st
import plotly.express as px
import pandas as pd
import sys
import os

# Add project root to system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dashboard.utils import (
    load_data,
    get_kpi_metrics
)
from backend.analytics import (
    calculate_moving_averages,
    calculate_premium_series
)

# Page Config
st.set_page_config(
    page_title="Silver Price Intelligence (India)",
    page_icon="🪙",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .stMetric {
        background-color: #1E1E1E;
        padding: 10px;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

st.title("🪙 Silver Price Intelligence (India)")
st.caption("Strategic Market Analysis: MCX Futures vs Spot Silver (INR/kg)")

# --- Sidebar ---
st.sidebar.header("Configuration")
st.sidebar.caption("Unit: INR (₹) / Kilogram")

# Data Loading (Hardcoded for INR)
currency = "INR"
unit_label = "kg"
currency_symbol = "₹"

try:
    df = load_data(currency=currency)
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

if df.empty:
    st.warning(f"No data available. Please wait for the next scheduled update.")
    st.stop()

# Unit Conversion Logic (INR/toz -> INR/kg)
df['price'] = df['price'] * 32.1507466

# Date Filter
min_date = df['timestamp'].min().date()
max_date = df['timestamp'].max().date()

start_date = st.sidebar.date_input("Start Date", min_value=min_date, max_value=max_date, value=min_date)
end_date = st.sidebar.date_input("End Date", min_value=min_date, max_value=max_date, value=max_date)

# Filter Data
mask = (df['timestamp'].dt.date >= start_date) & (df['timestamp'].dt.date <= end_date)
filtered_df = df.loc[mask]
spot_df = filtered_df[filtered_df['market'] == 'Spot'].sort_values('timestamp')
mcx_df = filtered_df[filtered_df['market'] == 'MCX'].sort_values('timestamp')

# --- KPI Section ---
st.subheader(f"Market Performance ({currency}/{unit_label})")
col1, col2, col3 = st.columns(3)

# Spot Price
spot_price, spot_change = get_kpi_metrics(df, "Silver", "Spot")
with col1:
    st.metric(f"Spot Silver", f"{currency_symbol}{spot_price:,.2f}", f"{spot_change:+.2f}%")

# MCX Price
mcx_price, mcx_change = get_kpi_metrics(df, "Silver", "MCX")
with col2:
    if mcx_price > 0:
        st.metric(f"MCX Silver", f"{currency_symbol}{mcx_price:,.2f}", f"{mcx_change:+.2f}%")
    else:
        st.metric(f"MCX Silver", "N/A", "0%")

# Premium %
if spot_price > 0 and mcx_price > 0:
    premium_pct = ((mcx_price - spot_price) / spot_price) * 100
    with col3:
        st.metric("MCX Premium", f"{premium_pct:+.2f}%", "Spread (Arbitrage)")
else:
    with col3:
        st.empty()

# --- Chart Section ---
st.markdown("---")
st.subheader("Price Trends & Indicators")

# Main Price Chart with SMA
fig = px.line(filtered_df, x='timestamp', y='price', color='market', 
              title=f'Silver Price Trend ({currency}/{unit_label})',
              labels={'price': f'Price ({currency_symbol})', 'timestamp': 'Date', 'market': 'Market'},
              template="plotly_dark")

# Add Moving Averages (Trend Detection)
if not spot_df.empty:
    smas = calculate_moving_averages(spot_df, windows=[7, 30])
    if 'SMA_7' in smas:
        fig.add_trace(px.line(x=spot_df['timestamp'], y=smas['SMA_7']).data[0])
        fig.data[-1].name = 'SMA 7 (Weekly)'
        fig.data[-1].line.color = 'yellow'
        fig.data[-1].line.dash = 'dot'
    if 'SMA_30' in smas:
        fig.add_trace(px.line(x=spot_df['timestamp'], y=smas['SMA_30']).data[0])
        fig.data[-1].name = 'SMA 30 (Monthly)'
        fig.data[-1].line.color = 'cyan'
        fig.data[-1].line.dash = 'dot'

st.plotly_chart(fig, use_container_width=True)

# Premium Chart
if not mcx_df.empty and not spot_df.empty:
    st.subheader("Arbitrage Opportunity (Premium %)")
    premium_series = calculate_premium_series(spot_df, mcx_df)
    
    if not premium_series.empty:
        fig_prem = px.area(x=premium_series.index, y=premium_series.values,
                           title="MCX Premium Over Spot (%)",
                           labels={'y': 'Premium %', 'x': 'Date'},
                           template="plotly_dark")
        fig_prem.add_hline(y=0, line_color="white")
        st.plotly_chart(fig_prem, use_container_width=True)
        st.caption("Positive Premium: MCX > Spot. Negative Premium: MCX < Spot.")

if st.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()
