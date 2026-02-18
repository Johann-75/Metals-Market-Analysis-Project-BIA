import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# Add project root to system path to allow imports from backend and dashboard
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dashboard.utils import (
    load_data,
    get_kpi_metrics,
    calculate_moving_averages,
    get_correlation_matrix,
    detect_volatility,
    identify_most_volatile
)

# Page Config
st.set_page_config(
    page_title="Precious Metals Intelligence",
    page_icon="🪙",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title
st.title("🪙 Precious Metals Price Intelligence")
st.markdown("Real-time tracking, analysis, and forecasting of precious metal prices.")

# Load Data
df = load_data()

if df.empty:
    st.error("No data available. Please check if the ETL pipeline has run successfully.")
    st.stop()

# Sidebar Filters
st.sidebar.header("Configuration")

# Date Range
min_date = df['timestamp'].min().date()
max_date = df['timestamp'].max().date()
start_date = st.sidebar.date_input("Start Date", min_value=min_date, max_value=max_date, value=min_date)
end_date = st.sidebar.date_input("End Date", min_value=min_date, max_value=max_date, value=max_date)

# Filter Data
mask = (df['timestamp'].dt.date >= start_date) & (df['timestamp'].dt.date <= end_date)
filtered_df = df.loc[mask]

# Metal Selection
available_metals = sorted(df['metal'].unique())
default_metals = ["Gold", "Silver"] if "Gold" in available_metals and "Silver" in available_metals else available_metals[:2]
selected_metals = st.sidebar.multiselect("Select Metals", available_metals, default=default_metals)

# Market Selection
available_markets = sorted(df['market'].unique())
selected_market = st.sidebar.selectbox("Select Market", available_markets, index=0)

# Refresh Button
if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# --- KPI Section ---
st.markdown("### Market Overview")
kpi_cols = st.columns(4)

# Dynamic KPIs for selected metals (limit to first 4 for layout)
for i, metal in enumerate(selected_metals[:3]):
    price, change = get_kpi_metrics(df, metal, selected_market)
    with kpi_cols[i]:
        st.metric(
            label=f"{metal} ({selected_market})",
            value=f"${price:,.2f}",
            delta=f"{change:.2f}%"
        )

# Volatility KPI
most_volatile = identify_most_volatile(df)
with kpi_cols[3]:
    st.metric(
        label="Most Volatile Metal (Global)",
        value=most_volatile if most_volatile else "N/A",
        delta="High Variance",
        delta_color="off"
    )

st.markdown("---")

# --- Main Charts ---
st.markdown("### Price Trends")

chart_tab, analytics_tab, alerts_tab = st.tabs(["Price Charts", "Correlations", "Volatility Alerts"])

with chart_tab:
    # Filter for selected metals
    chart_data = filtered_df[
        (filtered_df['metal'].isin(selected_metals)) & 
        (filtered_df['market'] == selected_market)
    ]
    
    if not chart_data.empty:
        fig = px.line(
            chart_data, 
            x='timestamp', 
            y='price', 
            color='metal', 
            title=f"Price Changes ({selected_market})",
            template="plotly_dark",
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Moving Averages Toggle
        if st.checkbox("Show Moving Averages (Gold Spot Example)"):
            sma7, sma30 = calculate_moving_averages(df, "Gold", "Spot")
            if sma7 is not None:
                ma_fig = go.Figure()
                ma_fig.add_trace(go.Scatter(x=sma7.index, y=sma7, mode='lines', name='SMA 7 Days'))
                ma_fig.add_trace(go.Scatter(x=sma30.index, y=sma30, mode='lines', name='SMA 30 Days'))
                ma_fig.update_layout(title="Gold Spot Moving Averages", template="plotly_dark")
                st.plotly_chart(ma_fig, use_container_width=True)
    else:
        st.info("No data to display for current selection.")

with analytics_tab:
    st.markdown("#### Correlation Matrix")
    corr_matrix = get_correlation_matrix(df, selected_market)
    
    if not corr_matrix.empty:
        fig_corr = px.imshow(
            corr_matrix, 
            text_auto=True, 
            aspect="auto", 
            title=f"Metal Price Correlations ({selected_market})",
            template="plotly_dark",
            color_continuous_scale="RdBu_r"
        )
        st.plotly_chart(fig_corr, use_container_width=True)
    else:
        st.info("Insufficient data for correlation analysis.")

with alerts_tab:
    st.markdown("#### Spikes & Anomalies")
    st.caption("Detecting price changes > 3%")
    
    spikes = detect_volatility(df)
    if spikes:
        st.table(pd.DataFrame(spikes))
    else:
        st.success("No significant volatility detected in recent data.")

st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
