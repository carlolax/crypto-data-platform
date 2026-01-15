import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from google.cloud import storage
import io

# CONFIGURATION:
ST_PAGE_TITLE = "Crypto Strategy Command Center"

# Mode Switch - Set to "CLOUD" to read from GCP, or "LOCAL" for offline dev
DATA_SOURCE = "CLOUD"

# Calculates the project root by going up 2 levels: 1. src -> 2. crypto-project
BASE_DIR = Path(__file__).resolve().parent.parent

# Path that points to: /Users/<NAME>/Developer/crypto-project/data/gold/analyzed_market_summary.parquet file
LOCAL_GOLD_PATH = BASE_DIR / "data" / "gold" / "analyzed_market_summary.parquet"

# Cloud paths
CLOUD_BUCKET_NAME = "crypto-gold-crypto-platform-carlo-2026"
CLOUD_BLOB_NAME = "analytics/market_summary.parquet"

# Setup page
st.set_page_config(page_title=ST_PAGE_TITLE, layout="wide")
st.title(f"📊 {ST_PAGE_TITLE}")

# Data Loader
@st.cache_data(ttl=600) # Clear cache every 10 minutes for live data
def load_data():
    """
    Loads Gold Layer data based on DATA_SOURCE toggle.
    """
    if DATA_SOURCE == "LOCAL":
        st.info("🏠 Mode: LOCAL (Reading from disk)")
        if not LOCAL_GOLD_PATH.exists():
            st.error(f"❌ File not found: {LOCAL_GOLD_PATH}")
            return pd.DataFrame()
        return pd.read_parquet(LOCAL_GOLD_PATH)
        
    elif DATA_SOURCE == "CLOUD":
        st.info(f"☁️ Mode: CLOUD (Reading from {CLOUD_BUCKET_NAME})")
        try:
            # Download from GCS into memory
            storage_client = storage.Client()
            bucket = storage_client.bucket(CLOUD_BUCKET_NAME)
            blob = bucket.blob(CLOUD_BLOB_NAME)
            
            data_bytes = blob.download_as_bytes()
            return pd.read_parquet(io.BytesIO(data_bytes))
        except Exception as error:
            st.error(f"❌ Cloud Connection Failed: {error}")
            return pd.DataFrame()

# Main function
def main():
    # Load data
    df = load_data()
    
    if df.empty:
        st.warning("No data available to display.")
        return

    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    # Get unique coins from the data
    all_coins = df['coin_id'].unique()
    selected_coin = st.sidebar.selectbox("Select Asset", all_coins, index=0)

    # Filter data for just that coin
    coin_df = df[df['coin_id'] == selected_coin].copy()
    
    # Sort by time for charting (Oldest to Newest)
    coin_df = coin_df.sort_values("extraction_timestamp")

    # Key Metrics
    # Get the very latest row (the most recent price)
    if not coin_df.empty:
        latest = coin_df.iloc[-1]

        volatility = latest['volatility_7d']
        if pd.isna(volatility) or volatility is None:
            vol_display = "0.00"
        else:
            vol_display = f"{volatility:,.2f}"

        col1, col2, col3, col4 = st.columns(4)
        with col1: st.metric("Current Price", f"${latest['price_usd']:,.2f}")
        with col2: st.metric("7-Day SMA", f"${latest['sma_7d']:,.2f}")
        with col3: st.metric("Volatility", vol_display) 
        with col4: st.metric("Signal", latest['signal'])

    # Visualization chart
    st.subheader(f"Price vs. Moving Average ({selected_coin.upper()})")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=coin_df['extraction_timestamp'], y=coin_df['price_usd'], mode='lines', name='Price', line=dict(color='#00CC96')))
    fig.add_trace(go.Scatter(x=coin_df['extraction_timestamp'], y=coin_df['sma_7d'], mode='lines', name='7-Day SMA', line=dict(color='#EF553B', dash='dash')))
    fig.update_layout(template="plotly_dark", height=500, xaxis_title="Date", yaxis_title="Price")
    st.plotly_chart(fig, use_container_width=True)
    
if __name__ == "__main__":
    main()
