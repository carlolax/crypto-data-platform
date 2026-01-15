import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path

# CONFIGURATION:
ST_PAGE_TITLE = "Crypto Strategy Command Center"

# Calculates the project root by going up 2 levels: 1. src -> 2. crypto-project
BASE_DIR = Path(__file__).resolve().parent.parent

# Path that points to: /Users/<NAME>/Developer/crypto-project/data/gold/analyzed_market_summary.parquet file
LOCAL_GOLD_PATH = BASE_DIR / "data" / "gold" / "analyzed_market_summary.parquet"

# Setup page
st.set_page_config(page_title=ST_PAGE_TITLE, layout="wide")
st.title(f"📊 {ST_PAGE_TITLE}")

# Data Loader
def load_data():
    # Loads the Gold layer data
    if not LOCAL_GOLD_PATH.exists():
        st.error(f"File not found: {LOCAL_GOLD_PATH}")
        st.info("Hint: Run 'python src/pipeline/run_pipeline.py' first to generate data.")
        return pd.DataFrame()
    
    # Read parquet file
    df = pd.read_parquet(LOCAL_GOLD_PATH)
    return df

# Main function
def main():
    # Load data
    df = load_data()
    
    if df.empty:
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
    latest = coin_df.iloc[-1]
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Current Price", f"${latest['price_usd']:,.2f}")
    with col2:
        st.metric("7-Day SMA", f"${latest['sma_7d']:,.2f}")
    with col3:
        st.metric("Volatility (7D)", f"{latest['volatility_7d']:,.2f}")
    with col4:
        # Color code the signal
        signal = latest['signal']
        color = "normal"
        if signal == "BUY": color = "off"
        st.metric("Trading Signal", signal)

    # Interactive Chart using plotly
    st.subheader(f"Price vs. Moving Average ({selected_coin.upper()})")
    
    fig = go.Figure()

    # Line A - The Actual Price
    fig.add_trace(go.Scatter(
        x=coin_df['extraction_timestamp'], 
        y=coin_df['price_usd'],
        mode='lines',
        name='Price (USD)',
        line=dict(color='#00CC96', width=2)
    ))

    # Line B - The Moving Average
    fig.add_trace(go.Scatter(
        x=coin_df['extraction_timestamp'], 
        y=coin_df['sma_7d'],
        mode='lines',
        name='7-Day SMA',
        line=dict(color='#EF553B', width=2, dash='dash')
    ))

    # Update layout for dark mode style
    fig.update_layout(
        template="plotly_dark",
        height=500,
        xaxis_title="Date",
        yaxis_title="Price (USD)"
    )

    st.plotly_chart(fig, use_container_width=True)

    # Raw data viewer
    with st.expander("📂 View Raw Data"):
        st.dataframe(coin_df.sort_values("extraction_timestamp", ascending=False))

if __name__ == "__main__":
    main()
