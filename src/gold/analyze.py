import duckdb
import os

# Setup config
SILVER_PATH = "data/silver/market_history.parquet"
GOLD_PATH = "data/gold/market_summary.parquet"

# Ensure gold folder exists
os.makedirs("data/gold", exist_ok=True)

print("Starting Gold Layer Analysis.")

con = duckdb.connect()

query_gold = f"""
WITH base_metrics AS (
    SELECT
        recorded_at,
        coin_id,
        price_usd,
        
        -- 1. Calculate 7-Day Moving Average (The "Baseline")
        AVG(price_usd) OVER (
            PARTITION BY coin_id 
            ORDER BY recorded_at 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as sma_7d,
        
        -- 2. Calculate Volatility (Standard Deviation over 7 days)
        STDDEV(price_usd) OVER (
            PARTITION BY coin_id 
            ORDER BY recorded_at 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as volatility_7d

    FROM '{SILVER_PATH}'
)

SELECT
    recorded_at,
    coin_id,
    price_usd,
    CAST(sma_7d AS DECIMAL(18, 2)) as sma_7d,
    CAST(volatility_7d AS DECIMAL(18, 2)) as volatility_7d,
    
    -- 3. Generate the Trading Signal
    CASE 
        WHEN price_usd < sma_7d THEN 'BUY'    -- Price is below average (Cheap)
        WHEN price_usd > sma_7d THEN 'WAIT'   -- Price is above average (Expensive)
        ELSE 'HOLD'
    END as signal

FROM base_metrics
ORDER BY recorded_at DESC, coin_id
"""

print("\nMarket Analysis Preview:")
df = con.execute(query_gold).df()
print(df.head(10))

print(f"\nSaving Analytics to {GOLD_PATH}.")
con.execute(f"COPY ({query_gold}) TO '{GOLD_PATH}' (FORMAT PARQUET)")
print("Done!")
