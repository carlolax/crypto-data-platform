import duckdb
from pathlib import Path

# SETUP:
# Calculates the project root by going up 4 levels: 1. gold -> 2. pipeline -> 3. src -> 4. crypto-project
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent

# Path that points to: /Users/<NAME>/Developer/crypto-project/data/silver/cleaned_crypto_prices.parquet file
SILVER_PATH = BASE_DIR / "data" / "silver" / "cleaned_crypto_prices.parquet"

# Directory that points to: /Users/<NAME>/Developer/crypto-project/data/gold/
GOLD_DIR = BASE_DIR / "data" / "gold"

# Path that points to: /Users/<NAME>/Developer/crypto-project/data/gold/market_summary.parquet file
GOLD_PATH = GOLD_DIR / "analyzed_market_summary.parquet"

# Ensure Gold directory exists
GOLD_DIR.mkdir(parents=True, exist_ok=True)

print(f"Reading from: {SILVER_PATH}")
print("Starting Gold Layer Analysis.")

con = duckdb.connect()

# Conver the path object to string for DuckDB SQL
silver_file_str = str(SILVER_PATH)

# Define the business logic using a query
query_gold = f"""
WITH base_metrics AS (
    SELECT
        extraction_timestamp,
        coin_id,
        price_usd,
        
        -- A. Calculate 7-Day Moving Average (The "Baseline")
        -- "Look at the last 6 rows + this row (7 total) and average them"
        AVG(price_usd) OVER (
            PARTITION BY coin_id 
            ORDER BY extraction_timestamp 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as sma_7d,
        
        -- B. Calculate Volatility (Standard Deviation)
        STDDEV(price_usd) OVER (
            PARTITION BY coin_id 
            ORDER BY extraction_timestamp 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as volatility_7d

    FROM '{silver_file_str}'
)

SELECT
    extraction_timestamp,
    coin_id,
    price_usd,
    CAST(sma_7d AS DECIMAL(18, 2)) as sma_7d,
    CAST(volatility_7d AS DECIMAL(18, 2)) as volatility_7d,
    
    -- C. Generate the Trading Signal (Feature Engineering)
    CASE 
        WHEN price_usd < sma_7d THEN 'BUY'    -- Price is below average (Cheap)
        WHEN price_usd > sma_7d THEN 'WAIT'   -- Price is above average (Expensive)
        ELSE 'HOLD'
    END as signal

FROM base_metrics
ORDER BY extraction_timestamp DESC, coin_id
"""

# Execute and Preview
print("\nMarket Analysis Preview:")
# Converts the result to a Pandas DataFrame for pretty printing after running the query
df = con.execute(query_gold).df()
print(df.head(10))

# Save the Gold data
print(f"\nSaving the analytics to {GOLD_PATH}.")
con.execute(f"COPY ({query_gold}) TO '{str(GOLD_PATH)}' (FORMAT PARQUET)")
print("Saving complete.")
