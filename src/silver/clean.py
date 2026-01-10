import duckdb
import os

# Setup config
BRONZE_PATH = "data/bronze/*.json"
SILVER_PATH = "data/silver/market_history.parquet"

# Ensure silver folder exists
os.makedirs("data/silver", exist_ok=True)
con = duckdb.connect()

print("Starting Silver Layer Processing.")

query_clean = f"""
WITH raw_data AS (
    -- 1. Load the raw JSON and keep the filename
    SELECT * FROM read_json_auto('{BRONZE_PATH}', filename=True)
),

unpivoted_data AS (
    -- 2. "Unpivot" converts columns (bitcoin, eth) into rows
    UNPIVOT raw_data
    ON bitcoin, ethereum, solana
    INTO NAME coin_id VALUE metrics
)

SELECT
    -- 3. Extract Timestamp from filename (Regex Magic)
    -- Fixed: Used \\d instead of \d to avoid Python warnings
    strptime(
        regexp_extract(filename, 'raw_(\\d{{4}}-\\d{{2}}-\\d{{2}}_\\d{{6}})', 1),
        '%Y-%m-%d_%H%M%S'
    ) as recorded_at,
    
    -- 4. Clean the Coin Name
    coin_id,
    
    -- 5. Unpack the JSON Structs (Enforce Types!)
    CAST(metrics.usd AS DECIMAL(18, 2)) as price_usd,
    CAST(metrics.usd_market_cap AS DECIMAL(24, 2)) as market_cap,
    CAST(metrics.usd_24h_vol AS DECIMAL(24, 2)) as volume_24h

FROM unpivoted_data
ORDER BY recorded_at DESC, coin_id
"""

print("\nCleaned Data Preview:")
print(con.execute(query_clean).df())

# Save to Parquet
print(f"\nSaving to {SILVER_PATH}.")
con.execute(f"COPY ({query_clean}) TO '{SILVER_PATH}' (FORMAT PARQUET)")
print("Saving complete.")
