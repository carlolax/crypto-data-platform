import functions_framework
from google.cloud import storage
import duckdb
import os
import glob

# Setup config 
GOLD_BUCKET_NAME = os.environ.get("GOLD_BUCKET_NAME", "crypto-gold-REPLACE-ME")

@functions_framework.cloud_event
def process_gold(cloud_event):
    data = cloud_event.data
    source_bucket_name = data["bucket"]
    new_file_name = data["name"]

    print(f"Gold Triggered by: gs://{source_bucket_name}/{new_file_name}")

    # 1. Download History
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket_name)

    # Create a clean directory in /tmp
    local_dir = "/tmp/silver_history"
    os.makedirs(local_dir, exist_ok=True)

    # Clear old run data if any
    files = glob.glob(f"{local_dir}/*")
    for f in files:
        os.remove(f)

    # Download all parquet files from Silver layer
    blobs = list(source_bucket.list_blobs(prefix="processed/"))
    download_count = 0

    for blob in blobs:
        if blob.name.endswith(".parquet"):
            # Clean filename for local save
            safe_name = blob.name.split("/")[-1]
            blob.download_to_filename(f"{local_dir}/{safe_name}")
            download_count += 1
    
    print(f"Downloaded {download_count} historical files for analysis.")

    # 2. Analyze
    con = duckdb.connect()
    output_path = "/tmp/market_summary.parquet"

    query = f"""
    COPY (
        WITH base_metrics AS (
            SELECT
                recorded_at,
                coin_id,
                price_usd,

                -- Calculate 7-Day Moving Average
                AVG(price_usd) OVER (
                    PARTITION BY coin_id 
                    ORDER BY recorded_at 
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as sma_7d,

                -- Calculate Volatility
                STDDEV(price_usd) OVER (
                    PARTITION BY coin_id 
                    ORDER BY recorded_at 
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as volatility_7d

            FROM read_parquet('{local_dir}/*.parquet')
        )

        SELECT
            recorded_at,
            coin_id,
            price_usd,
            CAST(sma_7d AS DECIMAL(18, 2)) as sma_7d,
            CAST(volatility_7d AS DECIMAL(18, 2)) as volatility_7d,

            CASE 
                WHEN price_usd < sma_7d THEN 'BUY'
                WHEN price_usd > sma_7d THEN 'WAIT'
                ELSE 'HOLD'
            END as signal

        FROM base_metrics
        ORDER BY recorded_at DESC, coin_id
    ) TO '{output_path}' (FORMAT PARQUET);
    """

    con.execute(query)
    print(f"Gold Analysis Complete. Saved to {output_path}")

    # 3. Upload
    dest_bucket = storage_client.bucket(GOLD_BUCKET_NAME)
    dest_blob = dest_bucket.blob("analytics/market_summary.parquet")

    dest_blob.upload_from_filename(output_path)
    print(f"Published Dashboard Data: gs://{GOLD_BUCKET_NAME}/analytics/market_summary.parquet")
    