import functions_framework
from google.cloud import storage
import duckdb
import os

# Setup configuration
BUCKET_NAME = os.environ.get("BUCKET_NAME", "crypto-silver-REPLACE-ME")

@functions_framework.cloud_event
def process_silver(cloud_event):
    data = cloud_event.data

    # 1. Get Event Details
    file_name = data["name"]
    bucket_name = data["bucket"]
    
    print(f"Event Triggered! Processing file: gs://{bucket_name}/{file_name}")

    # Safety Check: Ignore folders or non-json files
    if not file_name.endswith(".json"):
        print("Not a JSON file. Skipping.")
        return
    
    # 2. Download (Input)
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(file_name)
    
    local_input_path = f"/tmp/{file_name}"
    local_output_path = f"/tmp/{file_name.replace('.json', '.parquet')}"
    
    source_blob.download_to_filename(local_input_path)
    print(f"Downloaded to {local_input_path}")

    # 3. Transform (DuckDB Logic)
    con = duckdb.connect()

    query = f"""
        COPY (
            WITH raw_data AS (
                SELECT * FROM read_json_auto('{local_input_path}', filename=True)
            ),
            unpivoted_data AS (
                UNPIVOT raw_data
                ON bitcoin, ethereum, solana
                INTO NAME coin_id VALUE metrics
            )
            SELECT
                strptime(
                    regexp_extract(filename, 'raw_(\\d{{4}}-\\d{{2}}-\\d{{2}}_\\d{{6}})', 1),
                    '%Y-%m-%d_%H%M%S'
                ) as recorded_at,
                coin_id,
                CAST(metrics.usd AS DECIMAL(18, 2)) as price_usd,
                CAST(metrics.usd_market_cap AS DECIMAL(24, 2)) as market_cap,
                CAST(metrics.usd_24h_vol AS DECIMAL(24, 2)) as volume_24h
            FROM unpivoted_data
        ) TO '{local_output_path}' (FORMAT PARQUET);
    """

    try:
        con.execute(query)
        print(f"Transformation Complete. Saved to {local_output_path}")
    except Exception as error:
        print(f"DuckDB Error: {error}")
        raise error

    # 4. Upload (Output)
    dest_bucket = storage_client.bucket(BUCKET_NAME)
    dest_blob_name = file_name.replace("raw_data/", "processed/").replace(".json", ".parquet")
    dest_blob = dest_bucket.blob(dest_blob_name)
    
    dest_blob.upload_from_filename(local_output_path)
    print(f"Uploaded to gs://{BUCKET_NAME}/{dest_blob_name}")

    # Cleanup /tmp to free up memory
    os.remove(local_input_path)
    os.remove(local_output_path)
    print("🧹 Cleanup complete.")