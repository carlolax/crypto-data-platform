import functions_framework
from google.cloud import storage
import duckdb
import os
from pathlib import Path

# --- CONFIGURATION ---
SILVER_BUCKET_NAME = os.environ.get("SILVER_BUCKET_NAME", "crypto-silver-data")

@functions_framework.cloud_event
def process_data_cleaning(cloud_event):
    """
    Event-Driven Cloud Function that transforms raw JSON into Parquet.

    Trigger:
        Google Cloud Storage (Object Finalize) on the Bronze Bucket.

    Process:
        1. Downloads the new JSON file from Bronze.
        2. Uses DuckDB to UNPIVOT the data (Wide -> Long format).
        3. Enforces Schema (Bitcoin, Ethereum, Solana, Cardano).
        4. Saves the result as Parquet in the Silver Bucket.
    """
    data = cloud_event.data

    # 1. Parse Event
    file_name = data["name"]
    source_bucket_name = data["bucket"]

    print("🚀 Event triggered! Starting Silver Layer - Data Cleaning")
    print(f"Source: gs://{source_bucket_name}/{file_name}")

    if not file_name.endswith(".json"):
        print("⚠️ Not a JSON file. Skipping.")
        return

    # 2. Setup Paths
    temp_dir = Path("/tmp")
    local_input_path = temp_dir / file_name

    # Create output filename: raw_prices_2026...json -> raw_prices_2026...parquet
    safe_filename = Path(file_name).with_suffix('.parquet').name
    local_output_path = temp_dir / safe_filename

    # 3. Download
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(file_name)

    # Using str(path_obj) since GCS library expects a string
    source_blob.download_to_filename(str(local_input_path))
    print(f"✅ Downloaded to {local_input_path}")

    # 4. Transform (DuckDB)
    duckdb_con = duckdb.connect(database=':memory:')

    query = f"""
        COPY (
            WITH raw_data AS (
                SELECT * FROM read_json('{local_input_path}',
                    columns={{
                        'bitcoin': 'STRUCT(usd DOUBLE, usd_market_cap DOUBLE, usd_24h_vol DOUBLE)',
                        'ethereum': 'STRUCT(usd DOUBLE, usd_market_cap DOUBLE, usd_24h_vol DOUBLE)',
                        'solana': 'STRUCT(usd DOUBLE, usd_market_cap DOUBLE, usd_24h_vol DOUBLE)',
                        'cardano': 'STRUCT(usd DOUBLE, usd_market_cap DOUBLE, usd_24h_vol DOUBLE)',
                    }},
                    filename=True
                )
            ),
            unpivoted_data AS (
                UNPIVOT raw_data
                ON bitcoin, ethereum, solana, cardano
                INTO NAME coin_id VALUE metrics
            )
            SELECT
                strptime(
                    regexp_extract(filename, 'raw_prices_(\\d{{8}}_\\d{{6}})', 1),
                    '%Y%m%d_%H%M%S'
                ) as extraction_timestamp,
                coin_id,
                CAST(metrics.usd AS DECIMAL(18, 2)) as price_usd,
                CAST(metrics.usd_market_cap AS DECIMAL(24, 2)) as market_cap,
                CAST(metrics.usd_24h_vol AS DECIMAL(24, 2)) as volume_24h
            FROM unpivoted_data
        ) TO '{local_output_path}' (FORMAT PARQUET);
    """

    try:
        duckdb_con.execute(query)
        print(f"✅ Transformation Complete. Saved to {local_output_path}")

        # 5. Upload to Silver
        dest_bucket = storage_client.bucket(SILVER_BUCKET_NAME)
        # Putting the processed files in a 'processed/' to keep bucket clean
        dest_blob_name = f"processed/{safe_filename}"
        dest_blob = dest_bucket.blob(dest_blob_name)

        dest_blob.upload_from_filename(str(local_output_path))
        print(f"💾 Uploaded to gs://{SILVER_BUCKET_NAME}/{dest_blob_name}")

    except Exception as error:
        print(f"❌ DuckDB Transformation Error: {error}")
        # Re-raise the error to stop the pipeline
        raise error

    finally:
        # 6. Cleanup using pathlib's unlink
        if local_input_path.exists():
            local_input_path.unlink()
        if local_output_path.exists():
            local_output_path.unlink()
        print("🧹 Cleanup complete.")
        duckdb_con.close()
