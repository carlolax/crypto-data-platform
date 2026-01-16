import functions_framework
from google.cloud import storage
import requests
import json
from datetime import datetime
import os
from typing import Tuple

# --- CONFIGURATION ---
BUCKET_NAME = os.environ.get("BRONZE_BUCKET_NAME", "crypto-bronze-crypto-platform-carlo-2026")
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
DEFAULT_COINS = "bitcoin,ethereum,solana,cardano"

@functions_framework.http
def process_data_ingestion(request) -> Tuple[str, int]:
    """
    Ingests crypto market data from CoinGecko and saves it to Google Cloud Storage (Bronze Layer).

    Trigger:
        HTTP Request (Cloud Scheduler).
        Optional Query Param: ?coins=bitcoin,dogecoin (overrides default list).

    Process:
        1. Parses the 'request' for custom coins (optional).
        2. Fetches real-time prices from CoinGecko.
        3. Uploads the raw data to the Bronze GCS Bucket.

    Returns:
        tuple: ("Success Message", 200) on success.

    Raises:
        Exception: Propagates any error to GCP Logging to trigger alerts.
    """

    # 1. Dynamic Configuration using requests
    request_args = request.args
    if request_args and "coins" in request_args:
        target_coins = request_args["coins"]
        print(f"🚀 Manual Override Detected. Fetching: {target_coins}")
    else:
        target_coins = DEFAULT_COINS
        print(f"🚀 Starting Bronze Layer - Data Ingestion for: {target_coins}")

    try:
        # 2. Fetch data
        params = {
            "ids": target_coins,
            "vs_currencies": "usd",
            "include_24hr_vol": "true"
        }

        response = requests.get(COINGECKO_URL, params=params, timeout=10) # Added timeout
        response.raise_for_status() # Raises error for 404, 500, etc.

        coingecko_data = response.json()
        print("✅ CoinGecko data fetched successfully.")

        # 3. Upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_name = f"raw_prices_{timestamp}.json"
        blob = bucket.blob(blob_name)

        blob.upload_from_string(
            data=json.dumps(coingecko_data),
            content_type="application/json"
        )

        print(f"💾 Uploaded to gs://{BUCKET_NAME}/{blob_name}")
        return f"Success: {blob_name}", 200 # Returns a tuple

    except Exception as error:
        print(f"❌ Critical Error in Bronze Cloud Function: {error}")
        # Re-raise the error to stop the pipeline
        raise error
