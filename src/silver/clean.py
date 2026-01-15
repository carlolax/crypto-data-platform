import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# SETUP:
# Calculates the project root: /Users/<NAME>/Developer/crypto-project/
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Directory that points to: /Users/<NAME>/Developer/crypto-project/data/bronze
BRONZE_DIR = BASE_DIR / "data" / "bronze"

# Directory that points to: /Users/<NAME>/Developer/crypto-project/data/silver
SILVER_DIR = BASE_DIR / "data" / "silver"

# Main function
def process_silver_local():
    print("Starting Silver Layer Cleaning.")
    
    # Ensure Silver directory exists
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    
    # Locate all JSON files in 'data/bronze' directory
    # Use list() to convert the generator to a list as counter
    json_files = list(BRONZE_DIR.glob("*.json"))
    
    if not json_files:
        print("No Bronze data found. Run ingest.py first.")
        return

    print(f"Found. Located: {len(json_files)} raw files to process.")

    # An empty list variable for appending the stored extracted data
    data_list = []

    # Loop through files and extract data
    for file_path in json_files:
        # Attempt to open and parse each Bronze Layer JSON file safely, if it fails, it will show an error message
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
                
                # Extract timestamp metadata from the filename for lineage tracking
                filename_parts = file_path.stem.split("_")
                if len(filename_parts) >= 3:
                    timestamp_str = f"{filename_parts[2]}_{filename_parts[3]}"
                else:
                    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")

                # Flatten the JSON data format
                for coin_id, metrics in data.items():
                    row = {
                        "coin_id": coin_id,
                        "price_usd": metrics.get("usd"),
                        "volume_24h": metrics.get("usd_24h_vol"),
                        "extraction_timestamp": timestamp_str, # Adding metadata
                        "source_file": file_path.name
                    }
                    data_list.append(row)

        except Exception as error:
            print(f"Error reading {file_path.name}: {error}")
            continue

    # Create DataFrame
    if data_list:
        df = pd.DataFrame(data_list)
        
        # Data cleaning by converting price to float
        df["price_usd"] = df["price_usd"].astype(float)
        
        # Shows the number of rows processed
        print(f"Processed {len(df)} rows of data.")

        # Previews the data
        print(df.head())

        # Implement idempotency by saving the entire cleaned data on a single parquet file
        output_file = SILVER_DIR / "silver_crypto_prices.parquet"
        
        df.to_parquet(output_file, index=False)
        print(f"Saved silver data to: {output_file}")
        
    else:
        print("No valid data extracted.")

# Entry point for running the silver layer (clean) locally
if __name__ == "__main__":
    process_silver_local()
