import json
import pandas as pd
import glob
import os

# Directories config
BRONZE_FOLDER = "data/bronze"
SILVER_FOLDER = "data/silver"

def get_latest_file():
    list_of_files = glob.glob(f"{BRONZE_FOLDER}/*.json")

    if not list_of_files:
        return None
    
    return max(list_of_files, key=os.path.getctime)

def process_data(file_path):
    print(f"Processing file: {file_path}")

    with open(file_path, "r") as file:
        raw_data = json.load(file)

    # Extract timestamp from filename
    filename = os.path.basename(file_path)
    # Strip "raw_" prefix to get the date
    date_str = filename.replace("raw_", "").replace(".json", "")

    # Transform the complex data into a list of rows
    clean_rows = []

    for coin_name, metrics in raw_data.items():
        row = {
            "timestamp": date_str,
            "coin": coin_name,
            "price_usd": metrics['usd'],
            "market_cap": metrics['usd_market_cap'],
            "volume_24h": metrics['usd_24h_vol'],
            "change_24h": metrics['usd_24h_change']
        }
        clean_rows.append(row)

    return pd.DataFrame(clean_rows)

def save_silver_data(df):
    if not os.path.exists(SILVER_FOLDER):
        os.makedirs(SILVER_FOLDER)

    # Save the new data to the existing CSV file
    csv_path = f"{SILVER_FOLDER}/market_history.csv"

    # If header exists, skip it
    header = not os.path.exists(csv_path)

    df.to_csv(csv_path, mode='a', header=header, index=False)
    print(f"Data saved to {csv_path}")
    print(f"Current shape: {df.shape}")

if __name__ == "__main__":
    latest_file = get_latest_file()

    if latest_file:
        df = process_data(latest_file)
        save_silver_data(df)
        print(f"\n Preview of data collected:")
        print(df.head())
    else:
        print("No data found. Run the ingestion script first!")