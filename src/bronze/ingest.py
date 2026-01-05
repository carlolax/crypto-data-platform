import requests
import json
import os
from datetime import datetime

# Setup Config
TARGET_FOLDER = "data/bronze"
OS_MAKEDIRS_MODE = 0o755 # Read/Write permissions

# CoinGecko API
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
COINGECKO_PARAMS = {
    "ids": "bitcoin,ethereum,solana",
    "vs_currencies": "usd",
    "include_market_cap": "true",
    "include_24hr_vol": "true",
    "include_24hr_change": "true"
}

def fetch_market_data():
    print(f"Fetching data from CoinGecko.")

    try:
        response = requests.get(COINGECKO_URL, params=COINGECKO_PARAMS, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as error:
        print(f"API Error: {error}")
        return None

def save_raw_data(data):
    # Ensure the folder exists
    if not os.path.exists(TARGET_FOLDER):
        os.makedirs(TARGET_FOLDER, mode=OS_MAKEDIRS_MODE)

    # Create a filename with a timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"{TARGET_FOLDER}/raw_{timestamp}.json"

    with open(filename, "w") as file:
        json.dump(data, file, indent=4)

    print(f"Data saved to: {filename}")

if __name__ == "__main__":
    market_data = fetch_market_data()

    if market_data:
        save_raw_data(market_data)
    else:
        print("Failed to fetch market data.")