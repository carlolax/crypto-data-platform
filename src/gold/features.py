import pandas as pd
import os

# File and directory config
SILVER_FILE = "data/silver/market_history.csv"
GOLD_FOLDER = "data/gold"

def load_silver_data():
    if not os.path.exists(SILVER_FILE):
        return None

    df = pd.read_csv(SILVER_FILE)

    return df

def create_features(df):
    print("Generating ML Features.")

    # Sort by coin and timestamp
    df = df.sort_values(by=['coin', 'timestamp'])

    # 1. Calculate a "Moving Average" (Simulated for now)
    df['SMA_7'] = df.groupby('coin')['price_usd'].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    )

    # 2. Calculate Volatility (Standard Deviation)
    df['volatility'] = df.groupby('coin')['price_usd'].transform(
        lambda x: x.rolling(window=7, min_periods=1).std()
    )

    # Fill NaN values (common in the first few rows of data) with 0
    df = df.fillna(0)

    return df

def save_gold_data(df):
    if not os.path.exists(GOLD_FOLDER):
        os.makedirs(GOLD_FOLDER)

    output_path = f"{GOLD_FOLDER}/ml_training_set.csv"
    df.to_csv(output_path, index=False)
    print(f"Data saved to: {output_path}")
    print(f"Final ML Set Shape: {df.shape}")

if __name__ == "__main__":
    df = load_silver_data()

    if df is not None:
        df_features = create_features(df)
        save_gold_data(df_features)
    
        print("Data Preview:")
        print(df_features[['timestamp', 'coin', 'price_usd', 'SMA_7']].tail())
    else:
        print("No data to process. Run the clean script first!")