import os
import time
from datetime import datetime, timedelta, timezone

import kaggle
import pandas as pd
import requests
from tvDatafeed import TvDatafeed, Interval




# Download the latest dataset from Kaggle
def download_latest_dataset(dataset_slug):
    """Download the latest dataset from Kaggle."""
    # Use Kaggle Python API to download the dataset directly to memory
    kaggle.api.dataset_download_files(dataset_slug, path="upload", unzip=True)


def download_latest_metadata(dataset_slug):
    """Download the dataset metadata from Kaggle."""
    kaggle.api.dataset_metadata(dataset_slug, path="upload")

def fetch_and_update_data(symbol,exchange,data_interval,output_file,lookback_days = 20):
    # Load existing data if available
    if os.path.exists(output_file):
        df_existing = pd.read_csv(output_file, parse_dates=['datetime'])
        df_existing['datetime'] = pd.to_datetime(df_existing['datetime'])
        last_datetime = df_existing['datetime'].max()
        print(f"Last datetime in dataset: {last_datetime}")
    else:
        df_existing = pd.DataFrame()
        last_datetime = None
        print("No existing data found. Starting fresh...")

    # Fetch latest data: 375 bars/day * 375 days
    print(f"Fetching {lookback_days * 375} 1-minute bars from TradingView...")
    df_new = tv.get_hist(
        symbol=symbol,
        exchange=exchange,
        interval=data_interval,
        n_bars=lookback_days * 375
    )

    if df_new is None or df_new.empty:
        print("No data fetched.")
        return

    df_new.reset_index(inplace=True)
    df_new.rename(columns={'index': 'datetime'}, inplace=True)

    if last_datetime:
        df_new = df_new[df_new['datetime'] > last_datetime]

    if df_new.empty:
        print("No new data to append.")
        return

    # Combine and save
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    df_combined.drop_duplicates(subset='datetime', inplace=True)
    df_combined.sort_values('datetime', inplace=True)

    df_combined.to_csv(output_file, index=False)
    print(f"Data updated. Total rows: {len(df_combined)}")


# Main execution
if __name__ == "__main__":
    dataset_slug = "endgamelama/intraday-dataset"  # Kaggle dataset slug
    #currency_pair = "btcusd"
    upload_dir = "upload"

    # Ensure the 'upload/' directory exists
    if not os.path.exists(upload_dir):
        os.makedirs(upload_dir)

    existing_data_filename = os.path.join(
        upload_dir, "nifty_1min_data.csv"
    )  # The dataset file
    output_filename = existing_data_filename  # Output filename (same as the dataset name on Kaggle)

    print(f"Current time (UTC): {datetime.now(timezone.utc)}")
    
    # Step 1: Download the latest dataset and metadata from Kaggle
    print("Downloading dataset metadata from Kaggle...")
    download_latest_metadata(dataset_slug)  # Download metadata to 'upload/'
    
    print("Downloading dataset from Kaggle...")
    download_latest_dataset(dataset_slug)  # Download dataset to 'upload/'

    # Configuration
    symbol = 'NIFTY'
    exchange = 'NSE'
    data_interval = Interval.in_1_minute
    output_dir = upload_dir
    output_file = existing_data_filename
    lookback_days = 20  # How many trading days of data to fetch
    fetch_and_update_data(symbol,exchange,data_interval,output_file)
