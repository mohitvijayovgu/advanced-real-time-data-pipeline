import pandas as pd
import os
import time
import random
from datetime import datetime
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src.config_loader import load_config

config = load_config()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_FILE = os.path.join(BASE_DIR, "iot_telemetry_data.csv")
DATA_DIR = os.path.join(BASE_DIR, config['pipeline']['watch_folder'])

DEVICE_LOCATION_MAP = {
    'b8:27:eb:bf:9d:51': 'Lab-A',
    '00:0f:00:70:91:0a': 'Lab-B',
    '1c:bf:ce:15:ec:4d': 'Lab-C',
}

def load_source_data():
    df = pd.read_csv(SOURCE_FILE)
    # Convert unix epoch to readable timestamp directly in same column
    df['ts'] = pd.to_datetime(df['ts'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
    # Rename columns
    df = df.rename(columns={'ts': 'timestamp', 'device': 'sensor_id'})
    df = df.reset_index(drop=True)
    return df

def introduce_corruption(df):
    df = df.copy()
    # 1.5% of 405184 = ~6077 rows with null sensor_id
    corrupt_ids = random.sample(range(len(df)), max(1, int(len(df) * 0.015)))
    df.loc[corrupt_ids, 'sensor_id'] = None
    # 1.5% of 405184 = ~6077 rows with impossible temperature
    corrupt_temp = random.sample(range(len(df)), max(1, int(len(df) * 0.015)))
    df.loc[corrupt_temp, 'temp'] = 999.0
    return df

def split_and_drop_files(df):
    # Get settings from config
    chunk_size = config['pipeline']['chunk_size']
    poll_interval = config['pipeline']['poll_interval']

    # Split dataframe into list of chunks and Each chunk is 500 rows
    chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    print(f"Total files to drop: {len(chunks)}")
    # Create data/ folder if it doesn't exist
    os.makedirs(DATA_DIR, exist_ok=True)

    for i, chunk in enumerate(chunks):
        filename = f"iot_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{i}.csv"
        filepath = os.path.join(DATA_DIR, filename)
        chunk.to_csv(filepath, index=False)
        print(f"Dropped: {filename}")
        time.sleep(poll_interval)


def main():
    print("Loading source data...")
    df = load_source_data()

    print("Introducing corruption...")
    df = introduce_corruption(df)

    print(f"Dataset ready: {df.shape[0]} rows, {df.shape[1]} columns")
    print("Starting to drop files into data/ folder...")
    split_and_drop_files(df)

    print("All files dropped!")

if __name__ == "__main__":
    main()