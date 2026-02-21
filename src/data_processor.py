import pandas as pd
import logging
from src.config_loader import load_config

config = load_config()
logger = logging.getLogger(__name__)

def parse_timestamp(df):
    try:
        # Convert timestamp string back to pandas datetime object
        # This is needed for database storage and time-based calculations
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
        logger.info('Timestamp parsed successfully')
    except Exception as e:
        logger.warning("Timestamp parse failed: %s", str(e))
    return df

def ensure_numeric_columns(df):
    numeric_columns = config['validation']['numeric_fields']
    
    for col in numeric_columns:
        if col in df.columns:
            # Convert numeric_fields data to numeric value
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    logger.info("Numeric columns verified")
    return df


def ensure_boolean_columns(df):
    bool_columns = config['validation']['boolean_fields']
    
    for col in bool_columns:
        if col in df.columns:
            # Ensure light and motion are stored as proper booleans
            df[col] = df[col].astype(bool)
    
    logger.info("Boolean columns verified")
    return df


def rename_columns(df):
    column_mapping = {
        'temp': 'temperature',
        'light': 'light_detected',
        'motion': 'motion_detected'
    }
    
    # Only rename columns that actually exist in dataframe
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    
    logger.info("Columns renamed successfully")
    return df


def process(df):
    logger.info("Starting data processing")

    df = parse_timestamp(df)
    df = ensure_numeric_columns(df)
    df = ensure_boolean_columns(df)
    df = rename_columns(df)

    logger.info("Data processing completed. Shape: %s", str(df.shape))
    return df