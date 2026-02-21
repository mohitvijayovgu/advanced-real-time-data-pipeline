import pandas as pd
import logging
import os
from src.config_loader import load_config

config = load_config()
logger = logging.getLogger(__name__)

def validate_required_fields(df):
    # Pull required fields list from config
    required_fields = config['validation']['required_fields']
    errors = []

    for field in required_fields:
        # Count nulls in this field
        null_count = df[field].isnull().sum()
        if null_count > 0:
            errors.append(f"Column '{field}' has {null_count} null values")

    return errors

def validate_numeric_fields(df):
    numeric_fields = config['validation']['numeric_fields']
    errors = []

    for field in numeric_fields:
        if field in df.columns:
            before_nulls = df[field].isnull().sum()
            converted = pd.to_numeric(df[field], errors='coerce')
            after_nulls = converted.isnull().sum()
            # New nulls after conversion means non-numeric values existed
            new_non_numeric = after_nulls - before_nulls
            if new_non_numeric > 0:
                errors.append(f"Column '{field}' has {new_non_numeric} non-numeric values")

    return errors

def validate_ranges(df):
    errors = []

    temp_min = config['validation']['temperature_min']
    temp_max = config['validation']['temperature_max']
    humidity_min = config['validation']['humidity_min']
    humidity_max = config['validation']['humidity_max']

    # Check temperature range
    if 'temp' in df.columns:
        out_of_range = df[
            (df['temp'] < temp_min) | (df['temp'] > temp_max)
        ].shape[0]
        if out_of_range > 0:
            errors.append(f"Column 'temp' has {out_of_range} values outside range [{temp_min}, {temp_max}]")

    # Check humidity range
    if 'humidity' in df.columns:
        out_of_range = df[
            (df['humidity'] < humidity_min) | (df['humidity'] > humidity_max)
        ].shape[0]
        if out_of_range > 0:
            errors.append(f"Column 'humidity' has {out_of_range} values outside range [{humidity_min}, {humidity_max}]")

    return errors

def quarantine_file(filepath, errors, df=None):
    filename = os.path.basename(filepath)
    quarantine_dir = config['pipeline']['quarantine_folder']

    os.makedirs(quarantine_dir, exist_ok=True)
    
    # Move file from data/ to quarantine/
    quarantine_path = os.path.join(quarantine_dir, filename)
    os.rename(filepath, quarantine_path)

    log_dir = config['pipeline']['log_folder']
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, 'quarantine.log')

    # Append mode so previous log entries are never overwritten
    with open(log_path, 'a') as f:
        f.write(f"\n{'='*50}\n")
        f.write(f"File: {filename}\n")
        f.write(f"Reasons:\n")
        for error in errors:
            f.write(f"  - {error}\n")
        
        if df is not None:
            f.write(f"\nRow level details:\n")
            
            # Filter rows where sensor_id is null
            null_rows = df[df['sensor_id'].isnull()]
            if not null_rows.empty:
                f.write(f"  Null sensor_id ({len(null_rows)} rows):\n")
                for idx, row in null_rows.iterrows():
                    # Log key fields so issue is immediately identifiable
                    f.write(f"    Row {idx}: timestamp={row['timestamp']}, temp={row['temp']}, humidity={row['humidity']}\n")
            
            # Filter rows where temp is physically impossible
            bad_temp_rows = df[df['temp'] > 50]
            if not bad_temp_rows.empty:
                f.write(f"  Bad temp ({len(bad_temp_rows)} rows):\n")
                for idx, row in bad_temp_rows.iterrows():
                    # Log sensor_id so we know which device sent bad reading
                    f.write(f"    Row {idx}: timestamp={row['timestamp']}, sensor_id={row['sensor_id']}, temp={row['temp']}\n")

    logger.warning("File quarantined: %s", filename)

def validate(filepath):
    filename = os.path.basename(filepath)
    logger.info("Validating file: %s", filename)

    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        logger.error("Could not read file %s: %s", filename, str(e))
        return None, [f"Could not read file: {str(e)}"]

    all_errors = []

    errors1 = validate_required_fields(df)
    all_errors.extend(errors1)

    errors2 = validate_numeric_fields(df)
    all_errors.extend(errors2)

    errors3 = validate_ranges(df)
    all_errors.extend(errors3)

    if all_errors:
        logger.warning("File %s failed validation: %s", filename, all_errors)
        # Pass df to log exact row details in quarantine
        quarantine_file(filepath, all_errors, df)
        return None, all_errors

    logger.info("File %s passed validation", filename)
    return df, []