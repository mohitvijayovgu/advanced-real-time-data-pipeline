import os
import sys
import logging
import threading

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.file_monitor import monitor
from src.data_validator import validate
from src.data_processor import process
from src.data_aggregator import aggregate
from src.db_handler import insert_raw_data, insert_aggregated_metrics, create_tables

# Import simulator functions
from data_simulator import load_source_data, introduce_corruption, split_and_drop_files

logger = logging.getLogger(__name__)


def handle_file(filepath):
    # Full pipeline for a single incoming file.
    filename = os.path.basename(filepath)

    # Step 1: Validate
    df, errors = validate(filepath)
    if df is None:
        logger.warning("Skipping invalid file: %s | Errors: %s", filename, errors)
        return

    # Step 2: Process
    df = process(df)

    # Step 3: Aggregate
    agg_df = aggregate(df, filename)

    # Step 4: Insert raw data into DB
    insert_raw_data(df, filename)

    # Step 5: Insert aggregated metrics into DB
    insert_aggregated_metrics(agg_df)

    logger.info("Pipeline completed for: %s", filename)


def run_simulator():
    # This will runs data simulator in background.
    logger.info("Starting data simulator...")
    df = load_source_data()
    df = introduce_corruption(df)
    split_and_drop_files(df)


if __name__ == "__main__":
    logger.info("Starting IoT data pipeline...")

    # Create DB tables if they don't exist
    create_tables()

    # Start simulator in background thread
    simulator_thread = threading.Thread(target=run_simulator)
    simulator_thread.daemon = True
    simulator_thread.start()

    # Start file monitor in main thread
    monitor(handle_file)