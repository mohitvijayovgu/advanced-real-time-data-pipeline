import os
import time
import logging
from src.config_loader import load_config
from src.db_handler import is_file_processed

config = load_config()

# Create required folders on startup if they don't exist So pipeline works on any fresh machine without manual setup
for folder in [
    config['pipeline']['watch_folder'],
    config['pipeline']['processed_folder'], 
    config['pipeline']['quarantine_folder'],
    config['pipeline']['log_folder'] 
]:
    os.makedirs(folder, exist_ok=True)

# Setup logging — every log message goes to both terminal and pipeline.log
log_path = os.path.join(config['pipeline']['log_folder'], 'pipeline.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_path),  # writes to logs/pipeline.log
        logging.StreamHandler()          # prints to terminal in real time
    ]
)

logger = logging.getLogger(__name__)


def get_new_files(data_dir, processed_files):
    # Get all CSV files in data/ that haven't been processed yet
    all_files = set(f for f in os.listdir(data_dir) if f.endswith('.csv'))
    new_files = all_files - processed_files
    return sorted(new_files)


def monitor(callback):
    data_dir = config['pipeline']['watch_folder']
    poll_interval = config['pipeline']['poll_interval']

    # Track processed files in memory for current session
    processed_files = set()

    logger.info("Pipeline started. Watching folder: %s", data_dir)
    logger.info("Polling every %s seconds", poll_interval)

    while True:
        new_files = get_new_files(data_dir, processed_files)

        for filename in new_files:
            filepath = os.path.join(data_dir, filename)

            # Check DB — skip if already processed in a previous run
            if is_file_processed(filename):
                logger.info("Skipping already processed file: %s", filename)
                processed_files.add(filename)
                continue

            logger.info("New file detected: %s", filename)
            try:
                # Call the pipeline function passed from main.py
                callback(filepath)
                # Mark as processed in memory
                processed_files.add(filename)
                logger.info("File processed successfully: %s", filename)
            except Exception as e:
                # Log error but keep running — pipeline never crashes
                logger.error("Failed to process %s: %s", filename, str(e))

        # Wait before checking again
        time.sleep(poll_interval)