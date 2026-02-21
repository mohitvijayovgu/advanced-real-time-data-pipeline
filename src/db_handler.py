import logging
import os
import time
from psycopg2.extras import execute_values
import psycopg2
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()

logger = logging.getLogger(__name__)


def get_connection():
    # Connect to Neon PostgreSQL using credentials from .env
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT', 5432),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        sslmode=os.getenv('DB_SSLMODE', 'require')
    )


def create_tables():
    # Read and run schema.sql to create tables if they don't exist
    schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sql', 'schema.sql')
    with open(schema_path, 'r') as f:
        schema_sql = f.read()
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(schema_sql)
        conn.commit()
        logger.info("Tables created successfully")
    except Exception as e:
        # Rollback if anything fails so that our DB stays clean
        conn.rollback()
        logger.error("Failed to create tables: %s", str(e))
    finally:
        # Always close connection even if error occurs
        conn.close()

def is_file_processed(filename):
    # Check if this file already exists in raw_sensor_data
    # Prevents duplicate inserts if pipeline restarts
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM raw_sensor_data WHERE source_file = %s",
            (filename,)
        )
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    except Exception as e:
        logger.error("Failed to check processed file: %s", str(e))
        return False

def insert_raw_data(df, filename, retries=3):
    # Add source file column so each row knows which file it came from
    df = df.copy()
    df['source_file'] = filename

    # Build list of tuples which will create one tuple per row for batch insert
    records = [
        (
            row['timestamp'],
            row['sensor_id'],
            row.get('co'),
            row.get('humidity'),
            row.get('light_detected'),
            row.get('lpg'),
            row.get('motion_detected'),
            row.get('smoke'),
            row.get('temperature'),
            row['source_file']
        )
        for _, row in df.iterrows()
    ]

    sql = """
        INSERT INTO raw_sensor_data
        (timestamp, sensor_id, co, humidity, light_detected, lpg, motion_detected, smoke, temperature, source_file)
        VALUES %s
    """

    # Retry up to 3 times if DB is unavailable
    for attempt in range(1, retries + 1):
        try:
            conn = get_connection()
            cursor = conn.cursor()
            # Batch insert all rows in one query — faster than row by row
            execute_values(cursor, sql, records)
            conn.commit()
            logger.info("Inserted %d rows into raw_sensor_data from %s", len(records), filename)
            return
        except Exception as e:
            logger.error("Attempt %d failed for raw insert: %s", attempt, str(e))
            # Wait 2 seconds before next retry
            time.sleep(2)
        finally:
            try:
                conn.close()
            except:
                pass

    logger.error("All %d attempts failed for raw insert of %s", retries, filename)


def insert_aggregated_metrics(agg_df, retries=3):
    # Build list of tuples — one tuple per device per file
    records = [
        (
            row['sensor_id'],
            row.get('co_min'), row.get('co_max'), row.get('co_mean'), row.get('co_std'),
            row.get('humidity_min'), row.get('humidity_max'), row.get('humidity_mean'), row.get('humidity_std'),
            row.get('lpg_min'), row.get('lpg_max'), row.get('lpg_mean'), row.get('lpg_std'),
            row.get('smoke_min'), row.get('smoke_max'), row.get('smoke_mean'), row.get('smoke_std'),
            row.get('temperature_min'), row.get('temperature_max'), row.get('temperature_mean'), row.get('temperature_std'),
            row.get('source_file')
        )
        for _, row in agg_df.iterrows()
    ]

    sql = """
        INSERT INTO aggregated_metrics
        (sensor_id,
         co_min, co_max, co_mean, co_std,
         humidity_min, humidity_max, humidity_mean, humidity_std,
         lpg_min, lpg_max, lpg_mean, lpg_std,
         smoke_min, smoke_max, smoke_mean, smoke_std,
         temperature_min, temperature_max, temperature_mean, temperature_std,
         source_file)
        VALUES %s
    """

    # Retry up to 3 times if DB is unavailable
    for attempt in range(1, retries + 1):
        try:
            conn = get_connection()
            cursor = conn.cursor()
            # Batch insert all rows in one query
            execute_values(cursor, sql, records)
            conn.commit()
            logger.info("Inserted %d rows into aggregated_metrics", len(records))
            return
        except Exception as e:
            logger.error("Attempt %d failed for aggregated insert: %s", attempt, str(e))
            # Wait 2 seconds before next retry
            time.sleep(2)
        finally:
            try:
                conn.close()
            except:
                pass

    logger.error("All %d attempts failed for aggregated insert", retries)