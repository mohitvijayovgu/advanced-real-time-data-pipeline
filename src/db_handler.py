import logging
import os
import time
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from psycopg2 import pool

# Load credentials from .env file
load_dotenv()

logger = logging.getLogger(__name__)

connection_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT', 5432),
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    sslmode=os.getenv('DB_SSLMODE', 'require')
)


def get_connection():
    return connection_pool.getconn()

def release_connection(conn):
    connection_pool.putconn(conn)


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
        release_connection(conn)

def is_file_processed(filename):
    # Check if this file already exists in raw_sensor_data
    # Prevents duplicate inserts if pipeline restarts
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM raw_sensor_data WHERE source_file = %s LIMIT 1",
            (filename,)
        )
        result = cursor.fetchone()
        release_connection(conn)
        return result is not None
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
        for row in df.to_dict('records')
    ]

    sql = """
        INSERT INTO raw_sensor_data
        (timestamp, sensor_id, co, humidity, light_detected, lpg, motion_detected, smoke, temperature, source_file)
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    # Retry up to 3 times if DB is unavailable
    for attempt in range(1, retries + 1):
        try:
            conn = get_connection()
            cursor = conn.cursor()
            # Batch insert all rows in one query — faster than row by row
            start = time.time()
            execute_values(cursor, sql, records)
            conn.commit()
            duration = time.time() - start
            logger.info("Inserted %d rows into raw_sensor_data from %s in %.2f seconds", len(records), filename, duration)
            return
        except Exception as e:
            logger.error("Attempt %d failed for raw insert: %s", attempt, str(e))
            # Wait 2 seconds before next retry
            time.sleep(2)
        finally:
            try:
                release_connection(conn)
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
        for row in agg_df.to_dict('records')
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
        ON CONFLICT DO NOTHING
    """

    # Retry up to 3 times if DB is unavailable
    for attempt in range(1, retries + 1):
        try:
            conn = get_connection()
            cursor = conn.cursor()
            # Batch insert all rows in one query
            start = time.time()
            execute_values(cursor, sql, records)
            conn.commit()
            duration = time.time() - start
            logger.info("Inserted %d rows into aggregated_metrics in %.2f seconds", len(records), duration)
            return
        except Exception as e:
            logger.error("Attempt %d failed for aggregated insert: %s", attempt, str(e))
            # Wait 2 seconds before next retry
            time.sleep(2)
        finally:
            try:
                release_connection(conn)
            except:
                pass

    logger.error("All %d attempts failed for aggregated insert", retries)