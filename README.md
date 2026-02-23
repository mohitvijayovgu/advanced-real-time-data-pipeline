# Advanced Real-Time IoT Data Pipeline

A real-time data pipeline that monitors a folder for incoming IoT sensor CSV files, validates and transforms each file, computes aggregated metrics per device, and stores everything in a PostgreSQL database — fully automated with fault tolerance and zero manual intervention.

---

## Overview

- **Dataset:** 405,184 rows of IoT sensor readings from 3 devices (CO, humidity, LPG, smoke, temperature, light, motion)
- **Database:** PostgreSQL hosted on [Neon](https://neon.tech) (cloud PostgreSQL)
- **Processing:** Real-time file monitoring, validation, transformation, aggregation, and storage
- **Fault Tolerance:** Retry mechanism (3 attempts), quarantine system, connection pooling, row-level deduplication via `PRIMARY KEY(sensor_id, timestamp)`

---

## Quick Start

### 1. Clone the repository
```bash
git clone https://github.com/mohitvijayovgu/advanced-real-time-data-pipeline.git
cd advanced-real-time-data-pipeline
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Download the dataset
Download `iot_telemetry_data.csv` from [Kaggle](https://www.kaggle.com/datasets/garystafford/environmental-sensor-data-132k) and place it in the project root directory.

### 4. Configure environment variables
```bash
cp .env.example .env
```

Edit `.env` with your database credentials:
```env
DB_HOST=your_postgresql_host
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
DB_SSLMODE=require
```

> Using local PostgreSQL? Set `DB_HOST=localhost` and `DB_SSLMODE=disable`

### 5. Run the pipeline
```bash
python3 main.py
```

The pipeline automatically creates all folders, sets up database tables, and starts processing files in real time. Stop with `Ctrl + C`.

---

## Useful Commands

```bash
# Monitor live logs
tail -f logs/pipeline.log

# Check quarantined files and reasons
cat logs/quarantine.log
```

---

## Project Structure

```
├── main.py                 # Entry point — starts simulator + monitor
├── data_simulator.py       # Simulates real-time sensor data with 15% corruption
├── config.yaml             # All pipeline settings
├── requirements.txt        # Python dependencies
├── .env.example            # Environment variable template
├── src/
│   ├── data_validator.py   # Null, type, range validation
│   ├── data_processor.py   # Timestamp parsing, type enforcement
│   ├── data_aggregator.py  # Min/max/mean/std per device per file
│   ├── file_monitor.py     # Folder polling, duplicate detection
│   └── db_handler.py       # Connection pool, batch inserts, retry
└── sql/
    └── schema.sql          # PostgreSQL schema with indexes
```
