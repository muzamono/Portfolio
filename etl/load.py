# etl/load.py
# ROLE: Load — insert transformed records into PostgreSQL.
# INPUT: Cleaned list of dicts from transform.py
# OUTPUT: Nothing returned. Side effect: rows written to DB.
# RULE: No business logic here. Just write data.

import psycopg2  # pip install psycopg2-binary
import os
from dotenv import load_dotenv  # pip install python-dotenv

load_dotenv()  # Reads the .env file in project root and loads it into os.environ

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),  # 2nd arg = fallback if var is missing
    "port":     os.getenv("DB_PORT", 5432),
    "dbname":   os.getenv("DB_NAME", "etl_db"),
    "user":     os.getenv("DB_USER", "etl_user"),
    "password": os.getenv("DB_PASSWORD", "etl_pass"),
}


def load(records: list[dict]) -> None:

    if not records:
        print("[load] No records to insert.")
        return

    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO stock_prices (symbol, date, open, high, low, close, volume)
        VALUES (%(Symbol)s, %(Date)s, %(Open)s, %(High)s, %(Low)s, %(Close)s, %(Volume)s)
        ON CONFLICT (symbol, date) DO NOTHING;
    """
    # The %(key)s syntax maps dict keys to query placeholders — safer than f-strings (prevents SQL injection).
   
    cursor.executemany(insert_query, records)  # Runs the query once per record
    conn.commit()                              # Saves all inserts to the DB

    print(f"[load] Inserted {cursor.rowcount} new rows")

    cursor.close()
    conn.close()
