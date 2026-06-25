# dags/etl_pipeline.py
# ROLE: Orchestration — tells Airflow WHAT to run and in WHAT ORDER.
# This file does NOT contain ETL logic. It just imports and calls your etl/ functions.
# Airflow scans this folder automatically and registers any DAG it finds.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import time

# Import your ETL functions — Airflow runs these as tasks
from etl.extract import extract
from etl.transform import transform
from etl.load import load


# --- DEFAULT ARGS ---
# Applied to every task in this DAG unless overridden.
default_args = {
    "owner": "brad",                          # Who owns this DAG (shown in UI)
    "retries": 1,                             # Retry a failed task once before marking it failed
    "retry_delay": timedelta(minutes=5),      # Wait 5 mins before retrying
}


# --- DAG DEFINITION ---
# The DAG object is the pipeline itself. Think of it as the container for all your tasks.
with DAG(
    dag_id="etl_pipeline",                    # Unique name shown in Airflow UI
    default_args=default_args,
    description="Extract, transform, load pipeline",
    schedule="@daily",                        # Run once per day. Options: @hourly, @weekly, "0 6 * * *" (cron)
    start_date=datetime(2024, 1, 1),          # Airflow won't run for dates before this
    catchup=False,                            # Don't backfill missed runs when you first deploy
    tags=["etl"],                             # Optional labels for filtering in the UI
) as dag:


    # --- TASKS ---
    # Each PythonOperator wraps one function as a task.
    # Airflow tracks each task's status (success/failed/running) independently.

    # XCom (cross-communication): Airflow passes data between tasks via XCom.
    # When a PythonOperator function returns a value, Airflow stores it automatically.
    # The next task retrieves it via ti.xcom_pull().

    def extract_task(**context):
        """Wrapper: calls extract() for each symbol and pushes results to XCom."""
        symbols = ["AAPL", "NVDA"]  # add or remove symbols here
        all_extracted = []
        for symbol in symbols:
            result = extract(symbol)
            if result:
                all_extracted.append(result)
            else:
                print(f"[extract_task] Skipping {symbol} — extract returned None")
            time.sleep(15) # Sleep to avoid hitting API rate limits
            context["ti"].xcom_push(key="raw_data", value=all_extracted)

    def transform_task(**context):
        """Wrapper: transforms each extracted result and pushes combined records."""
        all_extracted = context["ti"].xcom_pull(key="raw_data", task_ids="extract")
        all_records = []
        for extracted in all_extracted:
            all_records.extend(transform(extracted))
        context["ti"].xcom_push(key="cleaned_data", value=all_records)

    def load_task(**context):
        """Wrapper: pulls cleaned data from XCom, loads into DB."""
        cleaned = context["ti"].xcom_pull(key="cleaned_data", task_ids="transform")
        load(cleaned)


    # Create the actual Airflow task objects
    t_extract = PythonOperator(task_id="extract",   python_callable=extract_task)
    t_transform = PythonOperator(task_id="transform", python_callable=transform_task)
    t_load = PythonOperator(task_id="load",      python_callable=load_task)

    # --- DEPENDENCY CHAIN ---
    # >> means "then". This defines the order tasks run in.
    # extract must finish → then transform → then load
    t_extract >> t_transform >> t_load
