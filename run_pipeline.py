# run_pipeline.py
# Standalone test script — chains extract -> transform -> load manually.
# Use this to verify the whole pipeline works BEFORE wiring it into Airflow.
# Run from project root: python run_pipeline.py

from etl.extract import extract
from etl.transform import transform
from etl.load import load
import time

symbols = ["AAPL", "NVDA"]  # Start with just one symbol to conserve API requests

for symbol in symbols:
    print(f"\n--- Running pipeline for {symbol} ---")

    extracted = extract(symbol)

    if extracted is None:
        print(f"[run_pipeline] Skipping {symbol} — extract failed (check rate limit or API key)")
        continue  # Move to next symbol instead of crashing
    
    records = transform(extracted)
    load(records)

    time.sleep(15)

print("\n--- Pipeline run complete ---")