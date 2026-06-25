# etl/extract.py
# ROLE: Extract — pull raw data from an external source (API, CSV, etc.)
# OUTPUT: Returns raw data as a Python list of dicts.
# RULE: This file should do NO transformation. Just fetch and return.

import dotenv
import requests 
import json
import os
from dotenv import load_dotenv  
from pathlib import Path

load_dotenv()

api_key = os.getenv("API_KEY")

def extract(symbol) -> dict:
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}&outputsize=compact"  
    response = requests.get(url, timeout=10)
    response.raise_for_status()  
    raw = response.json()        

    if "Time Series (Daily)" not in raw:
        print("Couldn't get stock data. Check your API key!")
        return None

    print(f"[extract] Fetched data for {symbol}")
    return {"symbol": symbol, "data": raw}


# Main execution for quick test when running extract.py standalone. Run from project root: python etl/extract.py

"""
if __name__ == "__main__":
    symbols = ["NVDA"]
    for symbol in symbols:
        stock_data = extract(symbol)
        if stock_data:
            print(stock_data[0])  
"""
