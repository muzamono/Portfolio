# etl/transform.py
# ROLE: Transform — clean and reshape raw data into the format your DB table expects.
# INPUT: Raw list of dicts from extract.py
# OUTPUT: Cleaned list of dicts, ready to insert into PostgreSQL.
# RULE: No API calls, no DB connections here. Pure data manipulation only.


def transform(extracted: dict) -> list[dict]:
    """
    Takes raw API response and a symbol, returns cleaned records.
    Each output dict matches the columns in stock_prices table.
    """
    symbol = extracted["symbol"]
    raw = extracted["data"]

    stock_records = []
    for date, prices in raw["Time Series (Daily)"].items():
        stock_records.append({
            "Symbol": symbol,
            "Date": date,
            "Open": float(prices["1. open"]),
            "High": float(prices["2. high"]),
            "Low": float(prices["3. low"]),
            "Close": float(prices["4. close"]),
            "Volume": int(prices["5. volume"])
        })

    print(f"[transform] {len(stock_records)} records after transformation")
    return stock_records

if __name__ == "__main__":     # Quick test to run transform.py standalone with the sample_data.json file created by extract.py
    import json
    with open("sample_data.json") as f:
        raw_data = json.load(f)
    result = transform(raw_data)
    print(result)
