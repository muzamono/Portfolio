# Stock Price ETL Pipeline

An end-to-end ETL pipeline that pulls daily stock price data from the Alpha Vantage API, transforms it, and loads it into PostgreSQL — orchestrated by Apache Airflow running in Docker.

**Stack:** Python · Apache Airflow · PostgreSQL · Docker Compose

---

## Project Structure

```
stocks-tracker/
├── docker-compose.yml       # Spins up PostgreSQL + Airflow
├── run_pipeline.py          # Standalone script for local testing (no Airflow)
├── dags/
│   └── etl_pipeline.py      # Airflow DAG — orchestrates extract, transform, load
├── etl/
│   ├── extract.py           # Step 1: Fetch raw data from Alpha Vantage API
│   ├── transform.py         # Step 2: Clean and reshape raw data
│   └── load.py              # Step 3: Insert records into PostgreSQL
├── sql/
│   └── init.sql             # Creates the stock_prices table on first startup
└── logs/                    # Airflow writes task logs here automatically
```

---

## How the Files Interact

```
[Alpha Vantage API]
        |
        | HTTP request (requests.get)
        v
  extract.py  ──returns dict──>  transform.py  ──returns list[dict]──>  load.py
                                                                             |
                                                                             | psycopg2 INSERT
                                                                             v
                                                                      [PostgreSQL]
                                                                      (stock_prices table)

        ^────────────────────────────────────────────────────────────────────^
                       Airflow (dags/etl_pipeline.py) calls all three
                       in sequence and passes data between them via XCom
```

| File | Role | Interacts With |
|------|------|----------------|
| `docker-compose.yml` | Starts PostgreSQL and Airflow containers | Nothing — infrastructure only |
| `sql/init.sql` | Creates the `stock_prices` table | Runs once inside the postgres container on first start |
| `etl/extract.py` | Fetches raw data from Alpha Vantage | External API via HTTP |
| `etl/transform.py` | Reshapes and cleans raw API response | Pure Python — no network or DB calls |
| `etl/load.py` | Inserts cleaned records into PostgreSQL | PostgreSQL via psycopg2 |
| `dags/etl_pipeline.py` | Orchestrates the three steps above | Imports and calls extract, transform, load |

### Data flow:

1. `extract.py` calls the Alpha Vantage API for each ticker symbol and returns the raw JSON response bundled with the symbol name
2. `transform.py` reshapes the nested JSON into a flat list of records, renames fields, and casts types
3. `load.py` inserts each record into the `stock_prices` table, skipping duplicates
4. `dags/etl_pipeline.py` is what Airflow reads — it runs steps 1→2→3 in order on a daily schedule

---

## Setup

### Prerequisites
- Docker Desktop running
- Python environment with dependencies installed (`pip install requests psycopg2-binary python-dotenv`)

### 1. Configure environment variables

Create a `.env` file in the project root:
```
API_KEY=your_alpha_vantage_api_key
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_db
DB_USER=etl_user
DB_PASSWORD=etl_pass
```

Never commit `.env` to GitHub — it is listed in `.gitignore`.

### 2. Start containers
```bash
docker-compose up -d
```

### 3. Initialize Airflow (first time only)
```bash
docker-compose run airflow_webserver airflow db init
docker-compose run airflow_webserver airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com
```

Then restart:
```bash
docker-compose down
docker-compose up -d
```

---

## Running the Pipeline

### Option A — Local test (no Airflow)
Tests the full pipeline as plain Python, connecting to the PostgreSQL container directly:
```bash
python run_pipeline.py
```

Use this to verify ETL logic works before touching Airflow.

### Option B — Via Airflow
1. Open http://localhost:8080 and log in with `admin / admin`
2. Find `etl_pipeline` and toggle it on
3. Click the trigger button (▶) to run manually

The pipeline runs on a daily schedule automatically once enabled.

### Query loaded data
```bash
docker-compose exec postgres psql -U etl_user -d etl_db -c "SELECT symbol, COUNT(*) FROM stock_prices GROUP BY symbol;"
```

---

## Database Schema

```sql
CREATE TABLE stock_prices (
    id         SERIAL PRIMARY KEY,
    symbol     VARCHAR(10) NOT NULL,
    date       DATE NOT NULL,
    open       NUMERIC,
    high       NUMERIC,
    low        NUMERIC,
    close      NUMERIC,
    volume     BIGINT,
    fetched_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (symbol, date)
);
```

`UNIQUE (symbol, date)` prevents duplicate rows if the pipeline reruns for the same day. The `ON CONFLICT DO NOTHING` clause in `load.py` handles this gracefully.

---

## Key Concepts

**Why separate extract / transform / load?**
Each step has one job. If the API goes down, only extract fails — transform and load logic remain untouched and rerunnable independently.

**What is XCom?**
Airflow's mechanism for passing data between tasks. Since each task runs in its own process, data is pushed to XCom at the end of one task and pulled at the start of the next.

**Why `catchup=False`?**
Prevents Airflow from backfilling all missed runs when a DAG is first deployed with a historical `start_date`. Only runs from the current date forward.

**Why `time.sleep()` between API calls?**
Alpha Vantage's free tier enforces a limit of 5 requests per minute. A short delay between ticker requests avoids hitting that limit when running multiple symbols.