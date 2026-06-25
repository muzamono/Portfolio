-- sql/init.sql
-- This file runs ONCE when the postgres container first starts.
-- It creates the table where your transformed data will be stored.
-- This is the destination for the "Load" step in ETL.

-- PostgreSQL will skip this file on subsequent startups (data already exists).

CREATE TABLE IF NOT EXISTS stock_prices (
    id          SERIAL PRIMARY KEY,           -- Auto-incrementing row ID
    symbol      VARCHAR(10) NOT NULL,         -- ID from the original data source (prevents duplicate loads)
    date        DATE NOT NULL,                -- Date of the stock price
    open        NUMERIC,                      -- Opening price
    high        NUMERIC,                      -- Highest price
    low         NUMERIC,                      -- Lowest price
    close       NUMERIC,                      -- Closing price
    volume      BIGINT,                       -- Trading volume
    fetched_at  TIMESTAMP DEFAULT NOW(),      -- When the record was inserted into the database
    UNIQUE(symbol, date)                      -- Prevent duplicate records for the same symbol+date
);
