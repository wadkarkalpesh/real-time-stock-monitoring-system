# Real-Time Stock Monitoring System (ETL + OLAP)

A Python pipeline that pulls live stock market data, runs it through an ETL
process, and loads it into SQLite for OLAP-style analytical queries (trends,
aggregates, moving averages) instead of just raw price logs.

## Why this project

Most beginner stock projects just fetch a price and print it. This one treats
stock data like a real data engineering problem:

- **Extract** — pull live market data from Yahoo Finance
- **Transform** — clean and structure the raw data into analyzable form
- **Load** — store it in SQLite so it can be queried repeatedly, not just printed once
- **Analyze (OLAP)** — run aggregate/analytical SQL queries on top of the loaded data

## Tech Stack

- **Language:** Python
- **Data Source:** Yahoo Finance API
- **Storage:** SQLite
- **Pipeline:** Custom ETL script (`src/`)
- **Analytics:** OLAP-style SQL queries against the SQLite database

## How It Works

1. `src/q.py` triggers the pipeline
2. Live stock data is fetched from Yahoo Finance
3. Data is transformed and loaded into a local SQLite database
4. OLAP queries run on top of the loaded data to answer analytical questions
   (e.g. price trends, volume aggregates over time)

## Screenshots

**ETL + OLAP Dashboard**
![ETL Dashboard](https://github.com/wadkarkalpesh/real-time-stock-monitoring-system/blob/main/Screenshots/etl%2Bolap%20dashboard.png)

**Live Data Fetch from Yahoo Finance**
![Yahoo Data Fetch](https://github.com/wadkarkalpesh/real-time-stock-monitoring-system/blob/main/Screenshots/yahoo%20finance%20data%20fetched.png)

**OLAP Queries in Action**
![OLAP Queries](https://github.com/wadkarkalpesh/real-time-stock-monitoring-system/blob/main/Screenshots/OLAP%20Queries.png)

## How to Run

```bash
pip install -r requirements.txt
python src/q.py
```

## Possible Next Steps

- [ ] Add a scheduler to refresh data automatically at intervals
- [ ] Move storage to a proper time-series or production-grade DB (e.g. PostgreSQL)
- [ ] Add a simple API layer (FastAPI) to serve the OLAP results
- [ ] Add a small frontend/dashboard to visualize trends over time

## Status

Actively maintained as part of ongoing data engineering practice.



