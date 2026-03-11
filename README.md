# IBM ETL Project Overview

## Project Purpose
This project builds a small ETL pipeline that pulls IBM stock history from a public API, stages the raw history in PostgreSQL, then prepares cleaner and analytics-ready layers for reporting.

## Project Flow
1. Extract daily IBM stock data from the Alpha Vantage API.
2. Transform the API response into a pandas DataFrame.
3. Load new records into the staging table `IBM_history`.
4. Refresh the silver layer `IBM` to keep one clean latest record per `Date`.
5. Refresh the gold layer `IBM_gold` to calculate business metrics for analysis.

## Data Layers
### Stage Layer
- Table: `IBM_history`
- Purpose: store newly extracted API records before downstream processing
- Notes: this layer can contain repeated dates from multiple loads

### Silver Layer
- Table: `IBM`
- Purpose: keep the latest clean record for each business date
- Logic: use an upsert process on `Date` and keep the newest `Last_updated` value

### Gold Layer
- Table: `IBM_gold`
- Purpose: provide analytics-ready metrics from the silver table
- Metrics: daily return, 7-day moving average, 30-day moving average, 30-day volatility, and volume moving average

## Main Files
- [IBM_CDC-Enhanced.ipynb](IBM_CDC-Enhanced.ipynb): main notebook with extract, transform, load, silver refresh, gold refresh, and verification steps
- [IBM_CDC.ipynb](IBM_CDC.ipynb): earlier notebook version
- [IBM_StoredProcedure.sql](IBM_StoredProcedure.sql): PostgreSQL objects used to refresh the silver and gold layers
- [README.md](README.md): working rules and project instructions
- [.env](.env): local credentials and configuration values

## Configuration
The notebook reads local settings from `.env`.

Current keys:
- `API_URL`
- `DB_USERNAME`
- `DB_PASSWORD`
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `TABLE_NAME`

## How To Run
1. Open [IBM_CDC-Enhanced.ipynb](IBM_CDC-Enhanced.ipynb).
2. Run the import cell.
3. Run the config cell to load values from `.env`.
4. Run the extract and transform cells.
5. Run the load step into `IBM_history`.
6. Run the setup cell to create silver and gold SQL objects.
7. Run the silver and gold refresh cell.
8. Run the verification cell to review row counts and recent gold results.

## Verification
Use the notebook verification step to confirm:
- silver table row count and date range
- gold table row count and date range
- latest rows in `IBM_gold`

## Security Note
Database credentials are stored in `.env` and `.env` is excluded in `.gitignore` so secrets are not committed.
