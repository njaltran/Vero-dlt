# Data Ingestion with dlt

Vero uses [dlt](https://dlthub.com) (data load tool) to load data into DuckDB for analytics.

## Overview

dlt is a Python-first data pipeline framework that makes it easy to load data from any source into any destination. In Vero, dlt replaces traditional CSV loading with a proper pipeline that supports:

- Schema inference and evolution
- Primary key tracking
- Write dispositions (replace, append, merge)
- Pipeline state management

## Architecture

```
CSV Files (db/init/data/*.csv)
    → dlt Pipeline (pipeline.py)
    → DuckDB (local file-based warehouse)
    → BSL Semantic Model
    → MCP Server / Streamlit / FastAPI
```

## Running the Pipeline

```bash
# Load all tables from CSV into DuckDB
python pipeline.py
```

This will:
1. Read all CSV files from `db/init/data/`
2. Create/replace tables in DuckDB: `fact_sales`, `dim_customer`, `dim_store`, `dim_product`, `dim_date`, `orders`, `orderrows`, `currencyexchange`
3. Print load statistics

## Configuration

Pipeline settings are in `constants.py`:

```python
PIPELINE_NAME = "contoso"
DATASET_NAME = "contoso_data"
DESTINATION = "duckdb"
```

Additional dlt config in `.dlt/config.toml` and `.dlt/secrets.toml`.

## Data Sources

The pipeline loads from the Contoso Retail sample dataset (CSV files):

| Table | File | Primary Key | Rows |
|-------|------|-------------|------|
| `dim_date` | date.csv | date | ~3,654 |
| `currencyexchange` | currencyexchange.csv | (date, from_currency, to_currency) | ~91,326 |
| `dim_customer` | customer.csv | customerkey | ~104,991 |
| `dim_store` | store.csv | storekey | 75 |
| `dim_product` | product.csv | productkey | 2,518 |
| `orders` | orders.csv | orderkey | ~83,131 |
| `orderrows` | orderrows.csv | (orderkey, linenumber) | ~199,874 |
| `fact_sales` | sales.csv | (orderkey, linenumber) | ~199,874 |

## Extending

To add new data sources, create additional `@dlt.resource` functions in `sources.py` and add them to the `get_sources()` return list.
