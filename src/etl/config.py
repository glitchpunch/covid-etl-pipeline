import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATASET_URL = ("https://raw.githubusercontent.com/datasets/covid-19/main/"
               "data/countries-aggregated.csv")
RAW_PATH = ROOT / "data" / "raw" / "countries-aggregated.csv"
DAILY_DIR = ROOT / "data" / "processed" / "daily"
MONTHLY_DIR = ROOT / "data" / "processed" / "monthly"
METRICS_PATH = ROOT / "data" / "processed" / "run_metrics.json"
SCHEMA_SQL = ROOT / "sql" / "schema.sql"

DB = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", "5432")),
    dbname=os.getenv("PGDATABASE", "covid_dw"),
    user=os.getenv("PGUSER", "pipeline"),
    password=os.getenv("PGPASSWORD", "pipeline"),
)
