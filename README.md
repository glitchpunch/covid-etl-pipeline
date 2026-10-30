# COVID-19 Batch ETL Pipeline (PySpark + PostgreSQL)

A small, end-to-end batch data pipeline: public CSV -> data-quality gate -> Spark transforms ->
partitioned Parquet -> idempotent PostgreSQL load, with an Airflow DAG for scheduling.

```
GitHub CSV --> ingest --> quality checks --> Spark transforms --> Parquet (partitioned by year)
                              |                                        |
                       fail on bad data                      staging table + upsert --> PostgreSQL
```

**Dataset:** [datasets/covid-19](https://github.com/datasets/covid-19) `countries-aggregated.csv`
(~161k rows: cumulative confirmed / recovered / deaths per country per day).

## What it does
- **Ingest:** idempotent download (skips if the file exists).
- **Quality gate (hard fails):** empty dataset, null keys, duplicate (date, country), negative counts, null values.
  **Soft warnings:** deaths > confirmed. Hard failures stop the run before anything is written.
- **Transform (Spark window functions):** daily new cases/deaths from cumulative counts (`lag`),
  7-day rolling average, case-fatality %, monthly rollup. Downward revisions in the source
  (cumulative count decreases) are flagged with `is_correction` and floored at 0 instead of silently dropped.
- **Storage:** Parquet partitioned by `year`.
- **Load:** `COPY` into a temp staging table, then `INSERT ... ON CONFLICT DO UPDATE`, in one transaction.
  Re-running never duplicates rows.
- **Observability:** per-stage timings + row counts written to `data/processed/run_metrics.json` and a `pipeline_runs` table.
- **Tests:** 6 pytest tests (delta logic, rolling window, per-country isolation, correction flagging, quality gates).

## Run it
```bash
docker compose up -d                      # PostgreSQL
pip install -r requirements.txt           # needs Java 11+ for Spark
cd src && python -m etl.run_pipeline      # full run
cd .. && pytest -q tests
psql -h localhost -U pipeline covid_dw -f sql/analytics.sql   # password: pipeline
```
Options: `--skip-load` (stop after Parquet), `--force-download`.
Scheduling: `dags/covid_etl_dag.py` (ingest >> quality_and_transform >> load_postgres, daily, 2 retries).

## Measured results
Measured on a cloud sandbox container (Spark local mode, PostgreSQL 16). **Re-run on your machine and replace these with your own numbers.**

| Metric | Value |
|---|---|
| Rows processed | 161,568 |
| Raw file size | 5.5 MB |
| Quality checks | 7.1-7.7 s |
| Transform + Parquet write | ~5.5 s |
| Load to PostgreSQL | ~1.4 s |
| End-to-end | ~25 s |
| Downward revisions flagged | 235 |
| Re-run behaviour | idempotent (row count unchanged after 2 runs) |

## Honest scope / limitations
- Batch only, single machine (Spark local mode); no cluster, no streaming/Kafka.
- The Airflow DAG is provided but not executed in my test environment; verify it in your own Airflow install.
- No cloud deployment included. Possible extensions: run on AWS/GCP free tier, write Parquet to S3/GCS,
  or swap the batch ingest for a Kafka producer/consumer.
