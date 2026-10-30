"""Airflow DAG: ingest -> quality+transform -> load, scheduled daily.

Requires Airflow 2.x with this repo's src/ on PYTHONPATH.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl import ingest, load, quality, transform


def _ingest():
    ingest.download(force=True)


def _transform():
    spark = transform.get_spark("covid-etl-airflow")
    try:
        raw = transform.read_raw(spark).cache()
        quality.run_checks(raw)            # raises -> task fails -> retry/alert
        daily = transform.build_daily(raw)
        transform.write_parquet(daily, transform.build_monthly(daily))
    finally:
        spark.stop()


with DAG(
    dag_id="covid_etl",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["etl", "spark", "postgres"],
) as dag:
    t_ingest = PythonOperator(task_id="ingest", python_callable=_ingest)
    t_transform = PythonOperator(task_id="quality_and_transform", python_callable=_transform)
    t_load = PythonOperator(task_id="load_postgres", python_callable=load.load_all)
    t_ingest >> t_transform >> t_load
