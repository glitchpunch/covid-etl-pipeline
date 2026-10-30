"""Run the full pipeline:  python -m etl.run_pipeline   (from the src/ directory)."""
import argparse
import json
import logging
import time
from datetime import datetime, timezone

from . import ingest, load, quality, transform
from .config import METRICS_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pipeline")


def timed(metrics: dict, name: str, fn, *a, **kw):
    t = time.perf_counter()
    result = fn(*a, **kw)
    metrics["stage_seconds"][name] = round(time.perf_counter() - t, 2)
    log.info("stage=%s seconds=%s", name, metrics["stage_seconds"][name])
    return result


def main(skip_load: bool = False, force_download: bool = False) -> dict:
    started = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    metrics = {"stage_seconds": {}}

    metrics["raw_bytes"] = timed(metrics, "ingest", ingest.download, force_download)

    spark = transform.get_spark()
    try:
        raw = transform.read_raw(spark).cache()
        metrics["quality"] = timed(metrics, "quality_checks", quality.run_checks, raw)
        metrics["rows_in"] = metrics["quality"]["rows"]

        daily = transform.build_daily(raw)
        monthly = transform.build_monthly(daily)
        timed(metrics, "transform_and_write_parquet", transform.write_parquet, daily, monthly)
        metrics["rows_out"] = daily.count()
        metrics["corrections_flagged"] = daily.filter("is_correction").count()
    finally:
        spark.stop()

    if not skip_load:
        metrics["load"] = timed(metrics, "load_postgres", load.load_all)

    metrics["seconds_total"] = round(time.perf_counter() - t0, 2)
    METRICS_PATH.write_text(json.dumps(metrics, indent=2, default=str))
    if not skip_load:
        load.record_run(started, metrics)
    log.info("Done: %s", json.dumps(metrics, default=str))
    return metrics


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--skip-load", action="store_true", help="stop after writing Parquet")
    p.add_argument("--force-download", action="store_true")
    args = p.parse_args()
    main(args.skip_load, args.force_download)
