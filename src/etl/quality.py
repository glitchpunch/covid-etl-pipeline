"""Stage 2: data-quality checks.

Hard checks stop the pipeline. Soft checks are recorded as warnings and
handled downstream (e.g. negative daily deltas are flagged, not dropped).
"""
from pyspark.sql import DataFrame, functions as F


class DataQualityError(Exception):
    pass


def run_checks(df: DataFrame) -> dict:
    total = df.count()
    report = {"rows": total, "hard_failures": [], "warnings": {}}

    if total == 0:
        report["hard_failures"].append("dataset is empty")
        raise DataQualityError(report["hard_failures"])

    null_keys = df.filter(F.col("date").isNull() | F.col("country").isNull()).count()
    if null_keys:
        report["hard_failures"].append(f"{null_keys} rows with null date/country")

    dupes = (df.groupBy("date", "country").count().filter("count > 1").count())
    if dupes:
        report["hard_failures"].append(f"{dupes} duplicated (date, country) keys")

    negatives = df.filter(
        (F.col("confirmed") < 0) | (F.col("recovered") < 0) | (F.col("deaths") < 0)
    ).count()
    if negatives:
        report["hard_failures"].append(f"{negatives} rows with negative counts")

    null_vals = df.filter(
        F.col("confirmed").isNull() | F.col("deaths").isNull()
    ).count()
    if null_vals:
        report["hard_failures"].append(f"{null_vals} rows with null confirmed/deaths")

    # soft check
    report["warnings"]["deaths_exceed_confirmed"] = df.filter(
        F.col("deaths") > F.col("confirmed")
    ).count()

    if report["hard_failures"]:
        raise DataQualityError(report["hard_failures"])
    return report
