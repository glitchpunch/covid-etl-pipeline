import datetime as dt
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from etl import quality, transform  # noqa: E402
from etl.transform import RAW_SCHEMA  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = transform.get_spark("tests")
    yield s
    s.stop()


def d(n):
    return dt.date(2020, 1, n)


def make(spark, rows):
    return spark.createDataFrame(rows, RAW_SCHEMA)


def test_daily_deltas_and_rolling_avg(spark):
    df = make(spark, [(d(1), "A", 10, 0, 1), (d(2), "A", 15, 0, 1), (d(3), "A", 30, 0, 2)])
    out = {r["date"]: r for r in transform.build_daily(df).collect()}
    assert out[d(1)]["new_confirmed"] == 10
    assert out[d(2)]["new_confirmed"] == 5
    assert out[d(3)]["new_deaths"] == 1
    assert out[d(3)]["new_confirmed_7d_avg"] == round((10 + 5 + 15) / 3, 2)


def test_downward_revision_is_flagged_and_floored(spark):
    df = make(spark, [(d(1), "A", 100, 0, 5), (d(2), "A", 90, 0, 5)])
    row = [r for r in transform.build_daily(df).collect() if r["date"] == d(2)][0]
    assert row["is_correction"] is True and row["new_confirmed"] == 0


def test_windows_do_not_leak_across_countries(spark):
    df = make(spark, [(d(1), "A", 100, 0, 0), (d(1), "B", 5, 0, 0)])
    out = {r["country"]: r["new_confirmed"] for r in transform.build_daily(df).collect()}
    assert out == {"A": 100, "B": 5}


def test_quality_fails_on_duplicates(spark):
    df = make(spark, [(d(1), "A", 1, 0, 0), (d(1), "A", 2, 0, 0)])
    with pytest.raises(quality.DataQualityError):
        quality.run_checks(df)


def test_quality_fails_on_negative_and_null_key(spark):
    with pytest.raises(quality.DataQualityError):
        quality.run_checks(make(spark, [(d(1), "A", -1, 0, 0)]))
    with pytest.raises(quality.DataQualityError):
        quality.run_checks(make(spark, [(None, "A", 1, 0, 0)]))


def test_quality_passes_clean_data(spark):
    rep = quality.run_checks(make(spark, [(d(1), "A", 1, 0, 0), (d(2), "A", 2, 0, 0)]))
    assert rep["rows"] == 2 and not rep["hard_failures"]
