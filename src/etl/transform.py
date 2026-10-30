"""Stage 3: Spark transformations."""
from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import (DateType, LongType, StringType, StructField,
                               StructType)

from .config import DAILY_DIR, MONTHLY_DIR, RAW_PATH

RAW_SCHEMA = StructType([
    StructField("date", DateType(), True),
    StructField("country", StringType(), True),
    StructField("confirmed", LongType(), True),
    StructField("recovered", LongType(), True),
    StructField("deaths", LongType(), True),
])


def get_spark(app: str = "covid-etl") -> SparkSession:
    return (SparkSession.builder.appName(app).master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def read_raw(spark: SparkSession, path=RAW_PATH) -> DataFrame:
    # Raw CSV headers are capitalised; positional schema + header skip.
    return (spark.read.option("header", True).option("mode", "FAILFAST")
            .schema(RAW_SCHEMA).csv(str(path), enforceSchema=True))


def build_daily(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("country").orderBy("date")
    w7 = w.rowsBetween(-6, 0)

    out = (df
           .withColumn("raw_new_confirmed",
                       F.col("confirmed") - F.coalesce(F.lag("confirmed").over(w), F.lit(0)))
           .withColumn("raw_new_deaths",
                       F.col("deaths") - F.coalesce(F.lag("deaths").over(w), F.lit(0)))
           # cumulative counts are occasionally revised downwards by the source:
           # flag those rows and floor the daily delta at 0 instead of dropping data
           .withColumn("is_correction",
                       (F.col("raw_new_confirmed") < 0) | (F.col("raw_new_deaths") < 0))
           .withColumn("new_confirmed", F.greatest(F.col("raw_new_confirmed"), F.lit(0)))
           .withColumn("new_deaths", F.greatest(F.col("raw_new_deaths"), F.lit(0)))
           .withColumn("new_confirmed_7d_avg",
                       F.round(F.avg("new_confirmed").over(w7), 2))
           .withColumn("case_fatality_pct",
                       F.when(F.col("confirmed") > 0,
                              F.round(F.col("deaths") / F.col("confirmed") * 100, 3)))
           .withColumn("year", F.year("date"))
           .withColumn("month", F.month("date"))
           .drop("raw_new_confirmed", "raw_new_deaths"))
    return out


def build_monthly(daily: DataFrame) -> DataFrame:
    return (daily.groupBy("country", "year", "month")
            .agg(F.sum("new_confirmed").alias("new_confirmed"),
                 F.sum("new_deaths").alias("new_deaths"),
                 F.max("confirmed").alias("confirmed_eom")))


def write_parquet(daily: DataFrame, monthly: DataFrame) -> None:
    (daily.repartition("year").write.mode("overwrite")
     .partitionBy("year").parquet(str(DAILY_DIR)))
    monthly.coalesce(1).write.mode("overwrite").parquet(str(MONTHLY_DIR))
