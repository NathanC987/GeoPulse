"""Batch layer, step 1: raw LBSN files -> cleaned, enriched Parquet data lake.

Inputs  (data/raw):  Foursquare TSMC2014 NYC/TKY check-ins, Gowalla check-ins + friendship edges
Outputs (data/lake): checkins/ (partitioned by dataset), venues/, friendships/
The Hive external tables over these folders are created by hive/create_tables.hql.
"""
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType)

from batch.spark_utils import get_spark, h3_udf, step
from common import config

FSQ_SCHEMA = StructType([
    StructField("user_id", StringType()),
    StructField("venue_id", StringType()),
    StructField("category_id", StringType()),
    StructField("category", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("tz_offset", IntegerType()),   # minutes from UTC
    StructField("utc_time", StringType()),     # e.g. "Tue Apr 03 18:00:09 +0000 2012"
])

GOWALLA_SCHEMA = StructType([
    StructField("user_id", StringType()),
    StructField("utc_time", StringType()),     # e.g. "2010-10-19T23:55:27Z"
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("venue_id", StringType()),
])


def read_foursquare(spark, city: str) -> DataFrame:
    path = os.path.join(config.RAW_DIR, f"dataset_TSMC2014_{city.upper()}.txt")
    df = spark.read.csv(path, schema=FSQ_SCHEMA, sep="\t", encoding="ISO-8859-1")
    # Drop the weekday prefix ("Tue ") - the remainder parses with the default parser.
    ts = F.to_timestamp(F.substring("utc_time", 5, 100), "MMM dd HH:mm:ss Z yyyy")
    return df.select(
        F.lit(city).alias("dataset"), "user_id", "venue_id", "category", "lat", "lon",
        ts.alias("ts_utc"), "tz_offset",
    )


def read_gowalla(spark) -> DataFrame:
    path = os.path.join(config.RAW_DIR, "loc-gowalla_totalCheckins.txt.gz")
    df = spark.read.csv(path, schema=GOWALLA_SCHEMA, sep="\t").repartition(24)
    # Gowalla has no timezone field: approximate the offset from longitude (15° per hour).
    tz = (F.round(F.col("lon") / 15) * 60).cast("int")
    return df.select(
        F.lit("gowalla").alias("dataset"), "user_id", "venue_id",
        F.lit(None).cast("string").alias("category"), "lat", "lon",
        F.to_timestamp("utc_time", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("ts_utc"),
        tz.alias("tz_offset"),
    )


def enrich(df: DataFrame) -> DataFrame:
    ts_local = F.timestamp_seconds(F.unix_timestamp("ts_utc") + F.col("tz_offset") * 60)
    return (
        df.withColumn("ts_local", ts_local)
          .withColumn("date_local", F.to_date("ts_local"))
          .withColumn("hour", F.hour("ts_local"))
          # dayofweek(): 1=Sunday..7=Saturday  ->  dow: 0=Monday..6=Sunday
          .withColumn("dow", (F.dayofweek("ts_local") + 5) % 7)
          .withColumn("how", F.col("dow") * 24 + F.col("hour"))   # hour of week 0..167
          .withColumn("h3_5", h3_udf(config.H3_GLOBAL)("lat", "lon"))
          .withColumn("h3_7", h3_udf(config.H3_FLOW)("lat", "lon"))
          .withColumn("h3_8", h3_udf(config.H3_CITY)("lat", "lon"))
    )


def main():
    spark = get_spark("GeoPulse-ETL")
    lake = config.LAKE_DIR

    with step("Read raw check-ins"):
        raw = (read_foursquare(spark, "nyc")
               .unionByName(read_foursquare(spark, "tky"))
               .unionByName(read_gowalla(spark)))
        raw_counts = {r["dataset"]: r["count"] for r in raw.groupBy("dataset").count().collect()}

    with step("Clean + enrich (local time, hour-of-week, H3 cells)"):
        clean = (raw
                 .filter(F.col("ts_utc").isNotNull() & F.col("user_id").isNotNull()
                         & F.col("venue_id").isNotNull())
                 .filter(F.col("lat").between(-90, 90) & F.col("lon").between(-180, 180))
                 .filter(~((F.abs("lat") < 1e-6) & (F.abs("lon") < 1e-6)))   # (0,0) junk in Gowalla
                 .dropDuplicates(["dataset", "user_id", "venue_id", "ts_utc"]))
        checkins = enrich(clean).repartition(24)
        checkins.write.mode("overwrite").partitionBy("dataset").parquet(os.path.join(lake, "checkins"))

    checkins = spark.read.parquet(os.path.join(lake, "checkins"))
    clean_counts = {r["dataset"]: r["count"] for r in checkins.groupBy("dataset").count().collect()}
    print("\n  dataset    raw rows   clean rows")
    for ds in sorted(raw_counts):
        print(f"  {ds:<8} {raw_counts[ds]:>10,} {clean_counts.get(ds, 0):>12,}")

    with step("Build venue dimension"):
        cat_rank = (checkins.filter(F.col("category").isNotNull())
                    .groupBy("dataset", "venue_id", "category").count()
                    .withColumn("rk", F.row_number().over(
                        Window.partitionBy("dataset", "venue_id").orderBy(F.desc("count"))))
                    .filter("rk = 1").select("dataset", "venue_id", "category"))
        venues = (checkins.groupBy("dataset", "venue_id")
                  .agg(F.avg("lat").alias("lat"), F.avg("lon").alias("lon"),
                       F.count("*").alias("n_checkins"),
                       F.countDistinct("user_id").alias("n_users"),
                       F.min("ts_utc").alias("first_seen"), F.max("ts_utc").alias("last_seen"))
                  .join(cat_rank, ["dataset", "venue_id"], "left")
                  .withColumn("h3_8", h3_udf(config.H3_CITY)("lat", "lon")))
        venues.repartition(8).write.mode("overwrite").partitionBy("dataset").parquet(
            os.path.join(lake, "venues"))

    with step("Friendship graph (Gowalla)"):
        edges = (spark.read.csv(os.path.join(config.RAW_DIR, "loc-gowalla_edges.txt.gz"), sep="\t")
                 .toDF("user_a", "user_b"))
        edges.repartition(8).write.mode("overwrite").parquet(os.path.join(lake, "friendships"))
        print(f"  {edges.count():,} directed friendship edges")

    spark.stop()


if __name__ == "__main__":
    main()
