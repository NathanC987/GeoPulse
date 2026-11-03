"""Speed layer: Spark Structured Streaming over the Kafka `checkins` topic.

Four streaming queries share the same Kafka source:
  trending_categories  sliding 6 h windows (15 min slide) of check-ins per venue category
  trending_venues      sliding 6 h windows of check-ins per venue
  cells                tumbling 1 h windows per H3 r8 cell, scored against the batch-layer
                       hour-of-week baseline (Hive table geopulse.cell_baseline) -> burst alerts
  raw                  throughput metrics + the latest events for the live feed

Windows and the watermark use UTC event time, because NYC and Tokyo local clocks are 13 h
apart; results are converted to each city's local time before they are written to Postgres.
"""

import pandas as pd
import psycopg2
import psycopg2.extras
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from batch.spark_utils import get_spark, h3_udf
from common import config
from common.geo import cell_center, describe_location, nearest_place

EVENT_SCHEMA = StructType([
    StructField("dataset", StringType()), StructField("user_id", StringType()),
    StructField("venue_id", StringType()), StructField("category", StringType()),
    StructField("lat", DoubleType()), StructField("lon", DoubleType()),
    StructField("tz_offset", IntegerType()), StructField("ts_utc", StringType()),
    StructField("source", StringType()),
])

TREND_WINDOW, TREND_SLIDE = "6 hours", "15 minutes"   # trending: sliding window
CELL_WINDOW = "1 hour"                               # bursts: matches the hourly baseline
WATERMARK = "10 minutes"
ALERT_MIN_COUNT = 10      # ignore tiny cells
ALERT_MIN_Z = 4.0         # z-score vs. the cell's usual activity at this hour of the week
STD_FLOOR = 1.0           # avoid huge z-scores for cells that are normally empty

def pg():
    return psycopg2.connect(config.PG_DSN)

def upsert(table: str, cols: list[str], keys: list[str], rows: list[tuple]) -> None:
    if not rows:
        return
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in keys)
    sql = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
           f"ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {updates}")
    with pg() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)

def local_window(pdf: pd.DataFrame) -> pd.DataFrame:
    """Keep the UTC window start as the key and add the city's local window bounds for display."""
    offset = pd.to_timedelta(pdf["tz_offset"], unit="m")
    pdf["window_utc"] = pd.to_datetime(pdf["window_start"])
    pdf["window_start"] = pdf["window_utc"] + offset
    pdf["window_end"] = pd.to_datetime(pdf["window_end"]) + offset
    return pdf

# --------------------------------------------------------------------------- sinks
def sink_categories(batch: DataFrame, batch_id: int) -> None:
    pdf = batch.toPandas()
    if pdf.empty:
        return
    pdf = local_window(pdf)
    upsert("stream_trending", ["window_utc", "window_start", "window_end", "dataset", "kind", "key", "label", "n"],
           ["window_utc", "dataset", "kind", "key"],
           [(r.window_utc.to_pydatetime(), r.window_start.to_pydatetime(), r.window_end.to_pydatetime(), r.dataset, "category",
             r.category, r.category, int(r.n)) for r in pdf.itertuples()])

def sink_venues(batch: DataFrame, batch_id: int) -> None:
    pdf = batch.filter("n >= 2").toPandas()
    if pdf.empty:
        return
    pdf = local_window(pdf)
    rows = []
    for r in pdf.itertuples():
        place = nearest_place(r.dataset, r.lat, r.lon, max_km=2.0)
        label = f"{r.category or 'Venue'} · {place}" if place else (r.category or "Venue")
        rows.append((r.window_utc.to_pydatetime(), r.window_start.to_pydatetime(), r.window_end.to_pydatetime(),
                     r.dataset, "venue", r.venue_id, label, float(r.lat), float(r.lon), int(r.n)))
    upsert("stream_trending", ["window_utc", "window_start", "window_end", "dataset", "kind", "key", "label", "lat",
                               "lon", "n"], ["window_utc", "dataset", "kind", "key"], rows)

class CellSink:
    """Scores each hour-window cell count against its historical hour-of-week baseline."""

    def __init__(self, spark):
        base = spark.table(f"{config.HIVE_DB}.cell_baseline").toPandas()
        self.baseline = {(r.dataset, r.h3, int(r.how)): (r.mean, r.std) for r in base.itertuples()}
        print(f"Loaded {len(self.baseline):,} baseline rows from Hive", flush=True)

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        pdf = batch.toPandas()
        if pdf.empty:
            return
        pdf = local_window(pdf)
        cells, alerts = [], []
        for r in pdf.itertuples():
            how = r.window_start.dayofweek * 24 + r.window_start.hour
            mean, std = self.baseline.get((r.dataset, r.h3, how), (0.0, 0.0))
            z = (r.n - mean) / max(std, STD_FLOOR)
            wu, ws, we = r.window_utc.to_pydatetime(), r.window_start.to_pydatetime(), r.window_end.to_pydatetime()
            cells.append((wu, ws, we, r.dataset, r.h3, int(r.n), float(mean), float(z)))
            if r.n >= ALERT_MIN_COUNT and z >= ALERT_MIN_Z:
                lat, lon = cell_center(r.h3)
                alerts.append((wu, ws, r.dataset, r.h3, lat, lon, describe_location(r.dataset, lat, lon),
                               int(r.n), float(mean), float(z), r.top_category))
        upsert("stream_cells", ["window_utc", "window_start", "window_end", "dataset", "h3", "n", "baseline", "z"],
               ["window_utc", "dataset", "h3"], cells)
        upsert("stream_alerts", ["window_utc", "window_start", "dataset", "h3", "lat", "lon", "place", "n", "baseline",
                                 "z", "top_category"], ["window_utc", "dataset", "h3"], alerts)
        for a in alerts:
            print(f"  ALERT {a[2]} {a[6]}: {a[7]} check-ins in hour starting {a[1]:%Y-%m-%d %H:%M} local "
                  f"(usual {a[8]:.1f}, z={a[9]:.1f})", flush=True)

def sink_raw(batch: DataFrame, batch_id: int) -> None:
    stats = batch.agg(F.count("*").alias("n"), F.max("ts").alias("max_ts")).collect()[0]
    if not stats["n"]:
        return
    recent = (batch.orderBy(F.desc("ts")).limit(40)
                   .select("ts_local", "dataset", "user_id", "venue_id", "category", "lat", "lon").collect())
    with pg() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO stream_metrics (batch_id, rows, max_event_time) VALUES (%s, %s, %s) "
                    "ON CONFLICT (batch_id) DO NOTHING", (batch_id, int(stats["n"]), stats["max_ts"]))
        psycopg2.extras.execute_values(
            cur, "INSERT INTO stream_recent (event_time, dataset, user_id, venue_id, category, lat, lon) VALUES %s",
            [tuple(r) for r in recent])
        cur.execute("DELETE FROM stream_recent WHERE inserted_at < now() - interval '2 minutes'")

# --------------------------------------------------------------------------- main
def main():
    spark = get_spark("GeoPulse-Streaming")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    ckpt = f"{config.CHECKPOINT_DIR}/stream"

    events = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
              .option("subscribe", config.TOPIC_CHECKINS)
              .option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", 50000)
              .option("failOnDataLoss", "false")
              .load()
              .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
              .select("e.*")
              .withColumn("ts", F.to_timestamp("ts_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
              .withColumn("ts_local", F.timestamp_seconds(F.unix_timestamp("ts") + F.col("tz_offset") * 60))
              .filter(F.col("ts").isNotNull() & F.col("dataset").isin(*config.CITY_DATASETS))
              .withColumn("h3", h3_udf(config.H3_CITY)("lat", "lon"))
              .withWatermark("ts", WATERMARK))

    sliding = F.window("ts", TREND_WINDOW, TREND_SLIDE)
    queries = []

    def start(df: DataFrame, name: str, sink, trigger: str, mode: str = "update"):
        q = (df.writeStream.queryName(name).outputMode(mode).foreachBatch(sink)
               .option("checkpointLocation", f"{ckpt}/{name}")
               .trigger(processingTime=trigger).start())
        queries.append(q)

    cats = (events.filter(F.col("category").isNotNull())
                  .groupBy(sliding, "dataset", "category")
                  .agg(F.count("*").alias("n"), F.first("tz_offset").alias("tz_offset"))
                  .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                          "dataset", "category", "n", "tz_offset"))
    start(cats, "trending_categories", sink_categories, "5 seconds")

    venues = (events.groupBy(sliding, "dataset", "venue_id")
                    .agg(F.count("*").alias("n"), F.first("category").alias("category"),
                         F.avg("lat").alias("lat"), F.avg("lon").alias("lon"),
                         F.first("tz_offset").alias("tz_offset"))
                    .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                            "dataset", "venue_id", "category", "lat", "lon", "n", "tz_offset"))
    start(venues, "trending_venues", sink_venues, "5 seconds")

    cells = (events.groupBy(F.window("ts", CELL_WINDOW), "dataset", "h3")
                   .agg(F.count("*").alias("n"), F.first("tz_offset").alias("tz_offset"),
                        F.mode("category").alias("top_category"))
                   .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                           "dataset", "h3", "n", "tz_offset", "top_category"))
    start(cells, "cells", CellSink(spark), "5 seconds")

    start(events, "raw", sink_raw, "2 seconds", mode="append")

    print(f"Started {len(queries)} streaming queries on topic '{config.TOPIC_CHECKINS}'", flush=True)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
