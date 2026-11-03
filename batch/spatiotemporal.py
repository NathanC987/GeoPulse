"""Batch layer, step 2: spatiotemporal analytics over the Hive check-in tables.

Every result is a small aggregate written to the Postgres serving DB (table prefix st_)
for the dashboard. Also builds the hour-of-week baseline per H3 cell that the
streaming job uses for burst detection (Hive table geopulse.cell_baseline).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType
from pyspark.sql.window import Window

from batch.spark_utils import get_spark, haversine_km_col, step, to_postgres
from common import config
from common.geo import region_case_sql

CITY = list(config.CITY_DATASETS)


@F.udf(ArrayType(StringType()))
def ring_udf(cell):
    import h3
    return list(h3.grid_disk(cell, 1))


@F.udf(DoubleType())
def cell_lat(cell):
    import h3
    return float(h3.cell_to_latlng(cell)[0])


@F.udf(DoubleType())
def cell_lon(cell):
    import h3
    return float(h3.cell_to_latlng(cell)[1])


@F.udf(StringType())
def place_udf(dataset, lat, lon):
    from common.geo import nearest_place
    return nearest_place(dataset, lat, lon)


def slot_col(hour_col="hour"):
    (first, hours), *rest = config.TIME_SLOTS.items()
    expr = F.when(F.col(hour_col).between(hours.start, hours.stop - 1), first)
    for name, hours in rest:
        expr = expr.when(F.col(hour_col).between(hours.start, hours.stop - 1), name)
    return expr


def with_center(df: DataFrame, cell_col: str, prefix: str = "") -> DataFrame:
    return (df.withColumn(f"{prefix}lat", cell_lat(cell_col))
              .withColumn(f"{prefix}lon", cell_lon(cell_col)))


# --------------------------------------------------------------------------- temporal
def temporal_patterns(ck: DataFrame) -> None:
    to_postgres(ck.groupBy("dataset").agg(
        F.count("*").alias("checkins"), F.countDistinct("user_id").alias("users"),
        F.countDistinct("venue_id").alias("venues"),
        F.min("date_local").alias("first_day"), F.max("date_local").alias("last_day")),
        "st_overview")

    to_postgres(ck.groupBy("dataset", "date_local")
                  .agg(F.count("*").alias("n"), F.countDistinct("user_id").alias("users"))
                  .withColumnRenamed("date_local", "day"), "st_daily")

    to_postgres(ck.groupBy("dataset", "dow", "hour").count().withColumnRenamed("count", "n"),
                "st_hour_dow")

    city = ck.filter(F.col("dataset").isin(CITY))
    top = (city.groupBy("dataset", "category").count()
               .withColumn("rk", F.row_number().over(
                   Window.partitionBy("dataset").orderBy(F.desc("count"))))
               .filter("rk <= 30").select("dataset", "category"))
    to_postgres(city.join(top, ["dataset", "category"])
                    .groupBy("dataset", "category", "dow", "hour").count()
                    .withColumnRenamed("count", "n"), "st_category_rhythm")

    # Gowalla: growth of the network by continent.
    gw = ck.filter("dataset = 'gowalla'").withColumn("region", F.expr(region_case_sql()))
    to_postgres(gw.groupBy("region", F.date_format("date_local", "yyyy-MM").alias("month"))
                  .agg(F.count("*").alias("checkins"), F.countDistinct("user_id").alias("users")),
                "st_region_monthly")


# --------------------------------------------------------------------------- spatial
def cell_column(ds_col="dataset"):
    """City datasets use H3 r8 (~0.7 km²); the global Gowalla view uses r5."""
    return F.when(F.col(ds_col) == "gowalla", F.col("h3_5")).otherwise(F.col("h3_8"))


def hex_activity(ck: DataFrame) -> None:
    cells = ck.withColumn("h3", cell_column())
    totals = cells.groupBy("dataset", "h3").agg(F.count("*").alias("n_total"),
                                               F.countDistinct("user_id").alias("n_users"),
                                               F.avg("lat").alias("clat"), F.avg("lon").alias("clon"))
    top_cat = (cells.filter(F.col("category").isNotNull())
                    .groupBy("dataset", "h3", "category").count()
                    .withColumn("rk", F.row_number().over(
                        Window.partitionBy("dataset", "h3").orderBy(F.desc("count"))))
                    .filter("rk = 1").select("dataset", "h3", F.col("category").alias("top_category")))
    meta = with_center(totals.join(top_cat, ["dataset", "h3"], "left"), "h3")
    # Label with the check-in weighted centroid: more accurate than the hexagon centre.
    meta = meta.withColumn("place", place_udf("dataset", "clat", "clon")).drop("clat", "clon")
    to_postgres(meta, "st_cells")

    to_postgres(cells.groupBy("dataset", "h3", "hour").count().withColumnRenamed("count", "n"),
                "st_hex_hourly")


def getis_ord_hotspots(ck: DataFrame) -> None:
    """Getis-Ord Gi* z-scores per H3 r8 cell and time-of-day slot (binary k=1 ring weights)."""
    city = ck.filter(F.col("dataset").isin(CITY)).withColumn("slot", slot_col())
    universe = city.select("dataset", F.col("h3_8").alias("h3")).distinct().cache()
    slots = city.select("slot").distinct()

    counts = (universe.crossJoin(slots)
              .join(city.groupBy("dataset", "slot", F.col("h3_8").alias("h3")).count(),
                    ["dataset", "slot", "h3"], "left")
              .select("dataset", "slot", "h3", F.coalesce("count", F.lit(0)).cast("double").alias("x")))

    # Global moments per dataset & slot.
    stats = counts.groupBy("dataset", "slot").agg(
        F.count("*").alias("n_cells"), F.avg("x").alias("xbar"),
        F.sqrt(F.avg(F.col("x") ** 2) - F.avg("x") ** 2).alias("s"))

    # Neighbourhood (cell itself + 6 neighbours), restricted to the study area.
    nbrs = (universe.withColumn("nbr", F.explode(ring_udf("h3")))
            .join(universe.withColumnRenamed("h3", "nbr"), ["dataset", "nbr"]))
    lag = (nbrs.join(counts.withColumnRenamed("h3", "nbr").withColumnRenamed("x", "x_nbr"),
                     ["dataset", "nbr"])
           .groupBy("dataset", "slot", "h3")
           .agg(F.sum("x_nbr").alias("sum_wx"), F.count("*").alias("w")))

    gi = (lag.join(stats, ["dataset", "slot"]).join(counts, ["dataset", "slot", "h3"])
          .withColumn("gi_z", (F.col("sum_wx") - F.col("xbar") * F.col("w")) /
                      (F.col("s") * F.sqrt((F.col("n_cells") * F.col("w") - F.col("w") ** 2)
                                           / (F.col("n_cells") - 1))))
          .withColumn("cls", F.when(F.col("gi_z") >= 2.58, "Hot spot (99%)")
                              .when(F.col("gi_z") >= 1.96, "Hot spot (95%)")
                              .when(F.col("gi_z") >= 1.65, "Hot spot (90%)")
                              .when(F.col("gi_z") <= -1.96, "Cold spot (95%)")
                              .otherwise("Not significant")))
    gi = with_center(gi.select("dataset", "slot", "h3", F.col("x").cast("int").alias("n"),
                               "gi_z", "cls"), "h3")
    to_postgres(gi, "st_hotspots")
    universe.unpersist()


# --------------------------------------------------------------------------- mobility
def mobility(ck: DataFrame) -> None:
    w = Window.partitionBy("dataset", "user_id").orderBy("ts_utc")
    seq = (ck.withColumn("cell", F.when(F.col("dataset") == "gowalla", F.col("h3_5"))
                                  .otherwise(F.col("h3_7")))
             .withColumn("prev_cell", F.lag("cell").over(w))
             .withColumn("prev_cat", F.lag("category").over(w))
             .withColumn("prev_venue", F.lag("venue_id").over(w))
             .withColumn("gap_h", (F.unix_timestamp("ts_utc") - F.unix_timestamp(F.lag("ts_utc").over(w))) / 3600)
             .filter(F.col("prev_cell").isNotNull()))

    # Origin -> destination flows between cells (same day trips in cities, ≤ 7 days globally).
    max_gap = F.when(F.col("dataset") == "gowalla", 24 * 7).otherwise(4)
    trips = seq.filter((F.col("gap_h") <= max_gap) & (F.col("prev_cell") != F.col("cell")))
    flows = (trips.groupBy("dataset", F.col("prev_cell").alias("o_h3"), F.col("cell").alias("d_h3"))
                  .agg(F.count("*").alias("n"), F.countDistinct("user_id").alias("users"))
                  .filter("users >= 3")
                  .withColumn("rk", F.row_number().over(Window.partitionBy("dataset").orderBy(F.desc("n"))))
                  .filter(F.col("rk") <= F.when(F.col("dataset") == "gowalla", 600).otherwise(400)))
    flows = with_center(with_center(flows, "o_h3", "o_"), "d_h3", "d_")
    centroid = seq.groupBy("dataset", "cell").agg(F.avg("lat").alias("clat"), F.avg("lon").alias("clon"))
    for end in ("o", "d"):
        c = centroid.select("dataset", F.col("cell").alias(f"{end}_h3"), F.col("clat").alias(f"{end}_clat"),
                            F.col("clon").alias(f"{end}_clon"))
        flows = flows.join(c, ["dataset", f"{end}_h3"], "left")
    flows = (flows.withColumn("o_place", place_udf("dataset", "o_clat", "o_clon"))
                  .withColumn("d_place", place_udf("dataset", "d_clat", "d_clon"))
                  .drop("o_clat", "o_clon", "d_clat", "d_clon")
                  .withColumn("km", haversine_km_col(F.col("o_lat"), F.col("o_lon"),
                                                     F.col("d_lat"), F.col("d_lon"))))
    to_postgres(flows.drop("rk"), "st_od_flows")

    # Category transition (first-order Markov) matrix for the top 15 categories per city.
    city = seq.filter(F.col("dataset").isin(CITY) & (F.col("gap_h") <= 4)
                      & (F.col("prev_venue") != F.col("venue_id")))
    top = (ck.filter(F.col("dataset").isin(CITY)).groupBy("dataset", "category").count()
             .withColumn("rk", F.row_number().over(Window.partitionBy("dataset").orderBy(F.desc("count"))))
             .filter("rk <= 15").select("dataset", "category"))
    trans = (city.join(top.withColumnRenamed("category", "prev_cat"), ["dataset", "prev_cat"])
                 .join(top, ["dataset", "category"])
                 .groupBy("dataset", F.col("prev_cat").alias("from_cat"), F.col("category").alias("to_cat"))
                 .count().withColumnRenamed("count", "n"))
    trans = trans.withColumn("prob", F.col("n") / F.sum("n").over(Window.partitionBy("dataset", "from_cat")))
    to_postgres(trans, "st_transitions")

    # Radius of gyration per user: spread of each user's check-ins around their centre of mass.
    centre = ck.groupBy("dataset", "user_id").agg(F.avg("lat").alias("clat"), F.avg("lon").alias("clon"),
                                                   F.count("*").alias("n"))
    rg = (ck.join(centre.filter("n >= 10"), ["dataset", "user_id"])
            .withColumn("d", haversine_km_col(F.col("lat"), F.col("lon"), F.col("clat"), F.col("clon")))
            .groupBy("dataset", "user_id").agg(F.sqrt(F.avg(F.col("d") ** 2)).alias("rg_km")))
    edges = [0, 1, 2, 5, 10, 20, 50, 100, 500, 2000]
    bucket = F.lit(f"{edges[-1]}+ km")
    order = F.lit(len(edges) - 1)
    for i in range(len(edges) - 2, -1, -1):
        cond = F.col("rg_km") < edges[i + 1]
        bucket = F.when(cond, f"{edges[i]}-{edges[i + 1]} km").otherwise(bucket)
        order = F.when(cond, i).otherwise(order)
    to_postgres(rg.withColumn("bucket", bucket).withColumn("bucket_order", order)
                  .groupBy("dataset", "bucket", "bucket_order")
                  .agg(F.count("*").alias("users"), F.expr("percentile_approx(rg_km, 0.5)").alias("median_km")),
                "st_gyration")


# --------------------------------------------------------------------------- social
def social_colocation(spark, ck: DataFrame, sample_pairs: int = 200_000) -> None:
    """Do friends share places more than random pairs of users? (Gowalla)"""
    gw = ck.filter("dataset = 'gowalla'")
    uv = gw.select("user_id", "venue_id").distinct().cache()
    users = gw.groupBy("user_id").agg(F.countDistinct("venue_id").alias("nv"),
                                      F.avg("lat").alias("lat"), F.avg("lon").alias("lon")).cache()

    graph = spark.table(f"{config.HIVE_DB}.friendships")
    to_postgres(graph.agg((F.count("*") / 2).cast("long").alias("friendships"),
                          F.countDistinct("user_a").alias("users")), "st_graph")
    friends = (graph.filter("user_a < user_b")
               .join(users.select(F.col("user_id").alias("user_a")), "user_a")
               .join(users.select(F.col("user_id").alias("user_b")), "user_b"))
    n_friends = friends.count()
    friends = friends.sample(fraction=min(1.0, sample_pairs / n_friends), seed=7)

    idx = users.select("user_id").withColumn("i", F.row_number().over(Window.orderBy("user_id")) - 1).cache()
    n_users = idx.count()
    rand = (spark.range(sample_pairs)
            .select((F.rand(1) * n_users).cast("long").alias("ia"), (F.rand(2) * n_users).cast("long").alias("ib"))
            .join(idx.select(F.col("i").alias("ia"), F.col("user_id").alias("user_a")), "ia")
            .join(idx.select(F.col("i").alias("ib"), F.col("user_id").alias("user_b")), "ib")
            .filter("user_a != user_b").select("user_a", "user_b"))

    def pair_stats(pairs: DataFrame, label: str) -> DataFrame:
        common = (pairs.join(uv.withColumnRenamed("user_id", "user_a"), "user_a")
                       .join(uv.withColumnRenamed("user_id", "user_b"), ["user_b", "venue_id"])
                       .groupBy("user_a", "user_b").agg(F.count("*").alias("common")))
        ua = users.select(F.col("user_id").alias("user_a"), F.col("nv").alias("nva"),
                          F.col("lat").alias("lat_a"), F.col("lon").alias("lon_a"))
        ub = users.select(F.col("user_id").alias("user_b"), F.col("nv").alias("nvb"),
                          F.col("lat").alias("lat_b"), F.col("lon").alias("lon_b"))
        p = (pairs.join(common, ["user_a", "user_b"], "left").fillna(0, ["common"])
                  .join(ua, "user_a").join(ub, "user_b")
                  .withColumn("jaccard", F.col("common") / (F.col("nva") + F.col("nvb") - F.col("common")))
                  .withColumn("home_km", haversine_km_col(F.col("lat_a"), F.col("lon_a"),
                                                          F.col("lat_b"), F.col("lon_b"))))
        return p.agg(F.lit(label).alias("pair_type"), F.count("*").alias("pairs"),
                     F.avg((F.col("common") > 0).cast("double")).alias("share_with_common_place"),
                     F.avg("jaccard").alias("mean_jaccard"),
                     F.expr("percentile_approx(home_km, 0.5)").alias("median_home_distance_km"),
                     F.avg((F.col("home_km") < 100).cast("double")).alias("share_within_100km"))

    result = pair_stats(friends, "Friends").unionByName(pair_stats(rand, "Random pairs"))
    to_postgres(result, "st_social")
    result.show(truncate=False)
    uv.unpersist(); users.unpersist(); idx.unpersist()


# --------------------------------------------------------------------------- streaming baseline
def burst_baseline(spark, ck: DataFrame) -> None:
    """Expected check-ins per H3 r8 cell for each hour of the week (mean & std over all weeks)."""
    city = ck.filter(F.col("dataset").isin(CITY))
    weeks = city.groupBy("dataset").agg(
        ((F.datediff(F.max("date_local"), F.min("date_local")) + 1) / 7.0).alias("weeks"))
    hourly = city.groupBy("dataset", "h3_8", "date_local", "how").count()
    base = (hourly.groupBy("dataset", "h3_8", "how")
                  .agg(F.sum("count").alias("s1"), F.sum(F.col("count") ** 2).alias("s2"))
                  .join(weeks, "dataset")
                  .withColumn("mean", F.col("s1") / F.col("weeks"))
                  .withColumn("std", F.sqrt(F.greatest(F.lit(0.0),
                                                       F.col("s2") / F.col("weeks") - F.col("mean") ** 2)))
                  .select("dataset", F.col("h3_8").alias("h3"), "how", "mean", "std"))
    base.write.mode("overwrite").format("parquet").saveAsTable(f"{config.HIVE_DB}.cell_baseline")
    print(f"  baseline rows: {spark.table(f'{config.HIVE_DB}.cell_baseline').count():,}")


def main():
    spark = get_spark("GeoPulse-Spatiotemporal")
    ck = spark.table(f"{config.HIVE_DB}.checkins").cache()
    print(f"  {ck.count():,} check-ins loaded from Hive")

    with step("Temporal patterns (daily, hour x weekday, category rhythm, regional growth)"):
        temporal_patterns(ck)
    with step("Hex activity cube (H3 cell x hour)"):
        hex_activity(ck)
    with step("Getis-Ord Gi* hotspots per time slot"):
        getis_ord_hotspots(ck)
    with step("Mobility: OD flows, category transitions, radius of gyration"):
        mobility(ck)
    with step("Social co-location: friends vs random pairs (Gowalla)"):
        social_colocation(spark, ck)
    with step("Hour-of-week baseline per cell for streaming burst detection"):
        burst_baseline(spark, ck)

    spark.stop()


if __name__ == "__main__":
    main()
