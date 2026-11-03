"""Small helpers shared by the PySpark batch jobs."""
import time
from contextlib import contextmanager

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import config


def get_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def h3_udf(res: int):
    """Vectorised (Arrow) UDF mapping lat/lon columns to an H3 cell id at a fixed resolution."""
    @F.pandas_udf("string")
    def _cell(lat: pd.Series, lon: pd.Series) -> pd.Series:
        import h3
        return pd.Series([h3.latlng_to_cell(a, b, res) for a, b in zip(lat, lon)])
    return _cell


def haversine_km_col(lat1, lon1, lat2, lon2):
    """Haversine distance as a native Spark column expression (no Python UDF)."""
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return F.lit(2 * 6371.0088) * F.asin(F.sqrt(F.least(F.lit(1.0), a)))


def to_postgres(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    """Write a (small, aggregated) DataFrame to the Postgres serving DB."""
    (df.write.mode(mode)
       .option("truncate", "true")
       .jdbc(config.PG_JDBC_URL, table, properties=config.PG_JDBC_PROPS))


@contextmanager
def step(name: str):
    t0 = time.time()
    print(f"\n▶ {name} ...", flush=True)
    yield
    print(f"  ✓ {name} ({time.time() - t0:.1f}s)", flush=True)
