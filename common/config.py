"""Shared configuration for the GeoPulse batch, streaming and dashboard code.

Paths differ between the Spark containers (/opt/geopulse/...) and the app
containers (/app, /data), so everything is resolved from environment variables
with sensible defaults for each side.
"""
import os

DATA_DIR = os.environ.get("DATA_DIR", "/opt/geopulse/data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
LAKE_DIR = os.path.join(DATA_DIR, "lake")          # Parquet data lake (Hive external tables)
SERVING_DIR = os.path.join(DATA_DIR, "serving")    # model artefacts for the dashboard
CHECKPOINT_DIR = os.path.join(DATA_DIR, "checkpoints")

HIVE_DB = "geopulse"

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_CHECKINS = "checkins"

PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_DB = "geopulse"
PG_USER = "geopulse"
PG_PASSWORD = "geopulse"
PG_DSN = os.environ.get("PG_DSN", f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:5432/{PG_DB}")
PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:5432/{PG_DB}"
PG_JDBC_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

# Datasets: the two Foursquare cities and the global Gowalla network.
DATASETS = {
    "nyc": {"label": "New York City (Foursquare)", "center": (40.7380, -73.9600), "zoom": 10.3,
            "rec_sigma_km": 2.0},
    "tky": {"label": "Tokyo (Foursquare)", "center": (35.6900, 139.7300), "zoom": 10.3,
            "rec_sigma_km": 2.0},
    "gowalla": {"label": "Global (Gowalla)", "center": (35.0, -40.0), "zoom": 1.4,
                "rec_sigma_km": 10.0},
}
CITY_DATASETS = ("nyc", "tky")

# H3 resolutions: r5 ≈ 250 km² (global view), r7 ≈ 5 km² (mobility flows),
# r8 ≈ 0.7 km² (city hex maps, hotspots, bursts), r9 ≈ 0.1 km² (fine detail).
H3_GLOBAL = 5
H3_FLOW = 7
H3_CITY = 8

TIME_SLOTS = {  # local hour ranges used for per-slot hotspot analysis
    "night": range(0, 6),
    "morning": range(6, 12),
    "afternoon": range(12, 18),
    "evening": range(18, 24),
}


def time_slot(hour: int) -> str:
    for name, hours in TIME_SLOTS.items():
        if hour in hours:
            return name
    raise ValueError(hour)
