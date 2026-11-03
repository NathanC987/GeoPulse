-- Runs once when the Postgres volume is first created.
-- `metastore` holds the Hive metastore schema; `geopulse` (default DB) is the serving layer.
CREATE DATABASE metastore OWNER geopulse;

\connect geopulse

-- Speed-layer tables written by streaming/stream_checkins.py (foreachBatch upserts).
-- window_utc is the key; window_start/window_end are the city's local time (for display). Local
-- time alone is not unique: when DST ends, two different UTC hours map to the same local hour.
CREATE TABLE IF NOT EXISTS stream_trending (
    window_utc   TIMESTAMP NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    dataset      TEXT NOT NULL,
    kind         TEXT NOT NULL,          -- 'category' | 'venue'
    key          TEXT NOT NULL,
    label        TEXT,
    lat          DOUBLE PRECISION,
    lon          DOUBLE PRECISION,
    n            INTEGER NOT NULL,
    PRIMARY KEY (window_utc, dataset, kind, key)
);
CREATE INDEX IF NOT EXISTS stream_trending_end ON stream_trending (dataset, window_end);

CREATE TABLE IF NOT EXISTS stream_cells (
    window_utc   TIMESTAMP NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    dataset      TEXT NOT NULL,
    h3           TEXT NOT NULL,
    n            INTEGER NOT NULL,
    baseline     DOUBLE PRECISION,
    z            DOUBLE PRECISION,
    PRIMARY KEY (window_utc, dataset, h3)
);
CREATE INDEX IF NOT EXISTS stream_cells_end ON stream_cells (dataset, window_end);

CREATE TABLE IF NOT EXISTS stream_alerts (
    window_utc   TIMESTAMP NOT NULL,
    window_start TIMESTAMP NOT NULL,
    dataset      TEXT NOT NULL,
    h3           TEXT NOT NULL,
    lat          DOUBLE PRECISION,
    lon          DOUBLE PRECISION,
    place        TEXT,
    n            INTEGER NOT NULL,
    baseline     DOUBLE PRECISION,
    z            DOUBLE PRECISION,
    top_category TEXT,
    detected_at  TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (window_utc, dataset, h3)
);

CREATE TABLE IF NOT EXISTS stream_metrics (
    batch_id       BIGINT PRIMARY KEY,
    processed_at   TIMESTAMP NOT NULL DEFAULT now(),
    rows           INTEGER NOT NULL,
    max_event_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stream_recent (
    event_time TIMESTAMP NOT NULL,
    dataset    TEXT NOT NULL,
    user_id    TEXT,
    venue_id   TEXT,
    category   TEXT,
    lat        DOUBLE PRECISION,
    lon        DOUBLE PRECISION,
    inserted_at TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS stream_recent_time ON stream_recent (inserted_at DESC);
