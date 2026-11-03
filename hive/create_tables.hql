-- GeoPulse Hive schema: external tables over the Parquet data lake written by batch/etl.py.
-- Run with:  make hive-ddl   (beeline -u jdbc:hive2://spark-thrift:10000 -f hive/create_tables.hql)

CREATE DATABASE IF NOT EXISTS geopulse
COMMENT 'GeoPulse LBSN check-in analytics'
LOCATION '/opt/geopulse/data/warehouse/geopulse.db';

USE geopulse;

DROP TABLE IF EXISTS checkins;
CREATE EXTERNAL TABLE checkins (
    user_id     STRING    COMMENT 'user id (namespace per dataset)',
    venue_id    STRING    COMMENT 'venue / location id',
    category    STRING    COMMENT 'Foursquare venue category (NULL for Gowalla)',
    lat         DOUBLE,
    lon         DOUBLE,
    ts_utc      TIMESTAMP,
    tz_offset   INT       COMMENT 'minutes from UTC',
    ts_local    TIMESTAMP,
    date_local  DATE,
    hour        INT       COMMENT 'local hour 0-23',
    dow         INT       COMMENT 'day of week 0=Mon..6=Sun',
    how         INT       COMMENT 'hour of week 0-167',
    h3_5        STRING    COMMENT 'H3 cell, resolution 5 (~250 km2)',
    h3_7        STRING    COMMENT 'H3 cell, resolution 7 (~5 km2)',
    h3_8        STRING    COMMENT 'H3 cell, resolution 8 (~0.7 km2)'
)
COMMENT 'All check-ins: Foursquare NYC/Tokyo + Gowalla'
PARTITIONED BY (dataset STRING)
STORED AS PARQUET
LOCATION '/opt/geopulse/data/lake/checkins';

MSCK REPAIR TABLE checkins;

DROP TABLE IF EXISTS venues;
CREATE EXTERNAL TABLE venues (
    venue_id    STRING,
    lat         DOUBLE,
    lon         DOUBLE,
    n_checkins  BIGINT,
    n_users     BIGINT,
    first_seen  TIMESTAMP,
    last_seen   TIMESTAMP,
    category    STRING,
    h3_8        STRING
)
COMMENT 'Venue dimension aggregated from check-ins'
PARTITIONED BY (dataset STRING)
STORED AS PARQUET
LOCATION '/opt/geopulse/data/lake/venues';

MSCK REPAIR TABLE venues;

DROP TABLE IF EXISTS friendships;
CREATE EXTERNAL TABLE friendships (
    user_a STRING,
    user_b STRING
)
COMMENT 'Gowalla social graph (directed edge list, both directions present)'
STORED AS PARQUET
LOCATION '/opt/geopulse/data/lake/friendships';

-- Convenience view: Foursquare city check-ins only.
CREATE OR REPLACE VIEW city_checkins AS
SELECT * FROM checkins WHERE dataset IN ('nyc', 'tky');

SHOW TABLES;
SELECT dataset, COUNT(*) AS checkins, COUNT(DISTINCT user_id) AS users, COUNT(DISTINCT venue_id) AS venues
FROM checkins GROUP BY dataset ORDER BY dataset;
