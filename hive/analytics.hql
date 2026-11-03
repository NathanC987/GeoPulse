-- GeoPulse HiveQL analytics.
-- Each query starts with a "-- name:" header (and an optional "-- desc:" line); the dashboard's
-- Hive Explorer page parses this file and runs the queries live against HiveServer2.
-- Run all of them from the CLI with:  make hive

USE geopulse;

-- name: Dataset overview
-- desc: Size and time span of each dataset in the data lake.
SELECT dataset,
       COUNT(*)                 AS checkins,
       COUNT(DISTINCT user_id)  AS users,
       COUNT(DISTINCT venue_id) AS venues,
       MIN(date_local)          AS first_day,
       MAX(date_local)          AS last_day
FROM checkins
GROUP BY dataset
ORDER BY checkins DESC;

-- name: Top categories by time of day
-- desc: Most checked-in venue category in each local hour, NYC vs Tokyo.
SELECT dataset, hour, category, n
FROM (
  SELECT dataset, hour, category, COUNT(*) AS n,
         ROW_NUMBER() OVER (PARTITION BY dataset, hour ORDER BY COUNT(*) DESC) AS rk
  FROM checkins
  WHERE dataset IN ('nyc', 'tky')
    AND category NOT IN ('Home (private)', 'Office', 'Train Station', 'Subway')
  GROUP BY dataset, hour, category
) t
WHERE rk = 1
ORDER BY dataset, hour;

-- name: Weekend vs weekday category mix
-- desc: Categories whose share of check-ins changes most between weekdays and weekends (NYC).
WITH c AS (
  SELECT category,
         SUM(CASE WHEN dow >= 5 THEN 1 ELSE 0 END) AS weekend,
         SUM(CASE WHEN dow <  5 THEN 1 ELSE 0 END) AS weekday
  FROM checkins WHERE dataset = 'nyc'
  GROUP BY category HAVING COUNT(*) >= 500
), tot AS (
  SELECT SUM(weekend) AS we, SUM(weekday) AS wd FROM c
)
SELECT category,
       ROUND(100 * weekday / tot.wd, 2) AS weekday_pct,
       ROUND(100 * weekend / tot.we, 2) AS weekend_pct,
       ROUND((weekend / tot.we) / (weekday / tot.wd), 2) AS weekend_lift
FROM c CROSS JOIN tot
ORDER BY weekend_lift DESC
LIMIT 20;

-- name: Most loyal users
-- desc: Users with the most check-ins and how concentrated they are on their favourite venue.
WITH uv AS (
  SELECT dataset, user_id, venue_id, COUNT(*) AS n
  FROM checkins WHERE dataset IN ('nyc', 'tky')
  GROUP BY dataset, user_id, venue_id
)
SELECT dataset, user_id,
       SUM(n)                          AS checkins,
       COUNT(*)                        AS distinct_venues,
       ROUND(MAX(n) / SUM(n), 3)       AS top_venue_share
FROM uv
GROUP BY dataset, user_id
ORDER BY checkins DESC
LIMIT 20;

-- name: Busiest H3 cells
-- desc: The 15 most active ~0.7 km² hexagons per city, with their dominant category.
WITH cell AS (
  SELECT dataset, h3_8, COUNT(*) AS n, AVG(lat) AS lat, AVG(lon) AS lon
  FROM checkins WHERE dataset IN ('nyc', 'tky') GROUP BY dataset, h3_8
), cat AS (
  SELECT dataset, h3_8, category,
         ROW_NUMBER() OVER (PARTITION BY dataset, h3_8 ORDER BY COUNT(*) DESC) AS rk
  FROM checkins WHERE dataset IN ('nyc', 'tky') GROUP BY dataset, h3_8, category
), ranked AS (
  SELECT cell.*, cat.category AS top_category,
         ROW_NUMBER() OVER (PARTITION BY cell.dataset ORDER BY n DESC) AS pos
  FROM cell JOIN cat ON cell.dataset = cat.dataset AND cell.h3_8 = cat.h3_8 AND cat.rk = 1
)
SELECT dataset, pos, h3_8, n, ROUND(lat, 4) AS lat, ROUND(lon, 4) AS lon, top_category
FROM ranked WHERE pos <= 15
ORDER BY dataset, pos;

-- name: Monthly growth (Gowalla)
-- desc: Monthly active users and check-ins on the Gowalla network.
SELECT date_format(date_local, 'yyyy-MM') AS month,
       COUNT(*)                AS checkins,
       COUNT(DISTINCT user_id) AS active_users
FROM checkins
WHERE dataset = 'gowalla'
GROUP BY date_format(date_local, 'yyyy-MM')
ORDER BY month;

-- name: Social degree distribution
-- desc: How many friends Gowalla users have (log-scale buckets).
SELECT CASE WHEN deg < 2 THEN '1' WHEN deg < 5 THEN '2-4' WHEN deg < 10 THEN '5-9'
            WHEN deg < 50 THEN '10-49' WHEN deg < 200 THEN '50-199' ELSE '200+' END AS friends,
       COUNT(*) AS users
FROM (SELECT user_a, COUNT(*) AS deg FROM friendships GROUP BY user_a) d
GROUP BY CASE WHEN deg < 2 THEN '1' WHEN deg < 5 THEN '2-4' WHEN deg < 10 THEN '5-9'
              WHEN deg < 50 THEN '10-49' WHEN deg < 200 THEN '50-199' ELSE '200+' END
ORDER BY MIN(deg);

-- name: Late-night city
-- desc: Share of each city's check-ins between midnight and 4 a.m., by category.
SELECT dataset, category, COUNT(*) AS night_checkins,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY dataset), 2) AS pct_of_night
FROM checkins
WHERE dataset IN ('nyc', 'tky') AND hour BETWEEN 0 AND 3
GROUP BY dataset, category
ORDER BY dataset, night_checkins DESC
LIMIT 30;
