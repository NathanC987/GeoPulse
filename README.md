# GeoPulse

**Real-time check-in analytics, POI recommendation and spatiotemporal insights for location-based social networks (LBSNs)**

GeoPulse is a big-data application built on public LBSN data from Foursquare and Gowalla. It covers the three
required components:

| Requirement | How GeoPulse does it |
|---|---|
| **Streaming data analysis** | Check-ins are replayed into **Apache Kafka** and processed by **Spark Structured Streaming**: sliding event-time windows give trending categories and venues, a live H3 hexagon map and **burst / event detection** against a historical baseline. **Hive** (HiveQL over HiveServer2) and **PySpark** handle the batch analysis. |
| **Recommendation system** | A context-aware **hybrid POI recommender**: Spark MLlib implicit-feedback **ALS** re-ranked with **distance**, **time-of-day** affinity and **friends' visits**. It is evaluated with a temporal hold-out against baselines, with ablations. |
| **Spatiotemporal analytics** | Hour-of-day hexagon maps, **Getis-Ord Gi\*** hotspots per time of day, origin–destination flows, category transition (Markov) chains, radius of gyration, Hurricane Sandy's visible impact on NYC, global network growth, and friend vs. random co-location. |

## Datasets

| Dataset | Content | Size |
|---|---|---|
| [Foursquare NYC & Tokyo (TSMC2014)](https://sites.google.com/site/yangdingqi/home/foursquare-dataset), Yang et al. 2015 | user, venue, **category**, lat/lon, timezone, timestamp (Apr 2012 – Feb 2013) | 227 k + 573 k check-ins |
| [SNAP Gowalla](https://snap.stanford.edu/data/loc-gowalla.html), Cho, Myers & Leskovec, KDD 2011 | user, timestamp, lat/lon, location (Feb 2009 – Oct 2010) + **friendship graph** | 6.4 M check-ins, 950 k friendships |

`make data` downloads everything into `data/raw/` (237 MB).

## Architecture

```
                          ┌────────────── speed layer ─────────────────┐
 data/raw ─► replay producer ─► Kafka topic `checkins` ─► Spark Structured Streaming ──┐
              (time-compressed)                           sliding windows · H3 · z-scores │
                                                                  ▲ hourly baseline       │
                          ┌────────────── batch layer ────────────┼──────────────────┐    ▼
 data/raw ─► PySpark ETL ─► Parquet data lake ─► Hive external tables (metastore in Postgres)
                                                  │  ├─ HiveQL analytics (HiveServer2 / Spark Thrift)
                                                  │  ├─ spatiotemporal jobs (Gi*, OD flows, rhythms, social)
                                                  │  └─ ALS + hybrid recommender (+ evaluation)
                                                  ▼
                                      PostgreSQL serving layer  ◄──  Streamlit dashboard
```

Everything runs in Docker Compose:

| Service | Role | URL |
|---|---|---|
| `kafka` (+ `kafka-init`, `kafka-ui`) | Data capture: Apache Kafka 3.8 (KRaft), topic browser | Kafka UI http://localhost:8081 |
| `spark-master`, `spark-worker-1/2` | Spark 3.5 standalone cluster (12 cores, 14 GB) | http://localhost:8080 |
| `spark-thrift` | HiveServer2-compatible endpoint over the Hive metastore | `jdbc:hive2://localhost:10000` |
| `streamer` | Spark Structured Streaming job | Spark UI http://localhost:4042 |
| `producer` | Kafka replay producer | – |
| `postgres` | Hive metastore DB + serving DB | `localhost:5433` (geopulse/geopulse) |
| `dashboard` | Streamlit app | **http://localhost:8501** |

## Setup on a new machine (Arch Linux / Omarchy)

Spark, Kafka, Hive and Python all run in containers. The host only needs Docker, plus git, make, curl and
python (python is used to unzip the downloads).

**1. Prerequisites**

```bash
sudo pacman -S --needed git make curl python docker docker-compose docker-buildx
sudo systemctl enable --now docker
sudo usermod -aG docker $USER      # then log out and back in
```

Check:

```bash
docker info >/dev/null && echo docker ok
docker compose version
id -u            # should print 1000
```

* **User ID:** the images run as uid/gid 1000, so files written to `data/` stay owned by you. If `id -u` prints
  something else, change `ARG UID=1000` / `ARG GID=1000` in `docker/spark/Dockerfile` and `docker/app/Dockerfile`
  to your `id -u` / `id -g` before building.
* **Resources:** about 16 GB of RAM, 8 or more CPU cores and about 5 GB of disk. On a smaller machine, reduce
  `--cores 6 --memory 7g` for `spark-worker-1` and `spark-worker-2` in `docker-compose.yml`
  (e.g. `--cores 4 --memory 4g`).
* **Ports:** 8501, 8080, 8081, 4040, 4041, 4042, 5433, 9094 and 10000 must be free.

**2. Clone, build, download, run**

```bash
git clone https://github.com/NathanC987/GeoPulse.git
cd GeoPulse
make build       # Spark + app images, first time only (~5-10 min)
make data        # datasets into data/raw/ (237 MB)
make demo        # services, batch layer, live stream (~10 min)
# open http://localhost:8501
```

If the Foursquare server is down, `make data` says so. Download the dataset from Kaggle instead and put
`dataset_TSMC2014_NYC.txt` and `dataset_TSMC2014_TKY.txt` in `data/raw/`.

**3. Day to day**

| Command | What it does |
|---|---|
| `make down` | Stop everything; data and batch results are kept |
| `make up && make stream-reset stream replay` | Start again later without rerunning the batch layer |
| `make stream-reset stream replay` | Restart the live replay from the beginning (e.g. before a presentation) |
| `make clean` | Delete generated data (downloads are kept); `make demo` rebuilds it |

**Troubleshooting**

* `permission denied ... docker.sock`: log out and back in after `usermod -aG docker`.
* `port is already allocated`: free that port, or change the host side of the mapping in `docker-compose.yml`.
* Spark jobs stuck on "waiting for resources": the workers ask for more memory than the machine has; lower it as above.
* The dashboard says "Batch results not found": run `make batch`, then reload the page.

## Quick start

Requirements: Docker with Compose and about 16 GB of RAM. Nothing else is installed on the host.

```bash
make build     # build the Spark and app images (first time only)
make data      # download the datasets
make demo      # start everything, run the batch layer, start the live stream
# open http://localhost:8501
```

`make demo` runs these steps, which you can also run one at a time:

```bash
make up              # Kafka, Postgres, Spark cluster, HiveServer2, dashboard
make etl             # PySpark: raw files -> cleaned, enriched Parquet lake          (~45 s)
make hive-ddl        # beeline: CREATE EXTERNAL TABLE ... over the lake            (~5 s)
make spatiotemporal  # PySpark: Gi* hotspots, flows, rhythms, social, baseline      (~1 min)
make recommend       # PySpark + MLlib: ALS grid search, hybrid tuning, evaluation  (~7 min)
make stream          # start the Structured Streaming job
make replay          # start replaying check-ins into Kafka (from 26 Oct 2012, 1800x)
```

Other useful targets: `make hive` (run all HiveQL analytics in beeline), `make burst` (inject a flash crowd),
`make status`, `make logs`, `make stream-reset`, `make test`, `make down`, `make clean`.
Replay options: `make replay REPLAY_START=2012-06-01 REPLAY_SPEED=3600`.

## Demo script (≈ 10 minutes)

1. **Home**: the datasets, architecture diagram and the scale of the data.
2. **Live Pulse**: open Kafka UI to show messages arriving on `checkins`, and the Spark UI (:4042) to show the
   four streaming queries. The dashboard map and trending lists update every 3 s. Open *Simulate a flash crowd*
   and inject 80 check-ins at Penn Station / MSG. Within seconds a red ring and an alert appear
   (e.g. *61 check-ins vs usual 0.3, z = 60.7*). The replay starts just before **Hurricane Sandy** (29 Oct 2012),
   so NYC activity drops during the storm.
3. **City Rhythm**: move the hour slider (bars at night, coffee in the morning). Show Gi\* hotspots
   by time of day and how they move, commuter flow arcs, the category transition matrix (*Coffee Shop → Office*),
   and the timeline with Sandy's **−46 %** drop.
4. **Global & Social**: Gowalla worldwide, inter-city travel arcs, network growth, and the social result:
   **friends share a place 38.5 % of the time vs 0.9 % for random pairs (43×)**.
5. **Recommender**: pick a user, change location and hour and move the weight sliders, and the top-10
   and the "why" bars update. Then show the **Offline evaluation** tab.
6. **Hive Explorer**: run a HiveQL query live against HiveServer2.

## Results

### Recommender (test split, top-10, new-venue discovery)

| Dataset | Method | Precision@10 | Recall@10 | NDCG@10 | Hit rate | Coverage |
|---|---|---|---|---|---|---|
| Gowalla | **GeoPulse Hybrid** (ALS + geo + social) | **0.0361** | **0.0858** | **0.0750** | **0.270** | 0.025 |
| Gowalla | Hybrid without social | 0.0324 | 0.0734 | 0.0657 | 0.239 | 0.025 |
| Gowalla | Hybrid without geo | 0.0316 | 0.0673 | 0.0578 | 0.229 | 0.012 |
| Gowalla | ALS (implicit MF) | 0.0258 | 0.0510 | 0.0487 | 0.204 | 0.005 |
| Gowalla | Geo-Popularity | 0.0233 | 0.0575 | 0.0461 | 0.181 | 0.025 |
| Gowalla | Popularity | 0.0070 | 0.0119 | 0.0098 | 0.064 | 0.000 |
| NYC | **GeoPulse Hybrid** (ALS + geo) | **0.0341** | **0.0532** | **0.0513** | **0.291** | 0.174 |
| NYC | ALS (implicit MF) | 0.0323 | 0.0497 | 0.0493 | 0.275 | 0.108 |
| NYC | Popularity | 0.0243 | 0.0325 | 0.0363 | 0.217 | 0.001 |
| Tokyo | **GeoPulse Hybrid** (ALS + geo) | **0.0648** | **0.0736** | **0.0864** | **0.457** | 0.039 |
| Tokyo | ALS (implicit MF) | 0.0646 | 0.0725 | 0.0854 | 0.455 | 0.035 |
| Tokyo | Popularity | 0.0442 | 0.0474 | 0.0642 | 0.338 | 0.002 |

* On Gowalla the hybrid improves NDCG@10 by **+54 %** over plain ALS. Both context signals help: removing the
  social signal costs 12 % and removing the geographic signal costs 23 %.
* In the cities the gain is smaller (+4 % NYC, +1 % Tokyo), but the hybrid recommends a much wider part of
  the catalogue (coverage +61 % in NYC). Users there move within a few km, so the collaborative signal already
  carries most of the spatial information.
* The time-of-day weight was tuned to 0: for *new*-venue discovery a category-level hour profile adds nothing
  over ALS. It is still available as a slider in the dashboard.

Protocol: each user's check-ins are split in time 64 / 16 / 20 % (train / validation / test). ALS
hyper-parameters (rank, λ, α) and the hybrid weights and distance bandwidth σ are grid-searched on validation.
The final model is trained on train+validation and scored once on test. Relevant items are venues visited in
the test period that the user had not visited before. Gowalla is evaluated on a random sample of 800 users.

### Spatiotemporal highlights

* Gi\* finds 52–76 hotspot cells at 99 % confidence per city and time slot. Evening hotspots centre on Union Square,
  Times Square and Penn Station; in Tokyo they centre on the major rail hubs.
* Strongest Tokyo flows: Akihabara/Kanda ↔ Ueno, Shinjuku and Shimbashi. Strongest NYC transition:
  *American Restaurant → Bar* (37 %).
* Hurricane Sandy: NYC daily check-ins fell by 46 % between 29 Oct and 2 Nov 2012, compared with the two weeks before.
* Gowalla friends are **43×** more likely than random pairs to share a visited place. Their home areas are a
  median 434 km apart, against 6,406 km for random pairs.

## Project structure

```
docker-compose.yml  Makefile  .streamlit/config.toml
docker/spark/       Spark image: Kafka + JDBC jars, hive-site.xml, spark-defaults, log4j
docker/app/         Python image for the producer, dashboard and tests
scripts/download_data.sh
sql/                Postgres init: serving tables + Hive 2.3 metastore schema
common/             config.py, geo.py (H3, haversine, landmarks), scoring.py (hybrid recommender)
batch/              etl.py, spatiotemporal.py, recommender.py, spark_utils.py
hive/               create_tables.hql (DDL), analytics.hql (HiveQL analytics)
streaming/          stream_checkins.py (Structured Streaming job)
producer/           replay_producer.py (Kafka), burst.py (flash-crowd injector)
dashboard/          Home.py + pages/ (Live Pulse, City Rhythm, Global & Social, Recommender, Hive Explorer)
tests/              pytest unit tests for geo helpers and the recommender scoring
```

## Implementation notes

* **Hive**: Hive is used through Spark's built-in Hive support. The external tables live in a Hive metastore
  backed by Postgres, and HiveQL is executed by the Spark Thrift Server (HiveServer2 protocol: `beeline`, PyHive,
  JDBC). This is the standard modern deployment and avoids running a separate MapReduce/Tez engine. The metastore
  schema is pre-created from Hive's official `hive-schema-2.3.0.postgres.sql`, because DataNucleus' lazy schema
  creation deadlocks on Postgres.
* **Event time across cities**: NYC and Tokyo local clocks are 13 h apart, so the watermark and windows use UTC
  event time and results are converted to each city's local time. Live tables are keyed by the UTC window,
  because local time is not unique when daylight-saving time ends (4 Nov 2012 in NYC).
* **Burst detection**: for each H3 r8 cell × hour-of-week, the batch layer stores the mean and standard deviation
  of hourly check-ins over all ~45 weeks (`geopulse.cell_baseline`). The stream raises an alert when a cell has
  ≥ 10 check-ins in an hour and z ≥ 4.
* **Gi\***: binary weights over each cell and its six neighbours (H3 k-ring 1), computed separately for night,
  morning, afternoon and evening, with z thresholds 1.65 / 1.96 / 2.58 (90 / 95 / 99 %).
* **Gowalla local time** is approximated from longitude (15° per hour) because the dataset has no timezone field.
