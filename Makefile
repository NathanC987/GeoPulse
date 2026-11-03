# GeoPulse - one-command workflow. Run `make help` for the list of targets.
SHELL := /bin/bash
DC := docker compose
SUBMIT := $(DC) exec -T spark-master submit
BEELINE := $(DC) exec -T spark-thrift beeline -u jdbc:hive2://localhost:10000 --silent=true
PSQL := $(DC) exec -T postgres psql -U geopulse -d geopulse

REPLAY_START ?= 2012-10-26
REPLAY_SPEED ?= 1800
export REPLAY_START REPLAY_SPEED

.PHONY: help build up down clean data etl hive-ddl hive spatiotemporal recommend batch \
        stream replay stop-stream stream-reset burst status logs dashboard test demo

help:            ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

build:           ## Build the Spark and app Docker images
	$(DC) build spark-master dashboard

up:              ## Start Kafka, Postgres, the Spark cluster, Thrift Server (Hive) and the dashboard
	mkdir -p data/raw data/lake data/warehouse data/serving data/checkpoints
	$(DC) up -d kafka kafka-init kafka-ui postgres spark-master spark-worker-1 spark-worker-2 spark-thrift dashboard
	@echo "Waiting for HiveServer2 (Spark Thrift Server)..."
	@until $(DC) exec -T spark-thrift bash -c '</dev/tcp/localhost/10000' 2>/dev/null; do sleep 2; done
	@echo "Up:  dashboard http://localhost:8501 | Spark http://localhost:8080 | Kafka UI http://localhost:8081"

down:            ## Stop all containers (keeps data)
	$(DC) --profile stream --profile replay down

clean: down      ## Stop everything and delete derived data (keeps the raw downloads)
	$(DC) down -v
	rm -rf data/lake data/warehouse data/serving data/checkpoints

data:            ## Download the Foursquare + Gowalla datasets into data/raw
	./scripts/download_data.sh

# ---------------------------------------------------------------- batch layer
etl:             ## PySpark ETL: raw files -> Parquet data lake
	$(SUBMIT) batch/etl.py

hive-ddl:        ## Create the Hive external tables over the data lake
	$(BEELINE) --outputformat=table -f hive/create_tables.hql

hive:            ## Run the HiveQL analytics queries (hive/analytics.hql)
	$(BEELINE) --outputformat=table -f hive/analytics.hql

spatiotemporal:  ## Spatiotemporal analytics (Gi* hotspots, OD flows, rhythms, social) -> Postgres
	$(SUBMIT) batch/spatiotemporal.py

recommend:       ## Train + evaluate the hybrid POI recommender (all datasets)
	$(SUBMIT) batch/recommender.py

batch: etl hive-ddl spatiotemporal recommend   ## Whole batch layer

# ---------------------------------------------------------------- speed layer
stream:          ## Start the Spark Structured Streaming job (Kafka -> Postgres)
	$(DC) --profile stream up -d streamer

replay:          ## Start replaying check-ins into Kafka (REPLAY_START, REPLAY_SPEED)
	$(DC) --profile replay up -d --force-recreate producer

stop-stream:     ## Stop the producer and the streaming job
	$(DC) --profile stream --profile replay stop producer streamer

stream-reset: stop-stream   ## Clear the topic, checkpoints and live tables for a fresh demo
	$(DC) --profile stream --profile replay rm -f producer streamer
	-$(DC) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic checkins
	sleep 3
	$(DC) run --rm kafka-init
	rm -rf data/checkpoints/stream
	$(PSQL) -c "TRUNCATE stream_trending, stream_cells, stream_alerts, stream_metrics, stream_recent"

burst:           ## Inject a synthetic flash crowd near Penn Station / MSG (NYC)
	$(DC) run --rm --no-deps dashboard python -m producer.burst --dataset nyc --lat 40.7505 --lon -73.9934 --n 80

# ---------------------------------------------------------------- misc
status:          ## Show containers, Kafka topic and live-table row counts
	$(DC) --profile stream --profile replay ps --format 'table {{.Service}}\t{{.Status}}'
	@$(PSQL) -c "SELECT (SELECT count(*) FROM stream_metrics) AS micro_batches, (SELECT sum(rows) FROM stream_metrics) AS events, (SELECT max(max_event_time) FROM stream_metrics) AS sim_time_utc, (SELECT count(*) FROM stream_alerts) AS alerts"

logs:            ## Follow the streaming job + producer logs
	$(DC) --profile stream --profile replay logs -f streamer producer

dashboard:       ## (Re)start the Streamlit dashboard on http://localhost:8501
	$(DC) up -d --force-recreate dashboard

test:            ## Run the unit tests
	$(DC) run --rm --no-deps -e PYTHONDONTWRITEBYTECODE=1 dashboard pytest -q -p no:cacheprovider tests

demo: up batch stream-reset stream replay   ## Everything: infra, batch layer, live stream
	@echo "Open http://localhost:8501"
