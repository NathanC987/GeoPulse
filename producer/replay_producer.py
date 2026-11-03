"""Data capture: replay historical LBSN check-ins into Kafka as a live, time-compressed stream.

Each check-in becomes one JSON message on topic `checkins`, keyed by user. Events are emitted
in event-time order and paced so that `--speed` seconds of history pass per real second
(default 1800: one simulated hour every two seconds).

    python -m producer.replay_producer --datasets nyc,tky --speed 1800 --start 2012-10-25
"""
import argparse
import json
import os
import signal
import sys
import time

import pandas as pd
from confluent_kafka import Producer

from common import config

FSQ_COLS = ["user_id", "venue_id", "category_id", "category", "lat", "lon", "tz_offset", "utc_time"]


def load_foursquare(city: str) -> pd.DataFrame:
    path = os.path.join(config.RAW_DIR, f"dataset_TSMC2014_{city.upper()}.txt")
    df = pd.read_csv(path, sep="\t", names=FSQ_COLS, encoding="ISO-8859-1",
                     dtype={"user_id": str, "venue_id": str})
    df["ts_utc"] = pd.to_datetime(df["utc_time"].str[4:], format="%b %d %H:%M:%S %z %Y", utc=True)
    df["dataset"] = city
    return df[["dataset", "user_id", "venue_id", "category", "lat", "lon", "tz_offset", "ts_utc"]]


def load_gowalla(limit_days: int = 60) -> pd.DataFrame:
    path = os.path.join(config.RAW_DIR, "loc-gowalla_totalCheckins.txt.gz")
    df = pd.read_csv(path, sep="\t", names=["user_id", "utc_time", "lat", "lon", "venue_id"],
                     dtype={"user_id": str, "venue_id": str})
    df["ts_utc"] = pd.to_datetime(df["utc_time"], utc=True)
    df = df[df["ts_utc"] >= df["ts_utc"].max() - pd.Timedelta(days=limit_days)]
    df["dataset"] = "gowalla"
    df["category"] = None
    df["tz_offset"] = (df["lon"] / 15).round().astype(int) * 60
    return df[["dataset", "user_id", "venue_id", "category", "lat", "lon", "tz_offset", "ts_utc"]]


def to_message(row) -> dict:
    return {
        "dataset": row.dataset, "user_id": row.user_id, "venue_id": row.venue_id,
        "category": row.category if isinstance(row.category, str) else None,
        "lat": float(row.lat), "lon": float(row.lon), "tz_offset": int(row.tz_offset),
        "ts_utc": row.ts_utc.strftime("%Y-%m-%dT%H:%M:%SZ"), "source": "replay",
    }


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "linger.ms": 50,
                     "compression.type": "lz4", "client.id": "geopulse-replay"})


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--datasets", default=os.environ.get("REPLAY_DATASETS", "nyc,tky"))
    ap.add_argument("--speed", type=float, default=float(os.environ.get("REPLAY_SPEED", 1800)),
                    help="simulated seconds per real second")
    ap.add_argument("--start", default=os.environ.get("REPLAY_START"),
                    help="skip history before this UTC date, e.g. 2012-10-25")
    ap.add_argument("--loop", action="store_true", help="restart from the beginning when done")
    args = ap.parse_args()

    frames = []
    for ds in args.datasets.split(","):
        frames.append(load_gowalla() if ds == "gowalla" else load_foursquare(ds))
    events = pd.concat(frames).sort_values("ts_utc", kind="stable").reset_index(drop=True)
    if args.start:
        events = events[events["ts_utc"] >= pd.Timestamp(args.start, tz="UTC")].reset_index(drop=True)
    print(f"Loaded {len(events):,} check-ins ({args.datasets}) "
          f"{events.ts_utc.iloc[0]} -> {events.ts_utc.iloc[-1]}; speed x{args.speed:g}", flush=True)

    producer = make_producer()
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))   # `docker compose stop` -> clean exit

    while True:
        sim_start = events.ts_utc.iloc[0]
        real_start = time.monotonic()
        last_report, sent = time.monotonic(), 0
        for row in events.itertuples(index=False):
            target = real_start + (row.ts_utc - sim_start).total_seconds() / args.speed
            delay = target - time.monotonic()
            if delay > 0:
                producer.poll(0)
                time.sleep(min(delay, 1.0))
                while time.monotonic() < target:
                    time.sleep(min(target - time.monotonic(), 1.0))
            producer.produce(config.TOPIC_CHECKINS, key=f"{row.dataset}:{row.user_id}",
                             value=json.dumps(to_message(row)))
            sent += 1
            if time.monotonic() - last_report >= 5:
                producer.poll(0)
                rate = sent / (time.monotonic() - real_start)
                print(f"  sim time {row.ts_utc:%Y-%m-%d %H:%M} UTC | sent {sent:,} | {rate:,.0f} ev/s", flush=True)
                last_report = time.monotonic()
        producer.flush()
        print(f"Replay finished: {sent:,} events", flush=True)
        if not args.loop:
            break


if __name__ == "__main__":
    main()
