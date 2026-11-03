"""Inject a synthetic "flash crowd" into the live stream to demonstrate burst detection.

Publishes `n` check-ins scattered within ~150 m of a point, timestamped over the next
`minutes` of *simulated* time, so the streaming job sees an abnormal spike in one H3 cell.

    python -m producer.burst --dataset nyc --lat 40.7505 --lon -73.9934 --n 80
"""
import argparse
import json
import random
from datetime import datetime, timedelta, timezone

import psycopg2

from common import config
from producer.replay_producer import make_producer

TIMEZONES = {"nyc": "America/New_York", "tky": "Asia/Tokyo"}


def tz_offset_minutes(dataset: str, at: datetime) -> int:
    from zoneinfo import ZoneInfo
    return int(at.astimezone(ZoneInfo(TIMEZONES[dataset])).utcoffset().total_seconds() // 60)


def current_sim_time() -> datetime:
    """Latest event time seen by the streaming job (UTC); falls back to wall-clock now."""
    try:
        with psycopg2.connect(config.PG_DSN) as conn, conn.cursor() as cur:
            cur.execute("SELECT max(max_event_time) FROM stream_metrics")
            ts = cur.fetchone()[0]
            if ts:
                return ts.replace(tzinfo=timezone.utc)
    except psycopg2.Error:
        pass
    return datetime.now(timezone.utc)


def inject_burst(dataset: str, lat: float, lon: float, n: int = 80, minutes: int = 20,
                 category: str = "Flash Mob (simulated)", start: datetime | None = None) -> int:
    start = start or current_sim_time()
    producer = make_producer()
    rnd = random.Random()
    for i in range(n):
        ts = start + timedelta(seconds=rnd.uniform(0, minutes * 60))
        msg = {
            "dataset": dataset, "user_id": f"burst-{rnd.randrange(10**6)}",
            "venue_id": "burst-venue", "category": category,
            "lat": lat + rnd.gauss(0, 0.0012), "lon": lon + rnd.gauss(0, 0.0015),
            "tz_offset": tz_offset_minutes(dataset, ts),
            "ts_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ"), "source": "injected",
        }
        producer.produce(config.TOPIC_CHECKINS, key=msg["user_id"], value=json.dumps(msg))
    producer.flush(10)
    return n


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", default="nyc", choices=["nyc", "tky"])
    ap.add_argument("--lat", type=float, default=40.7505)
    ap.add_argument("--lon", type=float, default=-73.9934)
    ap.add_argument("--n", type=int, default=80)
    args = ap.parse_args()
    sent = inject_burst(args.dataset, args.lat, args.lon, args.n)
    print(f"Injected {sent} synthetic check-ins near ({args.lat}, {args.lon}) at {current_sim_time():%Y-%m-%d %H:%M} UTC")


if __name__ == "__main__":
    main()
