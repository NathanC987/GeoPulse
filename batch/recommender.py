"""Batch layer, step 3: context-aware hybrid POI recommender.

For each dataset:
  1. Temporal split per user: first 64% of check-ins = fit, next 16% = validation, last 20% = test.
  2. Spark MLlib implicit-feedback ALS on user x venue check-in counts (grid search on validation).
  3. Context features: venue location (geo), hour-of-day affinity (time), friends' visits (social).
  4. Tune the hybrid weights on validation, retrain on fit+validation, report on test against
     Popularity / Geo-Popularity / ALS baselines plus ablations.
  5. Export the model artefacts for the dashboard (data/serving/rec/<dataset>/) and the metrics to Postgres.

Usage: submit batch/recommender.py [nyc tky gowalla]
"""
import itertools
import os
import sys
import time
from dataclasses import dataclass

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import scipy.sparse as sp
from pyspark.ml.recommendation import ALS
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from batch.spark_utils import get_spark, step
from common import config
from common.scoring import COMPONENTS, RecModel, ranking_metrics, top_k

K = 10
SETTINGS = {
    # min_user: check-ins a user needs to be modelled; min_item: training check-ins a venue needs.
    "nyc": dict(min_user=10, min_item=3, eval_users=None, als_grid=True, sigmas=(0.5, 1.0, 2.0, 4.0),
                weight_grid=dict(geo=[0.0, 0.1, 0.25, 0.5, 1.0, 2.0], time=[0.0, 0.05, 0.1, 0.25, 0.5], social=[0.0])),
    "tky": dict(min_user=10, min_item=3, eval_users=None, als_grid=True, sigmas=(0.5, 1.0, 2.0, 4.0),
                weight_grid=dict(geo=[0.0, 0.1, 0.25, 0.5, 1.0, 2.0], time=[0.0, 0.05, 0.1, 0.25, 0.5], social=[0.0])),
    # Gowalla is ~25x larger: fixed ALS params, a coarser grid and a sample of evaluation users.
    "gowalla": dict(min_user=20, min_item=5, eval_users=800, als_grid=False, sigmas=(5.0, 20.0, 100.0),
                    weight_grid=dict(geo=[0.0, 0.5, 1.0, 2.0], time=[0.0, 0.25], social=[0.0, 0.5, 1.0])),
}
ALS_DEFAULT = dict(rank=64, regParam=0.05, alpha=10.0)
ALS_GRID = dict(rank=[32, 64], regParam=[0.01, 0.1], alpha=[5.0, 20.0])


@dataclass
class EvalSet:
    truth: dict          # user idx -> set of relevant (new) item idx
    hour: dict           # user idx -> local hour of the first held-out check-in


# =========================================================================== data preparation
def split_checkins(spark, ds: str, min_user: int) -> DataFrame:
    ck = (spark.table(f"{config.HIVE_DB}.checkins").filter(F.col("dataset") == ds)
          .select("user_id", "venue_id", "category", "lat", "lon", "ts_utc", "hour"))
    active = ck.groupBy("user_id").count().filter(F.col("count") >= min_user).select("user_id")
    w = Window.partitionBy("user_id").orderBy("ts_utc")
    pr = F.percent_rank().over(w)
    return (ck.join(active, "user_id")
              .withColumn("part", F.when(pr < 0.64, "fit").when(pr < 0.8, "val").otherwise("test"))
              .cache())


def build_model(spark, ds: str, train: DataFrame, holdout: DataFrame, als_params: dict, s: dict):
    """Train ALS + context features on `train`, build the evaluation set from `holdout`."""
    counts = train.groupBy("user_id", "venue_id").agg(F.count("*").alias("n"))
    item_n = counts.groupBy("venue_id").agg(F.sum("n").alias("pop")).filter(F.col("pop") >= s["min_item"])
    counts = counts.join(item_n.select("venue_id"), "venue_id")

    users = counts.select("user_id").distinct().toPandas()["user_id"].sort_values().to_numpy()
    items_pd = (item_n.join(spark.table(f"{config.HIVE_DB}.venues")
                             .filter(F.col("dataset") == ds)
                             .select("venue_id", "lat", "lon", "category"), "venue_id")
                .toPandas().sort_values("venue_id").reset_index(drop=True))
    items = items_pd["venue_id"].to_numpy()
    u_map = spark.createDataFrame(pd.DataFrame({"user_id": users, "u": np.arange(len(users))}))
    i_map = spark.createDataFrame(pd.DataFrame({"venue_id": items, "i": np.arange(len(items))}))

    idx = counts.join(u_map, "user_id").join(i_map, "venue_id").select(
        "u", "i", "n", F.log1p("n").alias("rating")).cache()

    als = ALS(userCol="u", itemCol="i", ratingCol="rating", implicitPrefs=True, nonnegative=False,
              maxIter=15, seed=42, coldStartStrategy="drop", **als_params)
    fitted = als.fit(idx)
    k = als_params["rank"]
    U = np.zeros((len(users), k), np.float32)
    V = np.zeros((len(items), k), np.float32)
    for r in fitted.userFactors.collect():
        U[r["id"]] = r["features"]
    for r in fitted.itemFactors.collect():
        V[r["id"]] = r["features"]

    tr = idx.select("u", "i", "n").toPandas()
    train_csr = sp.csr_matrix((tr["n"].to_numpy(np.float32), (tr["u"], tr["i"])),
                              shape=(len(users), len(items)))
    idx.unpersist()

    # Temporal context: how much more likely is a visit at hour h than on average?
    global_h = train.groupBy("hour").count().toPandas().set_index("hour")["count"].reindex(range(24), fill_value=0)
    p_h = (global_h / global_h.sum()).to_numpy()
    if items_pd["category"].notna().any():
        cat_h = (train.groupBy("category", "hour").count().toPandas()
                 .pivot(index="category", columns="hour", values="count").reindex(columns=range(24)).fillna(0))
        m = 24.0  # Bayesian smoothing towards the global hour profile
        p = (cat_h.to_numpy() + m * p_h) / (cat_h.sum(axis=1).to_numpy()[:, None] + m)
        lift = pd.DataFrame(p / p_h, index=cat_h.index)
        time_aff = lift.reindex(items_pd["category"]).fillna(1.0).to_numpy()
    else:
        vh = (train.join(i_map, "venue_id").groupBy("i", "hour").count().toPandas())
        c = np.zeros((len(items), 24))
        c[vh["i"].to_numpy(), vh["hour"].to_numpy()] = vh["count"].to_numpy()
        m = 10.0
        time_aff = ((c + m * p_h) / (c.sum(axis=1)[:, None] + m)) / p_h
    time_aff = (np.clip(time_aff, 0, 3) / 3).astype(np.float32)

    # Each user's last known location in the training period (default query location).
    last = (train.withColumn("rk", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("ts_utc"))))
                 .filter("rk = 1").join(u_map, "user_id").select("u", "lat", "lon").toPandas())
    user_last = np.zeros((len(users), 2))
    user_last[last["u"].to_numpy()] = last[["lat", "lon"]].to_numpy()

    friends = None
    if ds == "gowalla":
        fr = (spark.table(f"{config.HIVE_DB}.friendships")
              .join(u_map.withColumnRenamed("user_id", "user_a").withColumnRenamed("u", "ua"), "user_a")
              .join(u_map.withColumnRenamed("user_id", "user_b").withColumnRenamed("u", "ub"), "user_b")
              .select("ua", "ub").toPandas())
        friends = sp.csr_matrix((np.ones(len(fr), np.float32), (fr["ua"], fr["ub"])),
                                shape=(len(users), len(users)))

    model = RecModel(
        dataset=ds, user_ids=users.astype(str), item_ids=items.astype(str), U=U, V=V,
        item_lat=items_pd["lat"].to_numpy(), item_lon=items_pd["lon"].to_numpy(),
        item_pop=items_pd["pop"].to_numpy(np.int64),
        item_category=items_pd["category"].fillna("").to_numpy().astype(str),
        time_aff=time_aff, user_last=user_last, train=train_csr, friends=friends,
        sigma_km=config.DATASETS[ds]["rec_sigma_km"])

    # Evaluation set: venues visited in the hold-out period that the user had NOT visited before.
    ho = (holdout.join(u_map, "user_id").join(i_map, "venue_id")
                 .groupBy("u", "i").agg(F.min("ts_utc").alias("t"), F.min_by("hour", "ts_utc").alias("hour"))
                 .toPandas())
    seen = set(zip(*train_csr.nonzero()))
    ho = ho[[(u, i) not in seen for u, i in zip(ho["u"], ho["i"])]]
    truth = ho.groupby("u")["i"].apply(set).to_dict()
    first_hour = ho.sort_values("t").groupby("u")["hour"].first().to_dict()
    return model, EvalSet(truth, first_hour)


# =========================================================================== evaluation
def evaluate(model: RecModel, ev: EvalSet, configs: dict, max_users=None, seed=0) -> pd.DataFrame:
    """Evaluate many scoring configurations; components are computed once per user.

    configs: name -> dict(kind='hybrid'|'popularity'|'geo_pop', weights=..., sigma=...)
    """
    users = sorted(ev.truth)
    if max_users and len(users) > max_users:
        users = sorted(np.random.default_rng(seed).choice(users, max_users, replace=False))
    pop = model.popularity()
    acc = {name: {"precision": 0.0, "recall": 0.0, "ndcg": 0.0, "hit_rate": 0.0} for name in configs}
    covered = {name: set() for name in configs}
    for u in users:
        lat, lon = model.user_last[u]
        comp = model.components(u, lat, lon, ev.hour[u])
        seen = model.seen(u)
        geo_cache = {}
        for name, cfg in configs.items():
            sigma = cfg.get("sigma", model.sigma_km)
            if sigma not in geo_cache:
                geo_cache[sigma] = np.exp(-comp["dist_km"] / sigma)
            geo = geo_cache[sigma]
            if cfg["kind"] == "popularity":
                score = pop.copy()
            elif cfg["kind"] == "geo_pop":
                score = pop * geo
            else:
                score = model.combine({**comp, "geo": geo}, cfg["weights"])
            score[seen] = -np.inf
            top = top_k(score, K)
            covered[name].update(top.tolist())
            for m, v in ranking_metrics(top, ev.truth[u], K).items():
                acc[name][m] += v
    rows = []
    for name, cfg in configs.items():
        r = {"method": name, **{m: v / len(users) for m, v in acc[name].items()},
             "coverage": len(covered[name]) / model.n_items, "users": len(users)}
        rows.append(r)
    return pd.DataFrame(rows).sort_values("ndcg", ascending=False).reset_index(drop=True)


def weight_configs(s: dict) -> dict:
    g = s["weight_grid"]
    out = {}
    for sigma, g, t, so in itertools.product(s["sigmas"], g["geo"], g["time"], g["social"]):
        w = {"als": 1.0, "geo": g, "time": t, "social": so}
        out[f"σ={sigma} geo={g} time={t} social={so}"] = {"kind": "hybrid", "weights": w, "sigma": sigma}
    return out


# =========================================================================== persistence
def save_metrics(ds: str, metrics: pd.DataFrame, tuning: pd.DataFrame) -> None:
    with psycopg2.connect(config.PG_DSN) as conn, conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS rec_metrics (
            dataset TEXT, method TEXT, precision DOUBLE PRECISION, recall DOUBLE PRECISION,
            ndcg DOUBLE PRECISION, hit_rate DOUBLE PRECISION, coverage DOUBLE PRECISION,
            users INTEGER, k INTEGER, PRIMARY KEY (dataset, method))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS rec_tuning (
            dataset TEXT, stage TEXT, config TEXT, ndcg DOUBLE PRECISION, recall DOUBLE PRECISION)""")
        cur.execute("DELETE FROM rec_metrics WHERE dataset = %s", (ds,))
        cur.execute("DELETE FROM rec_tuning WHERE dataset = %s", (ds,))
        psycopg2.extras.execute_values(cur, "INSERT INTO rec_metrics VALUES %s", [
            (ds, r.method, r.precision, r.recall, r.ndcg, r.hit_rate, r.coverage, int(r.users), K)
            for r in metrics.itertuples()])
        psycopg2.extras.execute_values(cur, "INSERT INTO rec_tuning VALUES %s", [
            (ds, r.stage, r.config, r.ndcg, r.recall) for r in tuning.itertuples()])


def fmt(df: pd.DataFrame, n=None) -> str:
    cols = ["method", "precision", "recall", "ndcg", "hit_rate", "coverage"]
    return df[cols].head(n).to_string(index=False, float_format=lambda x: f"{x:.4f}")


# =========================================================================== main
def run_dataset(spark, ds: str) -> None:
    s = SETTINGS[ds]
    t0 = time.time()
    data = split_checkins(spark, ds, s["min_user"])
    fit, val, test = (data.filter(F.col("part") == p) for p in ("fit", "val", "test"))
    fit_val = data.filter(F.col("part") != "test")
    tuning_rows = []

    # 1) ALS hyper-parameters on the validation split (ALS score only).
    als_params = dict(ALS_DEFAULT)
    if s["als_grid"]:
        with step(f"[{ds}] ALS grid search on validation"):
            best = -1.0
            for rank, reg, alpha in itertools.product(*ALS_GRID.values()):
                p = dict(rank=rank, regParam=reg, alpha=alpha)
                m, ev = build_model(spark, ds, fit, val, p, s)
                r = evaluate(m, ev, {"als": {"kind": "hybrid", "weights": {"als": 1.0}}}, s["eval_users"])
                ndcg = r.loc[0, "ndcg"]
                tuning_rows.append(("als", f"rank={rank} reg={reg} alpha={alpha}", ndcg, r.loc[0, "recall"]))
                print(f"    rank={rank:<3} reg={reg:<5} alpha={alpha:<5} NDCG@10={ndcg:.4f}")
                if ndcg > best:
                    best, als_params = ndcg, p
            print(f"    best ALS params: {als_params}")

    # 2) Hybrid weights + spatial bandwidth on the validation split.
    with step(f"[{ds}] hybrid weight tuning on validation"):
        m, ev = build_model(spark, ds, fit, val, als_params, s)
        grid = evaluate(m, ev, weight_configs(s), s["eval_users"])
        for r in grid.itertuples():
            tuning_rows.append(("weights", r.method, r.ndcg, r.recall))
        best_cfg = weight_configs(s)[grid.loc[0, "method"]]
        weights, sigma = best_cfg["weights"], best_cfg["sigma"]
        print(fmt(grid, 5))
        print(f"    best: weights={weights} sigma={sigma} km")

    # 3) Final model on fit+val, evaluated on the untouched test split.
    with step(f"[{ds}] final model + test evaluation"):
        model, ev = build_model(spark, ds, fit_val, test, als_params, s)
        model.sigma_km = sigma
        configs = {
            "Popularity": {"kind": "popularity"},
            "Geo-Popularity": {"kind": "geo_pop", "sigma": sigma},
            "ALS (implicit MF)": {"kind": "hybrid", "weights": {"als": 1.0}},
            "GeoPulse Hybrid": {"kind": "hybrid", "weights": weights, "sigma": sigma},
        }
        for comp in COMPONENTS[1:]:
            if weights.get(comp):
                configs[f"Hybrid without {comp}"] = {"kind": "hybrid", "sigma": sigma,
                                                     "weights": {**weights, comp: 0.0}}
        metrics = evaluate(model, ev, configs, s["eval_users"], seed=1)
        print(fmt(metrics))

    model.meta = {
        "dataset": ds, "weights": weights, "als_params": als_params, "k": K,
        "n_users": int(len(model.user_ids)), "n_items": int(model.n_items),
        "n_interactions": int(model.train.nnz), "eval_users": int(metrics.loc[0, "users"]),
        "metrics": metrics.to_dict(orient="records"), "trained_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    model.save(os.path.join(config.SERVING_DIR, "rec", ds))
    save_metrics(ds, metrics, pd.DataFrame(tuning_rows, columns=["stage", "config", "ndcg", "recall"]))
    data.unpersist()
    print(f"  [{ds}] done in {time.time() - t0:.0f}s: {model.meta['n_users']:,} users, "
          f"{model.meta['n_items']:,} venues, {model.meta['n_interactions']:,} user-venue pairs")


def main():
    datasets = sys.argv[1:] or ["nyc", "tky", "gowalla"]
    spark = get_spark("GeoPulse-Recommender")
    spark.sparkContext.setCheckpointDir(os.path.join(config.CHECKPOINT_DIR, "als"))
    for ds in datasets:
        run_dataset(spark, ds)
    spark.stop()


if __name__ == "__main__":
    main()
