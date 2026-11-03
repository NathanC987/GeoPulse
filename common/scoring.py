"""Context-aware hybrid POI scoring (pure numpy).

Used both by batch/recommender.py (offline evaluation) and by the dashboard (live
re-ranking), so the model that is evaluated is exactly the one that is demonstrated.

    score(u, v | location, hour) = w_als    * ALS(u, v)                 collaborative filtering
                                 + w_geo    * exp(-dist(location, v)/σ)  spatial proximity
                                 + w_time   * time_affinity(v, hour)     temporal context
                                 + w_social * friends_visited(u, v)      social influence (Gowalla)
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

import numpy as np
import scipy.sparse as sp

from common.geo import haversine_km

COMPONENTS = ("als", "geo", "time", "social")
DEFAULT_WEIGHTS = {"als": 1.0, "geo": 0.5, "time": 0.2, "social": 0.0}


def minmax(x: np.ndarray) -> np.ndarray:
    lo, hi = float(x.min()), float(x.max())
    return (x - lo) / (hi - lo) if hi > lo else np.zeros_like(x)


@dataclass
class RecModel:
    dataset: str
    user_ids: np.ndarray           # model user index -> original user id (str)
    item_ids: np.ndarray           # model item index -> venue id (str)
    U: np.ndarray                  # user factors  (n_users x k)
    V: np.ndarray                  # item factors  (n_items x k)
    item_lat: np.ndarray
    item_lon: np.ndarray
    item_pop: np.ndarray           # training check-ins per item
    item_category: np.ndarray      # '' when unknown (Gowalla)
    time_aff: np.ndarray           # n_items x 24, temporal lift in [0, 1]
    user_last: np.ndarray          # n_users x 2, last known (training) location
    train: sp.csr_matrix           # n_users x n_items training check-in counts
    friends: sp.csr_matrix | None = None   # n_users x n_users adjacency (Gowalla)
    sigma_km: float = 2.0
    meta: dict = field(default_factory=dict)

    # ------------------------------------------------------------------ persistence
    def save(self, directory: str) -> None:
        os.makedirs(directory, exist_ok=True)
        np.savez_compressed(
            os.path.join(directory, "model.npz"),
            user_ids=self.user_ids, item_ids=self.item_ids, U=self.U, V=self.V,
            item_lat=self.item_lat, item_lon=self.item_lon, item_pop=self.item_pop,
            item_category=self.item_category, time_aff=self.time_aff, user_last=self.user_last)
        sp.save_npz(os.path.join(directory, "train.npz"), self.train)
        if self.friends is not None:
            sp.save_npz(os.path.join(directory, "friends.npz"), self.friends)
        with open(os.path.join(directory, "meta.json"), "w") as f:
            json.dump({**self.meta, "sigma_km": self.sigma_km}, f, indent=2)

    @classmethod
    def load(cls, directory: str, dataset: str) -> "RecModel":
        z = np.load(os.path.join(directory, "model.npz"), allow_pickle=False)
        friends_path = os.path.join(directory, "friends.npz")
        with open(os.path.join(directory, "meta.json")) as f:
            meta = json.load(f)
        return cls(dataset=dataset, user_ids=z["user_ids"], item_ids=z["item_ids"], U=z["U"], V=z["V"],
                   item_lat=z["item_lat"], item_lon=z["item_lon"], item_pop=z["item_pop"],
                   item_category=z["item_category"], time_aff=z["time_aff"], user_last=z["user_last"],
                   train=sp.load_npz(os.path.join(directory, "train.npz")).tocsr(),
                   friends=sp.load_npz(friends_path).tocsr() if os.path.exists(friends_path) else None,
                   sigma_km=meta.get("sigma_km", 2.0), meta=meta)

    # ------------------------------------------------------------------ scoring
    @property
    def n_items(self) -> int:
        return len(self.item_ids)

    def components(self, u: int, lat: float, lon: float, hour: int) -> dict[str, np.ndarray]:
        """Each signal for every item, scaled to [0, 1]."""
        als = minmax(self.V @ self.U[u])
        dist = haversine_km(lat, lon, self.item_lat, self.item_lon)
        geo = np.exp(-dist / self.sigma_km)
        time = self.time_aff[:, hour % 24].astype(np.float64)
        social = np.zeros(self.n_items)
        if self.friends is not None:
            row = self.friends[u]
            if row.nnz:
                visited = (self.train[row.indices] > 0).sum(axis=0).A1
                social = visited / row.nnz
                social = social / social.max() if social.max() > 0 else social
        return {"als": als, "geo": geo, "time": time, "social": social, "dist_km": dist}

    def popularity(self) -> np.ndarray:
        return minmax(np.log1p(self.item_pop.astype(np.float64)))

    @staticmethod
    def combine(comp: dict[str, np.ndarray], weights: dict[str, float]) -> np.ndarray:
        total = np.zeros_like(comp["als"])
        for name in COMPONENTS:
            w = weights.get(name, 0.0)
            if w:
                total = total + w * comp[name]
        return total

    def seen(self, u: int) -> np.ndarray:
        return self.train[u].indices

    def recommend(self, u: int, lat: float, lon: float, hour: int, weights: dict[str, float],
                  k: int = 10, exclude_seen: bool = True, max_km: float | None = None):
        """Top-k item indices plus the per-component breakdown used for explanations."""
        comp = self.components(u, lat, lon, hour)
        score = self.combine(comp, weights)
        if exclude_seen:
            score[self.seen(u)] = -np.inf
        if max_km is not None:
            score[comp["dist_km"] > max_km] = -np.inf
        top = top_k(score, k)
        return top, score, comp


def top_k(score: np.ndarray, k: int) -> np.ndarray:
    k = min(k, len(score))
    idx = np.argpartition(-score, k - 1)[:k]
    return idx[np.argsort(-score[idx])]


# ---------------------------------------------------------------------- ranking metrics
def ranking_metrics(recommended: np.ndarray, relevant: set[int], k: int = 10) -> dict[str, float]:
    rec = list(recommended[:k])
    hits = [1.0 if i in relevant else 0.0 for i in rec]
    n_hit = sum(hits)
    dcg = sum(h / np.log2(pos + 2) for pos, h in enumerate(hits))
    idcg = sum(1.0 / np.log2(pos + 2) for pos in range(min(len(relevant), k)))
    return {
        "precision": n_hit / k,
        "recall": n_hit / len(relevant) if relevant else 0.0,
        "ndcg": dcg / idcg if idcg else 0.0,
        "hit_rate": 1.0 if n_hit else 0.0,
    }
