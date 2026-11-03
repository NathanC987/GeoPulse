import numpy as np
import pytest
import scipy.sparse as sp

from common.scoring import RecModel, minmax, ranking_metrics, top_k


def toy_model(friends: bool = False) -> RecModel:
    """3 users x 4 items. Items 0/1 are near the origin, 2/3 are ~11 km north."""
    U = np.array([[1.0, 0.0], [0.0, 1.0], [0.5, 0.5]], np.float32)
    V = np.array([[1.0, 0.0], [0.9, 0.1], [0.0, 1.0], [0.8, 0.0]], np.float32)
    train = sp.csr_matrix(np.array([[3, 0, 0, 0], [0, 0, 2, 0], [0, 1, 0, 5]], np.float32))
    time_aff = np.zeros((4, 24), np.float32)
    time_aff[3, 20] = 1.0                                     # item 3 is an evening place
    fr = sp.csr_matrix(np.array([[0, 0, 1], [0, 0, 0], [1, 0, 0]], np.float32)) if friends else None
    return RecModel(dataset="toy", user_ids=np.array(["a", "b", "c"]), item_ids=np.array(list("wxyz")),
                    U=U, V=V, item_lat=np.array([0.0, 0.0, 0.1, 0.1]), item_lon=np.zeros(4),
                    item_pop=np.array([3, 1, 2, 5]), item_category=np.array(["", "", "", ""]),
                    time_aff=time_aff, user_last=np.zeros((3, 2)), train=train, friends=fr, sigma_km=2.0)


def test_minmax_and_constant_input():
    assert minmax(np.array([2.0, 4.0, 6.0])).tolist() == [0.0, 0.5, 1.0]
    assert minmax(np.array([3.0, 3.0])).tolist() == [0.0, 0.0]


def test_top_k_orders_by_score():
    assert top_k(np.array([0.1, 0.9, 0.5, 0.7]), 3).tolist() == [1, 3, 2]
    assert top_k(np.array([1.0, 2.0]), 5).tolist() == [1, 0]


def test_components_are_scaled():
    comp = toy_model().components(0, 0.0, 0.0, hour=12)
    for name in ("als", "geo", "time", "social"):
        assert comp[name].min() >= 0 and comp[name].max() <= 1
    assert comp["geo"][0] == pytest.approx(1.0)               # item at the query location
    assert comp["geo"][2] < 0.01                              # ~11 km away with sigma = 2 km
    assert comp["social"].sum() == 0                          # no friend graph


def test_recommend_excludes_seen_and_reacts_to_context():
    m = toy_model()
    top, _, _ = m.recommend(0, 0.0, 0.0, hour=12, weights={"als": 1.0}, k=3)
    assert 0 not in top                                       # user 0 already visited item 0
    # Pure proximity from the north: the two northern items win.
    top, _, _ = m.recommend(0, 0.1, 0.0, hour=12, weights={"als": 0.0, "geo": 1.0}, k=2, exclude_seen=False)
    assert set(top.tolist()) == {2, 3}
    # Time-of-day: at 20:00 the evening place is boosted to the top.
    top, _, _ = m.recommend(1, 0.0, 0.0, hour=20, weights={"als": 0.1, "time": 1.0}, k=1)
    assert top.tolist() == [3]


def test_social_signal_counts_friends_visits():
    m = toy_model(friends=True)
    comp = m.components(0, 0.0, 0.0, hour=0)                  # user 0's friend (user 2) visited items 1 and 3
    assert comp["social"][[1, 3]].tolist() == [1.0, 1.0]
    assert comp["social"][[0, 2]].tolist() == [0.0, 0.0]


def test_ranking_metrics():
    m = ranking_metrics(np.array([5, 1, 7, 2]), relevant={1, 2, 9}, k=4)
    assert m["precision"] == pytest.approx(2 / 4)
    assert m["recall"] == pytest.approx(2 / 3)
    assert m["hit_rate"] == 1.0
    dcg = 1 / np.log2(3) + 1 / np.log2(5)
    idcg = 1 + 1 / np.log2(3) + 1 / np.log2(4)
    assert m["ndcg"] == pytest.approx(dcg / idcg)
    assert ranking_metrics(np.array([3, 4]), relevant={1}, k=2)["ndcg"] == 0.0


def test_save_load_roundtrip(tmp_path):
    m = toy_model(friends=True)
    m.meta = {"weights": {"als": 1.0}}
    m.save(str(tmp_path))
    loaded = RecModel.load(str(tmp_path), "toy")
    assert np.allclose(loaded.V, m.V) and (loaded.train != m.train).nnz == 0
    assert loaded.friends is not None and loaded.meta["weights"] == {"als": 1.0}
