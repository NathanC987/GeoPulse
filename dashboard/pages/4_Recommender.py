"""Recommender - context-aware hybrid POI recommendation with explanations and evaluation."""
import os

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

from common import config
from common.geo import PLACES, nearest_place
from common.scoring import COMPONENTS, RecModel
from dashboard.lib import (DATASET_LABEL, SERIES, cached_query, deck, hex_to_rgb, page_setup, table_exists)

page_setup("Recommender", "🎯")
st.title("🎯 Context-aware place recommender")
st.caption("Spark MLlib **implicit ALS** learns taste from check-in history; the dashboard re-ranks its scores "
           "live with **where you are**, **what time it is** and (Gowalla) **where your friends go**.")

COMP_COLOR = dict(zip(COMPONENTS, SERIES))
COMP_LABEL = {"als": "Taste (ALS)", "geo": "Proximity", "time": "Time of day", "social": "Friends"}
WORLD_CITIES = [("Austin", 30.2672, -97.7431), ("San Francisco", 37.7749, -122.4194), ("New York", 40.7128, -74.0060),
                ("Los Angeles", 34.0522, -118.2437), ("Chicago", 41.8781, -87.6298), ("Dallas", 32.7767, -96.7970),
                ("Houston", 29.7604, -95.3698), ("Seattle", 47.6062, -122.3321), ("Stockholm", 59.3293, 18.0686),
                ("Oslo", 59.9139, 10.7522), ("London", 51.5072, -0.1276), ("Berlin", 52.5200, 13.4050),
                ("Paris", 48.8566, 2.3522), ("Tokyo", 35.6762, 139.6503)]


@st.cache_resource(show_spinner="Loading model artefacts…")
def load_model(ds: str) -> RecModel:
    return RecModel.load(os.path.join(config.SERVING_DIR, "rec", ds), ds)


@st.cache_data(show_spinner=False)
def item_labels(ds: str) -> np.ndarray:
    m = load_model(ds)
    if ds == "gowalla":
        return np.array([f"Location #{v}" for v in m.item_ids])
    return np.array([f"{c or 'Venue'} · {nearest_place(ds, la, lo, 2.5) or 'unnamed area'}"
                     for c, la, lo in zip(m.item_category, m.item_lat, m.item_lon)])


available = [d for d in ("nyc", "tky", "gowalla")
             if os.path.exists(os.path.join(config.SERVING_DIR, "rec", d, "model.npz"))]
if not available:
    st.warning("No trained model found. Run `make recommend`.")
    st.stop()

tab_rec, tab_eval = st.tabs(["🎯 Recommend", "📏 Offline evaluation"])

with tab_rec:
    dataset = st.radio("Dataset", available, format_func=lambda d: DATASET_LABEL[d], horizontal=True)
    model = load_model(dataset)
    labels = item_labels(dataset)
    tuned = model.meta.get("weights", {})

    activity = np.asarray(model.train.sum(axis=1)).ravel()
    top_users = np.argsort(-activity)[:400]

    with st.sidebar:
        st.header("Query")
        pick = st.selectbox("User", top_users, format_func=lambda u: f"user {model.user_ids[u]} "
                                                                     f"({int(activity[u])} check-ins)")
        typed = st.text_input("…or type a user id", "")
        if typed:
            hit = np.where(model.user_ids == typed.strip())[0]
            if len(hit):
                pick = int(hit[0])
            else:
                st.error("Unknown user id (users need enough history to be modelled).")
        u = int(pick)

        mode = st.radio("Location", ["User's last check-in", "Landmark", "Custom"], horizontal=False)
        lat, lon = map(float, model.user_last[u])
        if mode == "Landmark":
            options = PLACES.get(dataset, WORLD_CITIES) if dataset != "gowalla" else WORLD_CITIES
            names = [p[0] for p in options]
            name = st.selectbox("Landmark", names)
            _, lat, lon = options[names.index(name)]
        elif mode == "Custom":
            lat = st.number_input("Latitude", value=lat, format="%.4f")
            lon = st.number_input("Longitude", value=lon, format="%.4f")
        hour = st.slider("Hour of day", 0, 23, 19, format="%d:00")

        st.header("Model weights")
        st.caption("Defaults are the values tuned on the validation split.")
        weights = {"als": st.slider(COMP_LABEL["als"], 0.0, 2.0, float(tuned.get("als", 1.0)), 0.05)}
        weights["geo"] = st.slider(COMP_LABEL["geo"], 0.0, 3.0, float(tuned.get("geo", 0.5)), 0.05)
        weights["time"] = st.slider(COMP_LABEL["time"], 0.0, 1.0, float(tuned.get("time", 0.0)), 0.05)
        if model.friends is not None:
            weights["social"] = st.slider(COMP_LABEL["social"], 0.0, 2.0, float(tuned.get("social", 0.0)), 0.05)
        sigma_opts = [0.5, 1.0, 2.0, 4.0, 10.0] if dataset != "gowalla" else [5.0, 20.0, 100.0]
        if model.sigma_km not in sigma_opts:
            sigma_opts = sorted(sigma_opts + [model.sigma_km])
        model.sigma_km = st.select_slider("Distance decay σ (km)", sigma_opts, value=model.meta.get("sigma_km", model.sigma_km))
        k = st.slider("Recommendations", 5, 25, 10)
        exclude = st.toggle("Only places the user has never visited", True)
        radius = None
        if st.toggle("Limit search radius", False):
            radius = st.slider("Within (km)", 0.5, 50.0, 3.0 if dataset != "gowalla" else 25.0, 0.5)

    top, score, comp = model.recommend(u, lat, lon, hour, weights, k=k, exclude_seen=exclude, max_km=radius)
    top = top[np.isfinite(score[top])]
    rec = pd.DataFrame({
        "rank": np.arange(1, len(top) + 1), "venue": labels[top], "km": comp["dist_km"][top],
        "score": score[top], "lat": model.item_lat[top], "lon": model.item_lon[top],
        "popularity": model.item_pop[top],
        **{c: weights.get(c, 0.0) * comp[c][top] for c in COMPONENTS},
    })
    hist_idx = model.train[u].indices
    hist = pd.DataFrame({"venue": labels[hist_idx], "visits": model.train[u].data.astype(int),
                         "lat": model.item_lat[hist_idx], "lon": model.item_lon[hist_idx]}).sort_values("visits", ascending=False)

    st.markdown(f"**User {model.user_ids[u]}** · {int(activity[u])} training check-ins at {len(hist_idx)} places · "
                f"query at **{hour:02d}:00** from ({lat:.4f}, {lon:.4f})")
    c1, c2 = st.columns([3, 2])
    with c1:
        hist_layer = hist.assign(tip="visited " + hist.visits.astype(str) + "× · " + hist.venue)
        rec_layer = rec.assign(tip="#" + rec["rank"].astype(str) + " " + rec.venue + " · " + rec.km.round(1).astype(str) + " km",
                               label=rec["rank"].astype(str))
        here = pd.DataFrame({"lat": [lat], "lon": [lon], "tip": ["query location"]})
        layers = [
            pdk.Layer("ScatterplotLayer", hist_layer, id="hist", get_position=["lon", "lat"], get_radius=80,
                      radius_min_pixels=3, get_fill_color=[137, 135, 129, 150], pickable=True),
            pdk.Layer("ScatterplotLayer", rec_layer, id="recs", get_position=["lon", "lat"], get_radius=120,
                      radius_min_pixels=9, get_fill_color=hex_to_rgb(SERIES[0]) + [235], stroked=True,
                      get_line_color=[26, 26, 25, 255], line_width_min_pixels=2, pickable=True),
            pdk.Layer("TextLayer", rec_layer, id="rank", get_position=["lon", "lat"], get_text="label", get_size=12,
                      get_color=[240, 239, 236, 255], get_alignment_baseline="'center'"),
            pdk.Layer("ScatterplotLayer", here, id="here", get_position=["lon", "lat"], get_radius=150,
                      radius_min_pixels=8, filled=False, stroked=True, get_line_color=hex_to_rgb(SERIES[1]) + [255],
                      line_width_min_pixels=3, pickable=True),
        ]
        zoom = 11.5 if dataset != "gowalla" else 9
        deck(layers, pdk.ViewState(latitude=lat, longitude=lon, zoom=zoom), tooltip={"text": "{tip}"}, height=500)
        st.caption("Blue numbered = recommendations · grey = places the user already visited · orange ring = query location")
    with c2:
        st.markdown("**Why these places?** Score = weighted sum of the signals")
        st.caption("Default weights are the ones that scored best offline. Raise *Proximity* or limit the radius "
                   "for nearby suggestions, or raise *Time of day* to favour places that are busy at this hour.")
        r = rec.iloc[::-1]
        fig = go.Figure()
        for c in COMPONENTS:
            if weights.get(c, 0) > 0:
                fig.add_trace(go.Bar(y=r["rank"].astype(str) + ". " + r.venue.str.slice(0, 34), x=r[c],
                                     name=COMP_LABEL[c], orientation="h", marker_color=COMP_COLOR[c],
                                     marker_line=dict(color="#1a1a19", width=2),
                                     hovertemplate="%{y}<br>" + COMP_LABEL[c] + ": %{x:.3f}<extra></extra>"))
        fig.update_layout(barmode="stack", height=max(320, 34 * len(r) + 80), legend=dict(orientation="h", y=1.08),
                          margin=dict(l=10, r=10, t=30, b=10), yaxis=dict(tickfont=dict(size=11)))
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    show = rec[["rank", "venue", "km", "popularity", "score"] + [c for c in COMPONENTS if weights.get(c, 0) > 0]]
    st.dataframe(show.rename(columns={**COMP_LABEL, "km": "distance km", "popularity": "check-ins"}),
                 hide_index=True, use_container_width=True,
                 column_config={c: st.column_config.NumberColumn(format="%.3f")
                                for c in ["score"] + [COMP_LABEL[c] for c in COMPONENTS]} |
                               {"distance km": st.column_config.NumberColumn(format="%.2f")})

    with st.expander(f"User history: top places ({len(hist)})"):
        if dataset != "gowalla":
            cats = (pd.Series(model.train[u].data, index=model.item_category[hist_idx]).groupby(level=0).sum()
                    .sort_values(ascending=False).head(10))
            fig = go.Figure(go.Bar(x=cats.values[::-1], y=cats.index[::-1], orientation="h", marker_color=SERIES[0]))
            fig.update_layout(height=300, title="Favourite categories")
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.dataframe(hist.head(25)[["venue", "visits"]], hide_index=True, use_container_width=True)

with tab_eval:
    if not table_exists("rec_metrics"):
        st.info("Run `make recommend` to produce evaluation results.")
        st.stop()
    st.markdown("Each user's check-ins are split **in time**: the first 64 % train the model, the next 16 % tune "
                "the weights (validation) and the **last 20 % are the test set**. A hit is a venue the user visits "
                "in the test period that they had **never visited before** (new-place discovery). Queries use the "
                "user's last training location and the hour of their first test check-in.")
    ds = st.radio("Dataset ", available, format_func=lambda d: DATASET_LABEL[d], horizontal=True, key="eval_ds")
    met = cached_query("SELECT * FROM rec_metrics WHERE dataset = :d ORDER BY ndcg DESC", d=ds)
    meta = load_model(ds).meta
    c = st.columns(4)
    c[0].metric("Users modelled", f"{meta['n_users']:,}")
    c[1].metric("Venues", f"{meta['n_items']:,}")
    c[2].metric("User–venue pairs", f"{meta['n_interactions']:,}")
    best = met.set_index("method")
    if {"GeoPulse Hybrid", "ALS (implicit MF)"} <= set(best.index):
        lift = best.loc["GeoPulse Hybrid", "ndcg"] / best.loc["ALS (implicit MF)", "ndcg"] - 1
        c[3].metric("NDCG@10 gain over plain ALS", f"{lift:+.1%}")

    order = met.sort_values("ndcg").method
    fig = go.Figure()
    for i, (col, name) in enumerate([("ndcg", "NDCG@10"), ("recall", "Recall@10"), ("precision", "Precision@10")]):
        d = met.set_index("method").loc[order]
        fig.add_trace(go.Bar(y=d.index, x=d[col], name=name, orientation="h", marker_color=SERIES[i],
                             hovertemplate="%{y}: %{x:.4f}<extra>" + name + "</extra>"))
    fig.update_layout(barmode="group", height=120 + 70 * len(met), title="Top-10 ranking quality on the test split",
                      legend=dict(orientation="h", y=-0.12), bargap=0.25, bargroupgap=0.08)
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(met[["method", "precision", "recall", "ndcg", "hit_rate", "coverage", "users"]],
                 hide_index=True, use_container_width=True,
                 column_config={m: st.column_config.NumberColumn(format="%.4f")
                                for m in ["precision", "recall", "ndcg", "hit_rate", "coverage"]})
    st.caption("hit_rate = share of users with at least one hit in their top 10 · coverage = share of the catalogue "
               "that appears in anyone's top 10. 'Hybrid without X' rows are ablations: the tuned model with one "
               "signal removed.")
    st.markdown(f"**Tuned configuration:** weights `{meta['weights']}`, σ = {meta['sigma_km']} km, "
                f"ALS `{meta['als_params']}`")
    tun = cached_query("SELECT stage, config, ndcg, recall FROM rec_tuning WHERE dataset = :d ORDER BY ndcg DESC", d=ds)
    with st.expander("Validation grid search"):
        a, b = st.columns(2)
        a.markdown("**ALS hyper-parameters**")
        a.dataframe(tun[tun.stage == "als"].drop(columns="stage"), hide_index=True, use_container_width=True)
        b.markdown("**Hybrid weights (top 15)**")
        b.dataframe(tun[tun.stage == "weights"].drop(columns="stage").head(15), hide_index=True, use_container_width=True)
