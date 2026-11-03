"""Global & Social - Gowalla: worldwide spatiotemporal patterns and the social graph."""
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

from dashboard.lib import (DATASET_COLOR, INK_2, SERIES, cached_query, deck, fmt_int, hex_layer, hex_to_rgb,
                           page_setup, ramp_colors, require_tables, view)

page_setup("Global & Social", "🌍")
st.title("🌍 Global & Social")
st.caption("Gowalla (2009–2010): 6.4 M check-ins by 107 k users worldwide plus their friendship graph, "
           "processed with PySpark. Hexagons are H3 resolution 5 (~250 km²).")
require_tables("st_hex_hourly", "st_region_monthly", "st_social", "st_od_flows")

tabs = st.tabs(["🗺 World activity", "✈️ Travel flows", "📈 Network growth", "🤝 Friends & places"])

with tabs[0]:
    hexes = cached_query("""SELECT h.h3, sum(h.n) AS n, max(c.place) AS place, max(c.n_users) AS users
                            FROM st_hex_hourly h JOIN st_cells c ON c.dataset = h.dataset AND c.h3 = h.h3
                            WHERE h.dataset = 'gowalla' GROUP BY h.h3""")
    hexes["color"] = ramp_colors(hexes["n"])
    hexes["elev"] = np.log1p(hexes["n"])
    hexes["tip"] = hexes.n.map("{:,} check-ins".format) + " by " + hexes.users.map("{:,} users".format)
    deck([hex_layer(hexes, "world", extruded=True, elevation_scale=12000, elevation="elev")],
         view("gowalla", 0.2, pitch=35),
         tooltip={"text": "{tip}"}, height=560)
    st.caption("Height and colour: total check-ins per cell (log scale). Gowalla started in Austin, Texas, "
               "and the US, Northern Europe and Sweden in particular dominate.")

with tabs[1]:
    flows = cached_query("SELECT * FROM st_od_flows WHERE dataset = 'gowalla' ORDER BY n DESC")
    c0, c1 = st.columns([1, 3])
    min_km = c0.slider("Minimum distance (km)", 0, 3000, 300, step=50)
    n_top = c0.slider("Top routes", 20, 600, 250, step=10)
    f = flows[flows.km >= min_km].head(n_top).copy()
    w = np.log1p(f.n)
    f["width"] = 1 + 5 * (w - w.min()) / max(w.max() - w.min(), 1e-9)
    f["tip"] = f.n.astype(str) + " trips by " + f.users.astype(str) + " users · " + f.km.round(0).astype(int).astype(str) + " km"
    with c1:
        deck([pdk.Layer("ArcLayer", f, get_source_position=["o_lon", "o_lat"], get_target_position=["d_lon", "d_lat"],
                        get_source_color=hex_to_rgb(SERIES[0]) + [200], get_target_color=hex_to_rgb(SERIES[1]) + [220],
                        get_width="width", pickable=True, great_circle=True)],
             view("gowalla", 0.2, pitch=30), tooltip={"text": "{tip}"}, height=540)
    with c0:
        st.metric("Routes shown", len(f))
        st.metric("Median route length", f"{f.km.median():,.0f} km" if len(f) else "–")
    st.caption("Consecutive check-ins by the same user within 7 days that change H3 r5 cell, i.e. trips between "
               "cities. Great-circle arcs from origin (blue) to destination (orange).")

with tabs[2]:
    reg = cached_query("SELECT * FROM st_region_monthly WHERE month >= '2009-06' ORDER BY month")
    reg["region"] = reg.region.where(reg.region.isin(["North America", "Europe", "Asia", "Oceania", "South America"]),
                                     "Other")
    reg = reg.groupby(["region", "month"], as_index=False)[["checkins", "users"]].sum()
    metric = st.radio("Measure", ["checkins", "users"], horizontal=True,
                      format_func=lambda m: {"checkins": "Check-ins", "users": "Active users"}[m])
    order = reg.groupby("region")[metric].sum().sort_values(ascending=False).index
    fig = go.Figure()
    for i, r in enumerate(order):
        d = reg[reg.region == r]
        fig.add_trace(go.Scatter(x=d.month, y=d[metric], name=r, mode="lines+markers",
                                 line=dict(color=SERIES[i], width=2), marker=dict(size=6)))
        fig.add_annotation(x=d.month.iloc[-1], y=d[metric].iloc[-1], text=r, showarrow=False, xanchor="left",
                           xshift=6, font=dict(color=INK_2, size=11))
    fig.update_layout(title=f"Monthly {'check-ins' if metric == 'checkins' else 'active users'} by region "
                            "(log scale)", yaxis_type="log", height=460, hovermode="x unified",
                      legend=dict(orientation="h", y=-0.15), margin=dict(r=110))
    st.plotly_chart(fig, use_container_width=True)
    gy = cached_query("SELECT * FROM st_gyration WHERE dataset = 'gowalla' ORDER BY bucket_order")
    fig = go.Figure(go.Bar(x=gy.bucket, y=gy.users, marker_color=DATASET_COLOR["gowalla"],
                           hovertemplate="%{x}: %{y:,} users<extra></extra>"))
    fig.update_layout(title="Radius of gyration of Gowalla users (≥ 10 check-ins)", height=340)
    st.plotly_chart(fig, use_container_width=True)

with tabs[3]:
    so = cached_query("SELECT * FROM st_social").set_index("pair_type")
    fr, rd = so.loc["Friends"], so.loc["Random pairs"]
    st.markdown("#### Do friends go to the same places?")
    st.markdown("Spark joins a sample of friend pairs from the 950 k-edge social graph with each user's set of "
                "visited locations, and compares them with the same number of random user pairs.")
    c = st.columns(4)
    c[0].metric("Friends sharing ≥ 1 place", f"{fr.share_with_common_place:.1%}")
    c[1].metric("Random pairs sharing ≥ 1 place", f"{rd.share_with_common_place:.1%}")
    c[2].metric("Lift", f"{fr.share_with_common_place / rd.share_with_common_place:.0f}×")
    c[3].metric("Median distance between friends' home areas", f"{fr.median_home_distance_km:,.0f} km",
                f"vs {rd.median_home_distance_km:,.0f} km for random pairs", delta_color="off")
    comp = pd.DataFrame({
        "measure": ["Share with a common place", "Share living within 100 km"],
        "Friends": [fr.share_with_common_place, fr.share_within_100km],
        "Random pairs": [rd.share_with_common_place, rd.share_within_100km],
    })
    fig = go.Figure()
    for i, col in enumerate(["Friends", "Random pairs"]):
        fig.add_trace(go.Bar(x=comp.measure, y=comp[col], name=col, marker_color=SERIES[i],
                             text=[f"{v:.1%}" for v in comp[col]], textposition="outside",
                             textfont=dict(color=INK_2), hovertemplate="%{x}: %{y:.1%}<extra>" + col + "</extra>"))
    fig.update_layout(barmode="group", height=380, title="Friends vs random pairs of users",
                      legend=dict(orientation="h", y=-0.15), yaxis=dict(tickformat=".0%"))
    st.plotly_chart(fig, use_container_width=True)
    st.caption(f"Mean Jaccard overlap of visited places: friends {fr.mean_jaccard:.4f} vs random pairs "
               f"{rd.mean_jaccard:.5f} ({fr.mean_jaccard / rd.mean_jaccard:.0f}× higher).")
    st.info("This **social influence** is why the recommender adds a *friends visited* signal on Gowalla. "
            "The Recommender page's evaluation shows how much it adds (ablation 'Hybrid without social').")
    st.caption(f"{fmt_int(fr.pairs)} friend pairs and {fmt_int(rd.pairs)} random pairs. A user's home area is "
               "the centre of mass of their check-ins.")
