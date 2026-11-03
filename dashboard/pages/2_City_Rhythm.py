"""City Rhythm - spatiotemporal analytics for the Foursquare cities."""
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

from common.config import TIME_SLOTS
from dashboard.lib import (AXIS, BLUE_RAMP, DATASET_COLOR, DATASET_LABEL, INK_2, MUTED, ORANGE_RAMP, SERIES,
                           cached_query, deck, hex_layer, hex_to_rgb, page_setup, require_tables, view)

page_setup("City Rhythm", "🏙️")
st.title("🏙️ City Rhythm")
st.caption("Spatiotemporal analytics from the batch layer (PySpark over Hive tables): when and where each city is "
           "active, where the statistically significant hotspots are, and how people move between places.")
require_tables("st_hex_hourly", "st_hotspots", "st_od_flows", "st_category_rhythm", "st_daily")

dataset = st.radio("City", ["nyc", "tky"], format_func=lambda d: DATASET_LABEL[d], horizontal=True)
DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

tabs = st.tabs(["⏱ Hourly heartbeat", "🔥 Hotspots (Gi*)", "🔀 Mobility flows", "📊 Category rhythm",
                "📅 Timeline", "🧭 Movement range"])

# ------------------------------------------------------------------ hourly heartbeat
with tabs[0]:
    hourly = cached_query("""SELECT h.h3, h.hour, h.n, c.place, c.top_category
                             FROM st_hex_hourly h JOIN st_cells c ON c.dataset = h.dataset AND c.h3 = h.h3
                             WHERE h.dataset = :d""", d=dataset)
    totals = hourly.groupby("hour")["n"].sum().reindex(range(24), fill_value=0)
    hour = st.slider("Local hour of day", 0, 23, 19, format="%d:00")
    c1, c2 = st.columns([3, 1])
    with c1:
        h = hourly[hourly.hour == hour].copy()
        # Colour scale fixed across hours so the slider shows real changes in intensity.
        vmax = np.log1p(hourly.n.max())
        idx = np.clip((np.log1p(h.n) / vmax * (len(BLUE_RAMP) - 1)).round().astype(int), 0, len(BLUE_RAMP) - 1)
        h["color"] = [hex_to_rgb(BLUE_RAMP[i]) + [215] for i in idx]
        h["tip"] = (h.n.astype(str) + " check-ins at " + f"{hour}:00 · " + h.place.fillna("")
                    + " · mostly " + h.top_category.fillna("–"))
        deck([hex_layer(h, "hourly", extruded=True, elevation_scale=6)], view(dataset, 0.3, pitch=45),
             tooltip={"text": "{tip}"}, height=520)
    with c2:
        fig = go.Figure(go.Bar(x=totals.values, y=[f"{i:02d}:00" for i in totals.index], orientation="h",
                               marker_color=[SERIES[1] if i == hour else AXIS for i in totals.index],
                               hovertemplate="%{y}: %{x:,} check-ins<extra></extra>"))
        fig.update_layout(height=520, yaxis=dict(autorange="reversed", tickfont=dict(size=10)),
                          title="Check-ins by hour (all days)", margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    top_now = h.nlargest(5, "n")[["place", "top_category", "n"]].rename(
        columns={"place": "Area", "top_category": "Dominant category", "n": "Check-ins"})
    st.markdown(f"**Busiest cells at {hour}:00**")
    st.dataframe(top_now, hide_index=True, use_container_width=True)

# ------------------------------------------------------------------ hotspots
with tabs[1]:
    st.markdown("**Getis-Ord Gi\\*** compares each H3 cell *plus its six neighbours* with the whole city. "
                "A high z-score means a cluster of high activity that is very unlikely to be random, "
                "unlike a single busy venue. Computed separately for each time of day.")
    slot = st.radio("Time of day", list(TIME_SLOTS), horizontal=True, index=3,
                    format_func=lambda s: f"{s.title()} ({TIME_SLOTS[s].start:02d}–{TIME_SLOTS[s].stop:02d}h)")
    gi = cached_query("""SELECT g.*, c.place, c.top_category FROM st_hotspots g
                         JOIN st_cells c ON c.dataset = g.dataset AND c.h3 = g.h3
                         WHERE g.dataset = :d""", d=dataset)
    g = gi[gi.slot == slot].copy()
    level = {"Hot spot (99%)": 6, "Hot spot (95%)": 4, "Hot spot (90%)": 2}
    g["color"] = [hex_to_rgb(ORANGE_RAMP[level[c]]) + [225] if c in level else [137, 135, 129, 40] for c in g.cls]
    g["tip"] = (g.cls + " · z = " + g.gi_z.round(2).astype(str) + " · " + g.n.astype(str) + " check-ins · "
                + g.place.fillna("") + " · " + g.top_category.fillna(""))
    c1, c2 = st.columns([3, 1])
    with c1:
        deck([hex_layer(g, "gi", pickable=True)], view(dataset, 0.4), tooltip={"text": "{tip}"}, height=520)
        st.caption("Orange = hot spot (darker → lower confidence: 99 % / 95 % / 90 %); grey = not significant.")
    with c2:
        counts = gi[gi.cls.str.startswith("Hot")].groupby("slot").size().reindex(list(TIME_SLOTS), fill_value=0)
        st.metric(f"Hot-spot cells ({slot})", int(counts[slot]))
        hot = g[g.cls.str.startswith("Hot")].copy()
        st.markdown("**Strongest hotspots**")
        st.dataframe(hot.nlargest(10, "gi_z")[["place", "gi_z", "top_category"]]
                     .rename(columns={"place": "Area", "gi_z": "z", "top_category": "Category"}),
                     hide_index=True, use_container_width=True,
                     column_config={"z": st.column_config.NumberColumn(format="%.1f")})
    # How hotspots move during the day: which areas are hot in only one slot?
    piv = (gi[gi.cls.str.startswith("Hot")].assign(place=lambda x: x.place.fillna("(unnamed area)"))
           .groupby(["place", "slot"]).gi_z.max().unstack().reindex(columns=list(TIME_SLOTS)))
    piv = piv.loc[piv.max(axis=1).sort_values(ascending=False).index[:18]]
    fig = go.Figure(go.Heatmap(z=piv.values, x=[s.title() for s in piv.columns], y=piv.index,
                               colorscale=[[0, ORANGE_RAMP[0]], [1, ORANGE_RAMP[-1]]], xgap=2, ygap=2,
                               colorbar=dict(title="max z"), hovertemplate="%{y} · %{x}: z=%{z:.1f}<extra></extra>"))
    fig.update_layout(title="Hotspot migration: peak Gi* z-score per area and time of day (blank = not hot)",
                      height=520, yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------------ flows
with tabs[2]:
    flows = cached_query("SELECT * FROM st_od_flows WHERE dataset = :d ORDER BY n DESC", d=dataset)
    c0, c1 = st.columns([1, 3])
    n_top = c0.slider("Top corridors", 20, 400, 150, step=10)
    min_km = c0.slider("Minimum trip length (km)", 0.0, 10.0, 2.0, step=0.5)
    f = flows[flows.km >= min_km].head(n_top).copy()
    w = np.log1p(f.n)
    f["width"] = 1 + 6 * (w - w.min()) / max(w.max() - w.min(), 1e-9)
    f["tip"] = (f.o_place.fillna("?") + " → " + f.d_place.fillna("?") + ": " + f.n.astype(str) + " trips by "
                + f.users.astype(str) + " users (" + f.km.round(1).astype(str) + " km)")
    with c1:
        arc = pdk.Layer("ArcLayer", f, id="flows", get_source_position=["o_lon", "o_lat"],
                        get_target_position=["d_lon", "d_lat"], get_source_color=hex_to_rgb(SERIES[0]) + [200],
                        get_target_color=hex_to_rgb(SERIES[1]) + [220], get_width="width", get_height=0.4,
                        pickable=True, auto_highlight=True)
        deck([arc], view(dataset, 0.2, pitch=40), tooltip={"text": "{tip}"}, height=520)
        st.caption("Consecutive check-ins by the same user within 4 hours, between H3 r7 cells (~5 km²). "
                   "Arc colour runs from origin (blue) to destination (orange).")
    with c0:
        st.markdown("**Top corridors**")
        st.dataframe(f.head(10).assign(route=f.o_place.fillna("?") + " → " + f.d_place.fillna("?"))[["route", "n"]],
                     hide_index=True, use_container_width=True)

    trans = cached_query("SELECT * FROM st_transitions WHERE dataset = :d", d=dataset)
    m = trans.pivot(index="from_cat", columns="to_cat", values="prob").fillna(0)
    order = m.sum(axis=0).sort_values(ascending=False).index
    m = m.reindex(index=order, columns=order).fillna(0)
    fig = go.Figure(go.Heatmap(z=m.values, x=m.columns, y=m.index, xgap=2, ygap=2,
                               colorscale=[[i / (len(BLUE_RAMP) - 1), c] for i, c in enumerate(BLUE_RAMP)],
                               colorbar=dict(title="P(next)"),
                               hovertemplate="after %{y} → %{x}: %{z:.0%}<extra></extra>"))
    fig.update_layout(title="What do people do next? Category transition probabilities (Markov chain)",
                      height=560, xaxis=dict(tickangle=-40), yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------------ category rhythm
with tabs[3]:
    rh = cached_query("SELECT * FROM st_category_rhythm WHERE dataset = :d", d=dataset)
    by_hour = rh.groupby(["category", "hour"]).n.sum().unstack(fill_value=0).reindex(columns=range(24), fill_value=0)
    share = by_hour.div(by_hour.sum(axis=1), axis=0)
    share = share.iloc[np.argsort(share.values.argmax(axis=1), kind="stable")]   # order by peak hour
    fig = go.Figure(go.Heatmap(z=share.values, x=[f"{h:02d}" for h in share.columns], y=share.index, xgap=1, ygap=1,
                               colorscale=[[i / (len(BLUE_RAMP) - 1), c] for i, c in enumerate(BLUE_RAMP)],
                               colorbar=dict(title="share of<br>category", tickformat=".0%"),
                               hovertemplate="%{y} at %{x}:00 · %{z:.1%} of its check-ins<extra></extra>"))
    fig.update_layout(title="Daily rhythm of the 30 most popular categories (sorted by peak hour)",
                      height=760, yaxis=dict(autorange="reversed"), xaxis_title="Local hour")
    st.plotly_chart(fig, use_container_width=True)

    hd = cached_query("SELECT dow, hour, n FROM st_hour_dow WHERE dataset = :d", d=dataset)
    grid = hd.pivot(index="dow", columns="hour", values="n").reindex(index=range(7), columns=range(24)).fillna(0)
    fig = go.Figure(go.Heatmap(z=grid.values, x=[f"{h:02d}" for h in grid.columns], y=DAYS, xgap=2, ygap=2,
                               colorscale=[[i / (len(BLUE_RAMP) - 1), c] for i, c in enumerate(BLUE_RAMP)],
                               colorbar=dict(title="check-ins"),
                               hovertemplate="%{y} %{x}:00 · %{z:,} check-ins<extra></extra>"))
    fig.update_layout(title="Week heartbeat: check-ins by weekday and hour", height=330,
                      yaxis=dict(autorange="reversed"), xaxis_title="Local hour")
    st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------------ timeline
with tabs[4]:
    daily = cached_query("SELECT dataset, day, n, users FROM st_daily WHERE dataset IN ('nyc','tky') ORDER BY day")
    daily["day"] = pd.to_datetime(daily["day"])
    fig = go.Figure()
    for ds in ["nyc", "tky"]:
        d = daily[daily.dataset == ds].set_index("day").n
        d = d.reindex(pd.date_range(d.index.min(), d.index.max(), freq="D"))   # gaps in the data stay gaps
        fig.add_trace(go.Scatter(x=d.index, y=d.rolling(7, center=True, min_periods=4).mean(), mode="lines",
                                 name=DATASET_LABEL[ds] + " (7-day avg)", line=dict(color=DATASET_COLOR[ds], width=2)))
        fig.add_trace(go.Scatter(x=d.index, y=d.values, mode="lines", name=DATASET_LABEL[ds] + " (daily)",
                                 line=dict(color=DATASET_COLOR[ds], width=1), opacity=0.35, showlegend=False,
                                 hovertemplate="%{x|%a %d %b %Y}: %{y:,}<extra>" + ds.upper() + "</extra>"))
    events = [("2012-10-29", "Hurricane Sandy hits NYC"), ("2012-11-22", "Thanksgiving"),
              ("2012-12-25", "Christmas"), ("2013-01-01", "New Year")]
    for day, label in events:
        fig.add_vline(x=pd.Timestamp(day).timestamp() * 1000, line=dict(color=MUTED, dash="dot", width=1))
        fig.add_annotation(x=day, y=1, yref="paper", text=label, showarrow=False, textangle=-90,
                           xanchor="right", yanchor="top", font=dict(color=INK_2, size=11))
    fig.update_layout(title="Daily check-ins (dotted lines: notable dates; breaks = days missing from the dataset)", height=420, hovermode="x unified",
                      legend=dict(orientation="h", y=-0.12))
    st.plotly_chart(fig, use_container_width=True)
    nyc = daily[daily.dataset == "nyc"].set_index("day").n
    before = nyc["2012-10-15":"2012-10-27"].mean()
    during = nyc["2012-10-29":"2012-11-02"].mean()
    st.metric("NYC daily check-ins during Hurricane Sandy (29 Oct – 2 Nov) vs the two weeks before",
              f"{during:,.0f} / day", f"{(during / before - 1):+.0%}")

# ------------------------------------------------------------------ gyration
with tabs[5]:
    gy = cached_query("SELECT * FROM st_gyration WHERE dataset = :d ORDER BY bucket_order", d=dataset)
    fig = go.Figure(go.Bar(x=gy.bucket, y=gy.users, marker_color=SERIES[0],
                           hovertemplate="%{x}: %{y:,} users<extra></extra>"))
    fig.update_layout(title="Radius of gyration: how far users typically range from their centre of activity",
                      height=380, xaxis_title=None, yaxis_title="users")
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Radius of gyration = root-mean-square distance of a user's check-ins from their own centre of mass "
               "(users with ≥ 10 check-ins).")
