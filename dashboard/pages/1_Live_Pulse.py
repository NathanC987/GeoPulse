"""Live Pulse - streaming analytics from Kafka via Spark Structured Streaming."""
from datetime import timedelta

import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

from common.geo import PLACES
from dashboard.lib import (DATASET_LABEL, INK_2, SERIES, deck, fmt_int,
                           hex_layer, page_setup, query, ramp_colors, view)

page_setup("Live Pulse", "⚡")
st.title("⚡ Live Pulse")
st.caption("Check-ins replayed into **Kafka** are processed by **Spark Structured Streaming** in event-time "
           "windows (6 h sliding windows for trends, 1 h tumbling windows per H3 cell for bursts). "
           "The page refreshes every 3 seconds.")

top = st.columns([2, 1, 1])
dataset = top[0].radio("City", ["nyc", "tky"], format_func=lambda d: DATASET_LABEL[d], horizontal=True)
map_hours = top[1].select_slider("Map window", options=[1, 3, 6, 12], value=3, format_func=lambda h: f"last {h} h")
show_events = top[2].toggle("Show latest events", value=True)

with st.expander("🧪 Simulate a flash crowd (burst-detection demo)"):
    st.write("Publishes ~80 synthetic check-ins around a landmark into Kafka at the current simulated time. "
             "Within a few seconds the streaming job should flag the cell as anomalous.")
    places = {name: (lat, lon) for name, lat, lon in PLACES[dataset]}
    default = "Penn Station / MSG" if dataset == "nyc" else "Shibuya"
    b1, b2, b3 = st.columns([2, 1, 1])
    place = b1.selectbox("Landmark", sorted(places), index=sorted(places).index(default))
    n_events = b2.number_input("Check-ins", 20, 400, 80, step=20)
    if b3.button("Inject burst", type="primary", use_container_width=True):
        from producer.burst import inject_burst
        lat, lon = places[place]
        inject_burst(dataset, lat, lon, int(n_events))
        st.toast(f"Injected {n_events} check-ins near {place}", icon="📣")


@st.fragment(run_every=3)
def live():
    m = query("""SELECT count(*) AS batches, coalesce(sum(rows),0) AS events, max(max_event_time) AS sim_utc,
                        coalesce(sum(rows) FILTER (WHERE processed_at > now() - interval '30 seconds'), 0) / 30.0 AS eps
                 FROM stream_metrics""").iloc[0]
    if not m.batches:
        st.info("No streaming data yet. Start it with `make stream` and `make replay`.")
        return
    # Current simulated local time: newest event seen, else the end of the newest hourly cell window.
    now = query("""SELECT coalesce((SELECT max(event_time) FROM stream_recent WHERE dataset = :d),
                                   (SELECT max(window_end) FROM stream_cells WHERE dataset = :d)) AS t""",
                d=dataset).t[0]
    alerts = query("SELECT * FROM stream_alerts WHERE dataset = :d ORDER BY window_start DESC, z DESC LIMIT 12",
                   d=dataset)

    k = st.columns(5)
    k[0].metric("Events processed", fmt_int(m.events))
    k[1].metric("Throughput", f"{m.eps:,.0f} ev/s")
    k[2].metric("Micro-batches", fmt_int(m.batches))
    k[3].metric(f"Simulated local time ({dataset.upper()})", f"{now:%a %d %b %Y, %H:%M}" if now is not None else "–")
    k[4].metric("Burst alerts", fmt_int(len(query("SELECT 1 FROM stream_alerts WHERE dataset=:d", d=dataset))))

    left, right = st.columns([3, 2])
    with left:
        cells = query("""SELECT h3, sum(n) AS n, max(z) AS z FROM stream_cells
                         WHERE dataset = :d AND window_start >= :since GROUP BY h3""",
                      d=dataset, since=(now - timedelta(hours=map_hours)) if now is not None else None)
        layers = []
        if not cells.empty:
            cells["color"] = ramp_colors(cells["n"])
            cells["tip"] = cells["n"].astype(int).astype(str) + " check-ins in the last " + str(map_hours) + " h"
            layers.append(hex_layer(cells, "live-cells", extruded=True, elevation_scale=25))
        recent_alerts = alerts[alerts.window_start >= now - timedelta(hours=map_hours)] if now is not None else alerts.iloc[:0]
        if not recent_alerts.empty:
            recent_alerts = recent_alerts.assign(
                tip="🚨 " + recent_alerts.place + ": " + recent_alerts.n.astype(str) + " check-ins")
            layers.append(pdk.Layer("ScatterplotLayer", recent_alerts, id="alerts", get_position=["lon", "lat"],
                                    get_radius=450, radius_min_pixels=10, stroked=True, filled=False,
                                    get_line_color=[208, 59, 59, 255], line_width_min_pixels=3, pickable=True))
        if show_events:
            ev = query("""SELECT lat, lon, category FROM stream_recent WHERE dataset = :d
                          ORDER BY inserted_at DESC, event_time DESC LIMIT 60""", d=dataset)
            ev["tip"] = ev["category"].fillna("check-in")
            layers.append(pdk.Layer("ScatterplotLayer", ev, id="events", get_position=["lon", "lat"],
                                    get_radius=60, radius_min_pixels=3, get_fill_color=[240, 239, 236, 230],
                                    pickable=True))
        deck(layers, view(dataset, 0.4, pitch=40),
             tooltip={"text": "{tip}"}, height=520)
        st.caption(f"Hexagons: H3 resolution 8 (~0.7 km²) cells, height and colour = check-ins in the last "
                   f"{map_hours} h of event time. Red rings = burst alerts. White dots = latest events.")

    with right:
        # With sliding windows the newest window has only just started; the one covering the
        # last 6 h is the first window that ends after the current event time.
        trend = query("""WITH w AS (SELECT min(window_end) AS e FROM stream_trending
                                    WHERE dataset=:d AND kind='category' AND window_end > :now)
                         SELECT t.label, t.n, coalesce(p.n, 0) AS prev
                         FROM stream_trending t CROSS JOIN w
                         LEFT JOIN stream_trending p ON p.dataset = t.dataset AND p.kind = t.kind AND p.key = t.key
                              AND p.window_end = w.e - interval '1 hour'
                         WHERE t.dataset = :d AND t.kind = 'category' AND t.window_end = w.e
                         ORDER BY t.n DESC LIMIT 12""", d=dataset, now=now)
        st.markdown("**Trending categories** · last 6 h · ▲ = up ≥ 50 % on the window one hour earlier")
        if not trend.empty:
            trend = trend.iloc[::-1]
            fig = go.Figure(go.Bar(x=trend.n, y=trend.label, orientation="h", marker_color=SERIES[0],
                                   marker_line_width=0,
                                   customdata=trend.prev, hovertemplate="%{y}: %{x} (1 h earlier: %{customdata})<extra></extra>"))
            rising = trend[(trend.n >= 3) & (trend.n >= 1.5 * trend.prev.clip(lower=1))]
            fig.add_trace(go.Scatter(x=rising.n, y=rising.label, mode="text", text=["  ▲ rising"] * len(rising),
                                     textposition="middle right", textfont=dict(color=INK_2, size=11),
                                     hoverinfo="skip", showlegend=False))
            fig.update_layout(height=360, showlegend=False, margin=dict(l=10, r=60, t=10, b=10),
                              xaxis_title=None, yaxis_title=None)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        venues = query("""SELECT label AS venue, n AS checkins FROM stream_trending
                          WHERE dataset=:d AND kind='venue'
                            AND window_end = (SELECT min(window_end) FROM stream_trending
                                              WHERE dataset=:d AND kind='venue' AND window_end > :now)
                          ORDER BY n DESC LIMIT 8""", d=dataset, now=now)
        st.markdown("**Hottest venues** · last 6 h")
        st.dataframe(venues, hide_index=True, use_container_width=True, height=250)

    a, b = st.columns([3, 2])
    with a:
        st.markdown("**🚨 Burst alerts** · cells far above their usual activity for this hour of the week")
        if alerts.empty:
            st.markdown('<div class="gp-muted">No anomalies detected yet.</div>', unsafe_allow_html=True)
        for r in alerts.head(5).itertuples():
            st.markdown(f"""<div class="gp-card gp-alert"><b>{r.place}</b> · {r.window_start:%a %d %b %H:%M}
                <span class="gp-muted">local</span><br>
                <b>{r.n}</b> check-ins vs usual <b>{r.baseline:.1f}</b> (z = {r.z:.1f}) ·
                top category: {r.top_category or '–'}</div>""", unsafe_allow_html=True)
    with b:
        tp = query("""SELECT date_trunc('second', processed_at) AS t, rows FROM stream_metrics
                      WHERE processed_at > now() - interval '3 minutes' ORDER BY processed_at""")
        st.markdown("**Throughput** · rows per micro-batch (last 3 min)")
        if not tp.empty:
            fig = go.Figure(go.Scatter(x=tp.t, y=tp.rows, mode="lines", line=dict(color=SERIES[2], width=2),
                                       hovertemplate="%{x|%H:%M:%S}: %{y} rows<extra></extra>"))
            fig.update_layout(height=220, xaxis_title=None, yaxis_title=None, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


live()
