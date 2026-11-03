"""GeoPulse dashboard - landing page."""
import streamlit as st

from dashboard.lib import (DATASET_LABEL, cached_query, fmt_int, page_setup, query, table_exists)

page_setup("Overview")

st.title("GeoPulse")
st.markdown("#### Real-time check-in analytics, POI recommendation and spatiotemporal insights "
            "for location-based social networks")
st.caption("Foursquare NYC & Tokyo (TSMC2014) · SNAP Gowalla check-ins + friendship graph · "
           "Kafka → Spark Structured Streaming · PySpark + Hive · Spark MLlib ALS")

if table_exists("st_overview"):
    ov = cached_query("SELECT * FROM st_overview ORDER BY checkins DESC")
    graph = cached_query("SELECT * FROM st_graph") if table_exists("st_graph") else None
    c = st.columns(4)
    c[0].metric("Check-ins analysed", fmt_int(ov.checkins.sum()))
    c[1].metric("Users", fmt_int(ov.users.sum()))
    c[2].metric("Venues / locations", fmt_int(ov.venues.sum()))
    c[3].metric("Friendships (Gowalla)", fmt_int(graph.friendships[0]) if graph is not None else "–")
    ov = ov.assign(dataset=ov.dataset.map(DATASET_LABEL))
    st.dataframe(ov.rename(columns={"dataset": "Dataset", "checkins": "Check-ins", "users": "Users",
                                    "venues": "Venues", "first_day": "From", "last_day": "To"}),
                 hide_index=True, use_container_width=True)
else:
    st.info("Batch layer not run yet: `make batch` populates the data lake, Hive tables and serving DB.")

live = query("SELECT count(*) AS batches, coalesce(sum(rows), 0) AS events, max(max_event_time) AS t "
             "FROM stream_metrics")
if live.batches[0]:
    st.success(f"Speed layer is live: {fmt_int(live.events[0])} streamed check-ins processed in "
               f"{fmt_int(live.batches[0])} micro-batches (simulated time {live.t[0]:%Y-%m-%d %H:%M} UTC). "
               "Open **Live Pulse**.")

st.subheader("Architecture")
st.graphviz_chart("""
digraph G {
  rankdir=LR; bgcolor="transparent"; nodesep=0.35; ranksep=0.5;
  node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11, color="#383835",
        fillcolor="#1a1a19", fontcolor="#f0efec"];
  edge [color="#898781", fontcolor="#c3c2b7", fontname="Helvetica", fontsize=9];

  raw   [label="Public datasets\\nFoursquare NYC/TKY\\nGowalla + friends", fillcolor="#262624"];
  prod  [label="Replay producer\\n(time-compressed)"];
  kafka [label="Apache Kafka\\ntopic: checkins", fillcolor="#104281"];
  sss   [label="Spark Structured\\nStreaming\\nsliding windows · H3\\nburst z-scores", fillcolor="#104281"];
  etl   [label="PySpark ETL\\nclean · local time · H3"];
  lake  [label="Parquet data lake\\n+ Hive metastore", fillcolor="#8a3517"];
  hive  [label="HiveQL\\n(HiveServer2 /\\nSpark Thrift)", fillcolor="#8a3517"];
  st    [label="Spatiotemporal jobs\\nGi* hotspots · OD flows\\nrhythms · social"];
  rec   [label="Hybrid recommender\\nALS + geo + time + social"];
  pg    [label="PostgreSQL\\nserving layer", fillcolor="#0f5c44"];
  dash  [label="Streamlit dashboard", fillcolor="#0f5c44"];

  raw -> prod -> kafka -> sss -> pg [label=""];
  raw -> etl -> lake;
  lake -> hive; lake -> st; lake -> rec;
  lake -> sss [label="baseline", style=dashed];
  st -> pg; rec -> pg; rec -> dash [label="model", style=dashed];
  pg -> dash; hive -> dash [style=dashed];
  { rank=same; prod; etl; }
}
""", use_container_width=True)

st.subheader("What each page shows")
c1, c2, c3 = st.columns(3)
with c1:
    st.markdown("**① Live Pulse**: streaming analytics\n\n"
                "Kafka-fed check-ins aggregated by Spark Structured Streaming in sliding event-time windows: "
                "trending categories and venues, a live hexagon map, and **burst detection** that flags "
                "cells whose activity is far above their usual level for that hour of the week.")
with c2:
    st.markdown("**② City Rhythm & ③ Global & Social**: spatiotemporal analytics\n\n"
                "Hour-by-hour hexagon maps, **Getis-Ord Gi\\*** hotspots per time of day, origin–destination "
                "flows, category transition chains, radius of gyration, Hurricane Sandy in the data, "
                "and how friendship relates to place on Gowalla.")
with c3:
    st.markdown("**④ Recommender**: context-aware POI recommendation\n\n"
                "Implicit-feedback ALS (Spark MLlib) re-ranked with distance, time-of-day and friends' "
                "visits. Pick a user, a location and an hour, and see why each venue is recommended. "
                "Offline evaluation against baselines included.")
st.markdown("**⑤ Hive Explorer** runs the project's HiveQL queries live against HiveServer2.")

st.divider()
st.caption("Service UIs: Spark master http://localhost:8080 · Streaming job http://localhost:4042 · "
           "Kafka UI http://localhost:8081 · HiveServer2 jdbc:hive2://localhost:10000")
