"""Hive Explorer - run the project's HiveQL against HiveServer2 (Spark Thrift Server)."""
import os
import re
import time

import pandas as pd
import streamlit as st

from dashboard.lib import page_setup

page_setup("Hive Explorer", "🐝")
st.title("🐝 Hive Explorer")
st.caption("Queries from `hive/analytics.hql` run live over JDBC/Thrift against **HiveServer2** "
           "(Spark Thrift Server) on the `geopulse` Hive database: external Parquet tables over the data lake, "
           "registered in a Postgres-backed Hive metastore.")

HQL = os.path.join(os.path.dirname(__file__), "..", "..", "hive", "analytics.hql")


@st.cache_data
def load_queries(path: str) -> dict[str, tuple[str, str]]:
    text = open(path).read()
    out = {}
    for block in re.split(r"(?m)^-- name:\s*", text)[1:]:
        name, _, rest = block.partition("\n")
        desc = ""
        m = re.match(r"-- desc:\s*(.*)\n", rest)
        if m:
            desc, rest = m.group(1), rest[m.end():]
        out[name.strip()] = (desc.strip(), rest.strip().rstrip(";"))
    return out


def connect():
    from pyhive import hive
    return hive.connect(host=os.environ.get("HIVE_HOST", "spark-thrift"), port=int(os.environ.get("HIVE_PORT", 10000)),
                        username="geopulse", database="geopulse")


def run(sql: str) -> tuple[pd.DataFrame, float]:
    t0 = time.time()
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [c[0].split(".")[-1] for c in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()
    return df, time.time() - t0


queries = load_queries(HQL)
tab_saved, tab_free, tab_schema = st.tabs(["📚 Project queries", "✍️ Ad-hoc HiveQL", "🗂 Schema"])

with tab_saved:
    name = st.selectbox("Query", list(queries))
    desc, sql = queries[name]
    st.markdown(f"_{desc}_")
    st.code(sql, language="sql")
    if st.button("▶ Run on HiveServer2", type="primary"):
        with st.spinner("Running on Spark via HiveServer2…"):
            try:
                df, secs = run(sql)
                st.success(f"{len(df):,} rows in {secs:.1f}s")
                st.dataframe(df, hide_index=True, use_container_width=True)
            except Exception as e:  # noqa: BLE001 - show any driver error to the user
                st.error(f"Query failed: {e}")

with tab_free:
    sql = st.text_area("HiveQL (read-only: SELECT / SHOW / DESCRIBE)",
                       "SELECT dataset, hour, COUNT(*) AS checkins\nFROM checkins\nGROUP BY dataset, hour\n"
                       "ORDER BY dataset, hour", height=180)
    if st.button("▶ Run", key="free"):
        if not re.match(r"(?is)^\s*(select|show|describe|desc|with|explain)\b", sql):
            st.error("Only read-only statements are allowed here.")
        else:
            try:
                df, secs = run(sql.strip().rstrip(";"))
                st.success(f"{len(df):,} rows in {secs:.1f}s")
                st.dataframe(df.head(5000), hide_index=True, use_container_width=True)
            except Exception as e:  # noqa: BLE001
                st.error(f"Query failed: {e}")

with tab_schema:
    if st.button("Load table list"):
        try:
            tables, _ = run("SHOW TABLES IN geopulse")
            st.dataframe(tables, hide_index=True, use_container_width=True)
            for t in tables.iloc[:, 1] if tables.shape[1] > 1 else tables.iloc[:, 0]:
                with st.expander(t):
                    d, _ = run(f"DESCRIBE geopulse.{t}")
                    st.dataframe(d, hide_index=True, use_container_width=True)
        except Exception as e:  # noqa: BLE001
            st.error(f"Could not reach HiveServer2: {e}")
