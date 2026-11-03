"""Shared helpers for the GeoPulse Streamlit dashboard: DB access, theme, map layers."""
from __future__ import annotations

import os

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import pydeck as pdk
import streamlit as st
from sqlalchemy import create_engine, text

from common import config

# ---------------------------------------------------------------- palette (dark mode, validated)
SURFACE = "#1a1a19"
PAGE = "#0d0d0d"
INK = "#f0efec"
INK_2 = "#c3c2b7"
MUTED = "#898781"
GRID = "#2c2c2a"
AXIS = "#383835"
SERIES = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300"]   # fixed order
DATASET_COLOR = {"nyc": SERIES[0], "tky": SERIES[1], "gowalla": SERIES[2]}
CRITICAL = "#d03b3b"
GOOD = "#0ca30c"
# Sequential blue ramp (steps 700 -> 100); on the dark map low values recede into the surface.
BLUE_RAMP = ["#0d366b", "#104281", "#184f95", "#1c5cab", "#256abf", "#2a78d6", "#3987e5",
             "#5598e7", "#6da7ec", "#86b6ef", "#9ec5f4", "#b7d3f6", "#cde2fb"]
ORANGE_RAMP = ["#5a2410", "#8a3517", "#b9461d", "#d95926", "#ec835a", "#f5a98a", "#fbd3c2"]

DATASET_LABEL = {k: v["label"] for k, v in config.DATASETS.items()}

pio.templates["geopulse"] = go.layout.Template(layout=go.Layout(
    paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
    font=dict(family="system-ui, -apple-system, Segoe UI, sans-serif", color=INK_2, size=13),
    title=dict(font=dict(color=INK, size=15)),
    colorway=SERIES,
    xaxis=dict(gridcolor=GRID, linecolor=AXIS, zerolinecolor=AXIS, tickfont=dict(color=MUTED)),
    yaxis=dict(gridcolor=GRID, linecolor=AXIS, zerolinecolor=AXIS, tickfont=dict(color=MUTED)),
    legend=dict(font=dict(color=INK_2), bgcolor="rgba(0,0,0,0)"),
    hoverlabel=dict(bgcolor="#262624", bordercolor=AXIS, font=dict(color=INK)),
    margin=dict(l=10, r=10, t=40, b=10),
    bargap=0.15,
))
pio.templates.default = "geopulse"


def page_setup(title: str, icon: str = "🌐") -> None:
    st.set_page_config(page_title=f"GeoPulse · {title}", page_icon=icon, layout="wide")
    st.markdown(f"""<style>
      .block-container {{ padding-top: 2.2rem; }}
      [data-testid="stMetricValue"] {{ font-size: 1.7rem; }}
      .gp-card {{ background:{SURFACE}; border:1px solid rgba(255,255,255,.08); border-radius:10px;
                 padding:.8rem 1rem; margin-bottom:.6rem; }}
      .gp-alert {{ border-left:4px solid {CRITICAL}; }}
      .gp-muted {{ color:{MUTED}; font-size:.85rem; }}
    </style>""", unsafe_allow_html=True)


# ---------------------------------------------------------------- data access
@st.cache_resource
def engine():
    return create_engine(config.PG_DSN.replace("postgresql://", "postgresql+psycopg2://"), pool_pre_ping=True)


def query(sql: str, **params) -> pd.DataFrame:
    with engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


@st.cache_data(ttl=600, show_spinner=False)
def cached_query(sql: str, **params) -> pd.DataFrame:
    return query(sql, **params)


def table_exists(name: str) -> bool:
    return bool(cached_query("SELECT to_regclass(:t) IS NOT NULL AS ok", t=name).iloc[0, 0])


def require_tables(*names: str) -> None:
    missing = [n for n in names if not table_exists(n)]
    if missing:
        st.warning(f"Batch results not found ({', '.join(missing)}). Run `make batch` first.")
        st.stop()


# ---------------------------------------------------------------- colour helpers
def hex_to_rgb(h: str) -> list[int]:
    h = h.lstrip("#")
    return [int(h[i:i + 2], 16) for i in (0, 2, 4)]


def ramp_colors(values: pd.Series, ramp=BLUE_RAMP, log: bool = True, alpha: int = 210) -> list[list[int]]:
    """Map magnitudes onto a sequential ramp (log scale by default: check-ins are heavy-tailed)."""
    v = np.log1p(values.astype(float)) if log else values.astype(float)
    lo, hi = float(v.min()), float(v.max())
    t = (v - lo) / (hi - lo) if hi > lo else v * 0
    idx = np.clip((t * (len(ramp) - 1)).round().astype(int), 0, len(ramp) - 1)
    rgb = [hex_to_rgb(c) for c in ramp]
    return [rgb[i] + [alpha] for i in idx]


# ---------------------------------------------------------------- maps
def view(dataset: str, zoom_delta: float = 0.0, pitch: float = 0) -> pdk.ViewState:
    lat, lon = config.DATASETS[dataset]["center"]
    return pdk.ViewState(latitude=lat, longitude=lon, zoom=config.DATASETS[dataset]["zoom"] + zoom_delta,
                         pitch=pitch, bearing=0)


def deck(layers, view_state, tooltip=None, height=560) -> None:
    st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view_state, map_provider="carto",
                             map_style=pdk.map_styles.CARTO_DARK, tooltip=tooltip),
                    use_container_width=True, height=height)


def hex_layer(df: pd.DataFrame, id_: str, extruded: bool = False, elevation_scale: float = 4,
              pickable: bool = True, line: bool = False, elevation: str = "n") -> pdk.Layer:
    return pdk.Layer("H3HexagonLayer", df, id=id_, get_hexagon="h3", get_fill_color="color",
                     extruded=extruded, get_elevation=elevation if extruded else 0, elevation_scale=elevation_scale,
                     pickable=pickable, stroked=line, get_line_color=[255, 255, 255, 60], line_width_min_pixels=0.5,
                     opacity=0.85, coverage=0.92)


def fmt_int(n) -> str:
    return f"{int(n):,}" if n is not None and not pd.isna(n) else "–"


def dataset_picker(options=("nyc", "tky"), key="dataset", horizontal=True) -> str:
    return st.radio("Dataset", options, format_func=lambda d: DATASET_LABEL[d], horizontal=horizontal, key=key)


DATA_DIR = os.environ.get("DATA_DIR", config.DATA_DIR)
