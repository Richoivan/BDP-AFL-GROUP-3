"""
Live dashboard for the RetailRocket Big Data pipeline.

Reads two folders that are written by the Spark jobs:

    /results/stream/events_per_minute/   <- streaming metric  (live)
    /results/batch/top_items/            <- batch insight     (top viewed)
    /results/batch/event_distribution/   <- batch insight
    /results/batch/daily_visitors/       <- batch insight

Auto-refreshes every few seconds to show the live metric.
"""

import glob
import os
import time
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

RESULTS_DIR = os.getenv("RESULTS_DIR", "/results")
STREAM_DIR = os.path.join(RESULTS_DIR, "stream", "events_per_minute")
BATCH_DIR = os.path.join(RESULTS_DIR, "batch")
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "5"))


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def read_spark_csv(folder: str) -> pd.DataFrame:
    """Spark writes csv as a folder of part-*.csv files. Concatenate them."""
    if not os.path.isdir(folder):
        return pd.DataFrame()
    files = sorted(glob.glob(os.path.join(folder, "part-*.csv")))
    if not files:
        return pd.DataFrame()
    frames = []
    for f in files:
        try:
            frames.append(pd.read_csv(f))
        except Exception:  # noqa: BLE001
            continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_stream_metric() -> pd.DataFrame:
    df = read_spark_csv(STREAM_DIR)
    if df.empty:
        return df
    if "window_start" in df.columns:
        df["window_start"] = pd.to_datetime(df["window_start"], errors="coerce")
        df = df.sort_values("window_start")
    return df


def load_batch_results():
    top_items = read_spark_csv(os.path.join(BATCH_DIR, "top_items"))
    event_dist = read_spark_csv(os.path.join(BATCH_DIR, "event_distribution"))
    daily_visitors = read_spark_csv(os.path.join(BATCH_DIR, "daily_visitors"))
    if not daily_visitors.empty and "event_date" in daily_visitors.columns:
        daily_visitors["event_date"] = pd.to_datetime(
            daily_visitors["event_date"], errors="coerce"
        )
        daily_visitors = daily_visitors.sort_values("event_date")
    return top_items, event_dist, daily_visitors


# ----------------------------------------------------------------------
# Page
# ----------------------------------------------------------------------
st.set_page_config(
    page_title="RetailRocket Big Data Pipeline",
    page_icon=":bar_chart:",
    layout="wide",
)

st.title("RetailRocket - Big Data Pipeline Dashboard")
st.caption(
    "Kafka -> Spark Structured Streaming -> MinIO -> Streamlit | "
    "Batch insights + live real-time metrics"
)

# Sidebar controls
st.sidebar.header("Settings")
auto_refresh = st.sidebar.checkbox("Auto refresh", value=True)
refresh_every = st.sidebar.slider(
    "Refresh interval (seconds)", 2, 30, REFRESH_SECONDS
)
st.sidebar.write(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")

# ----------------------------------------------------------------------
# Real-time section
# ----------------------------------------------------------------------
st.header("Real-time metric: events per minute")

stream_df = load_stream_metric()

if stream_df.empty:
    st.warning(
        "Waiting for the streaming job to produce results... "
        "(no data yet at " + STREAM_DIR + ")"
    )
else:
    # KPI row
    latest_window = stream_df["window_start"].max()
    last_minute = stream_df[stream_df["window_start"] == latest_window]
    total_last_min = int(last_minute["event_count"].sum()) if not last_minute.empty else 0
    total_seen = int(stream_df["event_count"].sum())
    unique_event_types = stream_df["event"].nunique() if "event" in stream_df.columns else 0

    col_a, col_b, col_c = st.columns(3)
    col_a.metric("Events in latest minute", f"{total_last_min:,}")
    col_b.metric("Total events processed (live)", f"{total_seen:,}")
    col_c.metric("Distinct event types", f"{unique_event_types}")

    # Chart: events per minute by type
    chart_df = (
        stream_df.pivot_table(
            index="window_start",
            columns="event",
            values="event_count",
            aggfunc="sum",
            fill_value=0,
        )
        .sort_index()
        .tail(60)  # last hour
    )
    st.line_chart(chart_df, height=320)

    with st.expander("Latest minute breakdown"):
        st.dataframe(
            last_minute[["window_start", "window_end", "event", "event_count"]]
            .sort_values("event_count", ascending=False)
            .reset_index(drop=True)
        )

# ----------------------------------------------------------------------
# Batch section
# ----------------------------------------------------------------------
st.header("Batch insights")

top_items, event_dist, daily_visitors = load_batch_results()

if top_items.empty and event_dist.empty and daily_visitors.empty:
    st.info(
        "No batch results yet. Run the batch job:\n\n"
        "`docker compose run --rm spark-master spark-submit "
        "--master spark://spark-master:7077 /opt/jobs/batch_analysis.py`"
    )
else:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 20 most viewed items")
        if not top_items.empty:
            top_items_sorted = top_items.sort_values("view_count", ascending=False).head(20)
            st.bar_chart(
                top_items_sorted.set_index("itemid")["view_count"], height=360
            )
            st.dataframe(top_items_sorted.reset_index(drop=True))
        else:
            st.write("(not available)")

    with col2:
        st.subheader("Event type distribution")
        if not event_dist.empty:
            st.bar_chart(event_dist.set_index("event")["count"], height=360)
            st.dataframe(event_dist.reset_index(drop=True))
        else:
            st.write("(not available)")

    # ------------------------------------------------------------------
    # Conversion funnel (view -> addtocart -> transaction)
    # ------------------------------------------------------------------
    st.subheader("Conversion Funnel")
    if not event_dist.empty and "event" in event_dist.columns:
        funnel_order = ["view", "addtocart", "transaction"]
        funnel_labels = {"view": "View", "addtocart": "Add to Cart", "transaction": "Transaction"}

        counts = {}
        for evt in funnel_order:
            row = event_dist[event_dist["event"] == evt]
            counts[evt] = int(row["count"].values[0]) if not row.empty else 0

        view_cnt = counts.get("view", 0)
        addtocart_cnt = counts.get("addtocart", 0)
        transaction_cnt = counts.get("transaction", 0)

        view_to_addtocart = (addtocart_cnt / view_cnt * 100) if view_cnt > 0 else 0.0
        addtocart_to_transaction = (transaction_cnt / addtocart_cnt * 100) if addtocart_cnt > 0 else 0.0
        overall_conversion = (transaction_cnt / view_cnt * 100) if view_cnt > 0 else 0.0

        m1, m2, m3 = st.columns(3)
        m1.metric("View → Add to Cart", f"{view_to_addtocart:.2f}%")
        m2.metric("Add to Cart → Transaction", f"{addtocart_to_transaction:.2f}%")
        m3.metric("Overall Conversion (View → Transaction)", f"{overall_conversion:.2f}%")

        fig = go.Figure(
            go.Funnel(
                y=[funnel_labels[e] for e in funnel_order],
                x=[counts[e] for e in funnel_order],
                textinfo="value+percent initial",
                marker={"color": ["#4C78A8", "#F58518", "#54A24B"]},
            )
        )
        fig.update_layout(title="User Journey Funnel", height=420, margin={"t": 60})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("(not available)")

    st.subheader("Daily active visitors")
    if not daily_visitors.empty:
        st.line_chart(
            daily_visitors.set_index("event_date")["active_visitors"], height=300
        )
    else:
        st.write("(not available)")

# ----------------------------------------------------------------------
# Auto refresh
# ----------------------------------------------------------------------
if auto_refresh:
    time.sleep(refresh_every)
    st.rerun()
