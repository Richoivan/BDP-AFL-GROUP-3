"""
Live dashboard for the RetailRocket Big Data pipeline.

Reads:

    /results/stream/events_per_minute/   <- streaming metric  (live)
    /results/batch/top_items/            <- batch insight     (top viewed)
    /results/batch/event_distribution/   <- batch insight
    /results/batch/daily_visitors/       <- batch insight
    /data/events.csv                     <- raw RetailRocket dataset
                                            (used for the dataset-date indicator)

Auto-refreshes every few seconds to show the live metric.
"""

import glob
import json
import os
import time
from datetime import datetime

import pandas as pd
import streamlit as st
import plotly.express as px
import streamlit.components.v1 as components

RESULTS_DIR = os.getenv("RESULTS_DIR", "/results")
STREAM_DIR = os.path.join(RESULTS_DIR, "stream", "events_per_minute")
BATCH_DIR = os.path.join(RESULTS_DIR, "batch")
DATA_FILE = os.getenv("DATA_FILE", "/data/events.csv")
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


@st.cache_data(show_spinner=False)
def get_dataset_date_range(csv_path: str) -> dict:
    """
    Return min/max event timestamp from the raw RetailRocket dataset.

    Reads only the `timestamp` column (ms since epoch) so it is fast even
    on a ~90 MB file. Cached across reruns so this is computed once.
    """
    if not os.path.exists(csv_path):
        return {"min_ts": None, "max_ts": None, "n_events": 0, "source": "missing"}
    try:
        ts_series = pd.read_csv(
            csv_path,
            usecols=["timestamp"],
            dtype={"timestamp": "Int64"},
            engine="c",
        )["timestamp"].dropna()
        if ts_series.empty:
            return {"min_ts": None, "max_ts": None, "n_events": 0, "source": "empty"}
        min_ts = pd.to_datetime(int(ts_series.min()), unit="ms", utc=True)
        max_ts = pd.to_datetime(int(ts_series.max()), unit="ms", utc=True)
        return {
            "min_ts": min_ts,
            "max_ts": max_ts,
            "n_events": int(ts_series.shape[0]),
            "source": "csv",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "min_ts": None,
            "max_ts": None,
            "n_events": 0,
            "source": f"error: {exc}",
        }


def format_dataset_date(ts: pd.Timestamp) -> str:
    """Return e.g. 'Friday, 18 September 2015' from a pandas timestamp."""
    if ts is None or pd.isna(ts):
        return "-"
    return ts.strftime("%A, %d %B %Y")


def render_funnel_graph(
    view_count: int,
    addtocart_count: int,
    transaction_count: int,
    height: int = 460,
    key_suffix: str = "live",
) -> None:
    """
    Render a horizontal funnel using FunnelGraph.js (loaded from CDN).

    Produces the flowing-gradient style: View -> Add to Cart -> Transaction,
    with smooth color transitions and automatic percentage labels.
    """
    labels = ["View", "Add to Cart", "Transaction"]
    values = [int(view_count), int(addtocart_count), int(transaction_count)]

    # Single-channel funnel (no subLabels) -> use a flat array of colors that
    # blend into a smooth gradient flowing through the whole funnel.
    # Pink -> Orange -> Light Purple -> Cyan -> Blue
    colors = ["#FF6E7F", "#FFB178", "#A0BBFF", "#7795FF", "#56CCF2"]

    labels_json = json.dumps(labels)
    values_json = json.dumps(values)
    colors_json = json.dumps(colors)

    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <link rel="stylesheet"
        href="https://cdn.jsdelivr.net/npm/funnel-graph-js@1.4.2/dist/css/main.min.css">
    <link rel="stylesheet"
        href="https://cdn.jsdelivr.net/npm/funnel-graph-js@1.4.2/dist/css/theme.min.css">
    <style>
        html, body {{
            margin: 0;
            padding: 0;
            background: transparent;
            color: #FAFAFA;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI",
                Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            overflow: hidden;
        }}
        .funnel-wrapper {{
            padding: 16px;
            background: linear-gradient(135deg, #1f2233 0%, #2a2e44 100%);
            border-radius: 12px;
            margin: 4px;
        }}
        .svg-funnel-js {{
            font-family: inherit !important;
        }}
        .svg-funnel-js__labels .svg-funnel-js__label .label__title {{
            color: #FAFAFA !important;
            font-size: 14px !important;
            font-weight: 600 !important;
            opacity: 0.95;
            letter-spacing: 0.3px;
        }}
        .svg-funnel-js__labels .svg-funnel-js__label .label__value {{
            color: #FFFFFF !important;
            font-size: 28px !important;
            font-weight: 700 !important;
            margin-top: 2px;
        }}
        .svg-funnel-js__labels .svg-funnel-js__label .label__percentage {{
            color: #A78BFA !important;
            font-size: 14px !important;
            font-weight: 600 !important;
            margin-top: 2px;
        }}
        .svg-funnel-js .svg-funnel-js__container {{
            width: 100% !important;
        }}
    </style>
</head>
<body>
    <div class="funnel-wrapper">
        <div class="funnel funnel-{key_suffix}"></div>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/funnel-graph-js@1.4.2/dist/js/funnel-graph.min.js"></script>
    <script>
        (function() {{
            function drawFunnel() {{
                if (typeof FunnelGraph === 'undefined') {{
                    setTimeout(drawFunnel, 50);
                    return;
                }}
                var container = document.querySelector('.funnel-{key_suffix}');
                if (!container) return;
                container.innerHTML = '';
                var width = container.parentElement.clientWidth - 30;
                if (width < 400) width = 400;
                var graph = new FunnelGraph({{
                    container: '.funnel-{key_suffix}',
                    gradientDirection: 'horizontal',
                    data: {{
                        labels: {labels_json},
                        colors: {colors_json},
                        values: {values_json}
                    }},
                    displayPercent: true,
                    direction: 'horizontal',
                    width: width,
                    height: {height - 90}
                }});
                graph.draw();
            }}
            drawFunnel();
            var resizeTimer;
            window.addEventListener('resize', function() {{
                clearTimeout(resizeTimer);
                resizeTimer = setTimeout(drawFunnel, 150);
            }});
        }})();
    </script>
</body>
</html>
"""
    components.html(html, height=height, scrolling=False)


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
    "Kafka -> Spark Structured Streaming -> HDFS -> Streamlit | "
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
# Dataset date indicator - ALWAYS shown, independent of streaming state.
# Read from data/events.csv (mounted read-only into the dashboard container).
# ----------------------------------------------------------------------
date_range = get_dataset_date_range(DATA_FILE)
if date_range["min_ts"] is not None and date_range["max_ts"] is not None:
    min_str = format_dataset_date(date_range["min_ts"])
    max_str = format_dataset_date(date_range["max_ts"])
    n_events = date_range["n_events"]
    st.markdown(
        f"""
        <div style="
            background: rgba(76, 154, 255, 0.10);
            padding: 14px 18px;
            border-left: 4px solid #4C9AFF;
            border-radius: 4px;
            margin: 10px 0 18px 0;
            font-size: 15px;
            line-height: 1.6;
        ">
            <div style="font-size:17px;"><b>📅 Dataset coverage (RetailRocket events.csv)</b></div>
            <div><b>Earliest event:</b> {min_str}</div>
            <div><b>Latest event:</b> {max_str}</div>
            <div style="opacity:0.8;"><b>Total events in dataset:</b> {n_events:,}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        f"""
        <div style="
            background: rgba(255, 176, 32, 0.10);
            padding: 12px 16px;
            border-left: 4px solid #FFB020;
            border-radius: 4px;
            margin: 8px 0 18px 0;
            font-size: 14px;
        ">
            <b>📅 Dataset date:</b> {DATA_FILE} not accessible from dashboard
            (source: {date_range['source']}). Make sure the data folder is
            mounted into the dashboard container.
        </div>
        """,
        unsafe_allow_html=True,
    )

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

    # Chart: events per minute by type — Plotly so the user can hover over
    # the line and see the exact date/time + event count tooltip.
    chart_df = (
        stream_df.pivot_table(
            index="window_start",
            columns="event",
            values="event_count",
            aggfunc="sum",
            fill_value=0,
        )
        .sort_index()
        .tail(60)
        .reset_index()
    )

    chart_long = chart_df.melt(
        id_vars="window_start",
        var_name="event",
        value_name="event_count",
    )

    line_fig = px.line(
        chart_long,
        x="window_start",
        y="event_count",
        color="event",
        labels={
            "window_start": "Time (dataset event_time)",
            "event_count": "Events",
            "event": "Event type",
        },
        markers=True,
    )
    line_fig.update_traces(
        hovertemplate=(
            "<b>%{fullData.name}</b><br>"
            "Date: %{x|%A, %d %B %Y}<br>"
            "Time: %{x|%H:%M}<br>"
            "Events: %{y:,}<extra></extra>"
        )
    )
    line_fig.update_layout(
        hovermode="x unified",
        height=380,
        margin=dict(l=20, r=20, t=30, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(
            tickformat="%d %b %Y\n%H:%M",
            title=None,
        ),
        yaxis=dict(title="Events per minute"),
    )
    st.plotly_chart(line_fig, use_container_width=True)

    with st.expander("Latest minute breakdown"):
        st.dataframe(
            last_minute[["window_start", "window_end", "event", "event_count"]]
            .sort_values("event_count", ascending=False)
            .reset_index(drop=True)
        )

    # ------------------------------------------------------------------
    # Funnel chart: View -> Add to Cart -> Transaction (live)
    # ------------------------------------------------------------------
    st.header("Conversion funnel (live)")
    st.caption(
        "Aggregated from the live stream: shows how visitors flow from "
        "viewing products to adding them to cart and ultimately purchasing."
    )

    event_totals = stream_df.groupby("event")["event_count"].sum().to_dict()
    view_count = int(event_totals.get("view", 0))
    addtocart_count = int(event_totals.get("addtocart", 0))
    transaction_count = int(event_totals.get("transaction", 0))

    view_to_cart = (addtocart_count / view_count * 100) if view_count > 0 else 0
    cart_to_tx = (transaction_count / addtocart_count * 100) if addtocart_count > 0 else 0
    overall_conv = (transaction_count / view_count * 100) if view_count > 0 else 0

    fcol_a, fcol_b, fcol_c = st.columns(3)
    fcol_a.metric("View → Add to Cart", f"{view_to_cart:.2f} %")
    fcol_b.metric("Add to Cart → Transaction", f"{cart_to_tx:.2f} %")
    fcol_c.metric("Overall View → Purchase", f"{overall_conv:.2f} %")

    render_funnel_graph(
        view_count,
        addtocart_count,
        transaction_count,
        height=460,
        key_suffix="live",
    )

# ----------------------------------------------------------------------
# Batch section
# ----------------------------------------------------------------------
st.header("Batch insights")

top_items, event_dist, daily_visitors = load_batch_results()

if top_items.empty and event_dist.empty and daily_visitors.empty:
    st.info(
        "No batch results yet. Run the batch job:\n\n"
        "`docker exec spark-master /opt/bitnami/spark/bin/spark-submit "
        "--master spark://spark-master:7077 /opt/jobs/batch_analysis.py`"
    )
else:
    # If the batch produced daily_visitors, show the dataset's real date range
    # (this is computed from the actual dataset timestamps by Spark).
    if not daily_visitors.empty and "event_date" in daily_visitors.columns:
        ds_min = daily_visitors["event_date"].min()
        ds_max = daily_visitors["event_date"].max()
        n_days = daily_visitors["event_date"].nunique()
        st.markdown(
            f"""
            <div style="
                background: rgba(54, 179, 126, 0.10);
                padding: 12px 16px;
                border-left: 4px solid #36B37E;
                border-radius: 4px;
                margin: 4px 0 18px 0;
                font-size: 15px;
                line-height: 1.6;
            ">
                <b>🗓️ Batch dataset window (from batch_analysis.py):</b>
                {format_dataset_date(ds_min)} → {format_dataset_date(ds_max)}
                ({n_days} days)
            </div>
            """,
            unsafe_allow_html=True,
        )

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

    # ------------------ Batch funnel (full dataset) ------------------
    if not event_dist.empty:
        st.subheader("Batch funnel - full dataset")
        batch_totals = dict(zip(event_dist["event"], event_dist["count"]))
        b_view = int(batch_totals.get("view", 0))
        b_cart = int(batch_totals.get("addtocart", 0))
        b_tx = int(batch_totals.get("transaction", 0))

        b_view_to_cart = (b_cart / b_view * 100) if b_view > 0 else 0
        b_cart_to_tx = (b_tx / b_cart * 100) if b_cart > 0 else 0
        b_overall = (b_tx / b_view * 100) if b_view > 0 else 0

        bcol_a, bcol_b, bcol_c = st.columns(3)
        bcol_a.metric("View → Add to Cart (batch)", f"{b_view_to_cart:.2f} %")
        bcol_b.metric("Add to Cart → Transaction (batch)", f"{b_cart_to_tx:.2f} %")
        bcol_c.metric("Overall View → Purchase (batch)", f"{b_overall:.2f} %")

        render_funnel_graph(
            b_view,
            b_cart,
            b_tx,
            height=460,
            key_suffix="batch",
        )

    st.subheader("Daily active visitors")
    if not daily_visitors.empty:
        dv_fig = px.line(
            daily_visitors,
            x="event_date",
            y="active_visitors",
            labels={
                "event_date": "Date (RetailRocket dataset)",
                "active_visitors": "Active visitors",
            },
            markers=True,
        )
        dv_fig.update_traces(
            line=dict(color="#36B37E", width=2),
            marker=dict(size=5),
            hovertemplate=(
                "<b>Date:</b> %{x|%A, %d %B %Y}<br>"
                "<b>Active visitors:</b> %{y:,}<extra></extra>"
            ),
        )
        dv_fig.update_layout(
            hovermode="x unified",
            height=340,
            margin=dict(l=20, r=20, t=20, b=20),
            xaxis=dict(tickformat="%d %b %Y", title=None),
            yaxis=dict(title="Active visitors"),
        )
        st.plotly_chart(dv_fig, use_container_width=True)
    else:
        st.write("(not available)")

# ----------------------------------------------------------------------
# Auto refresh
# ----------------------------------------------------------------------
if auto_refresh:
    time.sleep(refresh_every)
    st.rerun()
