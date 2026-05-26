"""
Spark batch analysis job for RetailRocket.

Reads the full events CSV directly from HDFS (uploaded by the
`hdfs-setup` container at startup) and computes a batch insight:

    -> Top 20 most viewed items
    -> Event-type distribution (view / addtocart / transaction)
    -> Daily active visitors

Default input:  hdfs://namenode:9000/raw-data/events.csv
Fallback input: /opt/data/events.csv (local Docker mount)

Results are written:
    - to HDFS  hdfs://namenode:9000/batch-results/  as parquet
    - to /opt/results/batch/                        as csv (consumed by Streamlit)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
)

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
HDFS_BASE = os.getenv("HDFS_BASE", "hdfs://namenode:9000")

# Primary input: events.csv on HDFS. Fallback: local Docker volume.
DATA_FILE = os.getenv("DATA_FILE", f"{HDFS_BASE}/raw-data/events.csv")
DATA_FILE_LOCAL_FALLBACK = os.getenv("DATA_FILE_LOCAL", "/opt/data/events.csv")

LOCAL_OUT = os.getenv("BATCH_OUT_LOCAL", "/opt/results/batch")
HDFS_OUT = f"{HDFS_BASE}/batch-results"


def is_hdfs_path(path: str) -> bool:
    return path.startswith("hdfs://") or path.startswith("hdfs:/")


def pick_data_file(spark: SparkSession) -> str:
    """
    Decide whether to read events.csv from HDFS or from the local mount.

    Tries the configured HDFS path first (via the Hadoop FileSystem API),
    then falls back to the local file if HDFS isn't reachable or the file
    isn't present in HDFS yet.
    """
    # Try HDFS path first
    if is_hdfs_path(DATA_FILE):
        try:
            jvm = spark.sparkContext._jvm
            hconf = spark.sparkContext._jsc.hadoopConfiguration()
            hpath = jvm.org.apache.hadoop.fs.Path(DATA_FILE)
            fs = hpath.getFileSystem(hconf)
            if fs.exists(hpath):
                print(f"[batch] Using HDFS input: {DATA_FILE}")
                return DATA_FILE
            else:
                print(f"[batch] HDFS path {DATA_FILE} does not exist yet, falling back to local.")
        except Exception as exc:  # noqa: BLE001
            print(f"[batch] WARN: could not check HDFS path ({exc}). Falling back to local.")
    else:
        # DATA_FILE itself is a local path — use as-is if it exists
        if os.path.exists(DATA_FILE):
            print(f"[batch] Using local input: {DATA_FILE}")
            return DATA_FILE

    # Local fallback
    if os.path.exists(DATA_FILE_LOCAL_FALLBACK):
        print(f"[batch] Using local fallback input: {DATA_FILE_LOCAL_FALLBACK}")
        return DATA_FILE_LOCAL_FALLBACK

    raise FileNotFoundError(
        f"Could not find events.csv in HDFS ({DATA_FILE}) or local "
        f"({DATA_FILE_LOCAL_FALLBACK}). Download the RetailRocket dataset "
        "from https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset "
        "and place events.csv at ./data/events.csv before running this job. "
        "The hdfs-setup container will upload it to HDFS automatically on the "
        "next docker compose up."
    )


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("RetailRocketBatchAnalysis")
        .config("spark.hadoop.fs.defaultFS", HDFS_BASE)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .getOrCreate()
    )


def main() -> None:
    os.makedirs(LOCAL_OUT, exist_ok=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    data_path = pick_data_file(spark)

    schema = StructType(
        [
            StructField("timestamp", LongType(), True),
            StructField("visitorid", StringType(), True),
            StructField("event", StringType(), True),
            StructField("itemid", StringType(), True),
            StructField("transactionid", StringType(), True),
        ]
    )

    print(f"[batch] Reading {data_path}")
    df = (
        spark.read.option("header", True)
        .schema(schema)
        .csv(data_path)
        .withColumn(
            "event_time",
            F.to_timestamp(F.from_unixtime(F.col("timestamp") / 1000)),
        )
        .withColumn("event_date", F.to_date("event_time"))
    )

    total_events = df.count()
    print(f"[batch] Total events loaded: {total_events:,}")

    # ---------------- Insight 1: Top 20 most viewed items ----------------
    top_items = (
        df.filter(F.col("event") == "view")
        .groupBy("itemid")
        .agg(F.count("*").alias("view_count"))
        .orderBy(F.desc("view_count"))
        .limit(20)
    )

    # ---------------- Insight 2: Event type distribution -----------------
    event_dist = (
        df.groupBy("event")
        .agg(F.count("*").alias("count"))
        .orderBy(F.desc("count"))
    )

    # ---------------- Insight 3: Daily active visitors -------------------
    daily_visitors = (
        df.groupBy("event_date")
        .agg(F.countDistinct("visitorid").alias("active_visitors"))
        .orderBy("event_date")
    )

    # ---------------- Write results: local CSV ---------------------------
    # IMPORTANT: prefix with file:// so Spark writes to the local
    # filesystem of the worker (mounted from host ./results), and NOT to
    # HDFS (which is the default fs.defaultFS configured for this job).
    def write_local(df_out, name: str) -> None:
        out_path = os.path.join(LOCAL_OUT, name)
        spark_path = f"file://{out_path}"
        # coalesce so the dashboard sees a single CSV file
        df_out.coalesce(1).write.mode("overwrite").option("header", True).csv(spark_path)
        print(f"[batch] wrote {out_path}")

    write_local(top_items, "top_items")
    write_local(event_dist, "event_distribution")
    write_local(daily_visitors, "daily_visitors")

    # ---------------- Write results: HDFS parquet ------------------------
    try:
        top_items.write.mode("overwrite").parquet(f"{HDFS_OUT}/top_items")
        event_dist.write.mode("overwrite").parquet(f"{HDFS_OUT}/event_distribution")
        daily_visitors.write.mode("overwrite").parquet(f"{HDFS_OUT}/daily_visitors")
        print(f"[batch] wrote parquet outputs to {HDFS_OUT}")
    except Exception as exc:  # noqa: BLE001
        print(f"[batch] WARN: could not write to HDFS ({exc}). Local CSVs are still available.")

    print("[batch] === Top 20 viewed items ===")
    top_items.show(truncate=False)
    print("[batch] === Event distribution ===")
    event_dist.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
