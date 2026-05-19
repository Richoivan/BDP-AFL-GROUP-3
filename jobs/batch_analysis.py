"""
Spark batch analysis job for RetailRocket.

Reads the full events CSV from /opt/data (and/or the raw events that the
streaming job has archived to MinIO) and computes a batch insight:

    -> Top 20 most viewed items
    -> Event-type distribution (view / addtocart / transaction)
    -> Daily active visitors

Results are written:
    - to MinIO bucket  s3a://batch-results/  as parquet + csv
    - to /opt/results/batch/                  as csv (consumed by Streamlit)
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
DATA_FILE = os.getenv("DATA_FILE", "/opt/data/events.csv")
LOCAL_OUT = os.getenv("BATCH_OUT_LOCAL", "/opt/results/batch")
S3_OUT = os.getenv("BATCH_OUT_S3", "s3a://batch-results")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def pick_data_file() -> str:
    if os.path.exists(DATA_FILE):
        return DATA_FILE
    raise FileNotFoundError(
        f"{DATA_FILE} not found. Download the RetailRocket dataset from "
        "https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset "
        "and place events.csv at ./data/events.csv before running this job."
    )


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("RetailRocketBatchAnalysis")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main() -> None:
    data_path = pick_data_file()
    os.makedirs(LOCAL_OUT, exist_ok=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

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
    def write_local(df_out, name: str) -> None:
        out_path = os.path.join(LOCAL_OUT, name)
        # coalesce so the dashboard sees a single CSV file
        df_out.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)
        print(f"[batch] wrote {out_path}")

    write_local(top_items, "top_items")
    write_local(event_dist, "event_distribution")
    write_local(daily_visitors, "daily_visitors")

    # ---------------- Write results: MinIO parquet -----------------------
    try:
        top_items.write.mode("overwrite").parquet(f"{S3_OUT}/top_items")
        event_dist.write.mode("overwrite").parquet(f"{S3_OUT}/event_distribution")
        daily_visitors.write.mode("overwrite").parquet(f"{S3_OUT}/daily_visitors")
        print(f"[batch] wrote parquet outputs to {S3_OUT}")
    except Exception as exc:  # noqa: BLE001
        print(f"[batch] WARN: could not write to MinIO ({exc}). Local CSVs are still available.")

    print("[batch] === Top 20 viewed items ===")
    top_items.show(truncate=False)
    print("[batch] === Event distribution ===")
    event_dist.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
