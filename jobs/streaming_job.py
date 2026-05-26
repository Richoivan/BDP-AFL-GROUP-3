"""
Spark Structured Streaming job for RetailRocket events.

Reads JSON events from Kafka topic `events`, computes a real-time
metric (events per minute by event type), and continuously writes
results to:

    - MinIO  s3a://stream-results/events_per_minute          (parquet)
    - MinIO  s3a://raw-events/                               (raw archive, parquet)
    - HDFS   hdfs://namenode:9000/results/stream/            (parquet)
    - Local  /opt/results/stream/events_per_minute.csv       (consumed by dashboard)

It also prints a console sink so you can watch the live metric in
the streaming-job container logs.
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


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")

STREAM_LOCAL_OUT = "/opt/results/stream"
STREAM_S3_OUT = "s3a://stream-results"
RAW_S3_OUT = "s3a://raw-events"
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
HDFS_STREAM_OUT = f"{HDFS_NAMENODE}/results/stream"
HDFS_CHECKPOINT_BASE = f"{HDFS_NAMENODE}/user/spark/checkpoints"

CHECKPOINT_BASE = "/opt/results/checkpoints"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("RetailRocketStreamingJob")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def main() -> None:
    os.makedirs(STREAM_LOCAL_OUT, exist_ok=True)
    os.makedirs(CHECKPOINT_BASE, exist_ok=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    event_schema = StructType(
        [
            StructField("timestamp", LongType(), True),
            StructField("event_time", StringType(), True),
            StructField("visitorid", StringType(), True),
            StructField("event", StringType(), True),
            StructField("itemid", StringType(), True),
            StructField("transactionid", StringType(), True),
            StructField("ingest_time", StringType(), True),
        ]
    )

    # -----------------------------------------------------------------
    # 1. Read raw bytes from Kafka and parse the JSON payload
    # -----------------------------------------------------------------
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
        .select(
            F.from_json(F.col("json_str"), event_schema).alias("e"),
            F.col("kafka_ts"),
        )
        .select("e.*", "kafka_ts")
        .withColumn(
            "event_ts",
            F.coalesce(
                F.to_timestamp(F.col("event_time")),
                F.col("kafka_ts"),
            ),
        )
    )

    # -----------------------------------------------------------------
    # 2. Real-time metric: events per minute by event type
    # -----------------------------------------------------------------
    per_minute = (
        parsed.withWatermark("event_ts", "2 minutes")
        .groupBy(
            F.window(F.col("event_ts"), "1 minute").alias("w"),
            F.col("event"),
        )
        .agg(F.count("*").alias("event_count"))
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("event"),
            F.col("event_count"),
        )
    )

    # -----------------------------------------------------------------
    # 3. Sinks
    # -----------------------------------------------------------------

    # 3a. Console sink (for live logs)
    console_query = (
        per_minute.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime="20 seconds")
        .start()
    )

    # 3b. Local CSV sink the Streamlit dashboard reads.
    #     We use foreachBatch so we can overwrite a single CSV that
    #     always contains the latest aggregated state.
    def write_to_local_csv(batch_df, batch_id):
        # Persist a full snapshot of the latest aggregates every micro-batch
        out_dir = os.path.join(STREAM_LOCAL_OUT, "events_per_minute")
        os.makedirs(out_dir, exist_ok=True)
        try:
            (
                batch_df.orderBy(F.col("window_start").desc())
                .coalesce(1)
                .write.mode("overwrite")
                .option("header", True)
                .csv(out_dir)
            )
            print(f"[stream] batch {batch_id}: wrote {batch_df.count()} rows -> {out_dir}")
        except Exception as exc:  # noqa: BLE001
            print(f"[stream] WARN: local csv write failed for batch {batch_id}: {exc}")

    local_csv_query = (
        per_minute.writeStream.outputMode("complete")
        .foreachBatch(write_to_local_csv)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/local_csv")
        .trigger(processingTime="15 seconds")
        .start()
    )

    # 3c. MinIO parquet sink for the aggregated metric
    try:
        s3_metric_query = (
            per_minute.writeStream.outputMode("complete")
            .format("parquet")
            .option("path", f"{STREAM_S3_OUT}/events_per_minute")
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/s3_metric")
            .trigger(processingTime="30 seconds")
            .start()
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[stream] WARN: could not start MinIO metric sink: {exc}")
        s3_metric_query = None

    # 3d. MinIO raw archive (append, parquet) so the batch job can replay history
    try:
        raw_archive_query = (
            parsed.select(
                "timestamp",
                "event_time",
                "visitorid",
                "event",
                "itemid",
                "transactionid",
                "ingest_time",
            )
            .writeStream.outputMode("append")
            .format("parquet")
            .option("path", f"{RAW_S3_OUT}/")
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw_archive")
            .trigger(processingTime="30 seconds")
            .start()
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[stream] WARN: could not start raw archive sink: {exc}")
        raw_archive_query = None

    # 3e. HDFS parquet sink for the aggregated metric (foreachBatch for overwrite semantics)
    def write_to_hdfs(batch_df, batch_id):
        hdfs_dir = f"{HDFS_STREAM_OUT}/events_per_minute"
        try:
            (
                batch_df.orderBy(F.col("window_start").desc())
                .coalesce(1)
                .write.mode("overwrite")
                .parquet(hdfs_dir)
            )
            print(f"[stream] batch {batch_id}: wrote {batch_df.count()} rows -> HDFS {hdfs_dir}")
        except Exception as exc:  # noqa: BLE001
            print(f"[stream] WARN: HDFS write failed for batch {batch_id}: {exc}")

    try:
        hdfs_metric_query = (
            per_minute.writeStream.outputMode("complete")
            .foreachBatch(write_to_hdfs)
            .option("checkpointLocation", f"{HDFS_CHECKPOINT_BASE}/hdfs_metric")
            .trigger(processingTime="30 seconds")
            .start()
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[stream] WARN: could not start HDFS metric sink: {exc}")
        hdfs_metric_query = None

    # 3f. HDFS raw archive (append, parquet)
    try:
        hdfs_raw_query = (
            parsed.select(
                "timestamp",
                "event_time",
                "visitorid",
                "event",
                "itemid",
                "transactionid",
                "ingest_time",
            )
            .writeStream.outputMode("append")
            .format("parquet")
            .option("path", f"{HDFS_STREAM_OUT}/raw_archive/")
            .option("checkpointLocation", f"{HDFS_CHECKPOINT_BASE}/hdfs_raw_archive")
            .trigger(processingTime="30 seconds")
            .start()
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[stream] WARN: could not start HDFS raw archive sink: {exc}")
        hdfs_raw_query = None

    print("[stream] Streaming queries started. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
