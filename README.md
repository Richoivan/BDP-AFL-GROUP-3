# Big Data Processing - Final Project

End-to-end big data pipeline for the **RetailRocket E-commerce dataset**.
Built with **Kafka, Apache Spark (batch + Structured Streaming), MinIO,
and Streamlit**, fully orchestrated by **Docker Compose** and runnable on
a single laptop.

---

## 1. Architecture Diagram

```
                  +---------------------+
                  |  data/events.csv    |
                  |  (RetailRocket)     |
                  +----------+----------+
                             |
                             v
                  +---------------------+
                  |   Kafka Producer    |   (Python)
                  |  producer/producer  |
                  +----------+----------+
                             | JSON events
                             v
+----------+         +---------------------+
| Zookeeper|<------->|   Kafka Broker      |   topic: events
+----------+         +----------+----------+
                                |
                                v
                   +-----------------------------+
                   |   Spark Structured Streaming |
                   |   jobs/streaming_job.py     |
                   |   - events per minute       |
                   |   - raw event archive       |
                   +------+------------+---------+
                          |            |
              parquet     |            | csv snapshot
                          v            v
                +-----------------+   +------------------+
                |     MinIO       |   |  ./results/      |
                |  (S3-compatible)|   |  (local volume)  |
                |  raw-events/    |   |  stream/         |
                |  stream-results/|   |  batch/          |
                |  batch-results/ |   +---------+--------+
                +--------+--------+             |
                         ^                      |
                         |  parquet/csv         |
                         |                      v
              +----------+--------+    +------------------+
              | Spark Batch Job   |    |   Streamlit      |
              | jobs/batch_       |    |   dashboard/app  |
              | analysis.py       |    |   - live charts  |
              | top items, etc.   |    |   - batch charts |
              +-------------------+    +---------+--------+
                                                 |
                                                 v
                                       http://localhost:8501
```

Data-flow: `CSV -> Producer -> Kafka -> Spark Streaming -> MinIO -> Dashboard`,
and in parallel `CSV -> Spark Batch -> MinIO + CSV -> Dashboard`.

---
## 2. Project Description

The project implements a complete big data pipeline covering the five
required capabilities of the course:

1. **Distributed storage** - MinIO (S3-compatible object store) holds
   raw events, batch results, and streaming results in dedicated buckets.
2. **Batch processing** - Apache Spark reads the full `events.csv` and
   computes historical insights (top items, event distribution, daily
   active visitors).
3. **Stream ingestion** - A Python Kafka producer reads the CSV row by
   row and publishes JSON events to the Kafka topic `events`,
   simulating live user activity on an e-commerce site.
4. **Stream processing** - Spark Structured Streaming consumes the
   Kafka topic, computes a windowed real-time metric (events per
   minute by event type) with a 2-minute watermark, and writes both
   to MinIO (parquet) and to a local CSV snapshot.
5. **Live visualization** - A Streamlit dashboard reads the streaming
   CSV snapshot and the batch CSV outputs and refreshes every few
   seconds to show live KPIs, charts, and tables.

Everything is packaged in a single `docker-compose.yml` so the grader
can run the whole system with one command.

---

## 3. Problem Statement

E-commerce platforms generate millions of `view`, `addtocart`, and
`transaction` events per day. To run the business effectively the
analytics team must answer two very different kinds of questions:

- **Historical:** Which products are most popular over time? What is
  the breakdown of event types? How does daily active traffic evolve?
  These need **batch processing** over the full history.
- **Operational:** What is happening *right now*? Is traffic spiking?
  Are users still adding items to cart? These need **real-time stream
  processing** with sub-minute latency.

A traditional single-machine pipeline cannot serve both needs at the
laptop scale required by the course. This project demonstrates how a
distributed architecture (Kafka + Spark + object storage) cleanly
separates ingest, batch, and stream layers while running locally
under Docker Compose, and surfaces the results in a single live
dashboard.

---

## 4. Dataset

**RetailRocket E-commerce dataset** -
<https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset>

We use the `events.csv` file. Schema:

| Column          | Type    | Description                                          |
|-----------------|---------|------------------------------------------------------|
| `timestamp`     | long    | Event time in milliseconds since the epoch (UTC)     |
| `visitorid`     | string  | Anonymised visitor identifier                        |
| `event`         | string  | One of `view`, `addtocart`, `transaction`            |
| `itemid`        | string  | Product identifier                                   |
| `transactionid` | string  | Present only when `event = transaction`, else empty  |

Roughly **2.75M events** across ~1.4M visitors and ~235K items.

**The dataset is required** - this project uses the real RetailRocket
`events.csv` only (no synthetic fallback is bundled). Download
`events.csv` from the Kaggle link above and place it at
`data/events.csv` **before** starting the stack. The producer and the
batch job both fail fast with an instructive error message if the file
is missing.

The other files in the Kaggle download (`item_properties_part*.csv`,
`category_tree.csv`) are not used by this project and can be ignored.

---

## 5. How to Run

Complete step-by-step tutorial to reproduce the project end-to-end.
All commands are written for **PowerShell on Windows** (line continuation
is the backtick `` ` ``). For bash on Linux/macOS, replace each backtick
with a backslash `\`.

### 5.0. Prerequisites

- **Docker Desktop** running, with Docker Compose v2
- At least **8 GB of RAM** allocated to Docker
  (Settings -> Resources -> Memory)
- ~5 GB free disk for images and volumes
- **Python 3** installed locally (only used by the dataset download
  helper)
- A **Kaggle account** (required by `kagglehub` to download the dataset)

### 5.1. Clone the repository

```powershell
git clone <your-repo-url>
cd "BIG DATA  PROCESSING_ALP"
```

### 5.2. Download the RetailRocket dataset

The pipeline will not start without `data/events.csv`. Easiest way:

```powershell
pip install kagglehub
python download_dataset.py
```

The helper calls
`kagglehub.dataset_download("retailrocket/ecommerce-dataset")`,
finds `events.csv` inside the downloaded folder, and copies it to
`data/events.csv` automatically. On first use, kagglehub will prompt
you to authenticate with your Kaggle account.

**Alternative (manual download):** visit
<https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset>,
download the archive, and place `events.csv` directly at
`data/events.csv`.

Verify it landed correctly:

```powershell
dir data
# Expected: events.csv (~90 MB) and README.md
```

### 5.3. Start the full pipeline

```powershell
docker compose up -d --build
```

The first run downloads several GB of Docker images
(Zookeeper, Kafka, MinIO, Bitnami Spark, Python builds for the
producer and dashboard). Expect **5-10 minutes** on the first run;
subsequent starts are much faster.

### 5.4. Verify all services are running

```powershell
docker compose ps
```

You should see **9 containers**. All should be `Up`/`running` except
`minio-setup` which is a one-shot bucket creator and finishes as
`Exited (0)`:

| Container       | Role                                     |
|-----------------|------------------------------------------|
| zookeeper       | Kafka coordinator                        |
| kafka           | Stream-ingest broker                     |
| minio           | Distributed object storage               |
| minio-setup     | One-shot bucket creator (Exited 0 is OK) |
| spark-master    | Spark cluster master                     |
| spark-worker    | Spark cluster worker (4 cores / 4 GB)    |
| producer        | Kafka producer streaming events.csv      |
| streaming-job   | Spark Structured Streaming consumer      |
| dashboard       | Streamlit live dashboard                 |

### 5.5. Watch the streaming job warm up (optional)

```powershell
docker compose logs -f streaming-job
```

Within ~1 minute you should see lines like:

```
[stream] batch 0: wrote 6 rows -> /opt/results/stream/events_per_minute
+-------------------+-----------+-----------+
|window_start       |event      |event_count|
+-------------------+-----------+-----------+
|2026-05-19 11:35:00|view       |234        |
|2026-05-19 11:35:00|addtocart  |18         |
+-------------------+-----------+-----------+
```

Press **Ctrl + C** to leave the log stream (the container keeps
running in the background).

### 5.6. Open the live dashboard

In your browser: **<http://localhost:8501>**

You will see:

- A KPI row (events in latest minute, total events processed,
  distinct event types)
- A line chart **"events per minute"** split by event type
- A **"Latest minute breakdown"** table
- A **"Batch insights"** section that currently says
  *"No batch results yet. Run the batch job..."* - this is normal,
  it gets populated in the next step.

### 5.7. Run the batch job (Top viewed items insight)

This is where the **batch insight** is computed. There is one caveat:
Spark Standalone is greedy by default - the streaming job already
holds executor slots on the worker, so a second `spark-submit` would
hang with `Initial job has not accepted any resources`.

The **working sequence** is therefore:

1. **Stop the streaming job temporarily** to free the worker:

   ```powershell
   docker compose stop streaming-job
   ```

2. **Run the batch job:**

   ```powershell
   docker exec spark-master /opt/bitnami/spark/bin/spark-submit `
     --master spark://spark-master:7077 `
     --conf spark.cores.max=4 `
     --conf spark.executor.cores=2 `
     --conf spark.executor.memory=2g `
     --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 `
     --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
     --conf spark.hadoop.fs.s3a.access.key=minioadmin `
     --conf spark.hadoop.fs.s3a.secret.key=minioadmin `
     --conf spark.hadoop.fs.s3a.path.style.access=true `
     --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
     --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false `
     /opt/jobs/batch_analysis.py
   ```

   We use `docker exec` (not `docker compose run`) so the job reuses
   the already-running `spark-master` container instead of spawning a
   fresh one. Because the streaming job is stopped in step 1, the
   worker has all four cores free, so the batch job is given the full
   `cores.max=4 / executor.cores=2 / executor.memory=2g`.

   First run also downloads `hadoop-aws` + `aws-java-sdk-bundle` from
   Maven Central (~270 MB, ~1-2 minutes). After that the job reads
   the full 2.75M-row CSV and computes the insights - expect
   **2-5 minutes** total on a typical laptop.

   Final console output should look like:

   ```
   [batch] Total events loaded: 2,756,101
   [batch] wrote /opt/results/batch/top_items
   [batch] wrote /opt/results/batch/event_distribution
   [batch] wrote /opt/results/batch/daily_visitors
   [batch] wrote parquet outputs to s3a://batch-results
   [batch] === Top 20 viewed items ===
   +------+----------+
   |itemid|view_count|
   +------+----------+
   |187946|3410      |
   |461686|2539      |
   ...
   [batch] === Event distribution ===
   +-----------+-------+
   |event      |count  |
   +-----------+-------+
   |view       |2664312|
   |addtocart  |69332  |
   |transaction|22457  |
   +-----------+-------+
   ```

3. **Restart the streaming job** so the dashboard keeps showing live
   data:

   ```powershell
   docker compose start streaming-job
   ```

### 5.8. Refresh the dashboard

Reload <http://localhost:8501>. The **"Batch insights"** section is
now populated:

- **Top 20 most viewed items** bar chart (the required batch insight)
- **Event type distribution** bar chart
- **Daily active visitors** line chart

The real-time section keeps refreshing every 5 seconds and shows new
events flowing in from Kafka.

### 5.9. Explore MinIO and Spark UIs (optional)

- **MinIO console:** <http://localhost:9001>
  Login: `minioadmin` / `minioadmin`. Open the three buckets:
  - `raw-events/` - raw events archived by the streaming job
  - `stream-results/events_per_minute/` - parquet aggregates
  - `batch-results/top_items/` etc. - batch outputs
- **Spark master UI:** <http://localhost:8080>
  Shows the registered worker, running applications, and per-app
  resource usage.

### 5.10. Stop everything

```powershell
docker compose down            # stop containers, keep MinIO data
docker compose down -v         # stop AND wipe MinIO data + checkpoints
```

Use `down -v` if you want a fully clean run next time.

---

## 6. Expected Output

After `docker compose up -d --build`:

- `producer` logs show
  `[producer] sent 500 events (latest: view item=12345)` - the CSV
  is being streamed to Kafka.
- `streaming-job` logs show micro-batch output like
  `[stream] batch 12: wrote 8 rows -> /opt/results/stream/events_per_minute`
  and a console table of `(window_start, event, event_count)`.
- The MinIO console shows three buckets: `raw-events`,
  `stream-results`, `batch-results`. Files appear in them within
  ~30 seconds of the stream starting (and `batch-results` fills in
  once you run section 5.7).

In the **Streamlit dashboard at `http://localhost:8501`** you will see:

**Real-time metric (live, refreshes every 5 s)**
- KPI cards: events in latest minute, total events processed,
  distinct event types.
- A line chart of *events per minute* split by `view`, `addtocart`,
  and `transaction` (last 60 minutes).
- A table breakdown of the most recent minute.

**Batch insights (after running `batch_analysis.py`)**
- Bar chart of the **Top 20 most viewed items** (required batch
  insight).
- Bar chart of the **event-type distribution**.
- Line chart of **daily active visitors**.

---

## 7. Findings & Conclusion

The numbers below come from a **successful end-to-end run** of the
pipeline against the full RetailRocket `events.csv` (**2,756,101
events**, ~90 MB).

### Batch insight - Top viewed items

The most viewed product was `itemid = 187946` with **3,410 views**.
Full top 5:

| Rank | itemid  | view_count |
|------|---------|------------|
| 1    | 187946  | 3,410      |
| 2    | 461686  | 2,539      |
| 3    | 5411    | 2,325      |
| 4    | 370653  | 1,854      |
| 5    | 219512  | 1,740      |

The top 20 items together capture tens of thousands of views while
the catalogue contains ~235K items - a classic **long-tail
distribution**. A tiny fraction of products drives a
disproportionate share of attention, which is exactly the kind of
skew that justifies running recommendation and ranking models
against this data.

### Real-time metric - Events per minute

Across the full dataset the event-type distribution is:

| Event type   | Count     | Share   |
|--------------|-----------|---------|
| view         | 2,664,312 | 96.67%  |
| addtocart    | 69,332    | 2.52%   |
| transaction  | 22,457    | 0.81%   |

The `view -> addtocart -> transaction` funnel is therefore very
steep:

- Only **2.60%** of views lead to an add-to-cart
- Only **32.39%** of add-to-cart events convert into a transaction
- End-to-end **view-to-purchase conversion = 0.84%**

The live dashboard exposes this funnel per minute, which is exactly
the kind of operational signal a business team would want during
flash sales or marketing campaigns - if the add-to-cart rate
suddenly drops, the team can react in minutes instead of waiting
for tomorrow's batch report.

### Conclusion

The project demonstrates that a full big-data architecture
(distributed storage + batch + stream ingest + stream compute +
live visualization) can be assembled from open-source components
and run on a single laptop using Docker Compose. The clean
separation between Kafka (ingest), Spark (compute), MinIO
(storage), and Streamlit (presentation) means each layer can be
scaled or replaced independently - for example MinIO could be
swapped for HDFS or real S3, and the Spark cluster could be scaled
horizontally with additional `spark-worker` replicas, without any
change to the producer or dashboard code. The pipeline successfully
processed **2.75 million events end-to-end**, proving the
architecture works at realistic dataset sizes on commodity hardware.

---

## 8. Known Limitations

- **Laptop-scale, not production-scale.** Spark runs with a single
  worker (4 cores / 4 GB). The full RetailRocket dataset (2.75M
  events, ~90 MB) completes in a few minutes, but a real cluster
  would be needed for datasets significantly larger than ~10 GB.
- **Spark Standalone resource contention.** Spark Standalone is
  greedy by default: the streaming job claims executor slots on
  the worker the moment it starts. To run the batch job reliably
  on the same single worker, the streaming job must be temporarily
  stopped (`docker compose stop streaming-job`) and restarted
  afterwards (`docker compose start streaming-job`). A production
  deployment would use YARN, Kubernetes, or a larger Standalone
  cluster with dynamic allocation so both jobs can coexist without
  manual intervention.
- **Single-broker Kafka.** Kafka runs with replication factor 1 and
  one broker. This is sufficient for local demos but is not
  fault-tolerant - a broker restart loses in-flight data.
- **Streaming results are snapshot CSVs.** For dashboarding
  simplicity, the streaming job writes a full-state CSV snapshot
  every 15 seconds. A production pipeline would typically expose
  the aggregates through a low-latency store (Redis, Druid,
  ClickHouse) instead.
- **No authentication.** MinIO uses default credentials
  (`minioadmin / minioadmin`) and Streamlit is exposed without
  auth. Both must be hardened before exposing the stack outside
  `localhost`.
- **First-run latency.** The first `docker compose up` downloads
  several GB of images (Spark, Kafka, MinIO) and Spark must
  resolve the `spark-sql-kafka` and `hadoop-aws` packages from
  Maven Central. Allow ~5-10 minutes on the first run.
- **Dataset must be downloaded manually.** Kaggle requires
  authentication, so the `events.csv` file is not bundled in the
  repository. The pipeline will not start until you place
  `events.csv` in the `data/` folder.
- **Image namespace pinning.** Bitnami restructured its Docker Hub
  namespaces in 2025; this project uses `bitnamilegacy/spark:3.5.1`.
  If that image is ever removed, the same code works with
  `apache/spark:3.5.1` after adjusting the `spark-submit` paths in
  `docker-compose.yml`.
