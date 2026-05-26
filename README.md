# Big Data Processing - Final Project

End-to-end big data pipeline for the **RetailRocket E-commerce dataset**.
Built with **Kafka, Apache Spark (batch + Structured Streaming), HDFS
(Hadoop Distributed File System), and Streamlit**, fully orchestrated
by **Docker Compose** and runnable on a single laptop.

---

## 1. Architecture Diagram

```
                  +---------------------+
                  |  data/events.csv    |
                  |  (RetailRocket)     |
                  +----------+----------+
                             |
                  +----------+-----------+
                  |                      |
                  v                      v
       +---------------------+   +-------------------+
       |   Kafka Producer    |   |    hdfs-setup     |
       |  producer/producer  |   |   (one-shot)      |
       +----------+----------+   |  uploads CSV ->   |
                  | JSON events  |  HDFS /raw-data   |
                  v              +---------+---------+
+----------+   +-------------+             |
| Zookeeper|<->|  Kafka      |             |
+----------+   +-----+-------+             |
                     |                     |
                     v                     v
        +-----------------------------+   +-----------------------------+
        |  Spark Structured Streaming |   |     HDFS Cluster            |
        |  jobs/streaming_job.py      |   |    (Distributed FS)         |
        |  - events per minute        |   |  +-----------------------+  |
        |  - raw event archive        |   |  |     NameNode          |  |
        +------+------------+---------+   |  |    (metadata)         |  |
               |            |             |  +-----------+-----------+  |
       parquet |            | csv snap    |              |              |
               |            |             |  +-----------v-----------+  |
               v            v             |  |     DataNode          |  |
       +-------+-------+  +--+-------+    |  |    (blocks)           |  |
       |  HDFS write   |  | local    |    |  +-----------------------+  |
       +-------+-------+  | /results |    |                             |
               |          +----+-----+    |  /raw-data/events.csv       |
               v               |          |  /raw-events/               |
       +---------------+       |          |  /stream-results/           |
       |  HDFS dirs:   |<------+--------->|  /batch-results/            |
       |  /stream-     |       |          +-------------+---------------+
       |  /raw-events  |       |                        ^
       |  /batch-      |       |                        | reads
       +---------------+       |                        | events.csv
                               v                        | & writes
                       +---------------+    +-----------+---------+
                       |   Streamlit   |    | Spark Batch Job     |
                       |  dashboard    |    | jobs/batch_         |
                       |  - live KPIs  |    | analysis.py         |
                       |  - funnel     |    | top items, etc.     |
                       |  - batch      |    +---------------------+
                       +-------+-------+
                               |
                               v
                       http://localhost:8501
```

Data-flow (two parallel paths):

1. **Stream path:** `CSV -> Producer -> Kafka -> Spark Streaming -> HDFS (raw-events, stream-results) -> Dashboard`
2. **Batch path:** `CSV -> hdfs-setup (upload to HDFS /raw-data) -> Spark Batch reads from HDFS -> HDFS (batch-results) + local CSV -> Dashboard`

This means the dataset itself **lives in HDFS** (uploaded automatically
by the `hdfs-setup` container on first start). The batch job reads
`events.csv` directly from HDFS, demonstrating end-to-end use of
distributed storage as the dataset source — not just as a results
sink.

---
## 2. Project Description

The project implements a complete big data pipeline covering the five
required capabilities of the course:

1. **Distributed storage** - HDFS (Hadoop Distributed File System) with
   a dedicated NameNode (metadata) and DataNode (block storage), running
   in Docker containers. Holds the **raw dataset itself** (`events.csv`
   uploaded to `/raw-data/`), raw streamed events, batch results, and
   streaming results in dedicated HDFS directories. The dataset is
   uploaded automatically on the first `docker compose up` by the
   `hdfs-setup` bootstrap container.
2. **Batch processing** - Apache Spark reads the full `events.csv`
   **directly from HDFS** (`hdfs://namenode:9000/raw-data/events.csv`)
   and computes historical insights (top items, event distribution,
   daily active visitors).
3. **Stream ingestion** - A Python Kafka producer reads the CSV row by
   row and publishes JSON events to the Kafka topic `events`,
   simulating live user activity on an e-commerce site.
4. **Stream processing** - Spark Structured Streaming consumes the
   Kafka topic, computes a windowed real-time metric (events per
   minute by event type) with a 2-minute watermark, and writes both
   to HDFS (parquet) and to a local CSV snapshot.
5. **Live visualization** - A Streamlit dashboard reads the streaming
   CSV snapshot and the batch CSV outputs and refreshes every few
   seconds to show live KPIs, charts, a dataset-date indicator, and a
   conversion funnel.

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
  Are users still adding items to cart? What is the live conversion
  funnel? These need **real-time stream processing** with sub-minute
  latency.

A traditional single-machine pipeline cannot serve both needs at the
laptop scale required by the course. This project demonstrates how a
distributed architecture (Kafka + Spark + HDFS) cleanly separates
ingest, batch, and stream layers while running locally under Docker
Compose, and surfaces the results in a single live dashboard.

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
- ~6 GB free disk for images and HDFS volumes
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
(Zookeeper, Kafka, HDFS NameNode + DataNode, Bitnami Spark, Python builds
for the producer and dashboard). Expect **5-10 minutes** on the first
run; subsequent starts are much faster.

### 5.4. Verify all services are running

```powershell
docker compose ps
```

You should see **10 containers**. All should be `Up`/`running` except
`hdfs-setup` which is a one-shot HDFS directory creator and finishes as
`Exited (0)`:

| Container       | Role                                       |
|-----------------|--------------------------------------------|
| zookeeper       | Kafka coordinator                          |
| kafka           | Stream-ingest broker                       |
| namenode        | HDFS NameNode (metadata server)            |
| datanode        | HDFS DataNode (block storage)              |
| hdfs-setup     | One-shot HDFS dir creator + uploads events.csv to HDFS (Exited 0 is OK) |
| spark-master    | Spark cluster master                       |
| spark-worker    | Spark cluster worker (4 cores / 4 GB)      |
| producer        | Kafka producer streaming events.csv        |
| streaming-job   | Spark Structured Streaming consumer        |
| dashboard       | Streamlit live dashboard                   |

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
- A **dataset date indicator** showing the earliest and latest event
  dates **from the RetailRocket dataset itself** (e.g.,
  *Earliest event: Sunday, 03 May 2015 - Latest event: Friday,
  18 September 2015*), plus the total event count. These dates come
  from the `timestamp` column of `data/events.csv`, not from the
  wall-clock streaming time, so they truly reflect the dataset.
- A line chart **"events per minute"** split by event type
- A **"Latest minute breakdown"** table
- A **"Conversion funnel (live)"** chart showing view → add-to-cart →
  transaction with conversion rates from the live stream
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
     --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 `
     --conf spark.hadoop.dfs.client.use.datanode.hostname=true `
     /opt/jobs/batch_analysis.py
   ```

   We use `docker exec` (not `docker compose run`) so the job reuses
   the already-running `spark-master` container instead of spawning a
   fresh one. Because the streaming job is stopped in step 1, the
   worker has all four cores free, so the batch job is given the full
   `cores.max=4 / executor.cores=2 / executor.memory=2g`.

   The batch job reads the dataset **directly from HDFS**
   (`hdfs://namenode:9000/raw-data/events.csv`, uploaded by
   `hdfs-setup`) and writes results back to HDFS. No `--packages` are
   required because the Spark image already ships the HDFS client.
   If for any reason the HDFS path is unreachable, the job falls back
   automatically to the local mount at `/opt/data/events.csv`.
   Expect **2-5 minutes** total on a typical laptop.

   Final console output should look like:

   ```
   [batch] Total events loaded: 2,756,101
   [batch] wrote /opt/results/batch/top_items
   [batch] wrote /opt/results/batch/event_distribution
   [batch] wrote /opt/results/batch/daily_visitors
   [batch] wrote parquet outputs to hdfs://namenode:9000/batch-results
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
- **Batch funnel** - full-dataset view → add-to-cart → transaction
- **Daily active visitors** line chart

The real-time section keeps refreshing every 5 seconds and shows new
events flowing in from Kafka, along with the live conversion funnel.

### 5.9. Explore HDFS and Spark UIs (optional)

- **HDFS NameNode UI:** <http://localhost:9870>
  Click **Utilities → Browse the file system** to navigate into the
  four project directories:
  - `/raw-data/events.csv` - the **source dataset itself**, uploaded
    to HDFS by `hdfs-setup` at first start; this is what the batch
    job reads from
  - `/raw-events/` - raw events archived by the streaming job
  - `/stream-results/events_per_minute/` - parquet aggregates
  - `/batch-results/top_items/`, `/batch-results/event_distribution/`,
    `/batch-results/daily_visitors/` - batch outputs

  The same UI shows live cluster health, the registered DataNode, and
  per-block reports.
- **Spark master UI:** <http://localhost:8080>
  Shows the registered Spark worker, running applications, and per-app
  resource usage.

You can also inspect HDFS from the command line:

```powershell
docker exec namenode hdfs dfs -ls /
docker exec namenode hdfs dfs -ls -h /raw-data
docker exec namenode hdfs dfs -ls /batch-results/top_items
```

The second command should show `events.csv` (~90 MB) sitting inside
HDFS under `/raw-data`. That is the file the batch job reads.

### 5.10. Stop everything

```powershell
docker compose down            # stop containers, keep HDFS data
docker compose down -v         # stop AND wipe HDFS volumes + checkpoints
```

Use `down -v` if you want a fully clean run next time.

### 5.11. Troubleshooting (fresh clone from GitHub)

If you just pulled this repo from GitHub and something is not working,
work through this checklist before opening an issue.

**(a) "Cannot connect to the Docker daemon"**
Docker Desktop is not running. Open Docker Desktop, wait for the
whale icon to be steady, then retry `docker compose up -d --build`.

**(b) "events.csv not found" in `producer` or batch logs**
You skipped section 5.2. Run `python download_dataset.py` (or place
the file manually at `data/events.csv`), then `docker compose restart
producer streaming-job` and (only on first ever start) re-run section
5.3 so that `hdfs-setup` can upload the CSV into HDFS.

**(c) `dependency failed to start` for any service**
Run `docker compose down -v` then `docker compose up -d --build`
again. The `-v` flag wipes the partially-initialised HDFS volumes,
which is the most common cause of stuck startups on a fresh clone.

**(d) Dashboard says "Waiting for the streaming job to produce
results"**
The streaming job is still downloading the `spark-sql-kafka`
package from Maven Central on first run (~30 MB, 1-2 minutes).
Watch progress with `docker compose logs -f streaming-job`. Once you
see `[stream] batch 0: wrote N rows -> /opt/results/stream/...`, the
dashboard will populate on the next 5-second refresh.

**(e) Dashboard says "No batch results yet"**
Run section 5.7 (stop streaming-job → submit batch → restart
streaming-job). The dashboard refreshes automatically once
`/results/batch/top_items/`, `/results/batch/event_distribution/`,
and `/results/batch/daily_visitors/` are populated.

**(f) Port already in use (`bind: address already in use`)**
Another local service is using one of 2181 / 7077 / 8080 / 8501 /
9000 / 9092 / 9870 / 29092. Either stop that service or change the
left-hand side of the host:container port mapping in
`docker-compose.yml`.

**(g) Batch job hangs with "Initial job has not accepted any
resources"**
You forgot to `docker compose stop streaming-job` first. The
streaming job is holding all executor slots on the worker. Stop it,
re-run the batch command, then `docker compose start streaming-job`.

**(h) "Permission denied" on Windows file mounts**
Make sure the project folder is inside a directory that Docker
Desktop is allowed to bind-mount (Settings → Resources → File
Sharing). The default `C:\Users\<you>\Documents\...` is fine.

### 5.12. Ports & URLs reference

Every port the project exposes on the host machine, with the URL to
open it. All UI ports below are safe to open from your browser
(`localhost`).

| Port | Service       | URL                                                 | What you see                                                                                                                                                                                            |
|------|---------------|------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 8501 | Streamlit Dashboard | <http://localhost:8501>                        | The main live dashboard - real-time events per minute, dataset date banner, conversion funnel, batch insights (Top 20 items, distribution, daily visitors, batch funnel).                              |
| 9870 | HDFS NameNode Web UI | <http://localhost:9870>                       | Cluster overview, DataNode list, capacity, safemode state. Use **Utilities → Browse the file system** to inspect `/raw-data/events.csv`, `/raw-events/`, `/stream-results/`, and `/batch-results/`.    |
| 8080 | Spark Master UI | <http://localhost:8080>                            | Spark cluster status, the registered Spark worker, running applications and per-app resource usage.                                                                                                     |
| 9000 | HDFS NameNode RPC | (internal - no browser UI)                       | `hdfs://namenode:9000` — used internally by Spark to talk to HDFS. You typically never open this in a browser; it is exposed on the host only so external tools (e.g. `hdfs` CLI on the host) could connect. |
| 7077 | Spark Master RPC | (internal - no browser UI)                        | `spark://spark-master:7077` — Spark application submission endpoint. Used by `spark-submit` and Spark workers.                                                                                            |
| 9092 | Kafka broker (in-cluster) | (internal - no browser UI)               | `kafka:9092` — used by the producer and the streaming job inside the Docker network. Not normally consumed from the host.                                                                                |
| 29092 | Kafka broker (host listener) | (internal - no browser UI)             | `localhost:29092` — exposed for any host-side tool (e.g., `kafka-console-consumer.sh`) that wants to peek at the `events` topic from outside the container network.                                       |
| 2181 | Zookeeper | (internal - no browser UI)                              | Used by Kafka for coordination. No useful UI; rarely accessed directly.                                                                                                                                  |

**Important notes**

- **This project uses HDFS, NOT MinIO.** All distributed storage runs
  through HDFS NameNode (port 9870 UI / 9000 RPC) and a DataNode. There
  is no MinIO, no S3 endpoint, no `9001` console, and no
  `minioadmin/minioadmin` credentials in this stack.
- HDFS in this project runs with `dfs.permissions.enabled=false` for
  simplicity, so no HDFS authentication is required when browsing the
  Web UI.
- If any of the above ports is already in use on your machine (e.g.
  another local Kafka or another HDFS), change the host side of the
  mapping in `docker-compose.yml` (left of the colon in `"8080:8080"`).

---

## 6. Expected Output

After `docker compose up -d --build`:

- `producer` logs show
  `[producer] sent 500 events (latest: view item=12345)` - the CSV
  is being streamed to Kafka.
- `streaming-job` logs show micro-batch output like
  `[stream] batch 12: wrote 8 rows -> /opt/results/stream/events_per_minute`
  and a console table of `(window_start, event, event_count)`.
- The HDFS NameNode UI at `http://localhost:9870` shows four project
  directories under `/`: `raw-data` (containing `events.csv` uploaded
  by `hdfs-setup`), `raw-events`, `stream-results`, and `batch-results`.
  `raw-data/events.csv` appears immediately on first start. Files
  appear in `raw-events` and `stream-results` within ~30 seconds of
  the stream starting (and `batch-results` fills in once you run
  section 5.7).

In the **Streamlit dashboard at `http://localhost:8501`** you will see:

**Real-time metric (live, refreshes every 5 s)**
- KPI cards: events in latest minute, total events processed,
  distinct event types.
- A **dataset date indicator** showing the earliest and latest event
  dates from the **actual RetailRocket dataset** (e.g.,
  *03 May 2015 → 18 September 2015*), read directly from
  `data/events.csv` and cached in memory.
- A line chart of *events per minute* split by `view`, `addtocart`,
  and `transaction` (last 60 minutes).
- A table breakdown of the most recent minute.

**Conversion funnel (live)**
- Three KPIs: View→Cart conversion %, Cart→Transaction conversion %,
  and overall View→Purchase conversion %.
- A Plotly funnel chart visually showing the drop-off from views to
  add-to-cart to actual transactions.

**Batch insights (after running `batch_analysis.py`)**
- Bar chart of the **Top 20 most viewed items** (required batch
  insight).
- Bar chart of the **event-type distribution**.
- **Batch funnel** chart computed over the full 2.75M-event history.
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

### Real-time metric - Events per minute & conversion funnel

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

The live dashboard exposes this funnel **both per-minute as a line
chart and as a visual funnel diagram**, which is exactly the kind of
operational signal a business team would want during flash sales or
marketing campaigns - if the add-to-cart rate suddenly drops, the
team can react in minutes instead of waiting for tomorrow's batch
report.

### Conclusion

The project demonstrates that a full big-data architecture
(distributed storage + batch + stream ingest + stream compute +
live visualization) can be assembled from open-source components
and run on a single laptop using Docker Compose. The clean
separation between Kafka (ingest), Spark (compute), HDFS
(distributed storage), and Streamlit (presentation) means each
layer can be scaled or replaced independently - for example HDFS
could be scaled by adding more DataNodes, and the Spark cluster
could be scaled horizontally with additional `spark-worker`
replicas, without any change to the producer or dashboard code.
The pipeline successfully processed **2.75 million events
end-to-end**, proving the architecture works at realistic dataset
sizes on commodity hardware.

---

## 8. Known Limitations

- **Laptop-scale, not production-scale.** Spark runs with a single
  worker (4 cores / 4 GB) and HDFS runs with one DataNode and a
  replication factor of 1. The full RetailRocket dataset (2.75M
  events, ~90 MB) completes in a few minutes, but a real cluster
  would be needed for datasets significantly larger than ~10 GB and
  for HDFS fault tolerance (typically replication factor 3 with
  multiple DataNodes).
- **Single NameNode (no HA).** The HDFS NameNode is a single point
  of failure. In production this would be addressed with a
  Standby NameNode + JournalNodes (HDFS HA mode) or by switching to
  a cloud-managed metadata service.
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
  every 15 seconds (in addition to HDFS parquet). A production
  pipeline would typically expose the aggregates through a
  low-latency store (Redis, Druid, ClickHouse) instead.
- **No authentication.** HDFS runs with `dfs.permissions.enabled=false`
  for simplicity and Streamlit is exposed without auth. Both must be
  hardened (Kerberos for HDFS, OAuth/SSO for Streamlit) before
  exposing the stack outside `localhost`.
- **First-run latency.** The first `docker compose up` downloads
  several GB of images (Spark, Kafka, Hadoop NameNode/DataNode) and
  Spark must resolve the `spark-sql-kafka` package from Maven
  Central. Allow ~5-10 minutes on the first run.
- **Dataset must be downloaded manually.** Kaggle requires
  authentication, so the `events.csv` file is not bundled in the
  repository. The pipeline will not start until you place
  `events.csv` in the `data/` folder.
- **Image namespace pinning.** Bitnami restructured its Docker Hub
  namespaces in 2025; this project uses `bitnamilegacy/spark:3.5.1`
  for Spark and `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`
  /`bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` for HDFS. If
  any image is ever removed, the same code works after substituting
  equivalent images (e.g., `apache/spark:3.5.1` for Spark, or
  `apache/hadoop:3` for HDFS) and adjusting paths in
  `docker-compose.yml`.
