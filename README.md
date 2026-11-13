# SMART-MARITIME-VESSEL-TRAFFIC---PORT-INTELLIGENCE-PLATFORM

# 🚢 Smart Maritime Vessel Traffic & Port Intelligence Platform

A distributed, real-time Big Data and AI-driven architecture designed to ingest, process, store, analyze, and visualize global maritime AIS (Automatic Identification System) vessel telemetry and port congestion analytics.

## 🛠️ Technologies Used

![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000000?style=for-the-badge\&logo=apachekafka\&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge\&logo=apachespark\&logoColor=white)
![Spark MLlib](https://img.shields.io/badge/Spark%20MLlib-E25A1C?style=for-the-badge\&logo=apachespark\&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge\&logo=apacheairflow\&logoColor=white)
![Apache Superset](https://img.shields.io/badge/Apache%20Superset-20A7C9?style=for-the-badge\&logo=apache\&logoColor=white)
![Hadoop HDFS](https://img.shields.io/badge/Hadoop%20HDFS-66CCFF?style=for-the-badge\&logo=apachehadoop\&logoColor=black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge\&logo=postgresql\&logoColor=white)
![PostGIS](https://img.shields.io/badge/PostGIS-4169E1?style=for-the-badge\&logo=postgresql\&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge\&logo=python\&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge\&logo=databricks\&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge\&logo=docker\&logoColor=white)

---

## 📌 Key Architectural Highlights

* **Real Global AIS Telemetry:** High-throughput streaming integration using real AIS data.
* **Pure Streaming Ingest:** Handles continuous live telemetry through Apache Kafka.
* **Distributed Processing:** Large-scale stream and batch processing powered by Apache Spark / PySpark.
* **Distributed Machine Learning:** Vessel behavior clustering and anomaly detection using **Spark MLlib K-Means**.
* **Spatial Indexing:** Accelerated spatial lookups via PostGIS GIST indexing.
* **Interactive Dashboards:** Real-time visualization and spatial analytics powered by Apache Superset.
* **Workflow Orchestration:** Automated batch processing and pipeline monitoring using Apache Airflow.
* **Containerized Deployment:** Fully orchestrated using Docker Compose.

---

## 🏗️ Architecture Overview

The platform operates across a 6-layer distributed Big Data pipeline, orchestrated end-to-end using **Apache Airflow**.

### Architecture Flow

**AIS Data Sources**
↓
**Python AIS Producer / WebSocket Client**
↓
**Apache Kafka**
↓
**Apache Spark Structured Streaming**
↓
**HDFS + PostgreSQL/PostGIS + Kafka Alerts**
↓
**Apache Airflow Batch Processing**
↓
**Spark Batch Analytics + Spark MLlib K-Means**
↓
**PostgreSQL/PostGIS Analytical Tables**
↓
**Apache Superset Dashboards**

### Main Data Flow

* **AIS Producer** ingests historical NOAA AIS data and live AIS telemetry.
* **Apache Kafka** receives and distributes raw AIS position messages.
* **Spark Structured Streaming** processes incoming telemetry, performs validation, deduplication, and streaming analytics.
* Processed data is stored in **HDFS** as Parquet files for historical analysis.
* Current vessel state and operational analytics are stored in **PostgreSQL/PostGIS**.
* Speed alerts and other streaming events are published to dedicated **Kafka topics**.
* **Apache Airflow** orchestrates scheduled batch analytics and Spark jobs.
* **Spark Batch** calculates port dwell times, fleet speed KPIs, route density, and other operational metrics.
* **Spark MLlib K-Means** performs vessel behavior clustering and anomaly detection.
* **Apache Superset** provides dashboards for fleet monitoring, port congestion, vessel behavior, alerts, and spatial analytics.

### 1. Data Sources Layer

* **Historical Data (Batch):** NOAA MarineCadastre AIS Historical Archives (5+ GB raw voyage records, port boundaries, vessel track logs).
* **Real-time Streaming:** Live AIS WebSocket stream via `AISstream.io` (MMSI, Lat/Lon, Speed Over Ground (SOG), Course Over Ground (COG), Heading, Navigation Status).

### 2. Data Ingestion Layer

* **Python AISStream WebSocket Client:** Ingests live WebSocket feeds.
* **Apache Kafka (Streaming Platform):** Decouples ingestion into dedicated topics:

  * `raw_ais_positions`
  * `vessel_speed_alerts`
  * `port_geofence_events`
  * `collision_risk_telemetry`

### 3. Processing Layer

* **Apache Spark (Structured Streaming & Batch):** Distributed processing engine handling stream-batch unification.
* **Spatial & Kinematic Tasks:**

  * PostGIS Polygon Geofencing
  * Watermarking & Deduplication
  * 5-min Sliding Acceleration Vectors
  * Closest Point of Approach (CPA) calculations

### 4. Storage Layer

* **Data Lake (HDFS):** Stores raw JSON payloads, curated datasets, and Parquet files partitioned by `voyage_date` and `port`.
* **Serving Database (PostgreSQL + PostGIS):**

  * GIST Spatial Indexes on vessel paths.
  * Active Fleet State Table, Port Congestion Tables, Geofence Boundaries, and Collision-Risk Alert Logs.

### 5. Analytics & AI Layer

* **Machine Learning Engine:** Apache Spark MLlib.
* **Primary Algorithm:** **K-Means Clustering** for vessel behavior profiling.
* **Feature Engineering:** `VectorAssembler` and `StandardScaler` from Spark MLlib.
* **Anomaly Detection:** Euclidean distance from each vessel to its assigned cluster centroid, with cluster-specific percentile thresholds.
* **Operational Analytics:** Port turnaround efficiency, choke-point congestion metrics, berth utilization trends, and fleet activity patterns.

### 6. Visualization Layer

* **Apache Superset:** Interactive BI dashboards, geospatial map overlays, speed/heading time-series, port wait-time KPIs, vessel behavior clusters, and geofence breach alert tables.

---

## 🧰 Tech Stack & Tools

* **Languages:** Python, SQL
* **Streaming & Ingestion:** Apache Kafka, WebSockets
* **Big Data Processing:** Apache Spark, PySpark
* **Machine Learning:** Spark MLlib, K-Means, VectorAssembler, StandardScaler
* **Storage & Data Lake:** Hadoop HDFS, PostgreSQL, PostGIS
* **Orchestration:** Apache Airflow
* **Visualization:** Apache Superset
* **Infrastructure:** Docker, Docker Compose

---

## ⚡ Workflow Orchestration (Apache Airflow)

The platform utilizes Airflow DAGs to coordinate scheduled dependencies and pipeline monitoring:

1. **Scheduled Ingest:** Trigger batch processing jobs.
2. **Spark Spatial Windowing:** Windowed transformations on spatial streams.
3. **Spark MLlib Model Training:** Periodically train the vessel behavior clustering model using historical AIS data.
4. **Update PostGIS Spatial Tables:** Push aggregated analytics and ML results to the serving layer.
5. **Alert Processing:** Process and store collision/geofence/speed alerts.

---

## 🚀 Quickstart & Deployment

### 📋 Prerequisites

* **Docker & Docker Compose**: Docker Engine v24.0+ / Docker Compose v2.20+.
* **Host Operating System**: Windows with PowerShell 5.1+ / PowerShell 7 (or Linux/macOS with equivalent shell commands).
* **System Hardware**: Minimum recommended **16 GB RAM** and 4+ CPU cores to comfortably host all services:

  * Spark Standalone Cluster (Master: 1 GB, Worker: 3 GB)
  * Kafka Broker & KRaft Controller: 1.5 GB
  * PostGIS Spatial Database: 1 GB
  * Apache Airflow (Scheduler, Webserver, Postgres): 2 GB
  * Apache Superset: 2 GB
  * Hadoop HDFS (NameNode, DataNode): 2 GB

---

### ⚡ 1-Click Cluster Bootstrap (`setup_and_run.ps1`)

The platform includes an automated startup script `setup_and_run.ps1` for Windows / PowerShell.

Run from the project root:

```powershell
.\setup_and_run.ps1
```

#### What `setup_and_run.ps1` does step by step:

1. **Docker Engine Health Check**: Runs `docker info` to ensure the Docker daemon is accessible and running.
2. **Container Launch**: Deploys the complete containerized stack in detached mode using `docker compose up -d`.
3. **Core Services Health Polling**:

   * Loops up to 30 times (with 2-second intervals) checking PostGIS readiness via `pg_isready -U maritime -d maritime`.
   * Polls Kafka broker status via `/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092`.
4. **HDFS SafeMode Bypass**: Automatically disengages HDFS SafeMode (`hdfs dfsadmin -safemode leave`) so HDFS writes are immediately accepted.
5. **Driver Injection**: Installs the pure-Python `pg8000` database driver into `spark-master` and `spark-worker` (`pip3 install --no-cache-dir pg8000`) ensuring Spark executors can write to PostGIS without native C-library conflicts.
6. **Active Endpoint Summary**: Outputs web URLs for all active web interfaces:

   * Kafka UI: `http://localhost:8090`
   * Spark Master UI: `http://localhost:8080`
   * Spark Worker UI: `http://localhost:8081`
   * HDFS NameNode UI: `http://localhost:9870`
   * Apache Airflow: `http://localhost:8082` (`admin` / `admin`)
   * Apache Superset: `http://localhost:8089` (`admin` / `admin`)
   * Jupyter Lab: `http://localhost:8888` (token: `lab`)

---

## ⚙️ Batch Analytics & Orchestration (Phase 2)

### 1. Manual Batch KPI Spark Job Execution

You can manually trigger the batch analytical processor outside Airflow for any date partition present in HDFS:

```bash
docker compose exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=1g \
  /opt/spark-apps/batch_port_kpi_processor.py --exec-date 2024-12-25
```

Optional CLI parameters:

* `--exec-date YYYY-MM-DD`: Target HDFS partition date (defaults to yesterday).
* `--port-radius-nm 3.0`: Port catchment radius in nautical miles (default: 3.0).
* `--grid-cell-deg 0.05`: Spatial route density grid cell resolution in degrees (default: 0.05).

### 2. Airflow Orchestration DAG (`maritime_batch_kpi_pipeline`)

The DAG coordinates daily batch processing across four sequential tasks:

1. `check_hdfs_partition_exists`: Verifies `hdfs://namenode:9000/raw/ais_historical/date={{ ds }}` exists before triggering Spark; fails fast if missing.
2. `run_batch_kpi_job`: Submits `batch_port_kpi_processor.py` for the execution date.
3. `verify_postgres_rows_written`: Asserts rows landed in `fleet_daily_kpis` for `{{ ds }}` using PostGIS connection; raises `AirflowFailException` if zero rows found.
4. `pipeline_health_check`: Logs structured diagnostics for Kafka, Spark, HDFS, and PostGIS directly into task logs.

#### Triggering the Pipeline:

```bash
# Trigger execution for a specific date partition
docker compose exec airflow-scheduler airflow dags trigger -e 2024-12-25 maritime_batch_kpi_pipeline

# Check execution run status
docker compose exec airflow-scheduler airflow dags list-runs -d maritime_batch_kpi_pipeline

# Check individual task states for the run
docker compose exec airflow-scheduler airflow tasks states-for-dag-run maritime_batch_kpi_pipeline <run_id>
```

#### DAG Logs:

Task logs are persisted to the mounted volume at:

`./airflow/logs/dag_id=maritime_batch_kpi_pipeline/run_id=<run_id>/task_id=<task_id>/`

---

## 🔍 Verification & Demo SQL Queries

### Verification Commands

```bash
# 1. Check container health
docker compose ps

# 2. Check archived partitions in HDFS
docker compose exec namenode hdfs dfs -ls /raw/ais_historical/

# 3. Confirm rows landed in analytical tables
docker compose exec postgis psql -U maritime -d maritime -c "SELECT kpi_date, count(*) FROM fleet_daily_kpis GROUP BY kpi_date;"
docker compose exec postgis psql -U maritime -d maritime -c "SELECT kpi_date, count(*) FROM port_dwell_times GROUP BY kpi_date;"
docker compose exec postgis psql -U maritime -d maritime -c "SELECT kpi_date, count(*) FROM route_density_grid GROUP BY kpi_date;"
docker compose exec postgis psql -U maritime -d maritime -c "SELECT DATE(detected_at), count(*) FROM vessel_speed_alerts GROUP BY DATE(detected_at);"
```

### Demo Analytical Queries

#### Query 1: Top 5 Busiest Ports by Dwell Time

```sql
docker compose exec postgis psql -U maritime -d maritime -c "
SELECT
    port_id,
    COUNT(*) AS total_visits,
    ROUND(AVG(dwell_minutes), 1) AS avg_dwell_minutes,
    ROUND(MAX(dwell_minutes), 1) AS max_dwell_minutes
FROM port_dwell_times
WHERE kpi_date = '2024-12-25'
GROUP BY port_id
ORDER BY total_visits DESC
LIMIT 5;
"
```

#### Query 2: Fleet Speed Profiles & Activity by Vessel Type

```sql
docker compose exec postgis psql -U maritime -d maritime -c "
SELECT
    vessel_type,
    avg_sog AS avg_speed_kts,
    max_sog AS max_speed_kts,
    ping_count AS total_pings
FROM fleet_daily_kpis
WHERE kpi_date = '2024-12-25'
ORDER BY ping_count DESC
LIMIT 5;
"
```

#### Query 3: Top Traffic Hotspots (Route Density Grid)

```sql
docker compose exec postgis psql -U maritime -d maritime -c "
SELECT
    grid_lat,
    grid_lon,
    ping_count AS density_pings
FROM route_density_grid
WHERE kpi_date = '2024-12-25'
ORDER BY ping_count DESC
LIMIT 5;
"
```

---

## 📊 Apache Superset Dashboards

### Accessing Superset

* **URL**: `http://localhost:8089`
* **Username**: `admin`
* **Password**: `admin`

### Automated Provisioning

The dashboards, datasets, and database connections can be automatically provisioned by executing:

```bash
# From Windows PowerShell / Bash
docker compose exec superset python3 /tmp/setup_superset.py

# Or using the wrapper script:
bash superset/setup_superset.sh
```

### Pre-Configured Dashboards

1. **Real-time Fleet Tracking** (`/superset/dashboard/realtime-fleet-tracking/`):

   * Configured with a 30-second auto-refresh interval.
   * Geospatial Scatterplot displaying active vessels based on `v_active_fleet_state`.
   * Live Active Vessels Roster and headline fleet count.

2. **Speed Alerts & Safety Violations** (`/superset/dashboard/speed-alerts-safety-violations/`):

   * Tabular audit log of vessels exceeding 20.0 knots.
   * Breakdown of top speeding vessels by MMSI and maximum recorded speed.
   * Total violation count metric.

3. **Port Congestion & Operational Metrics** (`/superset/dashboard/port-congestion-operational-metrics/`):

   * Port dwell distribution (average and maximum turnaround time per port).
   * Speed profiles grouped by commercial vessel classification.
   * High-density traffic route cells ranked by AIS ping frequency.

4. **Vessel Behavior Clustering & Anomalies**:

   * Vessel behavior clusters generated using Spark MLlib K-Means.
   * Cluster distribution and vessel counts.
   * Anomaly counts based on distance from cluster centroids.
   * Spatial visualization of clustered and anomalous vessels.

### Manual UI Configuration (Fallback)

If manual dashboard creation is preferred:

1. Navigate to **Data ➔ Databases ➔ + Database**:

   * Connection: PostgreSQL
   * URI: `postgresql+psycopg2://maritime:maritime@postgis:5432/maritime`
   * Display Name: `Maritime PostGIS`

2. Navigate to **Data ➔ Datasets ➔ + Dataset**:

   * Add `v_active_fleet_state`
   * Add `fleet_daily_kpis`
   * Add `port_dwell_times`
   * Add `route_density_grid`
   * Add `vessel_speed_alerts`
   * Add `vessel_behavior_clusters`

3. Build charts using **Deck.gl Scatterplot** (using `lon` and `lat` columns) or standard **Table** / **ECharts Bar** views, and assemble them into dashboards.

---

## 🧠 Layer 5: Analytics & AI — Spark MLlib K-Means

Layer 5 delivers automated vector-based clustering and anomaly detection across daily historical AIS partitions, profiling vessel kinematic behaviors and identifying anomalous navigation patterns.

### 1. Machine Learning Architecture

The production ML pipeline uses **PySpark MLlib K-Means**.

The Spark ML pipeline:

1. Reads historical AIS Parquet partitions from HDFS.
2. Extracts vessel behavior features such as:

   * Speed Over Ground (SOG)
   * Course Over Ground (COG)
   * Heading
   * Latitude
   * Longitude
3. Uses **VectorAssembler** to combine the selected features into a feature vector.
4. Uses **StandardScaler** to normalize the feature vector.
5. Trains a **K-Means clustering model** with `k=5`.
6. Assigns each vessel to its nearest cluster.
7. Calculates the Euclidean distance between each vessel and its assigned cluster centroid.
8. Calculates cluster-specific 95th percentile thresholds.
9. Flags vessels exceeding the corresponding threshold as potential anomalies.
10. Persists the trained model artifacts to HDFS.
11. Writes the clustering and anomaly results to PostGIS.

### 2. K-Means Clustering

**Algorithm:** K-Means Clustering
**Framework:** PySpark MLlib
**Learning Type:** Unsupervised Learning

K-Means groups vessels according to similarities in their selected behavioral and movement features.

The model uses:

* `VectorAssembler`
* `StandardScaler`
* `pyspark.ml.clustering.KMeans`

The number of clusters is configured as:

```text
k = 5
```

The model is trained against the distributed AIS dataset using the Spark cluster.

### 3. Anomaly Detection

Anomaly detection is performed using the vessel's distance from its assigned cluster centroid.

For each cluster:

```text
Anomaly Threshold = 95th Percentile of Distance to Centroid
```

A vessel is marked as an anomaly when its distance exceeds the threshold for its assigned cluster.

This provides a scalable approach to identifying unusual vessel behavior without requiring pre-labeled training data.

### 4. Automated Execution

#### Standalone Manual Execution

```bash
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=1g \
  /opt/spark-apps/mahout/spark_mahout_clustering.py --exec-date 2024-12-25 --k 5 --anomaly-threshold-pct 95
```

### 5. Airflow ML Pipeline

The clustering job is integrated into the `maritime_batch_kpi_pipeline` DAG.

The pipeline executes:

```text
check_hdfs_partition_exists
    ->
run_batch_kpi_job
    ->
run_spark_ml_clustering_job
    ->
verify_postgres_rows_written
    ->
pipeline_health_check
```

To trigger the pipeline for an archived date partition:

```bash
docker exec airflow-scheduler airflow dags trigger maritime_batch_kpi_pipeline -e 2024-12-28
```

### 6. Output Artifact Locations

* **Trained Spark ML Model:** `hdfs://namenode:9000/models/vessel_clustering/date=YYYY-MM-DD/`
* **PostGIS Analytical Serving Table:** `vessel_behavior_clusters`

---

## 🗄️ PostGIS Database Schema & Idempotency

Table DDL (`db/init/04_vessel_behavior_clusters.sql`):

```sql
CREATE TABLE IF NOT EXISTS vessel_behavior_clusters (
    id BIGSERIAL PRIMARY KEY,
    kpi_date DATE NOT NULL,
    mmsi BIGINT NOT NULL,
    cluster_id INTEGER NOT NULL,
    lat NUMERIC,
    lon NUMERIC,
    sog_knots NUMERIC,
    cog_degrees NUMERIC,
    distance_to_centroid NUMERIC,
    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    geom GEOMETRY(Point, 4326),
    model_run_ts TIMESTAMP NOT NULL,
    UNIQUE (kpi_date, mmsi, model_run_ts)
);
```

* **True Idempotency:** The Spark job performs an explicit `DELETE FROM vessel_behavior_clusters WHERE kpi_date = :exec_date` before appending rows. Re-running the pipeline on the same date will not duplicate rows even if `model_run_ts` changes across retries.
* **Spatial Geometry:** Point geometries are populated using `ST_SetSRID(ST_MakePoint(lon, lat), 4326)` (longitude first) and indexed via GIST (`idx_vbc_geom`).

---

## 📈 Machine Learning Demo SQL Queries

### Query 1: Cluster Distribution & Anomaly Rate

```sql
docker compose exec postgis psql -U maritime -d maritime -c "
SELECT
    cluster_id,
    COUNT(*) AS total_vessels,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count,
    ROUND(AVG(sog_knots), 2) AS avg_sog_knots,
    ROUND(AVG(distance_to_centroid), 4) AS avg_distance
FROM vessel_behavior_clusters
WHERE kpi_date = '2024-12-25'
GROUP BY cluster_id
ORDER BY cluster_id;
"
```

### Query 2: Top Anomalous Vessels

```sql
docker compose exec postgis psql -U maritime -d maritime -c "
SELECT
    mmsi,
    cluster_id,
    sog_knots,
    cog_degrees,
    ROUND(distance_to_centroid, 4) AS distance_to_centroid,
    ST_AsText(geom) AS point_geometry
FROM vessel_behavior_clusters
WHERE kpi_date = '2024-12-25' AND is_anomaly = TRUE
ORDER BY distance_to_centroid DESC
LIMIT 5;
"
```

### Query 3: Anomaly Verification Joined to Active Fleet State

```sql
docker compose exec postgis psql -U maritime -d maritime -c "
SELECT
    c.mmsi,
    a.vessel_name,
    c.cluster_id,
    c.sog_knots AS cluster_sog,
    c.distance_to_centroid,
    c.is_anomaly
FROM vessel_behavior_clusters c
LEFT JOIN active_fleet_state a ON c.mmsi = a.mmsi
WHERE c.kpi_date = '2024-12-25' AND c.is_anomaly = TRUE
LIMIT 5;
"
```
## Contributors

- **Mohamed Hany Hamouda**
- **Mahmoud Gad**
- **Moustafa Mortada**
- **Youssef Fetyan**
- **Saleh Osama**

- ## Author

**ENG. Mohamed Hany Hamouda**

Computer Science Student — Data Science Major

- GitHub: [M-H26](https://github.com/M-H26)
