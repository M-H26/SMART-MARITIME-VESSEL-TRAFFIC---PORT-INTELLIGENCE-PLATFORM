# SMART-MARITIME-VESSEL-TRAFFIC---PORT-INTELLIGENCE-PLATFORM
# 🚢 Smart Maritime Vessel Traffic & Port Intelligence Platform

A distributed, real-time Big Data and AI-driven architecture designed to ingest, process, store, analyze, and visualize global maritime AIS (Automatic Identification System) vessel telemetry and port congestion analytics.

---

## 📌 Key Architectural Highlights

* **Real Global AIS Telemetry:** High-throughput streaming integration (no simulated data).
* **Pure Streaming Ingest:** Handles 50–300 messages/second continuous live telemetry.
* **Spatial Indexing:** Accelerated spatial lookups via PostGIS GIST indexing on all vessel paths.
* **Distributed Machine Learning:** Scalable Clustering and Anomaly Detection powered by Apache Mahout / Spark MLlib.
* **Interactive Dashboards:** Real-time visualization and spatial analytics powered by Apache Superset.
* **Geofencing & Alerts:** Real-time collision risk assessment and geofencing breach alerts.
* **Containerized Deployment:** Fully orchestrated using Docker Compose.

---

## 🏗️ Architecture Overview

```mermaid
flowchart LR
    subgraph INGEST["1. Ingestion & Streaming (Phase 1)"]
        direction TB
        PROD["AIS Producer<br/>(Historical NOAA / Live)"] -->|raw_ais_positions| KAFKA["Apache Kafka<br/>(KRaft Broker)"]
        KAFKA -->|Kafka Stream| SPARK_STR["Spark Structured Streaming<br/>(streaming_maritime_processor.py)"]
        SPARK_STR -->|Sink A: UPSERT| PG_ACTIVE[("PostGIS<br/>active_fleet_state")]
        SPARK_STR -->|Sink B: Speed Alerts| KAFKA_ALERTS["Kafka Topic<br/>vessel_speed_alerts"]
        SPARK_STR -->|Sink C: Parquet Archive| HDFS_RAW[("HDFS /raw/ais_historical/<br/>date=YYYY-MM-DD/")]
        SPARK_STR -->|Sink D: Rejects| HDFS_REJ[("HDFS /rejected/<br/>ais_streaming")]
    end

    subgraph BATCH["2. Batch Analytics & Orchestration (Phase 2)"]
        direction TB
        AIRFLOW["Apache Airflow<br/>(maritime_batch_kpi_pipeline)"] -->|1. Partition Check| HDFS_RAW
        AIRFLOW -->|2. spark-submit (Batch KPIs)| SPARK_BATCH["Spark Batch KPI Processor<br/>(batch_port_kpi_processor.py)"]
        HDFS_RAW -->|Historical Partition Read| SPARK_BATCH
        PG_REF[("PostGIS<br/>port_reference")] -->|Port Centroids| SPARK_BATCH
        SPARK_BATCH -->|Port Dwell Times| PG_DWELL[("PostGIS<br/>port_dwell_times")]
        SPARK_BATCH -->|Fleet Speed KPIs| PG_KPIS[("PostGIS<br/>fleet_daily_kpis")]
        SPARK_BATCH -->|Route Density Grid| PG_GRID[("PostGIS<br/>route_density_grid")]
        SPARK_BATCH -->|Materialized Alerts| PG_ALERTS[("PostGIS<br/>vessel_speed_alerts")]
    end

    subgraph LAYER5["3. Analytics & AI Layer (Layer 5)"]
        direction TB
        AIRFLOW -->|3. spark-submit (Clustering)| SPARK_ML["PySpark MLlib KMeans<br/>(spark_mahout_clustering.py)"]
        HDFS_RAW -->|StandardScaler & VectorAssembler| SPARK_ML
        SPARK_ML -->|Persist Model Artifacts| HDFS_MODELS[("HDFS /models/<br/>vessel_clustering/date=YYYY-MM-DD/")]
        SPARK_ML -->|Clustered Vessels & Anomalies| PG_CLUSTERS[("PostGIS<br/>vessel_behavior_clusters")]
        AIRFLOW -->|4. Verify Row Counts| PG_CLUSTERS

        MAHOUT_ZEPPELIN["Mahout / Zeppelin Environment<br/>(Interactive Exploration :8091)"] -.->|Ad-hoc Samsara DSL Matrix Ops| HDFS_RAW
        MAHOUT_ZEPPELIN -.->|Cluster Validation| HDFS_MODELS
    end

    subgraph SERVE["4. Visualization & BI Layer"]
        direction TB
        PG_ACTIVE --> SUPERSET["Apache Superset<br/>(Interactive BI Dashboards)"]
        PG_DWELL --> SUPERSET
        PG_KPIS --> SUPERSET
        PG_GRID --> SUPERSET
        PG_ALERTS --> SUPERSET
        PG_CLUSTERS --> SUPERSET
    end
```

The platform operates across a 6-layer data processing pipeline, orchestrated end-to-end via **Apache Airflow**:
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
* **Machine Learning Engine:** Apache Mahout / Spark MLlib.
* **Models & Algorithms:**
  * Vector-Based Clustering (K-Means / Fuzzy K-Means for vessel behavior profiling).
  * Classification & Anomaly Detection (Distributed scoring for vessel trajectory analysis).
* **Operational Analytics:** Port turnaround efficiency, choke-point congestion metrics, berth utilization trends.

### 6. Visualization Layer
* **Apache Superset:** Interactive BI Dashboards, geospatial map overlays, speed/heading time-series, port wait-time KPIs, and geofence breach alert tables.

---

## 🧰 Tech Stack & Tools

* **Languages:** Python, SQL
* **Streaming & Ingestion:** Apache Kafka, WebSockets
* **Big Data Processing:** Apache Spark (Spark Streaming)
* **Storage & Data Lake:** Hadoop HDFS, PostgreSQL, PostGIS
* **Analytics & Machine Learning:** Apache Mahout / Spark MLlib
* **Orchestration:** Apache Airflow
* **Visualization:** Apache Superset
* **Infrastructure:** Docker, Docker Compose

---

## ⚡ Workflow Orchestration (Apache Airflow)

The platform utilizes Airflow DAGs to coordinate scheduled dependencies and pipeline monitoring:

1. **Scheduled Ingest:** Trigger batch processing jobs.
2. **Spark Spatial Windowing:** Windowed transformations on spatial streams.
3. **Mahout Batch Model Training:** Periodic retrains on historical AIS logs.
4. **Update PostGIS Spatial Tables:** Push aggregated analytics to serving layer.
5. **Alert Dispatch:** Trigger real-time notifications for collision/geofence alerts.

---

## 🚀 Quickstart & Deployment

### 📋 Prerequisites
* **Docker & Docker Compose**: Docker Engine v24.0+ / Docker Compose v2.20+.
* **Host Operating System**: Windows with PowerShell 5.1+ / PowerShell 7 (or Linux/macOS with equivalent shell commands).
* **System Hardware**: Minimum recommended **16 GB RAM** and 4+ CPU cores to comfortably host all services:
  - Spark Standalone Cluster (Master: 1 GB, Worker: 3 GB)
  - Kafka Broker & KRaft Controller: 1.5 GB
  - PostGIS Spatial Database: 1 GB
  - Apache Airflow (Scheduler, Webserver, Postgres): 2 GB
  - Apache Superset: 2 GB
  - Hadoop HDFS (NameNode, DataNode): 2 GB

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
   - Loops up to 30 times (with 2-second intervals) checking PostGIS readiness via `pg_isready -U maritime -d maritime`.
   - Polls Kafka broker status via `/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092`.
4. **HDFS SafeMode Bypass**: Automatically disengages HDFS SafeMode (`hdfs dfsadmin -safemode leave`) so HDFS writes are immediately accepted.
5. **Driver Injection**: Installs the pure-Python `pg8000` database driver into `spark-master` and `spark-worker` (`pip3 install --no-cache-dir pg8000`) ensuring Spark executors can write to PostGIS without native C-library conflicts.
6. **Active Endpoint Summary**: Outputs web URLs for all active web interfaces:
   - Kafka UI: `http://localhost:8090`
   - Spark Master UI: `http://localhost:8080`
   - Spark Worker UI: `http://localhost:8081`
   - HDFS NameNode UI: `http://localhost:9870`
   - Apache Airflow: `http://localhost:8082` (`admin` / `admin`)
   - Apache Superset: `http://localhost:8089` (`admin` / `admin`)
   - Jupyter Lab: `http://localhost:8888` (token: `lab`)

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
- `--exec-date YYYY-MM-DD`: Target HDFS partition date (defaults to yesterday).
- `--port-radius-nm 3.0`: Port catchment radius in nautical miles (default: 3.0).
- `--grid-cell-deg 0.05`: Spatial route density grid cell resolution in degrees (default: 0.05).

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
   - Configured with a 30-second auto-refresh interval.
   - Geospatial Scatterplot displaying active vessels based on `v_active_fleet_state`.
   - Live Active Vessels Roster and headline fleet count.
2. **Speed Alerts & Safety Violations** (`/superset/dashboard/speed-alerts-safety-violations/`):
   - Tabular audit log of vessels exceeding 20.0 knots.
   - Breakdown of top speeding vessels by MMSI and maximum recorded speed.
   - Total violation count metric.
3. **Port Congestion & Operational Metrics** (`/superset/dashboard/port-congestion-operational-metrics/`):
   - Port dwell distribution (average and maximum turnaround time per port).
   - Speed profiles grouped by commercial vessel classification.
   - High-density traffic route cells ranked by AIS ping frequency.

### Manual UI Configuration (Fallback)
If manual dashboard creation is preferred:
1. Navigate to **Data ➔ Databases ➔ + Database**:
   - Connection: PostgreSQL
   - URI: `postgresql+psycopg2://maritime:maritime@postgis:5432/maritime`
   - Display Name: `Maritime PostGIS`
2. Navigate to **Data ➔ Datasets ➔ + Dataset**:
   - Add `v_active_fleet_state`, `fleet_daily_kpis`, `port_dwell_times`, `route_density_grid`, and `vessel_speed_alerts`.
3. Build charts using **Deck.gl Scatterplot** (using `lon` and `lat` columns) or standard **Table** / **ECharts Bar** views, and assemble into dashboards.

---

## 🧠 Layer 5: Analytics & AI (Vessel Behavior Clustering)

Layer 5 delivers automated vector-based clustering and anomaly detection across daily historical AIS partitions, profiling vessel kinematic behaviors and isolating anomalous navigation fixes.

### 1. Architectural Design & Trade-Off Clarification

The production architecture implements a dual-path design:
1. **Automated Production Pipeline (PySpark MLlib):**
   - Classic Apache Mahout batch CLI commands (`mahout kmeans`, `mahout fkmeans`) are legacy MapReduce jobs designed for Hadoop `SequenceFile<Text, VectorWritable>` input. In modern cloud and on-premise big data platforms, running MapReduce SequenceFile conversion inside an Airflow-orchestrated Spark 3.3.0 environment is fragile, slow, and operationally unsound.
   - Therefore, the production automated batch path executes **PySpark MLlib K-Means** (`pyspark.ml.clustering.KMeans`), submitted via `spark-submit` against the standalone `spark-master` cluster. It natively reads Parquet from HDFS, normalizes multi-dimensional features using `StandardScaler`, fits K-Means ($k=5$), computes Euclidean distance to centroid in scaled feature space, evaluates per-cluster 95th percentile anomaly thresholds, persists model artifacts to HDFS (`/models/vessel_clustering/date=YYYY-MM-DD/`), and writes results idempotently to PostGIS (`vessel_behavior_clusters`).
2. **Interactive Apache Mahout Environment (Apache Zeppelin):**
   - The `mahout` service (`apache/mahout-zeppelin:14.1`) provides an interactive Zeppelin notebook server.
   - Data scientists can explore Mahout Samsara linear algebra DSL operations, perform ad-hoc matrix transformations, validate cluster distributions, and inspect HDFS partitions without burdening the unattended daily batch pipeline.

### 2. Zeppelin Interactive UI & Host Port Remap

* **Zeppelin Web UI:** `http://localhost:8091`
* **Host Port Mapping:** `8091:8080`
  > [!NOTE]
  > Host port `8080` is bound to the `spark-master` Web UI, and host port `8090` is bound to `kafka-ui`. Therefore, Zeppelin was mapped to `8091:8080` to prevent port collisions while ensuring full host accessibility.
* **Persisted Volumes:**
  - `./mahout:/opt/mahout_workspace`: Scripts and workspace files.
  - `./mahout/zeppelin_notebooks:/zeppelin/notebook`: Persisted Zeppelin notebook definitions (including preloaded `Using Mahout_2BYEZ5EVK.zpln`).

### 3. Automated & Manual Execution

#### Standalone Manual Execution (Outside Airflow)
```bash
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=1g \
  /opt/spark-apps/mahout/spark_mahout_clustering.py --exec-date 2024-12-25 --k 5 --anomaly-threshold-pct 95
```

#### Orchestrated Airflow Execution
The clustering pipeline is task `run_mahout_clustering_job` inside DAG `maritime_batch_kpi_pipeline`:
```
check_hdfs_partition_exists
    -> run_batch_kpi_job
    -> run_mahout_clustering_job
    -> verify_postgres_rows_written
    -> pipeline_health_check
```
To trigger for an archived date partition:
```bash
docker exec airflow-scheduler airflow dags trigger maritime_batch_kpi_pipeline -e 2024-12-28
```

#### Output Artifact Locations
- **Trained Model Artifacts (HDFS):** `hdfs://namenode:9000/models/vessel_clustering/date=YYYY-MM-DD/`
- **PostGIS Analytical Serving Table:** `vessel_behavior_clusters`

### 4. PostGIS Database Schema & Idempotency

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

### 5. Demo SQL Queries

#### Query 1: Cluster Distribution & Anomaly Rate
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

#### Query 2: Top Anomalous Vessels
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

#### Query 3: Anomaly Verification Joined to Active Fleet State
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

