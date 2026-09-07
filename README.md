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

### Prerequisites
* Docker & Docker Compose installed on your system.

### Running the Stack
1. Clone the repository:
   ```bash
   git clone [https://github.com/your-username/maritime-intelligence-platform.git](https://github.com/your-username/maritime-intelligence-platform.git)
   cd maritime-intelligence-platform
