-- ============================================================================
-- Smart Maritime Vessel Traffic & Port Intelligence Platform
-- Layer 5: Vessel Behavior Clusters & Anomaly Detection
-- ============================================================================
--
-- Table: vessel_behavior_clusters
-- Purpose:
--   Materializes daily K-Means behavioral clustering results and distance-to-centroid
--   anomaly flags computed by the PySpark MLlib / Mahout clustering pipeline.
--
-- Foreign Key Note:
--   A foreign key to active_fleet_state(mmsi) is explicitly omitted because
--   active_fleet_state is a mutable, real-time snapshot table updated every micro-batch,
--   whereas vessel_behavior_clusters is an immutable historical daily analytical log.
-- ============================================================================

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

CREATE INDEX IF NOT EXISTS idx_vbc_date ON vessel_behavior_clusters(kpi_date);
CREATE INDEX IF NOT EXISTS idx_vbc_cluster ON vessel_behavior_clusters(cluster_id);
CREATE INDEX IF NOT EXISTS idx_vbc_anomaly ON vessel_behavior_clusters(is_anomaly) WHERE is_anomaly = TRUE;
CREATE INDEX IF NOT EXISTS idx_vbc_geom ON vessel_behavior_clusters USING GIST(geom);
