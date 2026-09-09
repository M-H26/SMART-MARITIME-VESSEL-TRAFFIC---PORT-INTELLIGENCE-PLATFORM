-- ============================================================================
-- Smart Maritime Vessel Traffic & Port Intelligence Platform
-- PostGIS Serving-Storage Schema
-- ============================================================================
--
-- Team Assignment: Spatial Database Engineer (الفرد الرابع)
--
-- This script runs automatically on first container initialisation
-- (mounted at /docker-entrypoint-initdb.d/). It only fires against an
-- EMPTY data volume — delete the postgis_data Docker volume to re-run.
--
-- ============================================================================

-- Enable PostGIS spatial extensions (required — do NOT remove)
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- ============================================================================
-- SERVING LAYER TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. active_fleet_state
--    Latest known position and speed per vessel.
--    Upserted by the Spark streaming job (streaming_maritime_processor.py)
--    every micro-batch.  One row per MMSI -- always the most recent fix.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS active_fleet_state (
    mmsi             BIGINT          PRIMARY KEY,
    vessel_name      VARCHAR(100),
    -- Geometry stored as POINT (WGS 84 / SRID 4326).
    -- NOTE: PostGIS convention is ST_MakePoint(longitude, latitude) --
    -- longitude FIRST.  The Spark job enforces this ordering explicitly.
    geom             GEOMETRY(POINT, 4326),
    sog_knots        DOUBLE PRECISION,
    cog_degrees      DOUBLE PRECISION,
    heading_degrees  DOUBLE PRECISION,
    nav_status       VARCHAR(50),
    last_updated     TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Spatial index for map queries and geofence intersection checks
CREATE INDEX IF NOT EXISTS idx_active_fleet_state_geom
    ON active_fleet_state USING GIST (geom);

-- B-tree index for time-based staleness queries
CREATE INDEX IF NOT EXISTS idx_active_fleet_state_last_updated
    ON active_fleet_state (last_updated DESC);

-- ----------------------------------------------------------------------------
-- 2. geofence_boundaries
--    Port / anchorage / restricted-zone polygons for geofencing.
--    Seeded with sample Egyptian ports below.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS geofence_boundaries (
    id           SERIAL          PRIMARY KEY,
    port_name    VARCHAR(100)    NOT NULL,
    zone_type    VARCHAR(50)     NOT NULL DEFAULT 'port',
    country_code VARCHAR(10)     NOT NULL DEFAULT 'EG',
    geom         GEOMETRY(POLYGON, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_geofence_boundaries_geom
    ON geofence_boundaries USING GIST (geom);

-- Seed: approximate bounding polygons for major Egyptian ports
-- (rectangles for simplicity; replace with precise geometries for production)
INSERT INTO geofence_boundaries (port_name, zone_type, country_code, geom) VALUES
(
    'Port Said',
    'port',
    'EG',
    ST_GeomFromText(
        'POLYGON((32.25 31.20, 32.35 31.20, 32.35 31.30, 32.25 31.30, 32.25 31.20))',
        4326
    )
),
(
    'Suez Port',
    'port',
    'EG',
    ST_GeomFromText(
        'POLYGON((32.52 29.95, 32.60 29.95, 32.60 30.05, 32.52 30.05, 32.52 29.95))',
        4326
    )
),
(
    'Alexandria Port',
    'port',
    'EG',
    ST_GeomFromText(
        'POLYGON((29.85 31.17, 29.95 31.17, 29.95 31.23, 29.85 31.23, 29.85 31.17))',
        4326
    )
)
ON CONFLICT DO NOTHING;

-- ----------------------------------------------------------------------------
-- 3. collision_risk_alerts
--    Written by the Spark streaming collision-detection module when two
--    vessels are within a configured CPA (closest point of approach) threshold.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS collision_risk_alerts (
    id               BIGSERIAL       PRIMARY KEY,
    vessel_1_mmsi    BIGINT          NOT NULL,
    vessel_2_mmsi    BIGINT          NOT NULL,
    distance_meters  DOUBLE PRECISION,
    cpa_minutes      DOUBLE PRECISION,
    risk_level       VARCHAR(20)     NOT NULL DEFAULT 'MEDIUM',
    -- Midpoint geometry between the two vessels at detection time
    geom             GEOMETRY(POINT, 4326),
    created_at       TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_collision_risk_alerts_geom
    ON collision_risk_alerts USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_collision_risk_alerts_created_at
    ON collision_risk_alerts (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_collision_risk_alerts_mmsi
    ON collision_risk_alerts (vessel_1_mmsi, vessel_2_mmsi);

-- ----------------------------------------------------------------------------
-- 4. port_congestion
--    Rolling per-port vessel counts and speed statistics.
--    Updated by the Airflow batch step or Spark streaming aggregations.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS port_congestion (
    id                   BIGSERIAL   PRIMARY KEY,
    port_id              INT         NOT NULL REFERENCES geofence_boundaries(id),
    active_vessels_count INT         NOT NULL DEFAULT 0,
    avg_speed_knots      DOUBLE PRECISION,
    congestion_risk      VARCHAR(20) NOT NULL DEFAULT 'LOW',
    calculated_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_port_congestion_port_id
    ON port_congestion (port_id, calculated_at DESC);

-- ----------------------------------------------------------------------------
-- 5. port_geofence_events
--    Log of vessel enter / inside / exit events per port zone.
--    Mirrors the Kafka topic port_geofence_events.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS port_geofence_events (
    id          BIGSERIAL   PRIMARY KEY,
    mmsi        BIGINT      NOT NULL,
    port_id     INT         NOT NULL REFERENCES geofence_boundaries(id),
    event_type  VARCHAR(20) NOT NULL,   -- 'ENTER', 'INSIDE', 'EXIT'
    event_time  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_port_geofence_events_mmsi
    ON port_geofence_events (mmsi, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_port_geofence_events_port_id
    ON port_geofence_events (port_id, event_time DESC);
