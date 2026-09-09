-- ============================================================================
-- Smart Maritime Vessel Traffic & Port Intelligence Platform
-- Phase 2 Analytical Tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS port_dwell_times (
    id BIGSERIAL PRIMARY KEY,
    kpi_date DATE NOT NULL,
    mmsi BIGINT NOT NULL,
    port_id VARCHAR NOT NULL,
    entry_ts TIMESTAMP,
    exit_ts TIMESTAMP,
    dwell_minutes NUMERIC,
    UNIQUE (kpi_date, mmsi, port_id, entry_ts)
);

CREATE TABLE IF NOT EXISTS fleet_daily_kpis (
    id BIGSERIAL PRIMARY KEY,
    kpi_date DATE NOT NULL,
    vessel_type VARCHAR NOT NULL,
    avg_sog NUMERIC,
    min_sog NUMERIC,
    max_sog NUMERIC,
    stddev_sog NUMERIC,
    ping_count BIGINT,
    UNIQUE (kpi_date, vessel_type)
);

CREATE TABLE IF NOT EXISTS route_density_grid (
    id BIGSERIAL PRIMARY KEY,
    kpi_date DATE NOT NULL,
    grid_lat NUMERIC NOT NULL,
    grid_lon NUMERIC NOT NULL,
    ping_count BIGINT,
    UNIQUE (kpi_date, grid_lat, grid_lon)
);

CREATE TABLE IF NOT EXISTS vessel_speed_alerts (
    id BIGSERIAL PRIMARY KEY,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    mmsi BIGINT NOT NULL,
    vessel_name VARCHAR(100),
    sog_knots NUMERIC,
    lat NUMERIC,
    lon NUMERIC,
    nav_status VARCHAR(50),
    alert_type VARCHAR(50) DEFAULT 'SPEED_VIOLATION',
    threshold_knots NUMERIC DEFAULT 20.0
);

CREATE INDEX IF NOT EXISTS idx_dwell_date ON port_dwell_times(kpi_date);
CREATE INDEX IF NOT EXISTS idx_dwell_mmsi ON port_dwell_times(mmsi);
CREATE INDEX IF NOT EXISTS idx_kpi_date ON fleet_daily_kpis(kpi_date);
CREATE INDEX IF NOT EXISTS idx_grid_date ON route_density_grid(kpi_date);
CREATE INDEX IF NOT EXISTS idx_speed_alerts_detected ON vessel_speed_alerts(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_speed_alerts_mmsi ON vessel_speed_alerts(mmsi);

-- Spatial helper view for Apache Superset map charts
CREATE OR REPLACE VIEW v_active_fleet_state AS
SELECT
    mmsi,
    vessel_name,
    ST_Y(geom) AS lat,
    ST_X(geom) AS lon,
    sog_knots,
    cog_degrees,
    heading_degrees,
    nav_status,
    last_updated
FROM active_fleet_state;

