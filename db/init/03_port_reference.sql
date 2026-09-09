-- ============================================================================
-- Smart Maritime Vessel Traffic & Port Intelligence Platform
-- Port Reference Table & Centroids
-- ============================================================================

CREATE TABLE IF NOT EXISTS port_reference (
    port_id     VARCHAR(50)     PRIMARY KEY,
    port_name   VARCHAR(100)    NOT NULL,
    geom        GEOMETRY(POINT, 4326) NOT NULL,
    radius_nm   DOUBLE PRECISION NOT NULL DEFAULT 3.0
);

CREATE INDEX IF NOT EXISTS idx_port_reference_geom
    ON port_reference USING GIST (geom);

-- ----------------------------------------------------------------------------
-- Seed Port Centroids:
-- Includes major commercial maritime ports corresponding to the NOAA dataset
-- alongside Mediterranean / Suez Canal reference ports.
--
-- Note: ST_MakePoint(longitude, latitude) -- longitude is X, latitude is Y.
-- ----------------------------------------------------------------------------
INSERT INTO port_reference (port_id, port_name, geom, radius_nm) VALUES
    ('US_HOU', 'Port of Houston', ST_SetSRID(ST_MakePoint(-95.100, 29.800), 4326), 5.0),
    ('US_SAN', 'Port of San Diego', ST_SetSRID(ST_MakePoint(-117.200, 32.700), 4326), 3.5),
    ('US_PEV', 'Port Everglades', ST_SetSRID(ST_MakePoint(-80.120, 26.090), 4326), 3.0),
    ('US_FOU', 'Port Fourchon', ST_SetSRID(ST_MakePoint(-90.200, 29.110), 4326), 4.0),
    ('US_SEA', 'Port of Seattle', ST_SetSRID(ST_MakePoint(-122.350, 47.600), 4326), 3.5),
    ('US_NYC', 'Port of New York & New Jersey', ST_SetSRID(ST_MakePoint(-74.050, 40.670), 4326), 4.5),
    ('US_NOR', 'Port of New Orleans', ST_SetSRID(ST_MakePoint(-90.060, 29.930), 4326), 4.0),
    ('EG_PSD', 'Port Said', ST_SetSRID(ST_MakePoint(32.300, 31.250), 4326), 3.0),
    ('EG_SUZ', 'Suez Port', ST_SetSRID(ST_MakePoint(32.560, 29.970), 4326), 3.0),
    ('EG_ALY', 'Alexandria Port', ST_SetSRID(ST_MakePoint(29.880, 31.190), 4326), 3.0)
ON CONFLICT (port_id) DO UPDATE SET
    port_name = EXCLUDED.port_name,
    geom = EXCLUDED.geom,
    radius_nm = EXCLUDED.radius_nm;
