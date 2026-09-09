"""
Smart Maritime Vessel Traffic & Port Intelligence Platform
Superset Automated Provisioning & Dashboard Hydration Engine
============================================================
Automates complete provisioning of:
  1. PostGIS Database Connection ('Maritime PostGIS')
  2. 6 Analytical Datasets:
     - vessel_behavior_clusters (Layer 5 K-Means clusters & anomaly fixes)
     - fleet_daily_kpis (Speed metrics & ping counts by vessel type)
     - port_dwell_times (Harbor dwell events & durations)
     - active_fleet_state (Real-time fleet snapshot)
     - v_active_fleet_state (Spatial map helper view with lat/lon)
     - vessel_speed_alerts (Safety threshold violations > 20 kts)
  3. Core Production Visualizations:
     - Chart 1: K-Means Behavior Cluster Distribution & Anomaly Breakdown (Pie & Table)
     - Chart 2: Live Fleet Spatial Geospatial Plot (Deck.gl Scatterplot)
     - Chart 3: Port Dwell Times per Port (Distribution Bar Chart)
     - Chart 4: Daily Fleet Speed & Ping Metrics (Table)
     - Headline Big Number KPIs: Active Vessels, Total Anomalies, Total Violations
  4. Unified Executive Dashboard:
     - "Maritime Fleet Operations & AI Analytics" (slug: maritime-fleet-operations-ai-analytics)
     Plus individual domain dashboards for specialized monitoring.
"""

from __future__ import annotations

import json
import logging
import os
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("setup_superset")

try:
    from superset.app import create_app
    app = create_app()
    app.app_context().push()
except Exception as exc:
    log.error("Failed to initialize Superset app context: %s", exc)
    sys.exit(1)

from superset import db, security_manager
from superset.connectors.sqla.models import SqlaTable
from superset.models.core import Database
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice

# Database connection credentials
POSTGIS_HOST = os.environ.get("POSTGIS_HOST", "postgis")
POSTGIS_PORT = os.environ.get("POSTGIS_PORT", "5432")
POSTGIS_DB = os.environ.get("POSTGIS_DB", "maritime")
POSTGIS_USER = os.environ.get("POSTGIS_USER", "maritime")
POSTGIS_PASSWORD = os.environ.get("POSTGIS_PASSWORD", "maritime")

SQLALCHEMY_URI = f"postgresql+psycopg2://{POSTGIS_USER}:{POSTGIS_PASSWORD}@{POSTGIS_HOST}:{POSTGIS_PORT}/{POSTGIS_DB}"
DB_NAME = "Maritime PostGIS"

# Free public basemap provider requiring zero API keys (CartoDB Positron)
MAPBOX_STYLE = os.environ.get(
    "MAPBOX_STYLE",
    "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
)


def setup_database() -> Database:
    """Creates or updates the Maritime PostGIS database connection."""
    print(f"[Superset-Setup] Configuring Database Connection: '{DB_NAME}' -> {SQLALCHEMY_URI}", flush=True)
    database = db.session.query(Database).filter_by(database_name=DB_NAME).first()
    if not database:
        database = Database(database_name=DB_NAME)
        db.session.add(database)

    try:
        database.set_sqlalchemy_uri(SQLALCHEMY_URI)
    except Exception:
        database.sqlalchemy_uri = SQLALCHEMY_URI
    database.password = POSTGIS_PASSWORD
    database.allow_run_async = True
    database.allow_ctas = True
    database.allow_cvas = True
    database.allow_dml = True
    db.session.commit()

    # Verify connection
    try:
        conn = database._get_sqla_engine().connect()
        conn.close()
        print(f"[Superset-Setup] Database connection to '{DB_NAME}' verified successfully!", flush=True)
    except Exception as exc:
        print(f"[Superset-Setup] Failed to connect to '{DB_NAME}': {exc}", flush=True)
        raise

    return database


def setup_datasets(database: Database, owners: list) -> dict[str, SqlaTable]:
    """Registers the 5 core analytical tables and views as Superset datasets."""
    tables_to_register = [
        "vessel_behavior_clusters",
        "fleet_daily_kpis",
        "port_dwell_times",
        "active_fleet_state",
        "v_active_fleet_state",
        "vessel_speed_alerts",
        "route_density_grid",
        "port_reference",
    ]
    datasets = {}
    for tbl_name in tables_to_register:
        print(f"[Superset-Setup] Registering dataset: {tbl_name}...", flush=True)
        dataset = (
            db.session.query(SqlaTable)
            .filter_by(database_id=database.id, table_name=tbl_name)
            .first()
        )
        if not dataset:
            dataset = SqlaTable(table_name=tbl_name, database=database)
            db.session.add(dataset)
            db.session.commit()

        dataset.owners = owners
        try:
            dataset.fetch_metadata()
            db.session.commit()
            print(f"[Superset-Setup]   -> Dataset '{tbl_name}' registered with {len(dataset.columns)} columns.", flush=True)
        except Exception as exc:
            print(f"[Superset-Setup]   -> Warning fetching metadata for '{tbl_name}': {exc}", flush=True)

        datasets[tbl_name] = dataset

    return datasets


def get_or_create_slice(
    slice_name: str,
    viz_type: str,
    dataset: SqlaTable,
    params: dict,
    owners: list,
) -> Slice:
    """Creates or updates a Superset chart/slice with admin ownership."""
    chart = (
        db.session.query(Slice)
        .filter_by(slice_name=slice_name, datasource_id=dataset.id, datasource_type="table")
        .first()
    )
    if not chart:
        chart = Slice(
            slice_name=slice_name,
            viz_type=viz_type,
            datasource_type="table",
            datasource_id=dataset.id,
        )
        db.session.add(chart)

    chart.viz_type = viz_type
    chart.params = json.dumps(params)
    chart.owners = owners
    db.session.commit()
    print(f"[Superset-Setup] Chart ready: [{slice_name}] ({viz_type})", flush=True)
    return chart


def build_dashboard_position(
    dashboard_title: str,
    rows: list[list[tuple[Slice, int, int]]],
) -> str:
    """
    Builds a complete, valid Superset position_json v2 layout hierarchy.
    Ensures ROOT_ID, GRID_ID, ROW-x, and CHART-x nodes have consistent metadata.
    """
    grid_children = []
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {
            "type": "ROOT",
            "id": "ROOT_ID",
            "children": ["GRID_ID"],
        },
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": grid_children,
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {"text": dashboard_title},
        },
    }

    for row_idx, row_slices in enumerate(rows, start=1):
        row_id = f"ROW-{row_idx}"
        grid_children.append(row_id)
        chart_ids = []

        for s, width, height in row_slices:
            chart_id = f"CHART-{s.id}"
            chart_ids.append(chart_id)
            position[chart_id] = {
                "type": "CHART",
                "id": chart_id,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {
                    "chartId": s.id,
                    "width": width,
                    "height": height,
                    "sliceName": s.slice_name,
                    "uuid": str(s.uuid) if getattr(s, "uuid", None) else "",
                },
            }

        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": chart_ids,
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {
                "0": "ROOT_ID",
                "background": "BACKGROUND_TRANSPARENT",
            },
        }

    return json.dumps(position)


def setup_dashboards(datasets: dict[str, SqlaTable], owners: list) -> None:
    """Creates the unified production dashboard and specialized domain dashboards."""

    fleet_ds = datasets["v_active_fleet_state"]
    cluster_ds = datasets["vessel_behavior_clusters"]
    dwell_ds = datasets["port_dwell_times"]
    kpi_ds = datasets["fleet_daily_kpis"]
    alerts_ds = datasets["vessel_speed_alerts"]
    grid_ds = datasets["route_density_grid"]

    # =========================================================================
    # CORE CHARTS CREATION
    # =========================================================================

    # Headline Big Numbers
    kpi_active_vessels = get_or_create_slice(
        slice_name="Active Tracked Vessels",
        viz_type="big_number_total",
        dataset=fleet_ds,
        params={
            "datasource": f"{fleet_ds.id}__table",
            "viz_type": "big_number_total",
            "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(DISTINCT mmsi)", "label": "Active Vessels"},
            "subheader": "Real-time Live Pings in PostGIS",
        },
        owners=owners,
    )

    kpi_total_anomalies = get_or_create_slice(
        slice_name="AI Behavioral Anomalies",
        viz_type="big_number_total",
        dataset=cluster_ds,
        params={
            "datasource": f"{cluster_ds.id}__table",
            "viz_type": "big_number_total",
            "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(*) FILTER (WHERE is_anomaly)", "label": "Flagged Anomalies"},
            "subheader": "Layer 5 K-Means > 95th Percentile Distance",
        },
        owners=owners,
    )

    kpi_total_alerts = get_or_create_slice(
        slice_name="Speeding Violations",
        viz_type="big_number_total",
        dataset=alerts_ds,
        params={
            "datasource": f"{alerts_ds.id}__table",
            "viz_type": "big_number_total",
            "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Speed Violations"},
            "subheader": "Vessels Exceeding 20.0 Knots",
        },
        owners=owners,
    )

    # Chart 1: K-Means Behavior Cluster Distribution & Anomaly Breakdown
    chart1_cluster_pie = get_or_create_slice(
        slice_name="K-Means Behavior Cluster Distribution",
        viz_type="pie",
        dataset=cluster_ds,
        params={
            "datasource": f"{cluster_ds.id}__table",
            "viz_type": "pie",
            "groupby": ["cluster_id"],
            "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Vessel Count"},
            "row_limit": 10,
            "show_legend": True,
            "donut": True,
            "labels_outside": True,
            "color_scheme": "supersetColors",
        },
        owners=owners,
    )

    chart1_cluster_table = get_or_create_slice(
        slice_name="Cluster Behavior & Anomaly Breakdown",
        viz_type="table",
        dataset=cluster_ds,
        params={
            "datasource": f"{cluster_ds.id}__table",
            "viz_type": "table",
            "groupby": ["cluster_id"],
            "metrics": [
                {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Vessels"},
                {"expressionType": "SQL", "sqlExpression": "COUNT(*) FILTER (WHERE is_anomaly)", "label": "Anomalies Flagged"},
                {"expressionType": "SQL", "sqlExpression": "ROUND(AVG(sog_knots), 2)", "label": "Avg Speed (kts)"},
                {"expressionType": "SQL", "sqlExpression": "ROUND(AVG(distance_to_centroid), 4)", "label": "Avg Centroid Dist"},
            ],
            "order_by_cols": ['["cluster_id", true]'],
            "row_limit": 10,
        },
        owners=owners,
    )

    # Chart 2: Live Fleet Spatial Geospatial Plot
    chart2_spatial_map = get_or_create_slice(
        slice_name="Live Fleet Geospatial Position Plot",
        viz_type="deck_scatter",
        dataset=fleet_ds,
        params={
            "datasource": f"{fleet_ds.id}__table",
            "viz_type": "deck_scatter",
            "spatial": {"type": "latlong", "lonCol": "lon", "latCol": "lat"},
            "row_limit": 10000,
            "filter_nulls": True,
            "point_radius_fixed": {"type": "fix", "value": 15},
            "point_unit": "pixels",
            "point_radius": 15,
            "point_color": {"r": 30, "g": 144, "b": 255, "a": 0.85},
            "viewport_zoom": 5,
            "viewport_latitude": 29.8,
            "viewport_longitude": -95.1,
            "mapbox_style": MAPBOX_STYLE,
            "deck_slices": [],
        },
        owners=owners,
    )

    # Chart 3: Port Dwell Times per Port
    chart3_dwell_bars = get_or_create_slice(
        slice_name="Port Dwell Times by Harbor",
        viz_type="dist_bar",
        dataset=dwell_ds,
        params={
            "datasource": f"{dwell_ds.id}__table",
            "viz_type": "dist_bar",
            "groupby": ["port_id"],
            "metrics": [
                {"expressionType": "SQL", "sqlExpression": "ROUND(AVG(dwell_minutes), 1)", "label": "Avg Dwell (mins)"},
                {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Dwell Events"},
            ],
            "bar_stacked": False,
            "order_bars": True,
            "row_limit": 20,
        },
        owners=owners,
    )

    # Chart 4: Daily Fleet Speed & Ping Metrics
    chart4_speed_metrics = get_or_create_slice(
        slice_name="Daily Fleet Speed & Ping Metrics",
        viz_type="table",
        dataset=kpi_ds,
        params={
            "datasource": f"{kpi_ds.id}__table",
            "viz_type": "table",
            "groupby": ["vessel_type"],
            "metrics": [
                {"expressionType": "SQL", "sqlExpression": "SUM(ping_count)", "label": "Total Pings"},
                {"expressionType": "SQL", "sqlExpression": "ROUND(AVG(avg_sog), 2)", "label": "Avg Speed (kts)"},
                {"expressionType": "SQL", "sqlExpression": "MAX(max_sog)", "label": "Max Speed (kts)"},
            ],
            "order_by_cols": ['["SUM(ping_count)", false]'],
            "row_limit": 50,
        },
        owners=owners,
    )

    # Additional Auxiliary Slices
    active_vessels_roster = get_or_create_slice(
        slice_name="Active Vessels Roster",
        viz_type="table",
        dataset=fleet_ds,
        params={
            "datasource": f"{fleet_ds.id}__table",
            "viz_type": "table",
            "all_columns": ["mmsi", "vessel_name", "sog_knots", "cog_degrees", "nav_status", "last_updated"],
            "order_by_cols": ['["last_updated", false]'],
            "row_limit": 100,
        },
        owners=owners,
    )

    speed_alerts_log = get_or_create_slice(
        slice_name="Speed Violation Events Log",
        viz_type="table",
        dataset=alerts_ds,
        params={
            "datasource": f"{alerts_ds.id}__table",
            "viz_type": "table",
            "all_columns": ["detected_at", "mmsi", "vessel_name", "sog_knots", "threshold_knots", "lat", "lon"],
            "order_by_cols": ['["detected_at", false]'],
            "row_limit": 100,
        },
        owners=owners,
    )

    # =========================================================================
    # PRIMARY PRODUCTION DASHBOARD: "Maritime Fleet Operations & AI Analytics"
    # =========================================================================
    print("\n--- Provisioning Primary Dashboard: Maritime Fleet Operations & AI Analytics ---", flush=True)
    dash_primary = db.session.query(Dashboard).filter_by(slug="maritime-fleet-operations-ai-analytics").first()
    if not dash_primary:
        dash_primary = Dashboard(slug="maritime-fleet-operations-ai-analytics")
        db.session.add(dash_primary)

    dash_primary.dashboard_title = "Maritime Fleet Operations & AI Analytics"
    dash_primary.slices = [
        kpi_active_vessels,
        kpi_total_anomalies,
        kpi_total_alerts,
        chart2_spatial_map,
        chart1_cluster_pie,
        chart3_dwell_bars,
        chart4_speed_metrics,
        chart1_cluster_table,
    ]

    # Grid Layout:
    # Row 1: 3 Headline Big Numbers (width 4 each)
    # Row 2: Geospatial Map (width 7) + K-Means Pie (width 5)
    # Row 3: Port Dwell Times (width 6) + Fleet Speed Metrics (width 6)
    # Row 4: Cluster Behavior & Anomaly Breakdown Table (width 12)
    dash_primary.position_json = build_dashboard_position(
        dash_primary.dashboard_title,
        [
            [(kpi_active_vessels, 4, 26), (kpi_total_anomalies, 4, 26), (kpi_total_alerts, 4, 26)],
            [(chart2_spatial_map, 7, 65), (chart1_cluster_pie, 5, 65)],
            [(chart3_dwell_bars, 6, 55), (chart4_speed_metrics, 6, 55)],
            [(chart1_cluster_table, 12, 45)],
        ],
    )
    dash_primary.json_metadata = json.dumps({
        "refresh_frequency": 30,
        "timed_refresh_immune_slices": [],
        "expanded_slices": {},
        "color_scheme": "supersetColors",
    })
    dash_primary.owners = owners
    dash_primary.published = True
    db.session.commit()
    print(f"[Superset-Setup] Primary Dashboard created: {dash_primary.dashboard_title} (slug={dash_primary.slug}, published={dash_primary.published})", flush=True)

    # =========================================================================
    # SPECIALIZED DOMAIN DASHBOARDS (Preserved for granular triage)
    # =========================================================================
    # 1. Real-time Fleet Tracking
    dash_fleet = db.session.query(Dashboard).filter_by(slug="realtime-fleet-tracking").first()
    if not dash_fleet:
        dash_fleet = Dashboard(slug="realtime-fleet-tracking")
        db.session.add(dash_fleet)
    dash_fleet.dashboard_title = "Real-time Fleet Tracking"
    dash_fleet.slices = [kpi_active_vessels, chart2_spatial_map, active_vessels_roster]
    dash_fleet.position_json = build_dashboard_position(
        dash_fleet.dashboard_title,
        [
            [(kpi_active_vessels, 12, 26)],
            [(chart2_spatial_map, 7, 60), (active_vessels_roster, 5, 60)],
        ],
    )
    dash_fleet.json_metadata = json.dumps({"refresh_frequency": 30, "color_scheme": "supersetColors"})
    dash_fleet.owners = owners
    dash_fleet.published = True

    # 2. Speed Alerts & Safety Violations
    dash_alerts = db.session.query(Dashboard).filter_by(slug="speed-alerts-safety-violations").first()
    if not dash_alerts:
        dash_alerts = Dashboard(slug="speed-alerts-safety-violations")
        db.session.add(dash_alerts)
    dash_alerts.dashboard_title = "Speed Alerts & Safety Violations"
    dash_alerts.slices = [kpi_total_alerts, speed_alerts_log]
    dash_alerts.position_json = build_dashboard_position(
        dash_alerts.dashboard_title,
        [
            [(kpi_total_alerts, 12, 26)],
            [(speed_alerts_log, 12, 60)],
        ],
    )
    dash_alerts.json_metadata = json.dumps({"refresh_frequency": 0, "color_scheme": "supersetColors"})
    dash_alerts.owners = owners
    dash_alerts.published = True

    # 3. Port Congestion & Operational Metrics
    dash_ports = db.session.query(Dashboard).filter_by(slug="port-congestion-operational-metrics").first()
    if not dash_ports:
        dash_ports = Dashboard(slug="port-congestion-operational-metrics")
        db.session.add(dash_ports)
    dash_ports.dashboard_title = "Port Congestion & Operational Metrics"
    dash_ports.slices = [chart3_dwell_bars, chart4_speed_metrics]
    dash_ports.position_json = build_dashboard_position(
        dash_ports.dashboard_title,
        [
            [(chart3_dwell_bars, 6, 55), (chart4_speed_metrics, 6, 55)],
        ],
    )
    dash_ports.json_metadata = json.dumps({"refresh_frequency": 0, "color_scheme": "supersetColors"})
    dash_ports.owners = owners
    dash_ports.published = True

    db.session.commit()

    print("\n" + "=" * 78)
    print(" APACHE SUPERSET PROVISIONING COMPLETE")
    print("=" * 78)
    print(" Primary Production Dashboard:")
    print("  * http://localhost:8089/superset/dashboard/maritime-fleet-operations-ai-analytics/")
    print("\n Specialized Monitoring Dashboards:")
    print("  1. http://localhost:8089/superset/dashboard/realtime-fleet-tracking/")
    print("  2. http://localhost:8089/superset/dashboard/speed-alerts-safety-violations/")
    print("  3. http://localhost:8089/superset/dashboard/port-congestion-operational-metrics/")
    print("=" * 78 + "\n")


def main():
    print("[Superset-Setup] Starting Apache Superset automated provisioning engine...", flush=True)
    admin_user = security_manager.find_user(username="admin")
    owners = [admin_user] if admin_user else []
    print(f"[Superset-Setup] Configured admin ownership: {admin_user}", flush=True)

    database = setup_database()
    datasets = setup_datasets(database, owners)
    setup_dashboards(datasets, owners)


if __name__ == "__main__":
    main()
