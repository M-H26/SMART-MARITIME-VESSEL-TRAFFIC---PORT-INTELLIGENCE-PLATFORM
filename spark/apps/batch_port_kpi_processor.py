"""
Smart Maritime Vessel Traffic & Port Intelligence Platform
Phase 2 Batch KPI Processor
============================================================
Batch analytical job calculating:
  1. Port Dwell Times (entry/exit timestamps, dwell duration in minutes)
  2. Fleet Daily KPIs (average, min, max, stddev SOG by vessel type)
  3. Route Density Grid (spatial ping counts across configurable lat/lon grid)
  4. Speed Violation Alerts (safety violations materialized for BI dashboards)

Source of Truth:
  HDFS Parquet archive: hdfs://namenode:9000/raw/ais_historical/date=YYYY-MM-DD/
Target Serving:
  PostGIS tables: port_dwell_times, fleet_daily_kpis, route_density_grid, vessel_speed_alerts
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
import logging
import os
import sys

import pg8000.dbapi as pg_driver
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Add current apps dir to sys.path so common.schemas can be imported
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.schemas import VESSEL_TYPE_MAP

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("batch_port_kpi_processor")


# =============================================================================
# ENVIRONMENT CONFIGURATION
# =============================================================================

HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "hdfs://namenode:9000")
HDFS_ARCHIVE_BASE = os.environ.get(
    "ARCHIVE_PATH", f"{HDFS_NAMENODE}/raw/ais_historical"
)

POSTGIS_HOST = os.environ.get("POSTGIS_HOST", "postgis")
POSTGIS_PORT = int(os.environ.get("POSTGIS_PORT", "5432"))
POSTGIS_DB = os.environ.get("POSTGIS_DB", "maritime")
POSTGIS_USER = os.environ.get("POSTGIS_USER", "maritime")
POSTGIS_PASSWORD = os.environ.get("POSTGIS_PASSWORD", "maritime")

JDBC_URL = f"jdbc:postgresql://{POSTGIS_HOST}:{POSTGIS_PORT}/{POSTGIS_DB}"
JDBC_PROPERTIES = {
    "user": POSTGIS_USER,
    "password": POSTGIS_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def get_pg_connection():
    """Create a pg8000 DB connection for pre-write deletions / DDL."""
    return pg_driver.connect(
        host=POSTGIS_HOST,
        port=POSTGIS_PORT,
        database=POSTGIS_DB,
        user=POSTGIS_USER,
        password=POSTGIS_PASSWORD,
    )


def delete_partition_date(table_name: str, date_col: str, exec_date: str) -> None:
    """Execute scoped DELETE to ensure idempotent delete-then-insert per partition day."""
    log.info("Deleting existing records from %s WHERE %s = '%s'", table_name, date_col, exec_date)
    conn = None
    cur = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        query = f"DELETE FROM {table_name} WHERE {date_col} = %s"
        cur.execute(query, (exec_date,))
        conn.commit()
        log.info("Deleted %d existing rows from %s", cur.rowcount, table_name)
    except Exception as exc:
        if conn:
            conn.rollback()
        log.error("Failed to delete partition date from %s: %s", table_name, exc)
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def load_port_references(spark: SparkSession) -> DataFrame:
    """Load port centroid coordinates and radii from PostGIS port_reference table."""
    log.info("Loading port references from %s.port_reference", POSTGIS_DB)
    # Extract ST_X(geom) as port_lon and ST_Y(geom) as port_lat
    query = """
        (SELECT
            port_id,
            port_name,
            ST_X(geom) AS port_lon,
            ST_Y(geom) AS port_lat,
            radius_nm
        FROM port_reference) AS ports
    """
    ports_df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", query)
        .option("user", POSTGIS_USER)
        .option("password", POSTGIS_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    return ports_df


# =============================================================================
# KPI 1: PORT DWELL TIMES
# =============================================================================

def compute_port_dwell_times(
    ais_df: DataFrame,
    ports_df: DataFrame,
    exec_date: str,
    default_radius_nm: float,
) -> DataFrame:
    """
    Computes vessel dwell time inside ports:
      - Filters AIS pings within port radius using Haversine formula
      - Orders pings per (mmsi, port_id) to detect entry and exit sessions
      - Calculates entry_ts, exit_ts, dwell_minutes
    """
    log.info("Computing Port Dwell Times for %s (default radius %.1f NM)...", exec_date, default_radius_nm)

    # Broadcast ports since reference table is tiny (~10 rows)
    ports_bc = F.broadcast(ports_df)

    # Cross join with ports to evaluate distance
    joined = ais_df.crossJoin(ports_bc)

    # Pre-filter using coarse bounding box (1 deg lat ~ 60 NM)
    # Allows fast exclusion before trigonometric calculations
    lat_deg_margin = (F.col("radius_nm") / 60.0) * 1.2
    lon_deg_margin = (F.col("radius_nm") / 45.0) * 1.2

    in_bbox = (
        (F.abs(F.col("LAT") - F.col("port_lat")) <= lat_deg_margin)
        & (F.abs(F.col("LON") - F.col("port_lon")) <= lon_deg_margin)
    )
    candidates = joined.filter(in_bbox)

    # Exact Haversine distance in nautical miles (1 NM = 1852 meters, Earth R = 3440.065 NM)
    dlat = F.radians(F.col("LAT") - F.col("port_lat"))
    dlon = F.radians(F.col("LON") - F.col("port_lon"))
    a = (
        F.sin(dlat / 2.0) ** 2
        + F.cos(F.radians(F.col("port_lat")))
        * F.cos(F.radians(F.col("LAT")))
        * F.sin(dlon / 2.0) ** 2
    )
    c = 2.0 * F.atan2(F.sqrt(a), F.sqrt(1.0 - a))
    dist_nm = 3440.065 * c

    effective_radius = F.coalesce(F.col("radius_nm"), F.lit(default_radius_nm))
    in_port_pings = candidates.withColumn("dist_nm", dist_nm).filter(
        F.col("dist_nm") <= effective_radius
    )

    # Identify dwell sessions: order pings by timestamp
    # A gap of > 2 hours (7200 seconds) signifies a separate visit
    w_order = Window.partitionBy("MMSI", "port_id").orderBy("BaseDateTime")
    in_port_pings = in_port_pings.withColumn("prev_ts", F.lag("BaseDateTime", 1).over(w_order))

    time_diff_sec = (
        F.unix_timestamp(F.col("BaseDateTime")) - F.unix_timestamp(F.col("prev_ts"))
    )
    is_new_session = F.when(
        F.col("prev_ts").isNull() | (time_diff_sec > 7200), 1
    ).otherwise(0)

    w_cumsum = (
        Window.partitionBy("MMSI", "port_id")
        .orderBy("BaseDateTime")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    session_df = in_port_pings.withColumn("session_id", F.sum(is_new_session).over(w_cumsum))

    # Aggregate by session
    dwell_summary = session_df.groupBy("MMSI", "port_id", "session_id").agg(
        F.min("BaseDateTime").alias("entry_ts"),
        F.max("BaseDateTime").alias("exit_ts"),
        F.count("*").alias("ping_count"),
    )

    dwell_minutes = F.round(
        (F.unix_timestamp(F.col("exit_ts")) - F.unix_timestamp(F.col("entry_ts"))) / 60.0,
        2,
    )

    result_df = (
        dwell_summary
        .withColumn("kpi_date", F.to_date(F.lit(exec_date)))
        .withColumn("mmsi", F.col("MMSI").cast(LongType()))
        .withColumn("port_id", F.col("port_id").cast(StringType()))
        .withColumn("dwell_minutes", dwell_minutes)
        .select("kpi_date", "mmsi", "port_id", "entry_ts", "exit_ts", "dwell_minutes")
        .distinct()
    )

    return result_df


# =============================================================================
# KPI 2: FLEET DAILY KPIS (SPEED PROFILES BY VESSEL TYPE)
# =============================================================================

def compute_fleet_daily_kpis(ais_df: DataFrame, exec_date: str) -> DataFrame:
    """
    Computes daily speed metrics grouped by vessel type:
      avg_sog, min_sog, max_sog, stddev_sog, ping_count
    """
    log.info("Computing Fleet Daily KPIs for %s...", exec_date)

    # Clean data: SOG must be valid (> 0 and < 102.2 sentinel)
    valid_speed = ais_df.filter(
        F.col("SOG").isNotNull()
        & (F.col("SOG") >= 0.0)
        & (F.col("SOG") < 102.2)
    )

    # Map vessel type code to name or string representation
    # Build Spark SQL mapping expression from VESSEL_TYPE_MAP
    type_col = F.col("VesselType")
    mapping_expr = F.when(type_col.isNull(), F.lit("Unknown"))
    for code, name in VESSEL_TYPE_MAP.items():
        mapping_expr = mapping_expr.when(type_col == code, F.lit(f"{name} ({code})"))
    mapping_expr = mapping_expr.otherwise(F.concat(F.lit("Type "), type_col.cast(StringType())))

    with_type = valid_speed.withColumn("vessel_type", mapping_expr)

    grouped = with_type.groupBy("vessel_type").agg(
        F.round(F.avg("SOG"), 2).alias("avg_sog"),
        F.round(F.min("SOG"), 2).alias("min_sog"),
        F.round(F.max("SOG"), 2).alias("max_sog"),
        F.round(F.coalesce(F.stddev("SOG"), F.lit(0.0)), 2).alias("stddev_sog"),
        F.count("*").alias("ping_count"),
    )

    result_df = (
        grouped
        .withColumn("kpi_date", F.to_date(F.lit(exec_date)))
        .select(
            "kpi_date",
            "vessel_type",
            "avg_sog",
            "min_sog",
            "max_sog",
            "stddev_sog",
            "ping_count",
        )
    )

    return result_df


# =============================================================================
# KPI 3: ROUTE DENSITY GRID
# =============================================================================

def compute_route_density_grid(
    ais_df: DataFrame,
    exec_date: str,
    grid_cell_deg: float,
) -> DataFrame:
    """
    Buckets lat/lon into regular spatial grid cells (e.g. 0.05 deg)
    and counts pings per cell per day.
    """
    log.info("Computing Route Density Grid for %s (cell size: %.3f deg)...", exec_date, grid_cell_deg)

    # Ensure valid coordinates
    valid_coords = ais_df.filter(
        F.col("LAT").isNotNull()
        & F.col("LON").isNotNull()
        & (F.col("LAT") >= -90.0)
        & (F.col("LAT") <= 90.0)
        & (F.col("LON") >= -180.0)
        & (F.col("LON") <= 180.0)
    )

    # Grid snap calculation
    step = float(grid_cell_deg)
    grid_lat = F.round(F.round(F.col("LAT") / step) * step, 4)
    grid_lon = F.round(F.round(F.col("LON") / step) * step, 4)

    gridded = valid_coords.withColumn("grid_lat", grid_lat).withColumn("grid_lon", grid_lon)

    counts = gridded.groupBy("grid_lat", "grid_lon").agg(
        F.count("*").alias("ping_count")
    )

    result_df = (
        counts
        .withColumn("kpi_date", F.to_date(F.lit(exec_date)))
        .select("kpi_date", "grid_lat", "grid_lon", "ping_count")
    )

    return result_df


# =============================================================================
# OPTIONAL MATERIALIZED SPEED ALERTS FOR BI DASHBOARDS
# =============================================================================

def extract_speed_alerts(ais_df: DataFrame, exec_date: str, speed_threshold: float = 20.0) -> DataFrame:
    """
    Materializes high-speed violations into vessel_speed_alerts table
    so Superset dashboards have access to historical violation records.
    """
    log.info("Extracting speed violation alerts (SOG > %.1f kts)...", speed_threshold)
    alerts = ais_df.filter(
        F.col("SOG").isNotNull() & (F.col("SOG") > speed_threshold)
    ).select(
        F.col("BaseDateTime").alias("detected_at"),
        F.col("MMSI").cast(LongType()).alias("mmsi"),
        F.col("VesselName").cast(StringType()).alias("vessel_name"),
        F.round(F.col("SOG"), 2).alias("sog_knots"),
        F.round(F.col("LAT"), 6).alias("lat"),
        F.round(F.col("LON"), 6).alias("lon"),
        F.col("Status").cast(StringType()).alias("nav_status"),
        F.lit("SPEED_VIOLATION").alias("alert_type"),
        F.lit(speed_threshold).alias("threshold_knots"),
    )
    return alerts


# =============================================================================
# MAIN RUNNER
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description="Smart Maritime Batch KPI Processor")
    parser.add_argument(
        "--exec-date",
        dest="exec_date",
        type=str,
        default=(date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Target date partition to process (format: YYYY-MM-DD)",
    )
    parser.add_argument(
        "--port-radius-nm",
        dest="port_radius_nm",
        type=float,
        default=3.0,
        help="Default port catchment radius in nautical miles (default: 3.0)",
    )
    parser.add_argument(
        "--grid-cell-deg",
        dest="grid_cell_deg",
        type=float,
        default=0.05,
        help="Spatial route density grid resolution in degrees (default: 0.05)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    exec_date = args.exec_date
    port_radius_nm = args.port_radius_nm
    grid_cell_deg = args.grid_cell_deg

    print("=" * 70)
    print("MARITIME BATCH PORT KPI PROCESSOR")
    print("=" * 70)
    print(f"Target Execution Date : {exec_date}")
    print(f"HDFS NameNode Base    : {HDFS_NAMENODE}")
    print(f"PostGIS Host          : {POSTGIS_HOST}:{POSTGIS_PORT}/{POSTGIS_DB}")
    print(f"Port Catchment Radius : {port_radius_nm} NM")
    print(f"Route Grid Resolution : {grid_cell_deg} deg")
    print("=" * 70)

    # Initialize Spark Session with tuned memory allocations
    spark = (
        SparkSession.builder
        .appName(f"Maritime-Batch-KPI-{exec_date}")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Read input partition from HDFS
        input_partition = f"{HDFS_ARCHIVE_BASE}/date={exec_date}"
        log.info("Reading HDFS partition: %s", input_partition)
        ais_df = spark.read.parquet(input_partition)
        ping_count = ais_df.count()
        log.info("Successfully loaded %d pings from %s", ping_count, input_partition)

        if ping_count == 0:
            log.warning("No records found in partition %s. Exiting gracefully.", input_partition)
            return

        # 2. Load port centroids
        ports_df = load_port_references(spark)

        # 3. Compute KPI 1: Port Dwell Times
        dwell_df = compute_port_dwell_times(ais_df, ports_df, exec_date, port_radius_nm)

        # 4. Compute KPI 2: Fleet Daily Speed KPIs
        kpis_df = compute_fleet_daily_kpis(ais_df, exec_date)

        # 5. Compute KPI 3: Route Density Grid
        density_df = compute_route_density_grid(ais_df, exec_date, grid_cell_deg)

        # 6. Extract speed alerts
        alerts_df = extract_speed_alerts(ais_df, exec_date, speed_threshold=20.0)

        # 7. Write to PostGIS with idempotent delete-then-insert per partition date
        # Table 1: port_dwell_times
        delete_partition_date("port_dwell_times", "kpi_date", exec_date)
        log.info("Writing port_dwell_times to PostGIS...")
        (
            dwell_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", "port_dwell_times")
            .options(**JDBC_PROPERTIES)
            .mode("append")
            .save()
        )
        log.info("Successfully wrote port_dwell_times.")

        # Table 2: fleet_daily_kpis
        delete_partition_date("fleet_daily_kpis", "kpi_date", exec_date)
        log.info("Writing fleet_daily_kpis to PostGIS...")
        (
            kpis_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", "fleet_daily_kpis")
            .options(**JDBC_PROPERTIES)
            .mode("append")
            .save()
        )
        log.info("Successfully wrote fleet_daily_kpis.")

        # Table 3: route_density_grid
        delete_partition_date("route_density_grid", "kpi_date", exec_date)
        log.info("Writing route_density_grid to PostGIS...")
        (
            density_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", "route_density_grid")
            .options(**JDBC_PROPERTIES)
            .mode("append")
            .save()
        )
        log.info("Successfully wrote route_density_grid.")

        # Table 4: vessel_speed_alerts
        # Delete alerts matching this date
        delete_partition_date("vessel_speed_alerts", "DATE(detected_at)", exec_date)
        alert_count = alerts_df.count()
        if alert_count > 0:
            log.info("Writing %d speed alerts to PostGIS...", alert_count)
            (
                alerts_df.write
                .format("jdbc")
                .option("url", JDBC_URL)
                .option("dbtable", "vessel_speed_alerts")
                .options(**JDBC_PROPERTIES)
                .mode("append")
                .save()
            )
            log.info("Successfully wrote vessel_speed_alerts.")
        else:
            log.info("No speed violation alerts to write for %s.", exec_date)

        print("=" * 70)
        print(f"BATCH PROCESSING FOR {exec_date} COMPLETED SUCCESSFULLY")
        print("=" * 70)

    except Exception as exc:
        log.error("Fatal error during batch KPI processing: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
