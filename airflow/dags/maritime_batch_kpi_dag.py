"""
Smart Maritime Vessel Traffic & Port Intelligence Platform
Airflow Batch KPI Orchestration Pipeline
============================================================
Orchestrates daily analytical processing of archived AIS data:
  1. Verifies HDFS date partition existence (/raw/ais_historical/date=YYYY-MM-DD/)
  2. Submits batch KPI Spark job to the Spark standalone cluster
  3. Verifies KPI row ingestion in PostGIS serving tables
  4. Performs diagnostic health check on core infrastructure services
"""

from __future__ import annotations

from datetime import datetime, timedelta
import logging
import os
import subprocess

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger("airflow.maritime_batch_kpi_dag")


def on_failure_callback(context: dict) -> None:
    """Structured callback logging task failures for triage."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    task_id = task_instance.task_id if task_instance else "unknown_task"
    exec_date = context.get("ds", "unknown_date")
    exception = context.get("exception", "No exception specified")
    log.error(
        "[PIPELINE FAILURE ALERT] DAG: %s | Task: %s | ExecDate: %s | Error: %s",
        dag_id,
        task_id,
        exec_date,
        exception,
    )


def verify_postgres_rows_written(ds: str, **context) -> None:
    """
    Verifies that rows for the execution date were successfully written
    to PostGIS analytical tables. Raises AirflowFailException if 0 rows landed.
    """
    log.info("Verifying PostgreSQL / PostGIS rows for execution date: %s", ds)
    
    # Connect using psycopg2 or Airflow PostgresHook
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="maritime_postgis")
        conn = hook.get_conn()
    except Exception:
        import psycopg2
        conn = psycopg2.connect(
            host=os.environ.get("POSTGIS_HOST", "postgis"),
            port=int(os.environ.get("POSTGIS_PORT", "5432")),
            dbname=os.environ.get("POSTGIS_DB", "maritime"),
            user=os.environ.get("POSTGIS_USER", "maritime"),
            password=os.environ.get("POSTGIS_PASSWORD", "maritime"),
        )

    try:
        cur = conn.cursor()

        # Check fleet_daily_kpis
        cur.execute("SELECT COUNT(*) FROM fleet_daily_kpis WHERE kpi_date = %s", (ds,))
        kpi_count = cur.fetchone()[0]
        log.info("fleet_daily_kpis row count for %s: %d", ds, kpi_count)

        # Check port_dwell_times
        cur.execute("SELECT COUNT(*) FROM port_dwell_times WHERE kpi_date = %s", (ds,))
        dwell_count = cur.fetchone()[0]
        log.info("port_dwell_times row count for %s: %d", ds, dwell_count)

        # Check route_density_grid
        cur.execute("SELECT COUNT(*) FROM route_density_grid WHERE kpi_date = %s", (ds,))
        grid_count = cur.fetchone()[0]
        log.info("route_density_grid row count for %s: %d", ds, grid_count)

        # Check vessel_behavior_clusters (Layer 5 MLlib / Mahout clustering)
        cur.execute("SELECT COUNT(*) FROM vessel_behavior_clusters WHERE kpi_date = %s", (ds,))
        cluster_count = cur.fetchone()[0]
        log.info("vessel_behavior_clusters row count for %s: %d", ds, cluster_count)

        if kpi_count == 0:
            raise AirflowFailException(
                f"Verification failed: 0 rows found in fleet_daily_kpis for date {ds}"
            )

        if cluster_count == 0:
            raise AirflowFailException(
                f"Verification failed: 0 rows found in vessel_behavior_clusters for date {ds}"
            )

        log.info(
            "PostGIS verification PASSED for %s: %d fleet KPIs, %d dwell events, %d grid cells, %d cluster fixes written.",
            ds,
            kpi_count,
            dwell_count,
            grid_count,
            cluster_count,
        )
    finally:
        conn.close()


def pipeline_health_check(**context) -> None:
    """
    Lightweight health check logging status of Kafka, Spark, HDFS, and PostGIS
    directly into the Airflow task log for rapid diagnosis without shelling into containers.
    """
    log.info("Starting cluster health check...")

    def run_check(label: str, cmd: list[str]):
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            status = "HEALTHY" if res.returncode == 0 else f"DEGRADED (exit {res.returncode})"
            log.info("Health Check [%s]: %s\nStdout: %s\nStderr: %s", label, status, res.stdout.strip(), res.stderr.strip())
        except Exception as exc:
            log.warning("Health Check [%s]: FAILED -- %s", label, exc)

    # 1. Container status overview
    run_check(
        "Docker Container Overview",
        [
            "docker", "ps",
            "--filter", "name=^(postgis|namenode|datanode|spark-master|spark-worker|kafka)$",
            "--format", "table {{.Names}}\t{{.Status}}\t{{.Ports}}",
        ],
    )

    # 2. HDFS SafeMode
    run_check("HDFS SafeMode", ["docker", "exec", "namenode", "hdfs", "dfsadmin", "-safemode", "get"])

    # 3. PostGIS Connectivity
    run_check("PostGIS Database", ["docker", "exec", "postgis", "pg_isready", "-U", "maritime", "-d", "maritime"])

    # 4. Kafka Broker
    run_check(
        "Kafka Broker",
        ["docker", "exec", "kafka", "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092"],
    )


# -----------------------------------------------------------------------------
# DAG DEFINITION
# -----------------------------------------------------------------------------

default_args = {
    "owner": "maritime-data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="maritime_batch_kpi_pipeline",
    description="Daily batch aggregation pipeline computing vessel dwell times, speed profiles, and traffic density",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 25),
    catchup=False,
    tags=["maritime", "batch", "kpi", "spark", "postgis"],
) as dag:

    # 1. Verify HDFS input partition exists before launching Spark
    check_hdfs_partition_exists = BashOperator(
        task_id="check_hdfs_partition_exists",
        bash_command=(
            "docker exec namenode hdfs dfs -test -d /raw/ais_historical/date={{ ds }} || "
            "(echo 'CRITICAL: HDFS partition /raw/ais_historical/date={{ ds }} not found! "
            "Streaming pipeline has not archived data for this date.' && exit 1)"
        ),
    )

    # 2. Submit the PySpark Batch KPI Job
    run_batch_kpi_job = BashOperator(
        task_id="run_batch_kpi_job",
        bash_command=(
            "docker exec -e PYSPARK_PYTHON=python3 -e PYSPARK_DRIVER_PYTHON=python3 spark-master /spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0 "
            "--conf spark.pyspark.python=python3 "
            "--conf spark.pyspark.driver.python=python3 "
            "--conf spark.sql.shuffle.partitions=8 "
            "--conf spark.executor.memory=2g "
            "--conf spark.driver.memory=1g "
            "/opt/spark-apps/batch_port_kpi_processor.py --exec-date {{ ds }}"
        ),
    )

    # 3. Submit the PySpark MLlib / Mahout Vessel Behavior Clustering Job
    run_mahout_clustering_job = BashOperator(
        task_id="run_mahout_clustering_job",
        bash_command=(
            "docker exec -e PYSPARK_PYTHON=python3 -e PYSPARK_DRIVER_PYTHON=python3 spark-master /spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0 "
            "--conf spark.pyspark.python=python3 "
            "--conf spark.pyspark.driver.python=python3 "
            "--conf spark.sql.shuffle.partitions=8 "
            "--conf spark.executor.memory=2g "
            "--conf spark.driver.memory=1g "
            "/opt/spark-apps/mahout/spark_mahout_clustering.py --exec-date {{ ds }} --k 5 --anomaly-threshold-pct 95"
        ),
    )

    # 4. Verify target rows landed in PostGIS
    verify_rows = PythonOperator(
        task_id="verify_postgres_rows_written",
        python_callable=verify_postgres_rows_written,
    )

    # 5. Cluster diagnostic health check
    health_check = PythonOperator(
        task_id="pipeline_health_check",
        python_callable=pipeline_health_check,
    )

    # Define task dependencies
    (
        check_hdfs_partition_exists
        >> run_batch_kpi_job
        >> run_mahout_clustering_job
        >> verify_rows
        >> health_check
    )
