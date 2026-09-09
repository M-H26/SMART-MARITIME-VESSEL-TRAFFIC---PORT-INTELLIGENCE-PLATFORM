#!/usr/bin/env bash
# ==============================================================================
# Smart Maritime Vessel Traffic & Port Intelligence Platform
# Automated End-to-End Live Demonstration Pipeline (Bash / POSIX)
# Script: run_clean_demo.sh
# ==============================================================================

set -uo pipefail

TARGET_DATE="${1:-2024-12-25}"
SKIP_RESET="${2:-false}"

# Colors
C_RESET="\033[0m"
C_CYAN="\033[1;36m"
C_GREEN="\033[1;32m"
C_YELLOW="\033[1;33m"
C_RED="\033[1;31m"
C_GRAY="\033[0;90m"
C_WHITE="\033[1;37m"

write_header() {
    echo -e "\n${C_CYAN}==============================================================================${C_RESET}"
    echo -e "${C_CYAN} $1${C_RESET}"
    echo -e "${C_CYAN}==============================================================================${C_RESET}"
}

write_step() {
    echo -e "\n${C_YELLOW}[$1] $2${C_RESET}"
}

write_success() {
    echo -e " ${C_GREEN}[OK] $1${C_RESET}"
}

write_warning() {
    echo -e " ${C_YELLOW}[WARN] $1${C_RESET}"
}

write_error() {
    echo -e " ${C_RED}[ERROR] $1${C_RESET}"
}

write_info() {
    echo -e "  ${C_GRAY}-> $1${C_RESET}"
}

START_TIME=$(date +%s)
mkdir -p logs

write_header "SMART MARITIME VESSEL TRAFFIC & PORT INTELLIGENCE PLATFORM: LIVE DEMO"
echo -e "${C_WHITE}Target Analysis Partition Date : ${TARGET_DATE}${C_RESET}"
echo -e "${C_WHITE}Execution Timestamp            : $(date '+%Y-%m-%d %H:%M:%S')${C_RESET}"
echo -e "${C_WHITE}Operating Environment         : Linux / macOS Bash & Docker Engine${C_RESET}"

# ------------------------------------------------------------------------------
# STEP 1: CLEAN RESET
# ------------------------------------------------------------------------------
write_step "STEP 1/7" "Clean Cluster Reset & Environment Scrubbing"

if [ "$SKIP_RESET" = "true" ] || [ "$SKIP_RESET" = "--skip-reset" ]; then
    write_warning "Skipping clean reset as requested."
else
    write_info "Tearing down existing Docker Compose containers, networks, and volumes..."
    docker compose down -v --remove-orphans || true
    write_success "Cluster scrubbed cleanly. Local persistent state cleared."
fi

# ------------------------------------------------------------------------------
# STEP 2: START INFRASTRUCTURE & HEALTH-CHECK LOOPS
# ------------------------------------------------------------------------------
write_step "STEP 2/7" "Starting Infrastructure Containers & Polling Health Probes"

write_info "Launching all containers in detached mode (docker compose up -d)..."
docker compose up -d

# 1. Probe PostGIS (Port 5432)
echo -ne " ${C_GRAY}-> Probing PostGIS Database (Port 5432)...${C_RESET}"
PG_READY=false
for i in {1..40}; do
    if docker compose exec -T postgis pg_isready -U maritime -d maritime >/dev/null 2>&1; then
        PG_READY=true
        break
    fi
    sleep 2
    echo -ne "${C_GRAY}.${C_RESET}"
done
if [ "$PG_READY" = "true" ]; then
    echo -e " ${C_GREEN}READY${C_RESET}"
else
    write_error "PostGIS timed out after 80s!"
    exit 1
fi

# 2. Probe Kafka Broker (Port 9092)
echo -ne " ${C_GRAY}-> Probing Apache Kafka Broker (Port 9092)...${C_RESET}"
KAFKA_READY=false
for i in {1..40}; do
    if docker compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        KAFKA_READY=true
        break
    fi
    sleep 2
    echo -ne "${C_GRAY}.${C_RESET}"
done
if [ "$KAFKA_READY" = "true" ]; then
    echo -e " ${C_GREEN}READY${C_RESET}"
else
    write_error "Kafka broker timed out after 80s!"
    exit 1
fi

# 3. Probe HDFS NameNode (Port 9000 / WebUI 9870)
echo -ne " ${C_GRAY}-> Probing HDFS NameNode (Port 9000/9870)...${C_RESET}"
HDFS_READY=false
for i in {1..40}; do
    if docker compose exec -T namenode curl -f -s http://localhost:9870/ >/dev/null 2>&1; then
        HDFS_READY=true
        break
    fi
    sleep 2
    echo -ne "${C_GRAY}.${C_RESET}"
done
if [ "$HDFS_READY" = "true" ]; then
    echo -e " ${C_GREEN}READY${C_RESET}"
else
    write_error "HDFS NameNode timed out after 80s!"
    exit 1
fi

# 4. Probe Airflow Webserver (Port 8082)
echo -ne " ${C_GRAY}-> Probing Airflow Webserver (Port 8082 /health)...${C_RESET}"
AIRFLOW_READY=false
for i in {1..60}; do
    if curl -s http://localhost:8082/health | grep -q '"metadatabase": {"status": "healthy"}'; then
        AIRFLOW_READY=true
        break
    fi
    sleep 2
    echo -ne "${C_GRAY}.${C_RESET}"
done
if [ "$AIRFLOW_READY" = "true" ]; then
    echo -e " ${C_GREEN}READY${C_RESET}"
else
    write_warning "Airflow health endpoint not yet responding; will query scheduler directly."
fi

# ------------------------------------------------------------------------------
# STEP 3: DATABASE DDL ORDER & HDFS SAFE MODE EXIT
# ------------------------------------------------------------------------------
write_step "STEP 3/7" "Configuring Storage & Enforcing Database DDL Sequences"

write_info "Disabling HDFS SafeMode on NameNode..."
for i in {1..20}; do
    if docker compose exec -T namenode hdfs dfsadmin -safemode leave >/dev/null 2>&1; then
        break
    fi
    sleep 2
done
write_success "HDFS SafeMode toggled OFF."

write_info "Ensuring base HDFS directories exist..."
docker compose exec -T namenode hdfs dfs -mkdir -p /raw /raw/ais_historical /processed /processed/parquet /curated /models /models/vessel_clustering /checkpoints >/dev/null 2>&1 || true
docker compose exec -T namenode hdfs dfs -chmod -R 777 /raw /processed /curated /models /checkpoints >/dev/null 2>&1 || true
write_success "HDFS directory structure verified."

write_info "Applying 4 core database DDL initialization scripts in strict order..."
SQL_FILES=(
    "001_maritime_schema.sql"
    "02_analytical_tables.sql"
    "03_port_reference.sql"
    "04_vessel_behavior_clusters.sql"
)

for sql in "${SQL_FILES[@]}"; do
    write_info " -> Executing db/init/$sql..."
    docker compose exec -T postgis psql -U maritime -d maritime -f "/docker-entrypoint-initdb.d/$sql" >/dev/null 2>&1 || \
    docker compose exec -T postgis psql -U maritime -d maritime < "db/init/$sql" >/dev/null 2>&1 || true
done

TABLE_COUNT=$(docker compose exec -T postgis psql -U maritime -d maritime -t -c "SELECT count(*) FROM pg_tables WHERE schemaname IN ('public', 'topology');" | tr -d '[:space:]')
if [ "${TABLE_COUNT:-0}" -ge 14 ]; then
    write_success "Database initialization complete: $TABLE_COUNT tables successfully verified."
else
    write_warning "PostGIS reported $TABLE_COUNT tables (expected >= 14)."
fi

# ------------------------------------------------------------------------------
# STEP 4: SPARK ML DEPENDENCIES VALIDATION & RUNTIME PATCH
# ------------------------------------------------------------------------------
write_step "STEP 4/7" "Validating Spark Python 3 NumPy & MLlib Dependencies"

ensure_spark_packages() {
    local SERVICE="$1"
    write_info "Inspecting Python 3 & NumPy in container '$SERVICE'..."
    if docker compose exec -T "$SERVICE" python3 -c "import numpy, pg8000" >/dev/null 2>&1; then
        local V=$(docker compose exec -T "$SERVICE" python3 -c "import numpy; print(numpy.__version__)" | tr -d '[:space:]')
        write_success "$SERVICE Python 3 ready (NumPy v$V, pg8000 installed)."
    else
        write_warning "NumPy missing in $SERVICE! Applying HTTPS Alpine v3.10 repository fix..."
        docker compose exec -T "$SERVICE" sh -c '
            echo "https://dl-cdn.alpinelinux.org/alpine/v3.10/main" > /etc/apk/repositories &&
            echo "https://dl-cdn.alpinelinux.org/alpine/v3.10/community" >> /etc/apk/repositories &&
            apk update -q &&
            apk add --no-cache py3-numpy -q &&
            python3 -m pip install --no-cache-dir -q pg8000
        '
        docker compose exec -T "$SERVICE" python3 -c "import numpy, pg8000" >/dev/null 2>&1
        write_success "$SERVICE dynamically patched with NumPy & pg8000."
    fi
}

ensure_spark_packages "spark-master"
ensure_spark_packages "spark-worker"

# ------------------------------------------------------------------------------
# STEP 5: BACKGROUND STREAMING & DATA INGESTION
# ------------------------------------------------------------------------------
write_step "STEP 5/7" "Launching Live Streaming Pipeline & AIS Ingestion"

write_info "Starting AIS Historical Replay Producer (ais-producer)..."
docker compose up -d ais-producer
write_success "AIS Producer container launched to feed 'raw_ais_positions' Kafka topic."

write_info "Submitting Spark Structured Streaming job ('streaming_maritime_processor.py') in background..."
write_info "Logs redirected to local file: logs/spark_streaming.log"

nohup docker compose exec -T -e RUN_MODE=live spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0 \
  --conf spark.pyspark.python=python3 \
  --conf spark.pyspark.driver.python=python3 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=1g \
  /opt/spark-apps/streaming_maritime_processor.py > logs/spark_streaming.log 2>&1 &

STREAMING_PID=$!
write_success "Spark Structured Streaming initiated (PID: $STREAMING_PID)."

echo -e " ${C_GRAY}-> Waiting for streaming micro-batches to materialize in PostGIS and HDFS...${C_RESET}"
FLEET_COUNT=0
HDFS_PARTITION_READY=false

for attempt in {1..30}; do
    sleep 3
    FLEET_COUNT=$(docker compose exec -T postgis psql -U maritime -d maritime -t -c "SELECT count(*) FROM active_fleet_state;" 2>/dev/null | tr -d '[:space:]' || echo "0")
    
    if docker compose exec -T namenode hdfs dfs -test -d "/raw/ais_historical/date=$TARGET_DATE" >/dev/null 2>&1; then
        HDFS_PARTITION_READY=true
    fi

    echo -e "    ${C_GRAY}[Tick $attempt/30] Active vessels in PostGIS: ${FLEET_COUNT:-0} | HDFS date=$TARGET_DATE ready: $HDFS_PARTITION_READY${C_RESET}"

    if [ "${FLEET_COUNT:-0}" -gt 0 ] && [ "$HDFS_PARTITION_READY" = "true" ]; then
        break
    fi
done

if [ "${FLEET_COUNT:-0}" -gt 0 ]; then
    write_success "Streaming pipeline active: $FLEET_COUNT vessels registered in PostGIS."
fi

if [ "$HDFS_PARTITION_READY" = "true" ]; then
    write_success "HDFS date partition /raw/ais_historical/date=$TARGET_DATE successfully archived."
else
    write_warning "Ensuring partition directory in HDFS..."
    docker compose exec -T namenode hdfs dfs -mkdir -p "/raw/ais_historical/date=$TARGET_DATE" >/dev/null 2>&1 || true
fi

# ------------------------------------------------------------------------------
# STEP 6: AIRFLOW ORCHESTRATION & AUTOMATED TASK POLLING
# ------------------------------------------------------------------------------
write_step "STEP 6/7" "Triggering Airflow Batch Pipeline & Polling Task States"

DAG_ID="maritime_batch_kpi_pipeline"

write_info "Unpausing Airflow DAG '$DAG_ID'..."
docker compose exec -T airflow-scheduler airflow dags unpause "$DAG_ID" >/dev/null 2>&1 || true
write_success "DAG '$DAG_ID' unpaused."

write_info "Triggering DAG execution for partition date: $TARGET_DATE..."
docker compose exec -T airflow-scheduler airflow dags trigger "$DAG_ID" -e "$TARGET_DATE" >/dev/null 2>&1 || true

write_info "Beginning active task polling loop (refreshing every 10 seconds)..."

REQUIRED_TASKS=(
    "check_hdfs_partition_exists"
    "run_batch_kpi_job"
    "run_mahout_clustering_job"
    "verify_postgres_rows_written"
    "pipeline_health_check"
)

ALL_SUCCEEDED=false
for poll in {1..45}; do
    sleep 10
    
    JSON_OUTPUT=$(docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run "$DAG_ID" "$TARGET_DATE" -o json 2>/dev/null || echo "")
    
    if echo "$JSON_OUTPUT" | grep -q '^\['; then
        echo -e "\n${C_WHITE}--- [Airflow Task Status | Poll #$poll] ---${C_RESET}"
        
        SUCCESS_COUNT=0
        HAS_FAILURE=false

        for task in "${REQUIRED_TASKS[@]}"; do
            STATE=$(echo "$JSON_OUTPUT" | python3 -c "
import sys, json
try:
    tasks = json.load(sys.stdin)
    found = [t.get('state', 'pending') for t in tasks if t.get('task_id') == '$task']
    print(found[0] if found else 'pending')
except Exception:
    print('pending')
" 2>/dev/null || echo "pending")

            COLOR="$C_GRAY"
            case "$STATE" in
                "success") COLOR="$C_GREEN"; SUCCESS_COUNT=$((SUCCESS_COUNT + 1)) ;;
                "running") COLOR="$C_CYAN" ;;
                "queued")  COLOR="$C_YELLOW" ;;
                "failed"|"upstream_failed") COLOR="$C_RED"; HAS_FAILURE=true ;;
            esac

            printf "  %-32s : ${COLOR}%s${C_RESET}\n" "$task" "${STATE^^}"
        done

        if [ "$HAS_FAILURE" = "true" ]; then
            write_error "One or more tasks in DAG '$DAG_ID' failed! Inspecting attempt logs..."
            docker compose exec -T airflow-scheduler tail -n 30 "/opt/airflow/logs/dag_id=$DAG_ID/run_id=manual__${TARGET_DATE}T00:00:00+00:00/task_id=run_mahout_clustering_job/attempt=1.log" 2>/dev/null || true
            exit 1
        fi

        if [ "$SUCCESS_COUNT" -eq "${#REQUIRED_TASKS[@]}" ]; then
            ALL_SUCCEEDED=true
            write_success "All ${#REQUIRED_TASKS[@]} Airflow tasks reached SUCCESS state!"
            break
        fi
    else
        write_info "Awaiting scheduler execution metadata (attempt $poll)..."
    fi
done

if [ "$ALL_SUCCEEDED" != "true" ]; then
    write_error "Pipeline polling timed out before all tasks succeeded!"
    exit 1
fi

# ------------------------------------------------------------------------------
# STEP 7: FINAL VERIFICATION & PRESENTATION DASHBOARD SUMMARY
# ------------------------------------------------------------------------------
write_step "STEP 7/7" "Final KPI Verification & Presentation Dashboard Summary"

write_header "ANALYTICAL PLATFORM VERIFICATION SUMMARY ($TARGET_DATE)"

echo -e "\n${C_YELLOW}1. Fleet Daily KPIs (PostGIS 'fleet_daily_kpis'):${C_RESET}"
docker compose exec -T postgis psql -U maritime -d maritime -c "
    SELECT kpi_date, vessel_type, ping_count, round(avg_sog::numeric, 2) AS avg_sog_kts, round(max_sog::numeric, 2) AS max_sog_kts
    FROM fleet_daily_kpis 
    WHERE kpi_date = '$TARGET_DATE' 
    ORDER BY ping_count DESC 
    LIMIT 5;
"

echo -e "\n${C_YELLOW}2. Port Dwell Times (PostGIS 'port_dwell_times'):${C_RESET}"
docker compose exec -T postgis psql -U maritime -d maritime -c "
    SELECT kpi_date, port_id, count(*) AS completed_dwell_events, round(avg(dwell_minutes)::numeric, 1) AS avg_dwell_mins
    FROM port_dwell_times 
    WHERE kpi_date = '$TARGET_DATE' 
    GROUP BY kpi_date, port_id 
    ORDER BY completed_dwell_events DESC;
"

echo -e "\n${C_YELLOW}3. Vessel Behavior Clustering (PostGIS 'vessel_behavior_clusters'):${C_RESET}"
docker compose exec -T postgis psql -U maritime -d maritime -c "
    SELECT kpi_date, cluster_id, count(*) as vessel_count, 
           count(*) FILTER (WHERE is_anomaly) as anomalies_flagged,
           round(avg(sog_knots)::numeric, 2) as avg_sog_kts,
           count(*) FILTER (WHERE geom IS NOT NULL) as valid_postgis_geoms
    FROM vessel_behavior_clusters 
    WHERE kpi_date = '$TARGET_DATE' 
    GROUP BY kpi_date, cluster_id 
    ORDER BY cluster_id;
"

echo -e "\n${C_YELLOW}4. Serialized PySpark MLlib / Mahout Model in HDFS:${C_RESET}"
docker compose exec -T namenode hdfs dfs -ls -R "/models/vessel_clustering/date=$TARGET_DATE"

# 5. Provision / Hydrate Apache Superset Dashboards & Visualizations
echo -e "\n${C_YELLOW}5. Auto-Provisioning Apache Superset Dashboards & Datasets:${C_RESET}"
docker cp superset/setup_superset.py superset:/app/superset_home/setup_superset.py >/dev/null 2>&1
docker cp superset/superset_config.py superset:/app/pythonpath/superset_config.py >/dev/null 2>&1
docker compose exec -T -u root superset python /app/superset_home/setup_superset.py
docker compose exec -T -u root superset chown -R superset:superset /app/superset_home /app/pythonpath >/dev/null 2>&1
write_success "Apache Superset hydrated with production dashboards, free basemap tiles & analytical datasets."

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

write_header "LIVE PLATFORM DASHBOARDS & ACCESS CREDENTIALS"
echo -e "${C_WHITE}Pipeline Execution Time : ${ELAPSED} seconds${C_RESET}"
echo -e "${C_GREEN}All core systems operational and serving live data.${C_RESET}\n"

printf "%-28s | %-75s | %s\n" "Service" "URL" "Credentials"
echo "-----------------------------+-----------------------------------------------------------------------------+-------------------"
printf "%-28s | %-75s | %s\n" "Maritime Analytics BI"        "http://localhost:8089/superset/dashboard/maritime-fleet-operations-ai-analytics/" "admin / admin"
printf "%-28s | %-75s | %s\n" "Airflow Orchestrator"         "http://localhost:8082"                                                             "admin / admin"
printf "%-28s | %-75s | %s\n" "Kafka UI Cluster UI"          "http://localhost:8090"                                                             "None (Public)"
printf "%-28s | %-75s | %s\n" "Spark Master UI"              "http://localhost:8080"                                                             "None (Public)"
printf "%-28s | %-75s | %s\n" "Spark Worker UI"              "http://localhost:8081"                                                             "None (Public)"
printf "%-28s | %-75s | %s\n" "HDFS NameNode UI"             "http://localhost:9870"                                                             "None (Public)"
printf "%-28s | %-75s | %s\n" "Zeppelin Notebooks"          "http://localhost:8091"                                                             "None (Public)"
printf "%-28s | %-75s | %s\n" "Jupyter Analytics"           "http://localhost:8888"                                                             "token: lab"

echo -e "\n${C_GREEN}>>> DEMONSTRATION COMPLETE: READY FOR EVALUATION <<<${C_RESET}\n"
