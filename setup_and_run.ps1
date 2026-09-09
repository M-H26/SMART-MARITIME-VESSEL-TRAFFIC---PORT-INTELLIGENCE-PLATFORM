<#
.SYNOPSIS
    Smart Maritime Vessel Traffic & Port Intelligence Platform
    Automated Setup & Cluster Bootstrap Script for Windows / PowerShell

.DESCRIPTION
    1. Starts all Docker Compose services in detached mode
    2. Waits for core services (Kafka, HDFS NameNode, PostGIS, Spark) to become healthy/ready
    3. Leaves HDFS safemode if active
    4. Installs pg8000 via pip3 in spark-master and spark-worker
    5. Displays operational commands for Terminal 1 (Spark Streaming) and Terminal 2 (AIS Producer)
#>

# Allow native commands with stderr output (like pip warnings) to run without terminating the script
$ErrorActionPreference = "Continue"

Write-Host "==========================================================================" -ForegroundColor Cyan
Write-Host " SMART MARITIME PLATFORM: AUTOMATED SETUP & BOOTSTRAP" -ForegroundColor Cyan
Write-Host "==========================================================================" -ForegroundColor Cyan

# 1. Verify Docker Engine is running
Write-Host "`n[1/5] Checking Docker Engine status..." -ForegroundColor Yellow
try {
    docker info *>$null
    Write-Host " -> Docker Engine is running." -ForegroundColor Green
} catch {
    Write-Host " [ERROR] Docker is not running or not accessible! Please launch Docker Desktop." -ForegroundColor Red
    exit 1
}

# 2. Start all Docker Compose services
Write-Host "`n[2/5] Starting Docker Compose cluster (docker compose up -d)..." -ForegroundColor Yellow
docker compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host " [ERROR] Failed to start docker compose services." -ForegroundColor Red
    exit 1
}
Write-Host " -> Containers launched." -ForegroundColor Green

# 3. Wait for Core Services (PostGIS, Kafka, NameNode)
Write-Host "`n[3/5] Waiting for core services to become healthy..." -ForegroundColor Yellow

# Wait for PostGIS
Write-Host " -> Waiting for PostGIS database..." -NoNewline
$pgReady = $false
for ($i = 0; $i -lt 30; $i++) {
    docker compose exec -T postgis pg_isready -U maritime -d maritime *>$null
    if ($LASTEXITCODE -eq 0) {
        $pgReady = $true
        break
    }
    Start-Sleep -Seconds 2
    Write-Host "." -NoNewline
}
if ($pgReady) {
    Write-Host " READY" -ForegroundColor Green
} else {
    Write-Host " WARNING: PostGIS may still be initializing." -ForegroundColor DarkYellow
}

# Wait for Kafka
Write-Host " -> Waiting for Kafka broker..." -NoNewline
$kafkaReady = $false
for ($i = 0; $i -lt 30; $i++) {
    docker compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 *>$null
    if ($LASTEXITCODE -eq 0) {
        $kafkaReady = $true
        break
    }
    Start-Sleep -Seconds 2
    Write-Host "." -NoNewline
}
if ($kafkaReady) {
    Write-Host " READY" -ForegroundColor Green
} else {
    Write-Host " WARNING: Kafka broker may still be initializing." -ForegroundColor DarkYellow
}

# 4. Disable HDFS SafeMode
Write-Host "`n[4/5] Checking HDFS NameNode & Disabling SafeMode..." -ForegroundColor Yellow
$hdfsReady = $false
for ($i = 0; $i -lt 30; $i++) {
    docker compose exec -T namenode hdfs dfsadmin -safemode leave *>$null
    if ($LASTEXITCODE -eq 0) {
        $hdfsReady = $true
        break
    }
    Start-Sleep -Seconds 2
}
if ($hdfsReady) {
    Write-Host " -> HDFS SafeMode is OFF." -ForegroundColor Green
} else {
    Write-Host " [WARNING] Could not toggle HDFS SafeMode automatically. Run manually: docker compose exec namenode hdfs dfsadmin -safemode leave" -ForegroundColor DarkYellow
}

# 5. Ensure Python dependencies (numpy, pg8000) are installed in spark-master and spark-worker
Write-Host "`n[5/5] Ensuring Python dependencies (numpy, pg8000) are ready in Spark containers..." -ForegroundColor Yellow
Write-Host " -> Checking & ensuring dependencies in spark-master..." -NoNewline
docker compose exec -T spark-master sh -c 'python3 -c "import numpy" 2>/dev/null || (echo "https://dl-cdn.alpinelinux.org/alpine/v3.10/main" > /etc/apk/repositories && echo "https://dl-cdn.alpinelinux.org/alpine/v3.10/community" >> /etc/apk/repositories && apk update -q && apk add --no-cache py3-numpy -q)' *>$null
docker compose exec -T spark-master pip3 install --no-cache-dir --disable-pip-version-check -q pg8000 *>$null
Write-Host " DONE" -ForegroundColor Green

Write-Host " -> Checking & ensuring dependencies in spark-worker..." -NoNewline
docker compose exec -T spark-worker sh -c 'python3 -c "import numpy" 2>/dev/null || (echo "https://dl-cdn.alpinelinux.org/alpine/v3.10/main" > /etc/apk/repositories && echo "https://dl-cdn.alpinelinux.org/alpine/v3.10/community" >> /etc/apk/repositories && apk update -q && apk add --no-cache py3-numpy -q)' *>$null
docker compose exec -T spark-worker pip3 install --no-cache-dir --disable-pip-version-check -q pg8000 *>$null
Write-Host " DONE" -ForegroundColor Green

# 6. Final Instructions
Write-Host "`n==========================================================================" -ForegroundColor Cyan
Write-Host " CLUSTER INITIALIZATION COMPLETE" -ForegroundColor Green
Write-Host "==========================================================================" -ForegroundColor Cyan

Write-Host "`nACTIVE WEB DASHBOARDS:" -ForegroundColor Yellow
Write-Host " Kafka UI:        http://localhost:8090"
Write-Host " Spark Master:    http://localhost:8080"
Write-Host " Spark Worker:    http://localhost:8081"
Write-Host " HDFS NameNode:   http://localhost:9870"
Write-Host " Apache Superset: http://localhost:8089 (admin / admin)"
Write-Host " Jupyter Lab:     http://localhost:8888 (token: lab)"

Write-Host "`nNEXT STEPS: RUN THE STREAMING PIPELINE IN TWO SEPARATE TERMINALS" -ForegroundColor Yellow

Write-Host "`n--- TERMINAL 1: START SPARK STRUCTURED STREAMING ---" -ForegroundColor White
Write-Host 'docker compose exec spark-master /spark/bin/spark-submit \' -ForegroundColor Cyan
Write-Host '  --master spark://spark-master:7077 \' -ForegroundColor Cyan
Write-Host '  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0 \' -ForegroundColor Cyan
Write-Host '  --conf spark.sql.shuffle.partitions=8 \' -ForegroundColor Cyan
Write-Host '  --conf spark.executor.memory=2g \' -ForegroundColor Cyan
Write-Host '  --conf spark.driver.memory=1g \' -ForegroundColor Cyan
Write-Host '  /opt/spark-apps/streaming_maritime_processor.py' -ForegroundColor Cyan

Write-Host "`n--- TERMINAL 2: RUN AIS HISTORICAL REPLAY PRODUCER ---" -ForegroundColor White
Write-Host 'docker compose run --rm ais-producer' -ForegroundColor Cyan

Write-Host "`n--- VERIFICATION COMMANDS ---" -ForegroundColor White
Write-Host '1. Check PostGIS fleet state count:' -ForegroundColor Gray
Write-Host '   docker compose exec postgis psql -U maritime -d maritime -c "SELECT COUNT(*) FROM active_fleet_state;"' -ForegroundColor DarkCyan
Write-Host '2. Inspect latest vessel updates in PostGIS:' -ForegroundColor Gray
Write-Host '   docker compose exec postgis psql -U maritime -d maritime -c "SELECT mmsi, vessel_name, ST_AsText(geom), sog_knots, last_updated FROM active_fleet_state ORDER BY last_updated DESC LIMIT 10;"' -ForegroundColor DarkCyan
Write-Host '3. Monitor live speed violation alerts in Kafka:' -ForegroundColor Gray
Write-Host '   docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic vessel_speed_alerts --from-beginning --max-messages 10' -ForegroundColor DarkCyan
Write-Host '4. Check HDFS archived Parquet files:' -ForegroundColor Gray
Write-Host '   docker compose exec namenode hdfs dfs -ls -R /raw/ais_historical' -ForegroundColor DarkCyan
Write-Host "==========================================================================`n" -ForegroundColor Cyan
