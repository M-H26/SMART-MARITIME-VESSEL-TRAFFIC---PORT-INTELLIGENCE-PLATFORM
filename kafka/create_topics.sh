#!/bin/bash
# Creates all Kafka topics required by the Maritime Vessel Traffic platform.
# Idempotent: safe to run every "docker compose up" - --if-not-exists skips
# topics that already exist instead of failing.
set -euo pipefail

BOOTSTRAP="kafka:9092"
KAFKA_BIN="/opt/kafka/bin"

declare -A TOPICS=(
  ["raw_ais_positions"]=3
  ["vessel_speed_alerts"]=1
  ["port_geofence_events"]=1
  ["collision_risk_telemetry"]=1
)

echo "Waiting for Kafka at ${BOOTSTRAP} to accept admin requests..."
until "${KAFKA_BIN}/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; do
  sleep 2
done

for topic in "${!TOPICS[@]}"; do
  partitions="${TOPICS[$topic]}"
  echo "Ensuring topic '${topic}' (partitions=${partitions}, replication=1)..."
  "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1
done

echo "Topics ready:"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list
