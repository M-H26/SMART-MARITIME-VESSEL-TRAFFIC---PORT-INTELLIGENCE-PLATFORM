#!/usr/bin/env bash
# =============================================================================
# Smart Maritime Vessel Traffic & Port Intelligence Platform
# Superset Automation Script
# =============================================================================
set -euo pipefail

echo "=========================================================================="
echo " PROVISIONING APACHE SUPERSET (DASHBOARDS, DATASETS, DATABASE)"
echo "=========================================================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Copy setup script and configuration into superset container
docker cp "${SCRIPT_DIR}/setup_superset.py" superset:/app/superset_home/setup_superset.py
docker cp "${SCRIPT_DIR}/superset_config.py" superset:/app/pythonpath/superset_config.py

# Execute inside the superset container as root and restore permissions
docker exec -u root superset python /app/superset_home/setup_superset.py
docker exec -u root superset chown -R superset:superset /app/superset_home /app/pythonpath

echo "Superset setup completed successfully."
