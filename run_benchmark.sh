#!/usr/bin/env bash
# ==============================================================================
# DBMS Research Project: Automated Reproduction Script (Bash)
# TimescaleDB vs. PostgreSQL 16 Benchmark Suite
# ==============================================================================
set -euo pipefail

echo "======================================================================"
echo "   DBMS RESEARCH PROJECT: IOT TIME-SERIES BENCHMARK HARNESS"
echo "   Simulating 20 Sensors for 1 Week (1.21 Million Records)"
echo "======================================================================"

# Detect Compose command
if command -v docker-compose &>/dev/null; then
    COMPOSE="docker-compose"
else
    COMPOSE="docker compose"
fi

# 1. Start Services
echo -e "\n[Step 1/3] Spinning up TimescaleDB, PostgreSQL Plain, and Grafana containers..."
$COMPOSE up -d timescaledb postgres_plain grafana

# 2. Wait for Healthchecks
echo -e "\n[Step 2/3] Awaiting database readiness via Docker healthchecks..."
timeout=60
elapsed=0
while [ $elapsed -lt $timeout ]; do
    ts_status=$(docker inspect --format='{{json .State.Health.Status}}' iot_timescaledb | tr -d '"')
    pg_status=$(docker inspect --format='{{json .State.Health.Status}}' iot_postgres_plain | tr -d '"')

    if [ "$ts_status" == "healthy" ] && [ "$pg_status" == "healthy" ]; then
        echo "  -> Both database engines report HEALTHY!"
        break
    fi
    echo "  -> Waiting for databases... (TimescaleDB: $ts_status, PostgreSQL: $pg_status)"
    sleep 3
    elapsed=$((elapsed + 3))
done

if [ $elapsed -ge $timeout ]; then
    echo "Error: Timed out waiting for database containers to become healthy." >&2
    exit 1
fi

# 3. Run Benchmark
echo -e "\n[Step 3/3] Executing Benchmark Suite..."
$COMPOSE run --rm --build benchmark_runner

echo "======================================================================"
echo "   BENCHMARK COMPLETE!"
echo "   - Access Grafana at: http://localhost:3000 (User: admin / Pass: admin)"
echo "   - Raw results exported to: ./benchmark_output/benchmark_results.json"
echo "   - Summary CSV exported to: ./benchmark_output/benchmark_summary.csv"
echo "======================================================================"
