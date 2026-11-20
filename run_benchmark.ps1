# ==============================================================================
# DBMS Research Project: Automated Reproduction Script (PowerShell)
# TimescaleDB vs. PostgreSQL 16 Benchmark Suite
# ==============================================================================

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "   DBMS RESEARCH PROJECT: IOT TIME-SERIES BENCHMARK HARNESS" -ForegroundColor Cyan
Write-Host "   Simulating 20 Sensors for 1 Week (1.21 Million Records)" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

# Detect appropriate Docker Compose CLI syntax
if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
    function Invoke-Compose { & docker-compose $args }
} else {
    function Invoke-Compose { & docker compose $args }
}

# 1. Start Docker Containers
Write-Host "`n[Step 1/3] Spinning up TimescaleDB, PostgreSQL Plain, and Grafana containers..." -ForegroundColor Yellow
Invoke-Compose up -d timescaledb postgres_plain grafana

# 2. Wait for health checks
Write-Host "`n[Step 2/3] Awaiting database readiness via Docker healthchecks..." -ForegroundColor Yellow
$timeoutSeconds = 60
$elapsed = 0
while ($elapsed -lt $timeoutSeconds) {
    $tsHealth = (docker inspect --format='{{json .State.Health.Status}}' iot_timescaledb) -replace '"',''
    $pgHealth = (docker inspect --format='{{json .State.Health.Status}}' iot_postgres_plain) -replace '"',''

    if ($tsHealth -eq "healthy" -and $pgHealth -eq "healthy") {
        Write-Host "  -> Both database engines report HEALTHY!" -ForegroundColor Green
        break
    }
    Write-Host "  -> Waiting for databases to become healthy... (Current: TimescaleDB=$tsHealth, PostgreSQL=$pgHealth)"
    Start-Sleep -Seconds 3
    $elapsed += 3
}

if ($elapsed -ge $timeoutSeconds) {
    Write-Error "Databases failed to reach healthy state within $timeoutSeconds seconds. Aborting."
    exit 1
}

# 3. Execute Benchmarking Suite
Write-Host "`n[Step 3/3] Executing Benchmark via Containerized Runner..." -ForegroundColor Yellow
Invoke-Compose run --rm --build benchmark_runner

Write-Host "`n======================================================================" -ForegroundColor Green
Write-Host "   BENCHMARK COMPLETE!" -ForegroundColor Green
Write-Host "   - Access Grafana at: http://localhost:3000 (User: admin / Pass: admin)" -ForegroundColor Green
Write-Host "   - Raw results exported to: ./benchmark_output/benchmark_results.json" -ForegroundColor Green
Write-Host "   - Summary CSV exported to: ./benchmark_output/benchmark_summary.csv" -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Green
