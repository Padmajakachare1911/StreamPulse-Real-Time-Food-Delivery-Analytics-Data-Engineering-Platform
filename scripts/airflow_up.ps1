<#
.SYNOPSIS
    Start Airflow (web UI on http://localhost:8080, user "admin", password = AIRFLOW_ADMIN_PASSWORD in .env).
    Creates the separate "airflow" database in the existing PostgreSQL first (never touches the streampulse data).
    Terminal T4. The container keeps running until .\scripts\airflow_down.ps1.
#>
. "$PSScriptRoot\_common.ps1"
$ErrorActionPreference = "Continue"
docker compose up -d kafka postgres | Out-Null

# Airflow keeps its own metadata in a separate database inside the same PostgreSQL server.
$exists = docker exec streampulse-postgres psql -U streampulse -d postgres -t -A -c "SELECT 1 FROM pg_database WHERE datname = 'airflow'"
if ($exists -ne "1") {
    docker exec streampulse-postgres psql -U streampulse -d postgres -c "CREATE DATABASE airflow" | Out-Null
    Write-Output "Created database airflow"
}

docker compose --profile airflow up -d --build airflow 2>&1 | Out-Host
for ($i = 1; $i -le 60; $i++) {
    try {
        $h = Invoke-RestMethod -Uri "http://localhost:8080/health" -TimeoutSec 3
        if ($h.metadatabase.status -eq "healthy" -and $h.scheduler.status -eq "healthy") {
            Write-Output "Airflow is healthy: metadatabase=$($h.metadatabase.status) scheduler=$($h.scheduler.status)"
            Write-Output "UI: http://localhost:8080   user: admin   password: see AIRFLOW_ADMIN_PASSWORD in .env"
            exit 0
        }
    } catch { }
    Start-Sleep -Seconds 5
}
Write-Error "Airflow did not become healthy within 5 minutes. See: docker logs streampulse-airflow"
exit 1
