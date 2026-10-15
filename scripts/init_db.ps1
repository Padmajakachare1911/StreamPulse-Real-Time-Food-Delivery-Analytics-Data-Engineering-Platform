<#
.SYNOPSIS
    Start PostgreSQL if needed and create the schemas and tables (sql/schema.sql).
    Safe to run again: it only uses CREATE ... IF NOT EXISTS and never drops anything.
#>
. "$PSScriptRoot\_common.ps1"
docker compose up -d postgres | Out-Null
for ($i = 1; $i -le 20; $i++) {
    $health = docker inspect --format "{{.State.Health.Status}}" streampulse-postgres 2>$null
    if ($health -eq "healthy") { break }
    Start-Sleep -Seconds 3
}
& "$script:Root\.venv\Scripts\python.exe" "$script:Root\scripts\init_db.py"
