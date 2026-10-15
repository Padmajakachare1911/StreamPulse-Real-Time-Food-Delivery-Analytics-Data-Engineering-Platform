<#
.SYNOPSIS
    Run the Spark streaming job inside Docker (host Spark needs winutils.exe, see docs/baseline_audit.md).
    Terminal T2. Stays open while the stream runs. Stop with Ctrl+C, or run .\scripts\stop_stream.ps1 in another terminal.

.EXAMPLE
    .\scripts\run_stream.ps1                          # continuous, a micro-batch every 5 seconds, Kafka -> Parquet + PostgreSQL
    .\scripts\run_stream.ps1 -Trigger availableNow     # finite: process what is in Kafka, then exit
    .\scripts\run_stream.ps1 -NoPostgres               # Parquet only
#>
param(
    [string]$Trigger = "5 seconds",
    [string]$StreamName = "orders",
    [switch]$NoPostgres
)
. "$PSScriptRoot\_common.ps1"
$ErrorActionPreference = "Continue"

# Start the services the job needs (does nothing if they already run).
docker compose up -d kafka postgres | Out-Null
& "$script:Root\.venv\Scripts\python.exe" "$script:Root\scripts\init_db.py" | Out-Null

$argsList = @("--stream-name", $StreamName, "--trigger", $Trigger)
if (-not $NoPostgres) { $argsList += "--postgres" }

# -T: no pseudo-terminal, so the output can also be captured to a file or by another script.
docker compose --profile spark run --rm --no-deps -T spark python -m src.streaming.order_stream @argsList
