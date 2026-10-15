<#
.SYNOPSIS
    Run the Spark streaming job inside Docker (host Spark needs winutils.exe, see docs/baseline_audit.md).
    Terminal T2. Stays open while the stream runs. Stop with Ctrl+C, or run .\scripts\stop_stream.ps1 in another terminal.

.EXAMPLE
    .\scripts\run_stream.ps1                          # continuous, a micro-batch every 5 seconds, Kafka -> Parquet + PostgreSQL
    .\scripts\run_stream.ps1 -Trigger availableNow     # finite: process what is in Kafka, then exit
    .\scripts\run_stream.ps1 -NoPostgres               # Parquet only
    .\scripts\run_stream.ps1 -Demo 4 -Trigger availableNow   # the ISOLATED environment of demo 4 (see scripts\demo_setup.ps1)
#>
param(
    [string]$Trigger = "5 seconds",
    [string]$StreamName = "orders",
    [int]$Demo = 0,
    [int]$MaxOffsetsPerTrigger = 0,
    [switch]$NoPostgres
)
. "$PSScriptRoot\_common.ps1"
$ErrorActionPreference = "Continue"

# Start the services the job needs (does nothing if they already run).
docker compose up -d kafka postgres | Out-Null

$argsList = @("--trigger", $Trigger)
$envList = @()
if ($Demo -gt 0) {
    # Isolated demo environment: own topic, database, checkpoint, Parquet and dead-letter folders.
    $StreamName = "demo$Demo"
    $base = "/app/data/test_runs/demo$Demo"
    $envList = @("-e", "KAFKA_TOPIC=demo${Demo}_orders", "-e", "POSTGRES_DB=streampulse_demo$Demo")
    $argsList += @("--topic", "demo${Demo}_orders", "--checkpoint-dir", "$base/checkpoints", "--parquet-dir", "$base/parquet",
                   "--dead-letter-dir", "$base/dead_letter")
} else {
    & "$script:Root\.venv\Scripts\python.exe" "$script:Root\scripts\init_db.py" | Out-Null
}
$argsList = @("--stream-name", $StreamName) + $argsList
if ($MaxOffsetsPerTrigger -gt 0) { $argsList += @("--max-offsets-per-trigger", $MaxOffsetsPerTrigger) }
if (-not $NoPostgres) { $argsList += "--postgres" }

# -T: no pseudo-terminal, so the output can also be captured to a file or by another script.
docker compose --profile spark run --rm --no-deps -T @envList spark python -m src.streaming.order_stream @argsList
