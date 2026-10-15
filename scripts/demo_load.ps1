<#
.SYNOPSIS
    Load the ingestion-run summaries and dead-letter records of an ISOLATED demo (3 or 4) into that demo's database,
    so the dashboard "Data quality" page can show them. Safe to repeat (replace-per-run, no duplicates).
    Only reads runs of the demo topic; the real warehouse is not touched.
.EXAMPLE
    .\scripts\demo_load.ps1 -Demo 3
#>
param([Parameter(Mandatory = $true)][ValidateSet(3, 4)][int]$Demo)
. "$PSScriptRoot\_common.ps1"
$env:POSTGRES_DB = "streampulse_demo$Demo"
$env:KAFKA_TOPIC = "demo${Demo}_orders"
$env:PYTHONUTF8 = "1"
& "$script:Root\.venv\Scripts\python.exe" -m src.warehouse.load_reference --only runs dlq
