<#
.SYNOPSIS
    Start the Streamlit dashboard on http://localhost:8501 (Terminal T4 / Browser). Stop with Ctrl+C.
    Needs PostgreSQL for data; Kafka is only used for the live status and message count.

.EXAMPLE
    .\scripts\run_dashboard.ps1              # the real warehouse
    .\scripts\run_dashboard.ps1 -Demo 3      # show the isolated database/topic/Parquet of demo 3
#>
param([int]$Demo = 0)
. "$PSScriptRoot\_common.ps1"
if ($Demo -gt 0) {
    $env:POSTGRES_DB = "streampulse_demo$Demo"
    $env:KAFKA_TOPIC = "demo${Demo}_orders"
    $env:PARQUET_OUTPUT_DIR = Join-Path $script:Root "data\test_runs\demo$Demo\parquet"
    Write-Output "Dashboard shows DEMO $Demo data (database $($env:POSTGRES_DB), topic $($env:KAFKA_TOPIC))."
}
& "$script:Root\.venv\Scripts\python.exe" -m streamlit run "$script:Root\src\dashboard\app.py" `
    --server.port 8501 --server.headless true --browser.gatherUsageStats false
