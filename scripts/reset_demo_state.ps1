<#
.SYNOPSIS
    Safe reset of ONE demo (3 or 4) so it can be repeated. It removes only things that belong to that demo:
      - Kafka topic demo<N>_orders
      - PostgreSQL database streampulse_demo<N>
      - folder data\test_runs\demo<N> (its checkpoint, Parquet, dead letters, logs)
      - its ingestion run summaries and ingest dead-letter files (names starting with demo<N>-)
    It NEVER touches data\checkpoints\orders, data\processed\parquet, the topic ecommerce_orders or the database streampulse.
    Asks for confirmation unless -Force is given.

.EXAMPLE
    .\scripts\reset_demo_state.ps1 -Demo 3
#>
param([Parameter(Mandatory = $true)][ValidateSet(3, 4)][int]$Demo, [switch]$Force)
. "$PSScriptRoot\_common.ps1"
$topic = "demo${Demo}_orders"
$db = "streampulse_demo$Demo"
$folder = Join-Path $script:Root "data\test_runs\demo$Demo"
Write-Output "This will remove: topic $topic, database $db, folder data\test_runs\demo$Demo and files named demo$Demo-* in data\processed\ingestion_runs and data\dead_letter\ingest."
Write-Output "It will NOT touch: topic ecommerce_orders, database streampulse, data\checkpoints\orders."
if (-not $Force) {
    $answer = Read-Host "Type YES to continue"
    if ($answer -ne "YES") { Write-Output "Cancelled. Nothing was removed."; exit 1 }
}
docker exec $script:KafkaContainer "$script:KafkaBin/kafka-topics.sh" --bootstrap-server localhost:9092 --delete --topic $topic 2>&1 | ForEach-Object { "$_" } | Out-Null
& "$script:Root\.venv\Scripts\python.exe" "$script:Root\scripts\demo_db.py" drop $db
if (Test-Path $folder) { Remove-Item $folder -Recurse -Force }
foreach ($dir in "data\processed\ingestion_runs", "data\dead_letter\ingest") {
    Get-ChildItem (Join-Path $script:Root $dir) -Filter "demo$Demo-*" -ErrorAction SilentlyContinue | Remove-Item -Force
}
Write-Output "Demo $Demo state removed."
