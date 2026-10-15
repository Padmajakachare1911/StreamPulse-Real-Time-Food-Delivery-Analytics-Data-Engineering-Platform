<#
.SYNOPSIS
    Prepare the ISOLATED environment of Demo 3 or Demo 4: own Kafka topic, own PostgreSQL database, own folders.
    The real topic, database and the production checkpoint (data\checkpoints\orders) are never used or changed.

.EXAMPLE
    .\scripts\demo_setup.ps1 -Demo 3     # topic demo3_orders, database streampulse_demo3, folder data\test_runs\demo3
    .\scripts\demo_setup.ps1 -Demo 4
#>
param([Parameter(Mandatory = $true)][ValidateSet(3, 4)][int]$Demo)
. "$PSScriptRoot\_common.ps1"
$topic = "demo${Demo}_orders"
$db = "streampulse_demo$Demo"
docker compose up -d kafka postgres | Out-Null
docker exec $script:KafkaContainer "$script:KafkaBin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --if-not-exists `
    --topic $topic --partitions 1 --replication-factor 1 2>&1 | ForEach-Object { "$_" } | Where-Object { $_ -notmatch "WARNING" }
& "$script:Root\.venv\Scripts\python.exe" "$script:Root\scripts\demo_db.py" create $db
New-Item -ItemType Directory -Force (Join-Path $script:Root "data\test_runs\demo$Demo") | Out-Null
Write-Output "Demo $Demo is ready: topic=$topic database=$db folder=data\test_runs\demo$Demo"
