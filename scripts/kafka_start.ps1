<# Start Kafka (and create the topic) with Docker Compose, then wait until it is healthy.
   Run from the repo root:  .\scripts\kafka_start.ps1   (Terminal T1 can then be reused) #>
. "$PSScriptRoot\_common.ps1"
docker compose up -d
for ($i = 1; $i -le 30; $i++) {
    $health = docker inspect --format "{{.State.Health.Status}}" $script:KafkaContainer 2>$null
    Write-Output "Kafka health: $health"
    if ($health -eq "healthy") { break }
    Start-Sleep -Seconds 5
}
if ($health -ne "healthy") { Write-Error "Kafka did not become healthy"; exit 1 }
