<# Stop the containers. Data stays in the kafka_data volume (nothing is deleted). #>
. "$PSScriptRoot\_common.ps1"
docker compose stop
