<# List topics on the broker. #>
. "$PSScriptRoot\_common.ps1"
docker exec $script:KafkaContainer "$script:KafkaBin/kafka-topics.sh" --bootstrap-server localhost:9092 --list
