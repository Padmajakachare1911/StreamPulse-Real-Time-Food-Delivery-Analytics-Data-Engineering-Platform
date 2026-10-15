<# Show partitions, leaders and replication factor of a topic.  .\scripts\kafka_describe_topic.ps1 [-Topic name] #>
param([string]$Topic = "")
. "$PSScriptRoot\_common.ps1"
if (-not $Topic) { $Topic = Get-DefaultTopic }
docker exec $script:KafkaContainer "$script:KafkaBin/kafka-topics.sh" --bootstrap-server localhost:9092 --describe --topic $Topic
