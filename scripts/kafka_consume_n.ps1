<# Read the first N messages of a topic (from the beginning) and print key + value.
   .\scripts\kafka_consume_n.ps1 -Count 3 [-Topic name] [-Partition 0] #>
param([string]$Topic = "", [int]$Count = 3, [int]$Partition = -1)
. "$PSScriptRoot\_common.ps1"
if (-not $Topic) { $Topic = Get-DefaultTopic }
$extra = @()
if ($Partition -ge 0) { $extra = @("--partition", "$Partition", "--offset", "earliest") } else { $extra = @("--from-beginning") }
docker exec $script:KafkaContainer "$script:KafkaBin/kafka-console-consumer.sh" --bootstrap-server localhost:9092 `
    --topic $Topic @extra --max-messages $Count --timeout-ms 20000 --property print.key=true --property print.partition=true --property print.offset=true
