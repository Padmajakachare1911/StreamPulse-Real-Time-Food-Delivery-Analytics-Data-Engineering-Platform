<# MEASURED messages per partition: end offset minus start offset of each partition.
   .\scripts\kafka_partition_counts.ps1 [-Topic name] #>
param([string]$Topic = "")
. "$PSScriptRoot\_common.ps1"
if (-not $Topic) { $Topic = Get-DefaultTopic }
$end   = docker exec $script:KafkaContainer "$script:KafkaBin/kafka-get-offsets.sh" --bootstrap-server localhost:9092 --topic $Topic --time -1
$start = docker exec $script:KafkaContainer "$script:KafkaBin/kafka-get-offsets.sh" --bootstrap-server localhost:9092 --topic $Topic --time -2
$starts = @{}
foreach ($line in $start) { $p = $line -split ":"; $starts[$p[1]] = [long]$p[2] }
$total = 0
foreach ($line in $end) {
    $p = $line -split ":"
    $count = [long]$p[2] - $starts[$p[1]]
    $total += $count
    Write-Output ("{0}  partition {1}: start_offset={2} end_offset={3} messages={4}" -f $p[0], $p[1], $starts[$p[1]], $p[2], $count)
}
Write-Output "TOTAL messages in topic: $total"
