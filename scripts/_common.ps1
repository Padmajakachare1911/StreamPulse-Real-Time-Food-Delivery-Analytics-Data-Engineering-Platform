# Shared helpers for the scripts in this folder (dot-source it: . "$PSScriptRoot\_common.ps1").
$script:Root = Split-Path -Parent $PSScriptRoot
Set-Location $script:Root
$script:KafkaBin = "/opt/kafka/bin"
$script:KafkaContainer = "streampulse-kafka"

function Get-DefaultTopic {
    # Topic comes from .env (KAFKA_TOPIC) so scripts and Python always agree.
    $envFile = Join-Path $script:Root ".env"
    if (Test-Path $envFile) {
        $line = Select-String -Path $envFile -Pattern "^\s*KAFKA_TOPIC\s*=\s*(.+)$" | Select-Object -First 1
        if ($line) { return $line.Matches[0].Groups[1].Value.Trim() }
    }
    return "ecommerce_orders"
}
