<#
.SYNOPSIS
    Ask the running stream to stop safely: it waits for the current micro-batch to finish, then exits.
    (Writes a sentinel file in the mounted checkpoint folder; the job checks for it every 2 seconds.)
.EXAMPLE
    .\scripts\stop_stream.ps1              # the production stream "orders"
    .\scripts\stop_stream.ps1 -Demo 4      # the isolated stream of demo 4
#>
param([string]$StreamName = "orders", [int]$Demo = 0)
. "$PSScriptRoot\_common.ps1"
if ($Demo -gt 0) {
    $StreamName = "demo$Demo"
    $dir = Join-Path $script:Root "data\test_runs\demo$Demo\checkpoints"
} else {
    $dir = Join-Path $script:Root "data\checkpoints"
}
New-Item -ItemType Directory -Force $dir | Out-Null
New-Item -ItemType File -Force (Join-Path $dir "STOP_$StreamName") | Out-Null
Write-Output "Stop requested for stream $StreamName ($dir\STOP_$StreamName created). It finishes the current batch first."
