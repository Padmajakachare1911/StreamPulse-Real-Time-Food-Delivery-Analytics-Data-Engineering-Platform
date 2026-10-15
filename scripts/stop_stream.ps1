<#
.SYNOPSIS
    Ask the running stream to stop safely: it finishes the current micro-batch, then exits.
    (Writes a sentinel file in the mounted checkpoint folder; the job checks for it every 2 seconds.)
#>
param([string]$StreamName = "orders")
. "$PSScriptRoot\_common.ps1"
$dir = Join-Path $script:Root "data\checkpoints"
New-Item -ItemType Directory -Force $dir | Out-Null
New-Item -ItemType File -Force (Join-Path $dir "STOP_$StreamName") | Out-Null
Write-Output "Stop requested for stream $StreamName (data\checkpoints\STOP_$StreamName created)."
