<#
.SYNOPSIS
    Run a command and save its real output as an evidence file with the standard header
    (Task ID, time, command, working directory, git commit) required by docs/06.

.EXAMPLE
    .\scripts\save_evidence.ps1 -TaskId ENV-01 -OutFile docs\evidence\00_environment\env_check.txt -Command 'python --version'
#>
param(
    [Parameter(Mandatory = $true)][string]$TaskId,
    [Parameter(Mandatory = $true)][string]$OutFile,
    [Parameter(Mandatory = $true)][string]$Command
)

$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$commit = (git rev-parse --short HEAD 2>$null)
if (-not $commit) { $commit = "no-commit-yet" }

$outDir = Split-Path -Parent $OutFile
if ($outDir -and -not (Test-Path $outDir)) { New-Item -ItemType Directory -Force $outDir | Out-Null }

$header = @(
    "Task ID: $TaskId",
    "Date/time (UTC): $((Get-Date).ToUniversalTime().ToString('o'))",
    "Date/time (local): $((Get-Date).ToString('o'))",
    "Command: $Command",
    "Working directory: $root",
    "Git commit: $commit",
    "--- output ---"
)
$header | Set-Content -Path $OutFile -Encoding utf8

# Run in a child PowerShell; cmd.exe merges stderr into stdout at OS level, so native
# stderr text is saved as plain text instead of PowerShell error records.
# The child exits with the last native command's real exit code ($LASTEXITCODE).
# Native stderr lines arrive as error records; ToString() keeps only their text.
$childScript = "`$ProgressPreference = 'SilentlyContinue'; `$ErrorActionPreference = 'Continue'; " +
    "& { $Command } *>&1 | ForEach-Object { `$_.ToString() }; " +
    "if (`$null -ne `$LASTEXITCODE) { exit `$LASTEXITCODE }"
$encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($childScript))
$output = cmd /c "powershell -NoProfile -NonInteractive -OutputFormat Text -EncodedCommand $encoded 2>&1" | Out-String
$exit = $LASTEXITCODE
Add-Content -Path $OutFile -Value $output -Encoding utf8
Add-Content -Path $OutFile -Value "--- exit code: $exit ---" -Encoding utf8
Write-Host "Saved evidence to $OutFile (exit code $exit)"
exit $exit
