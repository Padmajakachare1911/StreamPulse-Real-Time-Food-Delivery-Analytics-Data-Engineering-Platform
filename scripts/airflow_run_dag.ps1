<#
.SYNOPSIS
    Trigger an Airflow DAG, wait (bounded) until the run finishes, and print the state of every task.
    Exit code 0 only when the DAG run ends in state "success".

.EXAMPLE
    .\scripts\airflow_run_dag.ps1 -Dag streampulse_batch -Conf '{"limit": 300}'
    .\scripts\airflow_run_dag.ps1 -Dag streampulse_failure_demo -TimeoutMinutes 10     # ends "failed" on purpose
#>
param(
    [string]$Dag = "streampulse_batch",
    [string]$Conf = "{}",
    [int]$TimeoutMinutes = 30
)
$ErrorActionPreference = "Continue"
$runId = "manual__" + (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmss")

# Pre-flight: a DAG run is only started by the scheduler. If the scheduler is not healthy the run would stay "queued"
# for the whole timeout (this happened once: the scheduler had stopped without an error), so fail fast instead.
try {
    $health = Invoke-RestMethod -Uri "http://localhost:8080/health" -TimeoutSec 5
    if ($health.scheduler.status -ne "healthy") {
        Write-Output "AIRFLOW SCHEDULER NOT HEALTHY (status: $($health.scheduler.status), last heartbeat: $($health.scheduler.latest_scheduler_heartbeat)). No run was started."
        Write-Output "Fix: docker compose --profile airflow restart airflow   (then .\scripts\airflow_up.ps1 waits until it is healthy)"
        exit 3
    }
} catch {
    Write-Output "AIRFLOW IS NOT REACHABLE on http://localhost:8080. Start it with .\scripts\airflow_up.ps1. No run was started."
    exit 3
}

docker exec streampulse-airflow airflow dags unpause $Dag 2>&1 | Out-Host
# PowerShell 5.1 removes the inner double quotes of an argument passed to a native program, so escape them.
$confArg = $Conf -replace '"', '\"'
docker exec streampulse-airflow airflow dags trigger $Dag --run-id $runId --conf $confArg 2>&1 | ForEach-Object { "$_" } | Out-Host
if ($LASTEXITCODE -ne 0) { Write-Output "TRIGGER FAILED (exit code $LASTEXITCODE): no DAG run was created"; exit 2 }
Write-Output "Triggered $Dag run_id=$runId conf=$Conf"

function Get-RunState {
    # `airflow dags state` wants an execution date, so read the run list (JSON) and pick our run_id.
    $raw = docker exec streampulse-airflow airflow dags list-runs -d $Dag -o json 2>$null | Out-String
    $start = $raw.IndexOf("[")
    if ($start -lt 0) { return "unknown" }
    $run = ($raw.Substring($start) | ConvertFrom-Json) | Where-Object { $_.run_id -eq $runId }
    if ($run) { return $run.state } else { return "unknown" }
}

$deadline = (Get-Date).AddMinutes($TimeoutMinutes)
$state = "unknown"
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 10
    $state = Get-RunState
    Write-Output ("{0}  run state: {1}" -f (Get-Date -Format "HH:mm:ss"), $state)
    if ($state -in @("success", "failed")) { break }
}
if ($state -notin @("success", "failed")) { Write-Output "TIMEOUT after $TimeoutMinutes minutes (state: $state)" }
Write-Output ""
Write-Output "--- task states ---"
docker exec streampulse-airflow airflow tasks states-for-dag-run $Dag $runId 2>&1 | Out-Host
Write-Output "FINAL STATE: $state   (dag=$Dag run_id=$runId)"
if ($state -eq "success") { exit 0 } else { exit 1 }
