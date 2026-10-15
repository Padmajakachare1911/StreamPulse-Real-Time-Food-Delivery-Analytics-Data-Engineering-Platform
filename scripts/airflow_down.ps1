<#
.SYNOPSIS
    Stop the Airflow container. Nothing is deleted: DAG history stays in the "airflow" database, logs in airflow\logs.
#>
. "$PSScriptRoot\_common.ps1"
docker compose --profile airflow stop airflow
