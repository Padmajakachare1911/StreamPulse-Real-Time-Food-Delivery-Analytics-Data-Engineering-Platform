<#
.SYNOPSIS
    Run an Airflow CLI command inside the running Airflow container.
.EXAMPLE
    .\scripts\airflow.ps1 dags list
    .\scripts\airflow.ps1 dags trigger streampulse_batch --conf '{"limit": 300}'
    .\scripts\airflow.ps1 dags list-runs -d streampulse_batch
#>
docker exec streampulse-airflow airflow @args
