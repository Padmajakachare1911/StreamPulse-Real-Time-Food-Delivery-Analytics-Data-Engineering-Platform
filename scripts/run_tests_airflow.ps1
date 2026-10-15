<#
.SYNOPSIS
    Run the DAG import/structure tests inside the Airflow container (the airflow library is not on the host).
    Needs Airflow running: .\scripts\airflow_up.ps1
#>
. "$PSScriptRoot\_common.ps1"
docker exec -e DAGS_FOLDER=/opt/airflow/dags -w /opt/streampulse streampulse-airflow python -m pytest tests/unit/test_airflow_dag.py -m airflow -v -p no:cacheprovider
