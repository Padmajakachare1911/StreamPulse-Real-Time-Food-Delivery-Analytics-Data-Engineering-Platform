<#
.SYNOPSIS
    Run the Spark transform tests inside the Spark container (PySpark cannot start on this Windows host).
.EXAMPLE
    .\scripts\run_tests_docker.ps1
#>
. "$PSScriptRoot\_common.ps1"
docker compose --profile spark run --rm --no-deps spark python -m pytest tests/unit/test_spark_transforms.py -m spark -v -p no:cacheprovider
