<#
.SYNOPSIS
    Print tool versions, container status, Kafka topics and port reachability.
    Run from the repository root:  .\scripts\check_env.ps1
#>
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$py = Join-Path $root ".venv\Scripts\python.exe"

function Section($title) { Write-Output ""; Write-Output "## $title" }

Section "Python (venv)"
if (Test-Path $py) { & $py --version } else { Write-Output "MISSING: .venv (run: python -m venv .venv)" }

Section "Java"
cmd /c "java -version 2>&1"

Section "Docker"
docker --version
docker compose version

Section "PySpark / Kafka client versions (pip)"
if (Test-Path $py) { & $py -m pip freeze | Select-String -Pattern "pyspark|kafka|psycopg|pandas|pytest|dotenv|requests|streamlit|pyarrow|yaml" }

Section "HADOOP_HOME (Windows Spark note)"
Write-Output "HADOOP_HOME=$env:HADOOP_HOME"

Section "Containers"
docker ps -a --format "table {{.Names}}`t{{.Status}}`t{{.Ports}}"

Section "Kafka topics"
docker exec streampulse-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

Section "Port reachability"
foreach ($p in 9092, 5432, 8080, 8501) {
    $r = Test-NetConnection -ComputerName localhost -Port $p -WarningAction SilentlyContinue
    Write-Output ("localhost:{0} reachable = {1}" -f $p, $r.TcpTestSucceeded)
}

Section "Git"
git branch --show-current
git remote -v
