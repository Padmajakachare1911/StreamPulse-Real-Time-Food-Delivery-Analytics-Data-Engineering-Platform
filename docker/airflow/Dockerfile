FROM apache/airflow:2.10.5-python3.11

# Java for Spark (the DAG runs the finite Spark job inside this container).
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Project code runs in its OWN virtualenv, so its packages (pandas 3, PySpark 4.2, ...) never
# interfere with Airflow's own pinned dependencies.
USER airflow

# pytest for the DAG structure tests (installed next to Airflow, not in the project venv).
RUN pip install --no-cache-dir pytest==8.3.5

RUN python -m venv /home/airflow/project-venv \
    && /home/airflow/project-venv/bin/pip install --no-cache-dir \
       pyspark==4.2.0 kafka-python==3.0.11 psycopg2-binary==2.9.13 PyYAML==6.0.3 python-dotenv==1.2.4 \
       pandas==3.0.6 pyarrow==26.0.0 requests==2.34.2 pytest==9.1.1

# Download the Kafka connector into the image once.
RUN /home/airflow/project-venv/bin/python -c "from pyspark.sql import SparkSession; s = SparkSession.builder.master('local[1]').config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0').getOrCreate(); s.stop()"
