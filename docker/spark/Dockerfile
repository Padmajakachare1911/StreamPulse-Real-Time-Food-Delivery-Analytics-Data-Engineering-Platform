FROM python:3.11-slim-bookworm

# Spark needs Java; Spark runs here (Linux) because host Spark on Windows needs winutils.exe.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Same versions as requirements.txt (PySpark 4.2.0 = Scala 2.13).
RUN pip install --no-cache-dir pyspark==4.2.0 psycopg2-binary==2.9.13 PyYAML==6.0.3 python-dotenv==1.2.4 pandas==3.0.6 pyarrow==26.0.0 pytest==9.1.1

# Download the Kafka connector into the image once, so runs do not depend on Maven.
RUN python -c "from pyspark.sql import SparkSession; s = SparkSession.builder.master('local[1]').config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0').getOrCreate(); s.stop()"

WORKDIR /app
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
