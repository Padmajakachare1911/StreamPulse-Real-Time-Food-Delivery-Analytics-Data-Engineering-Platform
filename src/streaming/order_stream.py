from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PARQUET_OUTPUT_PATH,
    SPARK_APP_NAME,
)


ORDER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("restaurant_id", StringType(), True),
        StructField("restaurant_name", StringType(), True),
        StructField("item", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("order_timestamp", StringType(), True),
    ]
)


def main() -> None:
    spark = (
    SparkSession.builder
    .appName(SPARK_APP_NAME)
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
    )
    .getOrCreate()
)

    spark.sparkContext.setLogLevel("WARN")

    print("Starting StreamPulse Spark Structured Streaming...")
    print(f"Kafka topic: {KAFKA_TOPIC}")

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_orders = (
        kafka_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), ORDER_SCHEMA).alias("order"))
        .select("order.*")
        .filter(col("order_id").isNotNull())
        .withColumn("total_amount", col("total_amount").cast(DoubleType()))
    )

    output_path = str(
        (spark.sparkContext._jvm.java.io.File(PARQUET_OUTPUT_PATH).getCanonicalPath())
    )
    checkpoint_path = str(
        (spark.sparkContext._jvm.java.io.File("data/processed/checkpoints").getCanonicalPath())
    )

    query = (
        parsed_orders.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .option("truncate", False)
        .start()
    )

    print(f"Writing processed orders to: {output_path}")
    print("Streaming is running. Press Ctrl+C to stop.")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping Spark stream...")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
