"""Pure Spark DataFrame transformations (no Kafka, no files), so they can be tested alone.

Flow for each micro-batch:
  Kafka rows -> parse_messages -> split_valid_invalid -> flatten_valid -> dedup_in_batch
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

# Mapping types -> Spark types. Timestamps arrive as ISO text and are converted in flatten_valid.
_SPARK_TYPES = {"string": StringType(), "int": LongType(), "float": DoubleType(), "timestamp": StringType()}

ENRICHMENT_SCHEMA = StructType([
    StructField("fx_rate_brl_usd", DoubleType()),
    StructField("fx_rate_date", StringType()),
    StructField("fx_match_type", StringType()),
    StructField("payments_value_usd", DoubleType()),
    StructField("api_source_name", StringType()),
    StructField("api_endpoint", StringType()),
    StructField("api_retrieved_at", StringType()),
    StructField("api_http_status", LongType()),
    StructField("api_cache_key", StringType()),
])

MALFORMED_JSON = "MALFORMED_JSON"
SCHEMA_MISMATCH = "SCHEMA_MISMATCH"


def payload_schema(mapping: dict) -> StructType:
    """The payload struct is generated from config/field_mapping.yaml (single source of truth)."""
    return StructType([StructField(f["canonical_name"], _SPARK_TYPES[f["type"]]) for f in mapping["fields"]])


def envelope_schema(mapping: dict) -> StructType:
    """Schema of the Kafka message value (docs/04 section 1). `raw` is kept as JSON text separately."""
    return StructType([
        StructField("schema_version", StringType()),
        StructField("record_uid", StringType()),
        StructField("business_hash", StringType()),
        StructField("source_category", StringType()),
        StructField("source_file", StringType()),
        StructField("source_file_sha256", StringType()),
        StructField("source_row_number", LongType()),
        StructField("ingest_run_id", StringType()),
        StructField("ingested_at", StringType()),
        StructField("payload", payload_schema(mapping)),
        StructField("api_enrichment", ENRICHMENT_SCHEMA),
    ])


def parse_messages(kafka_df: DataFrame, mapping: dict) -> DataFrame:
    """Keep the Kafka position and try to parse the JSON value. Never raises on bad JSON."""
    text = F.col("value").cast("string")
    return kafka_df.select(
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        text.alias("json_str"),
    ).withColumn("env", F.from_json("json_str", envelope_schema(mapping))
    ).withColumn("raw_json", F.get_json_object("json_str", "$.raw"))


def split_valid_invalid(parsed: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(valid, invalid). Invalid rows carry a reason_code and are sent to the dead-letter queue."""
    # from_json never returns a null struct for broken text (it gives a struct full of nulls),
    # so JSON validity is checked separately: get_json_object('$') is null only for invalid JSON.
    is_valid_json = F.get_json_object("json_str", "$").isNotNull()
    reason = (F.when(~is_valid_json, F.lit(MALFORMED_JSON))
              .when(F.col("env.record_uid").isNull() | F.col("env.payload.order_id").isNull(), F.lit(SCHEMA_MISMATCH)))
    tagged = parsed.withColumn("reason_code", reason)
    return tagged.filter(F.col("reason_code").isNull()), tagged.filter(F.col("reason_code").isNotNull())


def flatten_valid(valid: DataFrame, mapping: dict) -> DataFrame:
    """One flat row per message: envelope fields, typed payload fields, enrichment and Kafka position."""
    columns = [
        F.col("env.record_uid").alias("record_uid"),
        F.col("env.business_hash").alias("business_hash"),
        F.col("env.source_category").alias("source_category"),
        F.col("env.source_file_sha256").alias("source_file_sha256"),
        F.col("env.source_row_number").alias("source_row_number"),
        F.col("env.ingest_run_id").alias("ingest_run_id"),
        F.to_timestamp("env.ingested_at").alias("ingested_at"),
    ]
    for field in mapping["fields"]:
        name = field["canonical_name"]
        value = F.col(f"env.payload.{name}")
        columns.append((F.to_timestamp(value) if field["type"] == "timestamp" else value).alias(name))
    columns += [
        F.col("env.api_enrichment.fx_rate_brl_usd").alias("fx_rate_brl_usd"),
        F.to_date("env.api_enrichment.fx_rate_date").alias("fx_rate_date"),
        F.col("env.api_enrichment.fx_match_type").alias("fx_match_type"),
        F.col("env.api_enrichment.payments_value_usd").alias("payments_value_usd"),
        F.col("env.api_enrichment.api_source_name").alias("api_source_name"),
        F.col("env.api_enrichment.api_endpoint").alias("api_endpoint"),
        F.to_timestamp("env.api_enrichment.api_retrieved_at").alias("api_retrieved_at"),
        F.col("env.api_enrichment.api_http_status").alias("api_http_status"),
        F.col("env.api_enrichment.api_cache_key").alias("api_cache_key"),
        F.col("env.api_enrichment.api_source_name").isNotNull().alias("api_enriched"),
        F.col("raw_json"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
    ]
    return valid.select(*columns)


def dedup_in_batch(flat: DataFrame) -> DataFrame:
    """Keep ONE row per record_uid inside a batch: the one with the smallest Kafka position."""
    ranking = Window.partitionBy("record_uid").orderBy("kafka_partition", "kafka_offset")
    return (flat.withColumn("_rank", F.row_number().over(ranking))
            .filter(F.col("_rank") == 1).drop("_rank"))
