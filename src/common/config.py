import os
from pathlib import Path

from dotenv import load_dotenv

# Find the root directory of the project.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load environment variables from the project root.
load_dotenv(PROJECT_ROOT / ".env")


def get_setting(name: str, default: str | None = None) -> str:
    """Return a configuration value or raise an error if it is missing."""
    value = os.getenv(name, default)

    if value is None or not value.strip():
        raise ValueError(f"Missing required configuration: {name}")

    return value


PROJECT_NAME = get_setting("PROJECT_NAME", "StreamPulse")
ENVIRONMENT = get_setting("ENVIRONMENT", "development")

KAFKA_BOOTSTRAP_SERVERS = get_setting(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC = get_setting("KAFKA_TOPIC", "food_orders")

SPARK_APP_NAME = get_setting(
    "SPARK_APP_NAME", "StreamPulseFoodOrderStreaming"
)

PARQUET_OUTPUT_PATH = get_setting(
    "PARQUET_OUTPUT_PATH", "data/processed/orders"
)
CHECKPOINT_PATH = get_setting(
    "CHECKPOINT_PATH", "data/processed/checkpoints"
)


if __name__ == "__main__":
    print(f"Project: {PROJECT_NAME}")
    print(f"Environment: {ENVIRONMENT}")
    print(f"Kafka server: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"Spark application: {SPARK_APP_NAME}")
    print(f"Parquet output: {PARQUET_OUTPUT_PATH}")
    print(f"Checkpoint path: {CHECKPOINT_PATH}")
