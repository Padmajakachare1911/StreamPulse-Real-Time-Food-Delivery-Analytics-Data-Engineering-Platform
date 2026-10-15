"""Single place where every setting is read.

Values come from environment variables (loaded from the git-ignored .env file),
with safe defaults for local development. Other modules import constants from here
instead of reading the environment themselves.
"""
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


def get_path(name: str, default: str) -> Path:
    """Return a path setting. Relative paths are relative to the project root."""
    path = Path(get_setting(name, default))
    return path if path.is_absolute() else PROJECT_ROOT / path


PROJECT_NAME = get_setting("PROJECT_NAME", "StreamPulse")
ENVIRONMENT = get_setting("ENVIRONMENT", "development")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = get_setting("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = get_setting("KAFKA_TOPIC", "ecommerce_orders")

# Files and folders
DATASET_DIR = get_path("DATASET_PATH", "data/raw")
PARQUET_OUTPUT_DIR = get_path("PARQUET_OUTPUT_DIR", "data/processed/parquet")
CHECKPOINT_DIR = get_path("CHECKPOINT_DIR", "data/checkpoints")
DEAD_LETTER_DIR = get_path("DEAD_LETTER_DIR", "data/dead_letter")
INGESTION_RUNS_DIR = get_path("INGESTION_RUNS_DIR", "data/processed/ingestion_runs")
EVIDENCE_DIR = get_path("EVIDENCE_DIR", "docs/evidence")
FIELD_MAPPING_FILE = get_path("FIELD_MAPPING_FILE", "config/field_mapping.yaml")
QUALITY_RULES_FILE = get_path("QUALITY_RULES_FILE", "config/quality_rules.yaml")

# Replay behaviour (records per second; 0 means as fast as possible)
REPLAY_RATE = float(get_setting("REPLAY_RATE", "0"))
BATCH_SIZE = int(get_setting("BATCH_SIZE", "500"))

# PostgreSQL
POSTGRES_HOST = get_setting("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(get_setting("POSTGRES_PORT", "5432"))
POSTGRES_DB = get_setting("POSTGRES_DB", "streampulse")
POSTGRES_USER = get_setting("POSTGRES_USER", "streampulse")
# No default on purpose: set POSTGRES_PASSWORD in .env (src/warehouse/db.py reports it clearly if missing).
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# External API (currency rates). API_KEY is optional: the chosen provider needs none.
API_BASE_URL = get_setting("API_BASE_URL", "https://api.frankfurter.dev/v2")
API_KEY = os.getenv("API_KEY", "")
API_USER_AGENT = get_setting("API_USER_AGENT", "StreamPulse-student-project/1.0")

# Spark
SPARK_APP_NAME = get_setting("SPARK_APP_NAME", "StreamPulseOrderStreaming")
SPARK_MAX_OFFSETS_PER_TRIGGER = int(get_setting("SPARK_MAX_OFFSETS_PER_TRIGGER", "10000"))


if __name__ == "__main__":
    print(f"Project: {PROJECT_NAME} ({ENVIRONMENT})")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS} topic={KAFKA_TOPIC}")
    print(f"Dataset dir: {DATASET_DIR}")
    print(f"Parquet: {PARQUET_OUTPUT_DIR}")
    print(f"Checkpoints: {CHECKPOINT_DIR}")
    print(f"Postgres: {POSTGRES_USER}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB} (password hidden)")
    print(f"API: {API_BASE_URL}")
