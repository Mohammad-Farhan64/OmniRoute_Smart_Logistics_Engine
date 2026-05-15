import os
from dotenv import load_dotenv

# ---------------------------------------------------
# LOAD ENV
# ---------------------------------------------------

load_dotenv("/home/ubuntu/project.env")

# ---------------------------------------------------
# PROJECT
# ---------------------------------------------------

PROJECT_NAME = "OmniRoute Smart Logistics Engine"

ENVIRONMENT = os.getenv(
    "ENVIRONMENT",
    "DEV"
)

# ---------------------------------------------------
# AWS
# ---------------------------------------------------

AWS_REGION = os.getenv(
    "AWS_REGION",
    "us-east-1"
)

# ---------------------------------------------------
# KAFKA
# ---------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "vehicle_telemetry"
)

# ---------------------------------------------------
# POSTGRES
# ---------------------------------------------------

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST"
)

POSTGRES_PORT = os.getenv(
    "POSTGRES_PORT",
    "5432"
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB"
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER"
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD"
)

# ---------------------------------------------------
# S3
# ---------------------------------------------------

S3_GOLD_BUCKET = os.getenv(
    "S3_GOLD_BUCKET"
)

S3_DRIVER_SAFETY_PATH = (
    f"{S3_GOLD_BUCKET}/driver_safety_status/"
)

S3_DRIVER_RISK_PATH = (
    f"{S3_GOLD_BUCKET}/driver_risk_aggregates/"
)

S3_ASSET_HISTORY_PATH = (
    f"{S3_GOLD_BUCKET}/asset_history_scd2/"
)

# ---------------------------------------------------
# CHECKPOINTS
# ---------------------------------------------------

CHECKPOINT_DRIVER_SAFETY = os.getenv(
    "CHECKPOINT_DRIVER_SAFETY"
)

CHECKPOINT_DRIVER_RISK = os.getenv(
    "CHECKPOINT_DRIVER_RISK"
)

CHECKPOINT_POSTGRES_STATE = os.getenv(
    "CHECKPOINT_POSTGRES_STATE"
)

# ---------------------------------------------------
# STREAM SETTINGS
# ---------------------------------------------------

WATERMARK_DELAY = os.getenv(
    "WATERMARK_DELAY",
    "10 minutes"
)

STREAM_TRIGGER = os.getenv(
    "STREAM_TRIGGER",
    "1 minute"
)

MAX_OFFSETS_PER_TRIGGER = int(
    os.getenv(
        "MAX_OFFSETS_PER_TRIGGER",
        "5000"
    )
)

SHUFFLE_PARTITIONS = int(
    os.getenv(
        "SHUFFLE_PARTITIONS",
        "4"
    )
)

# ---------------------------------------------------
# RISK THRESHOLDS
# ---------------------------------------------------

LOW_RISK_THRESHOLD = int(
    os.getenv(
        "LOW_RISK_THRESHOLD",
        "1"
    )
)

MEDIUM_RISK_THRESHOLD = int(
    os.getenv(
        "MEDIUM_RISK_THRESHOLD",
        "3"
    )
)

HIGH_RISK_THRESHOLD = int(
    os.getenv(
        "HIGH_RISK_THRESHOLD",
        "5"
    )
)

CRITICAL_RISK_THRESHOLD = int(
    os.getenv(
        "CRITICAL_RISK_THRESHOLD",
        "25"
    )
)

SUSPENSION_THRESHOLD = int(
    os.getenv(
        "SUSPENSION_THRESHOLD",
        "15"
    )
)

# ---------------------------------------------------
# BUSINESS RULES
# ---------------------------------------------------

OVERSPEED_LIMIT = int(
    os.getenv(
        "OVERSPEED_LIMIT",
        "110"
    )
)

RPM_LIMIT = int(
    os.getenv(
        "RPM_LIMIT",
        "4500"
    )
)

RESTRICTED_ZONE_MIN_LAT = float(
    os.getenv(
        "RESTRICTED_ZONE_MIN_LAT",
        "12.5"
    )
)

RESTRICTED_ZONE_MAX_LAT = float(
    os.getenv(
        "RESTRICTED_ZONE_MAX_LAT",
        "12.8"
    )
)

RESTRICTED_ZONE_MIN_LON = float(
    os.getenv(
        "RESTRICTED_ZONE_MIN_LON",
        "77.2"
    )
)

RESTRICTED_ZONE_MAX_LON = float(
    os.getenv(
        "RESTRICTED_ZONE_MAX_LON",
        "77.5"
    )
)

# ---------------------------------------------------
# SUSPENSION WINDOWS
# ---------------------------------------------------

WARNING_THRESHOLD = int(
    os.getenv(
        "WARNING_THRESHOLD",
        "8"
    )
)

SUSPENSION_24H_THRESHOLD = int(
    os.getenv(
        "SUSPENSION_24H_THRESHOLD",
        "15"
    )
)

SUSPENSION_7D_THRESHOLD = int(
    os.getenv(
        "SUSPENSION_7D_THRESHOLD",
        "25"
    )
)

# ---------------------------------------------------
# PENALTY CONFIG
# ---------------------------------------------------

RATE_DEDUCTION_PERCENT = float(
    os.getenv(
        "RATE_DEDUCTION_PERCENT",
        "0.05"
    )
)

# ---------------------------------------------------
# LOGGING
# ---------------------------------------------------

LOG_LEVEL = os.getenv(
    "LOG_LEVEL",
    "WARN"
)

# ---------------------------------------------------
# AIRFLOW
# ---------------------------------------------------

AIRFLOW_RETRIES = int(
    os.getenv(
        "AIRFLOW_RETRIES",
        "2"
    )
)

AIRFLOW_RETRY_DELAY_MINUTES = int(
    os.getenv(
        "AIRFLOW_RETRY_DELAY_MINUTES",
        "5"
    )
)

# ---------------------------------------------------
# DEBUG
# ---------------------------------------------------

ENABLE_DEBUG = os.getenv(
    "ENABLE_DEBUG",
    "false"
).lower() == "true"