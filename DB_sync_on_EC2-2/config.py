ATHENA_REGION = "us-east-1"

ATHENA_DATABASE = "farhan_analytics"

S3_STAGING_DIR = "s3://aws-athena-query-results-537124955775-us-east-1/"

POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_DB = "telemetry_db"
POSTGRES_USER = "farhan"
POSTGRES_PASSWORD = "Farhan@123"

TABLE_MAPPING = {
    "vw_active_vehicle_snapshot": "active_vehicle_snapshot",
    "vw_overspeed_summary": "overspeed_report",
    "vw_restricted_zone_summary": "restricted_zone_report",
    "vw_driver_risk_score": "driver_risk_report",
    "vw_hourly_overspeed_trend": "hourly_overspeed_trend",
    "vw_driver_behavior_trend": "driver_behavior_report",
    "vw_fleet_risk_trend": "fleet_risk_report",
    "vw_asset_history": "asset_history_report",
    "vw_driver_safety": "driver_safety_status"
}