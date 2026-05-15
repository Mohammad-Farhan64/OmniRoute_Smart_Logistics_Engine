from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from datetime import datetime, timedelta

# --------------------------------
# DEFAULT CONFIG
# --------------------------------

default_args = {
    'owner': 'farhan',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

# --------------------------------
# DAG CONFIG
# --------------------------------

dag = DAG(
    dag_id='farhan_daily_batch_pipeline',

    default_args=default_args,

    description='Daily Incremental BRD Batch Pipeline',

    start_date=datetime(2026, 5, 1),

    schedule_interval='@daily',

    catchup=False
)

# --------------------------------
# SILVER JOBS
# --------------------------------

silver_vehicle_assignment = GlueJobOperator(
    task_id='silver_vehicle_assignment',

    job_name='farhan_silver_vehicle_assignment',

    aws_conn_id='aws_default',

    region_name='us-east-1',

    wait_for_completion=True,

    dag=dag
)

silver_fuel_transactions = GlueJobOperator(
    task_id='silver_fuel_transactions',

    job_name='farhan_silver_fuel_transactions',

    aws_conn_id='aws_default',

    region_name='us-east-1',

    wait_for_completion=True,

    dag=dag
)

silver_maintenance = GlueJobOperator(
    task_id='silver_maintenance',

    job_name='farhan_silver_maintenance',

    aws_conn_id='aws_default',

    region_name='us-east-1',

    wait_for_completion=True,

    dag=dag
)

# --------------------------------
# GOLD JOBS
# --------------------------------

gold_fuel_efficiency = GlueJobOperator(
    task_id='gold_fuel_efficiency',

    job_name='farhan_gold_fuel_efficiency',

    aws_conn_id='aws_default',

    region_name='us-east-1',

    wait_for_completion=True,

    dag=dag
)

gold_active_snapshot = GlueJobOperator(
    task_id='gold_active_snapshot',

    job_name='farhan_gold_active_snapshot',

    aws_conn_id='aws_default',

    region_name='us-east-1',

    wait_for_completion=True,

    dag=dag
)

gold_scd2_job = GlueJobOperator(
    task_id='gold_scd2_job',

    job_name='farhan_gold_scd2_job',

    aws_conn_id='aws_default',

    region_name='us-east-1',

    wait_for_completion=True,

    dag=dag
)

# --------------------------------
# CRAWLERS
# --------------------------------

driver_safety_crawler = GlueCrawlerOperator(
    task_id='driver_safety_crawler',

    config={
        "Name": "farhan_gold_driver_safety_crawler"
    },

    aws_conn_id='aws_default',

    region_name='us-east-1',

    dag=dag
)

asset_history_crawler = GlueCrawlerOperator(
    task_id='asset_history_crawler',

    config={
        "Name": "farhan_gold_asset_history_crawler"
    },

    aws_conn_id='aws_default',

    region_name='us-east-1',

    dag=dag
)

# --------------------------------
# PIPELINE FLOW
# --------------------------------

# SILVER → GOLD

silver_vehicle_assignment >> [
    gold_active_snapshot,
    gold_scd2_job
]

silver_fuel_transactions >> [
    gold_fuel_efficiency
]

silver_maintenance >> [
    gold_active_snapshot
]

# GOLD → CRAWLERS

gold_fuel_efficiency >> driver_safety_crawler

[
    gold_active_snapshot,
    gold_scd2_job
] >> asset_history_crawler