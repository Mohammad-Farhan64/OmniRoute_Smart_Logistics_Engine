from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'farhan',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}

dag = DAG(
    dag_id='farhan_master_data_pipeline',

    default_args=default_args,

    description='Yearly Master Data & Reference Pipeline',

    start_date=datetime(2026, 5, 1),

    schedule_interval='@yearly',

    catchup=False
)

# --------------------------------
# MASTER DATA JOB
# --------------------------------

silver_vehicle_registry = GlueJobOperator(
    task_id='silver_vehicle_registry',

    job_name='farhan_silver_vehicle_registry',

    aws_conn_id='aws_default',

    region_name='us-east-1',

    wait_for_completion=True,

    dag=dag
)

# --------------------------------
# VEHICLE REGISTRY CRAWLER
# --------------------------------

vehicle_registry_crawler = GlueCrawlerOperator(
    task_id='vehicle_registry_crawler',

    config={
        "Name": "farhan_vehicle_registry_crawler"
    },

    aws_conn_id='aws_default',

    region_name='us-east-1',

    dag=dag
)

# --------------------------------
# PIPELINE FLOW
# --------------------------------

silver_vehicle_registry >> vehicle_registry_crawler