from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# --------------------------------
# DEFAULT CONFIG
# --------------------------------

default_args = {
    'owner': 'farhan',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# --------------------------------
# DAG CONFIG
# --------------------------------

dag = DAG(
    dag_id='farhan_streaming_pipeline',

    default_args=default_args,

    description='Kafka + Spark Stateful Risk Pipeline',

    start_date=datetime(2026, 5, 1),

    schedule_interval='30 9-20 * * *',

    catchup=False,

    max_active_runs=1
)

# --------------------------------
# VERIFY KAFKA
# --------------------------------

verify_kafka = BashOperator(
    task_id='verify_kafka',

    bash_command="""
    cd /home/ubuntu/kafka_2.13-3.6.1 &&

    bin/kafka-topics.sh \
    --list \
    --bootstrap-server localhost:9092
    """,

    dag=dag
)

# --------------------------------
# STOP OLD PRODUCER
# --------------------------------

stop_old_producer = BashOperator(
    task_id='stop_old_producer',

    bash_command="""
    ps -ef | grep farhan_producer.py | grep -v grep | awk '{print $2}' | xargs -r kill -9

    sleep 5

    echo "Old Producer Stopped"

    exit 0
    """,

    dag=dag
)

# --------------------------------
# START PRODUCER
# --------------------------------

start_kafka_producer = BashOperator(
    task_id='start_kafka_producer',

    bash_command="""
    nohup /home/ubuntu/kafka_env/bin/python \
    /home/ubuntu/farhan_producer.py \
    > /home/ubuntu/producer.log 2>&1 &

    sleep 10

    echo "Kafka Producer Started"

    exit 0
    """,

    dag=dag
)

# --------------------------------
# STOP OLD STREAM
# --------------------------------

stop_old_stream = BashOperator(
    task_id='stop_old_stream',

    bash_command="""
    ps -ef | grep farhan_streaming_job.py | grep -v grep | awk '{print $2}' | xargs -r kill -9

    sleep 10

    echo "Old Spark Stream Stopped"

    exit 0
    """,

    dag=dag
)

# --------------------------------
# START SPARK STREAMING
# --------------------------------

start_spark_streaming = BashOperator(
    task_id='start_spark_streaming',

    bash_command="""
    nohup /home/ubuntu/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
    /home/ubuntu/farhan_streaming_job.py \
    > /home/ubuntu/spark_streaming.log 2>&1 &

    sleep 30

    echo "Spark Streaming Started"

    exit 0
    """,

    dag=dag
)

# --------------------------------
# VERIFY STREAM RUNNING
# --------------------------------

verify_stream_running = BashOperator(
    task_id='verify_stream_running',

    bash_command="""
    ps -ef | grep farhan_streaming_job.py | grep -v grep

    if [ $? -eq 0 ]; then
        echo "Spark Streaming Running"
        exit 0
    else
        echo "Spark Streaming NOT Running"
        exit 1
    fi
    """,

    dag=dag
)

# --------------------------------
# DRIVER REINSTATEMENT ENGINE
# --------------------------------

driver_reinstatement = BashOperator(
    task_id='driver_reinstatement',

    bash_command="""
    python3 /home/ubuntu/driver_reinstatement_job.py \
    >> /home/ubuntu/reinstatement.log 2>&1

    echo "Driver Reinstatement Completed"

    exit 0
    """,

    dag=dag
)

# --------------------------------
# STRIKE DECAY ENGINE
# --------------------------------

driver_strike_decay = BashOperator(
    task_id='driver_strike_decay',

    bash_command="""
    python3 /home/ubuntu/driver_strike_decay_job.py \
    >> /home/ubuntu/strike_decay.log 2>&1

    echo "Strike Decay Completed"

    exit 0
    """,

    dag=dag
)

# --------------------------------
# PIPELINE FLOW
# --------------------------------

verify_kafka >> stop_old_producer >> start_kafka_producer

start_kafka_producer >> stop_old_stream >> start_spark_streaming

start_spark_streaming >> verify_stream_running

verify_stream_running >> driver_reinstatement

driver_reinstatement >> driver_strike_decay