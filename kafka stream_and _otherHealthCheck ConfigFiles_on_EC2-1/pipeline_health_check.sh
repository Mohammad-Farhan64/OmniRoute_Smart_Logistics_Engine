#!/bin/bash

echo "=================================="
echo "PIPELINE HEALTH CHECK"
echo "=================================="

# ------------------------------------------------
# CHECK PRODUCER
# ------------------------------------------------

PRODUCER_COUNT=$(ps -ef | grep farhan_producer.py | grep -v grep | wc -l)

if [ "$PRODUCER_COUNT" -eq 0 ]; then

    echo "Producer DOWN → Restarting..."

    nohup /home/ubuntu/kafka_env/bin/python \
    /home/ubuntu/farhan_producer.py \
    > /home/ubuntu/producer.log 2>&1 &

else

    echo "Producer RUNNING"

fi

# ------------------------------------------------
# CHECK SPARK STREAM
# ------------------------------------------------

SPARK_COUNT=$(ps -ef | grep farhan_streaming_job.py | grep -v grep | wc -l)

if [ "$SPARK_COUNT" -eq 0 ]; then

    echo "Spark Stream DOWN → Restarting..."

    nohup /home/ubuntu/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
    /home/ubuntu/farhan_streaming_job.py \
    > /home/ubuntu/spark_streaming.log 2>&1 &

else

    echo "Spark Stream RUNNING"

fi

echo "=================================="
echo "HEALTH CHECK COMPLETE"
echo "=================================="
