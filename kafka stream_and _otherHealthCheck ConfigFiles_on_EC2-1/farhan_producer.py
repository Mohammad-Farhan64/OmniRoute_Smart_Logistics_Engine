import config
import json
import random
import time

from kafka import KafkaProducer

# ---------------------------------------------------
# KAFKA PRODUCER
# ---------------------------------------------------

producer = KafkaProducer(

    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,

    value_serializer=lambda v:
    json.dumps(v).encode("utf-8")
)

# ---------------------------------------------------
# REALISTIC VIN DATA
# ---------------------------------------------------

vins = [
    "VIN1001",
    "VIN1002",
    "VIN1003",
    "VIN1004",
    "VIN1005"
]

# ---------------------------------------------------
# STREAMING LOOP
# ---------------------------------------------------

while True:

    telemetry_data = {

        "vin":
        random.choice(vins),

        "speed":
        random.randint(60, 140),

        "rpm":
        random.randint(1000, 5000),

        "lat":
        round(random.uniform(12.0, 13.0), 4),

        "lon":
        round(random.uniform(77.0, 78.0), 4),

        "event_timestamp":
        int(time.time())
    }

    producer.send(
        config.KAFKA_TOPIC,
        value=telemetry_data
    )

    producer.flush()

    print(
        f"Telemetry Event Sent: {telemetry_data}"
    )

    time.sleep(2)