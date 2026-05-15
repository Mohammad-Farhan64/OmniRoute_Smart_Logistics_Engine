import config
import psycopg2
import builtins

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ---------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------

spark = SparkSession.builder \
    .appName("driver_safety_streaming_pipeline") \
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    ) \
    .config(
        "spark.sql.shuffle.partitions",
        "4"
    ) \
    .config(
        "spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------
# KAFKA SOURCE
# ---------------------------------------------------

kafka_df = spark.readStream \
    .format("kafka") \
    .option(
        "kafka.bootstrap.servers",
        config.KAFKA_BOOTSTRAP_SERVERS
    ) \
    .option(
        "subscribe",
        config.KAFKA_TOPIC
    ) \
    .option(
        "startingOffsets",
        "latest"
    ) \
    .option(
        "failOnDataLoss",
        "false"
    ) \
    .load()

# ---------------------------------------------------
# TELEMETRY SCHEMA
# ---------------------------------------------------

telemetry_schema = StructType([

    StructField("vin", StringType(), True),

    StructField("speed", IntegerType(), True),

    StructField("rpm", IntegerType(), True),

    StructField("lat", DoubleType(), True),

    StructField("lon", DoubleType(), True),

    StructField("event_timestamp", LongType(), True)
])

# ---------------------------------------------------
# PARSE JSON
# ---------------------------------------------------

parsed_df = kafka_df.selectExpr(
    "CAST(value AS STRING) as json_string",
    "timestamp as kafka_ingestion_ts"
).select(
    from_json(
        col("json_string"),
        telemetry_schema
    ).alias("data"),
    "kafka_ingestion_ts"
).select(
    "data.*",
    "kafka_ingestion_ts"
)

# ---------------------------------------------------
# DATA QUALITY
# ---------------------------------------------------

clean_df = parsed_df \
    .filter(col("vin").isNotNull()) \
    .filter(col("event_timestamp").isNotNull()) \
    .filter(col("speed").isNotNull()) \
    .filter(col("rpm").isNotNull()) \
    .filter(col("lat").isNotNull()) \
    .filter(col("lon").isNotNull())

# ---------------------------------------------------
# EVENT TIME
# ---------------------------------------------------

event_df = clean_df.withColumn(
    "event_time",
    to_timestamp(
        from_unixtime(col("event_timestamp"))
    )
)

# ---------------------------------------------------
# WATERMARK
# ---------------------------------------------------

watermark_df = event_df.withWatermark(
    "event_time",
    config.WATERMARK_DELAY
)

# ---------------------------------------------------
# DEDUPLICATION
# ---------------------------------------------------

dedup_df = watermark_df.dropDuplicates([
    "vin",
    "event_time"
])

# ---------------------------------------------------
# ASSET HISTORY LOOKUP
# ---------------------------------------------------

asset_history_df = spark.read \
    .format("parquet") \
    .load(
        "s3a://ttn-de-bootcamp-gold-us-east-1/farhan_gold/asset_history_scd2/"
    ) \
    .filter(
        col("is_current") == 1
    ) \
    .select(
        "vin",
        "driver_id",
        "daily_rate",
        "region"
    )

# ---------------------------------------------------
# STREAM JOIN
# ---------------------------------------------------

joined_df = dedup_df.join(

    broadcast(asset_history_df),

    on="vin",

    how="left"
)

# ---------------------------------------------------
# BUSINESS RULES
# ---------------------------------------------------

business_df = (

    joined_df

    .withColumn(
        "overspeed_flag",
        when(
            col("speed") > 110,
            1
        ).otherwise(0)
    )

    .withColumn(
        "restricted_zone_flag",
        when(
            (
                (col("lat") >= 12.5) &
                (col("lat") <= 12.8) &
                (col("lon") >= 77.2) &
                (col("lon") <= 77.5)
            ),
            1
        ).otherwise(0)
    )

    .withColumn(
        "strike_score",
        (
            col("overspeed_flag") * 1 +
            col("restricted_zone_flag") * 2
        )
    )

    .withColumn(
        "risk_category",
        when(
            col("strike_score") >= config.HIGH_RISK_THRESHOLD,
            "HIGH_RISK"
        ).when(
            col("strike_score") >= config.MEDIUM_RISK_THRESHOLD,
            "MEDIUM_RISK"
        ).otherwise(
            "LOW_RISK"
        )
    )

    .withColumn(

        "adjusted_daily_rate",

        col("daily_rate") - (

            col("daily_rate") * 0.05 *
            col("strike_score")
        )
    )

    .withColumn(
        "processing_ts",
        current_timestamp()
    )
)

# ---------------------------------------------------
# PARTITIONS
# ---------------------------------------------------

final_df = business_df \
    .withColumn(
        "date",
        to_date(col("event_time"))
    ) \
    .withColumn(
        "hour",
        hour(col("event_time"))
    )

# ---------------------------------------------------
# OUTPUT DATASET
# ---------------------------------------------------

output_df = final_df.select(

    "driver_id",
    "vin",
    "speed",
    "rpm",
    "lat",
    "lon",
    "event_timestamp",
    "event_time",
    "daily_rate",
    "region",
    "overspeed_flag",
    "restricted_zone_flag",
    "strike_score",
    "risk_category",
    "adjusted_daily_rate",
    "processing_ts",
    "date",
    "hour"
)

# ---------------------------------------------------
# STATEFUL AGGREGATION
# ---------------------------------------------------

driver_risk_df = final_df \
    .groupBy(
        window(
            col("event_time"),
            "30 minutes"
        ),
        col("driver_id")
    ) \
    .agg(

        count("*").alias("total_events"),

        sum("overspeed_flag").alias(
            "overspeed_events"
        ),

        sum("restricted_zone_flag").alias(
            "restricted_zone_events"
        ),

        sum("strike_score").alias(
            "total_strike_score"
        ),

        avg("speed").alias(
            "avg_speed"
        ),

        max("speed").alias(
            "max_speed"
        ),

        max("event_time").alias(
            "last_event_time"
        )
    )

# ---------------------------------------------------
# POSTGRES UPDATE ENGINE
# ---------------------------------------------------

def update_driver_risk_state(batch_df, batch_id):

    rows = batch_df.collect()

    if len(rows) == 0:
        return

    conn = psycopg2.connect(
        host=config.POSTGRES_HOST,
        database=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        port=config.POSTGRES_PORT
    )

    cursor = conn.cursor()

    for row in rows:

        driver_id = row["driver_id"]

        strike_score = int(
            row["total_strike_score"]
        )

        overspeed_events = int(
            row["overspeed_events"]
        )

        restricted_zone_events = int(
            row["restricted_zone_events"]
        )

        risk_category = "LOW_RISK"

        suspension_status = "ACTIVE"

        suspension_reason = None

        warning_count = 0

        suspension_count = 0

        reinstatement_eligible = 0

        strike_decay_score = builtins.max(
            strike_score - 2,
            0
        )

        if strike_score >= 25:

            risk_category = "CRITICAL_RISK"

            suspension_status = "SUSPENDED_7D"

            suspension_reason = "Critical repeat offender"

            suspension_count = 3

        elif strike_score >= 15:

            risk_category = "HIGH_RISK"

            suspension_status = "SUSPENDED_24H"

            suspension_reason = "High strike threshold exceeded"

            suspension_count = 1

        elif strike_score >= 8:

            risk_category = "MEDIUM_RISK"

            suspension_status = "WARNING"

            suspension_reason = "Moderate risk behavior"

            warning_count = 1

        else:

            risk_category = "LOW_RISK"

            suspension_status = "ACTIVE"

            reinstatement_eligible = 1

        cursor.execute(
            """
            INSERT INTO driver_risk_state (

                driver_id,
                total_strike_score,
                risk_category,
                suspension_status,
                suspension_reason,
                cooldown_until,
                warning_count,
                suspension_count,
                reinstatement_eligible,
                strike_decay_score,
                total_overspeed_events,
                total_restricted_zone_events,
                last_event_ts,
                updated_ts

            )

            VALUES (

                %s,%s,%s,%s,%s,
                CURRENT_TIMESTAMP + INTERVAL '1 day',
                %s,%s,%s,%s,%s,%s,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP

            )

            ON CONFLICT (driver_id)

            DO UPDATE SET

                total_strike_score =
                driver_risk_state.total_strike_score
                + EXCLUDED.total_strike_score,

                total_overspeed_events =
                driver_risk_state.total_overspeed_events
                + EXCLUDED.total_overspeed_events,

                total_restricted_zone_events =
                driver_risk_state.total_restricted_zone_events
                + EXCLUDED.total_restricted_zone_events,

                risk_category =
                EXCLUDED.risk_category,

                suspension_status =
                EXCLUDED.suspension_status,

                suspension_reason =
                EXCLUDED.suspension_reason,

                cooldown_until =
                EXCLUDED.cooldown_until,

                warning_count =
                EXCLUDED.warning_count,

                suspension_count =
                EXCLUDED.suspension_count,

                reinstatement_eligible =
                EXCLUDED.reinstatement_eligible,

                strike_decay_score =
                EXCLUDED.strike_decay_score,

                updated_ts = CURRENT_TIMESTAMP
            """,
            (
                driver_id,
                strike_score,
                risk_category,
                suspension_status,
                suspension_reason,
                warning_count,
                suspension_count,
                reinstatement_eligible,
                strike_decay_score,
                overspeed_events,
                restricted_zone_events
            )
        )

        cursor.execute(
            """
            INSERT INTO suspension_event_history (
                driver_id,
                event_type,
                previous_status,
                new_status,
                strike_score,
                event_reason
            )
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (
                driver_id,
                "RISK_EVALUATION",
                "ACTIVE",
                suspension_status,
                strike_score,
                suspension_reason
            )
        )

    conn.commit()

    cursor.close()

    conn.close()

# ---------------------------------------------------
# DRIVER SAFETY STREAM
# ---------------------------------------------------

query = output_df.writeStream \
    .format("parquet") \
    .partitionBy(
        "date",
        "hour"
    ) \
    .option(
        "path",
        f"{config.S3_GOLD_BUCKET}/driver_safety_status/"
    ) \
    .option(
        "checkpointLocation",
        config.CHECKPOINT_DRIVER_SAFETY
    ) \
    .outputMode("append") \
    .trigger(
        processingTime=config.STREAM_TRIGGER
    ) \
    .queryName(
        "driver_safety_stream"
    ) \
    .start()

# ---------------------------------------------------
# AGGREGATE STREAM
# ---------------------------------------------------

risk_query = driver_risk_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option(
        "path",
        f"{config.S3_GOLD_BUCKET}/driver_risk_aggregates/"
    ) \
    .option(
        "checkpointLocation",
        config.CHECKPOINT_DRIVER_RISK
    ) \
    .trigger(
        processingTime="2 minutes"
    ) \
    .queryName(
        "driver_risk_aggregation_stream"
    ) \
    .start()

# ---------------------------------------------------
# POSTGRES STATE STREAM
# ---------------------------------------------------

postgres_query = driver_risk_df.writeStream \
    .foreachBatch(update_driver_risk_state) \
    .outputMode("update") \
    .option(
        "checkpointLocation",
        config.CHECKPOINT_POSTGRES_STATE
    ) \
    .trigger(
        processingTime="2 minutes"
    ) \
    .queryName(
        "postgres_driver_state_stream"
    ) \
    .start()

# ---------------------------------------------------
# TERMINATION
# ---------------------------------------------------

spark.streams.awaitAnyTermination()