import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# --------------------------------
# JOB INIT
# --------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(args['JOB_NAME'], args)

# --------------------------------
# SPARK OPTIMIZATION
# --------------------------------
spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)

spark.conf.set(
    "spark.sql.shuffle.partitions",
    "5"
)

spark.conf.set(
    "spark.sql.autoBroadcastJoinThreshold",
    "-1"
)

spark.conf.set(
    "spark.sql.adaptive.enabled",
    "true"
)

spark.conf.set(
    "spark.sql.adaptive.coalescePartitions.enabled",
    "true"
)

# --------------------------------
# PATHS
# --------------------------------
silver_path = "s3://ttn-de-bootcamp-silver-us-east-1/farhan_silver/vehicle_assignment/"

gold_path = "s3://ttn-de-bootcamp-gold-us-east-1/farhan_gold/asset_history_scd2/"

temp_path = "s3://ttn-de-bootcamp-gold-us-east-1/farhan_gold/temp_asset_history_scd2/"

# --------------------------------
# READ SILVER DATA
# --------------------------------
print("Reading Silver Data")

new_df = spark.read.parquet(silver_path)

# --------------------------------
# STANDARDIZE SCHEMA
# --------------------------------
new_df = new_df.select(
    col("vin").cast("string"),
    col("driver_id").cast("string"),
    col("start_date").cast("date"),
    col("end_date").cast("date"),
    col("daily_rate").cast("double"),
    col("region").cast("string")
)

# --------------------------------
# REMOVE NULL BUSINESS KEYS
# --------------------------------
new_df = new_df.filter(
    col("vin").isNotNull()
)

# --------------------------------
# LIGHT VALIDATION
# --------------------------------
if len(new_df.columns) != 6:
    raise Exception("Schema Validation Failed")

print("Schema Validation Passed")

# --------------------------------
# DEDUPLICATION
# --------------------------------
print("Deduplicating Source Data")

window_spec = Window.partitionBy(
    "vin",
    "driver_id",
    "start_date"
).orderBy(
    desc("daily_rate")
)

new_df = new_df.withColumn(
    "rn",
    row_number().over(window_spec)
).filter(
    col("rn") == 1
).drop("rn")

# --------------------------------
# READ EXISTING GOLD
# --------------------------------
print("Reading Existing Gold Data")

try:

    existing_df = spark.read.parquet(gold_path)

except:

    print("Gold Path Empty - Creating Initial Dataset")

    existing_df = spark.createDataFrame([], """
        vin string,
        driver_id string,
        start_date date,
        end_date date,
        daily_rate double,
        region string,
        status string,
        record_start_ts timestamp,
        record_end_ts timestamp,
        is_current integer,
        change_type string,
        created_ts timestamp
    """)

# --------------------------------
# ACTIVE RECORDS
# --------------------------------
active_df = existing_df.filter(
    col("is_current") == 1
)

# --------------------------------
# CHANGE DETECTION
# --------------------------------
print("Detecting Changes")

change_df = new_df.alias("new").join(
    active_df.alias("old"),
    col("new.vin") == col("old.vin"),
    "left"
).filter(
    (
        col("old.vin").isNull()
    ) |
    (
        (col("new.driver_id") != col("old.driver_id")) |
        (col("new.daily_rate") != col("old.daily_rate")) |
        (col("new.region") != col("old.region"))
    )
).select(
    col("new.vin").alias("vin"),
    col("new.driver_id").alias("driver_id"),
    col("new.start_date").alias("start_date"),
    col("new.end_date").alias("end_date"),
    col("new.daily_rate").alias("daily_rate"),
    col("new.region").alias("region")
)

# --------------------------------
# EXPIRE OLD RECORDS
# --------------------------------
print("Expiring Old Records")

expired_df = active_df.alias("old").join(
    change_df.select(
        "vin"
    ).distinct().alias("chg"),
    col("old.vin") == col("chg.vin"),
    "inner"
).select(
    col("old.vin").alias("vin"),
    col("old.driver_id").alias("driver_id"),
    col("old.start_date").alias("start_date"),
    current_date().alias("end_date"),
    col("old.daily_rate").alias("daily_rate"),
    col("old.region").alias("region"),
    lit("ARCHIVED").alias("status"),
    col("old.record_start_ts").alias("record_start_ts"),
    current_timestamp().alias("record_end_ts"),
    lit(0).alias("is_current"),
    lit("UPDATE").alias("change_type"),
    current_timestamp().alias("created_ts")
)

# --------------------------------
# KEEP UNCHANGED RECORDS
# --------------------------------
print("Keeping Unchanged Records")

unchanged_df = existing_df.alias("old").join(
    expired_df.alias("exp"),
    (
        (col("old.vin") == col("exp.vin")) &
        (col("old.record_start_ts") == col("exp.record_start_ts"))
    ),
    "left_anti"
)

# --------------------------------
# INSERT NEW RECORDS
# --------------------------------
print("Creating New Versions")

new_version_df = change_df.withColumn(
    "status",
    lit("IN-TRANSIT")
).withColumn(
    "record_start_ts",
    current_timestamp()
).withColumn(
    "record_end_ts",
    lit(None).cast("timestamp")
).withColumn(
    "is_current",
    lit(1)
).withColumn(
    "change_type",
    lit("INSERT")
).withColumn(
    "created_ts",
    current_timestamp()
)

# --------------------------------
# FINAL DATASET
# --------------------------------
print("Preparing Final Dataset")

final_df = unchanged_df.unionByName(
    expired_df
).unionByName(
    new_version_df
)

# --------------------------------
# REMOVE DUPLICATES
# --------------------------------
final_df = final_df.dropDuplicates()

# --------------------------------
# FINAL REPARTITION
# --------------------------------
final_df = final_df.repartition(5)

# --------------------------------
# LIGHT VALIDATION
# --------------------------------
required_columns = [
    "vin",
    "driver_id",
    "start_date",
    "daily_rate",
    "region",
    "status",
    "is_current"
]

missing_cols = [
    c for c in required_columns
    if c not in final_df.columns
]

if len(missing_cols) > 0:
    raise Exception(
        f"Missing Columns: {missing_cols}"
    )

print("Final Validation Passed")

# --------------------------------
# WRITE TEMP DATA
# --------------------------------
print("Writing Temp Data")

final_df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(temp_path)

# --------------------------------
# READ TEMP DATA
# --------------------------------
print("Reloading Temp Data")

validated_df = spark.read.parquet(temp_path)

# --------------------------------
# WRITE FINAL GOLD
# --------------------------------
print("Writing Final Gold Data")

validated_df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(gold_path)

print("SCD2 Gold Pipeline Completed Successfully")

job.commit()