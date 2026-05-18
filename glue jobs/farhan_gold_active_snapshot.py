# farhan_gold_active_snapshot

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Init
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -----------------------------
# READ GOLD SCD DATA
# -----------------------------
df = spark.read.parquet(
    "s3://ttn-de-bootcamp-gold-us-east-1/farhan_gold/asset_history_scd2/"
)

# -----------------------------
# FILTER ACTIVE VEHICLES
# -----------------------------
active_df = df.filter(col("status") == "IN-TRANSIT")

# -----------------------------
# AGGREGATION
# -----------------------------
snapshot_df = active_df.groupBy("region", "driver_id") \
                      .agg(countDistinct("vin").alias("active_vehicle_count"))

# -----------------------------
# WRITE TO GOLD
# -----------------------------
snapshot_df.write.mode("overwrite").parquet(
    "s3://ttn-de-bootcamp-gold-us-east-1/farhan_gold/active_fleet_snapshot/"
)

job.commit()