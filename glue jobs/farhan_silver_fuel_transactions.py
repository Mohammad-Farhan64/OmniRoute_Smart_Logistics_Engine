# FULL script inside Glue (not snippet)

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# READ BRONZE DATA
df = spark.read.csv(
    "s3://ttn-de-bootcamp-bronze-us-east-1/7002_mohammad_farhan_bronze/fuel_transactions/",
    header=True
)

# CLEAN + TRANSFORM
df = df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
).withColumn(
    "fuel_liters",
    col("fuel_liters").cast("double")
).withColumn(
    "odometer_reading",
    col("odometer_reading").cast("double")
).withColumn(
    "date",
    to_date(col("timestamp"))
)

# REMOVE BAD RECORDS
df = df.filter(
    (col("vin").isNotNull()) &
    (col("fuel_liters").isNotNull()) &
    (col("odometer_reading").isNotNull())
)

# DEDUPLICATE
df = df.dropDuplicates(["transaction_id"])

# WRITE TO SILVER
df.write.mode("overwrite").parquet(
    "s3://ttn-de-bootcamp-silver-us-east-1/farhan_silver/fuel_transactions/"
)

job.commit()