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
    "s3://ttn-de-bootcamp-bronze-us-east-1/7002_mohammad_farhan_bronze/vehicle_assignment/",
    header=True
)

# CLEAN + TRANSFORM
df = df.withColumn(
    "start_date",
    from_unixtime(col("start_timestamp")).cast("timestamp")
).withColumn(
    "end_date",
    from_unixtime(col("end_timestamp")).cast("timestamp")
).withColumn(
    "daily_rate",
    col("daily_rate").cast("double")
).withColumn(
    "region",
    col("region")
)

# REMOVE BAD RECORDS
df = df.filter(col("vin").isNotNull())

# DEDUPLICATE
df = df.dropDuplicates(["vin", "start_date"])

# WRITE TO SILVER
df.write.mode("overwrite").parquet(
    "s3://ttn-de-bootcamp-silver-us-east-1/farhan_silver/vehicle_assignment/"
)

job.commit()