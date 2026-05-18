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
    "s3://ttn-de-bootcamp-bronze-us-east-1/7002_mohammad_farhan_bronze/maintenance_logs/",
    header=True
)

# CLEAN + TRANSFORM
df = df.withColumn(
    "service_date",
    to_date(col("service_date"))
).withColumn(
    "service_type",
    col("service_type")
)

# REMOVE BAD RECORDS
df = df.filter(col("vin").isNotNull())

# DEDUPLICATE
df = df.dropDuplicates(["vin", "service_date"])

# WRITE TO SILVER
df.write.mode("overwrite").parquet(
    "s3://ttn-de-bootcamp-silver-us-east-1/farhan_silver/maintenance_logs/"
)

job.commit()