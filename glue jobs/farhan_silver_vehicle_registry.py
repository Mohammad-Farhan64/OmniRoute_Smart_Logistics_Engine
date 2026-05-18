# farhan_silver_vehicle_registry

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Glue + Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -----------------------------
# READ BRONZE DATA
# -----------------------------
df = spark.read.csv(
    "s3://ttn-de-bootcamp-bronze-us-east-1/7002_mohammad_farhan_bronze/vehicle_registry/",
    header=True
)

# -----------------------------
# CLEAN DATA
# -----------------------------
df = df.select(
    col("vin"),
    col("model"),
    col("mfg_year").cast("int"),
    col("fuel_type")
).dropDuplicates(["vin"])

# -----------------------------
# WRITE TO SILVER
# -----------------------------
df.write.mode("overwrite").parquet(
    "s3://ttn-de-bootcamp-silver-us-east-1/farhan_silver/vehicle_registry/"
)

job.commit()