# farhan_gold_fuel_efficiency

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Init
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -----------------------------
# READ SILVER DATA
# -----------------------------
fuel_df = spark.read.parquet(
    "s3://ttn-de-bootcamp-silver-us-east-1/farhan_silver/fuel_transactions/"
)

registry_df = spark.read.parquet(
    "s3://ttn-de-bootcamp-silver-us-east-1/farhan_silver/vehicle_registry/"
)

maintenance_df = spark.read.parquet(
    "s3://ttn-de-bootcamp-silver-us-east-1/farhan_silver/maintenance_logs/"
)

# -----------------------------
# CLEAN + PREP
# -----------------------------
fuel_df = fuel_df.withColumn("event_time", to_timestamp("event_time")) \
                 .withColumn("date", to_date("event_time"))

# -----------------------------
# DISTANCE CALCULATION
# -----------------------------
window_spec = Window.partitionBy("vin").orderBy("event_time")

fuel_df = fuel_df.withColumn("prev_odo", lag("odometer_reading").over(window_spec)) \
                 .withColumn("distance", col("odometer_reading") - col("prev_odo"))

fuel_df = fuel_df.filter(col("distance").isNotNull())

# -----------------------------
# KMPL
# -----------------------------
fuel_df = fuel_df.withColumn("kmpl", col("distance") / col("fuel_liters"))

# -----------------------------
# JOIN VEHICLE REGISTRY
# -----------------------------
fuel_df = fuel_df.join(registry_df.select("vin", "model"), "vin", "left")

# -----------------------------
# REMOVE MAINTENANCE DAYS
# -----------------------------
maintenance_df = maintenance_df.withColumn("service_date", to_date("service_date"))

fuel_df = fuel_df.join(
    maintenance_df.select("vin", col("service_date").alias("date")),
    ["vin", "date"],
    "left_anti"
)

# -----------------------------
# REMOVE WEEKENDS
# -----------------------------
fuel_df = fuel_df.withColumn("day_of_week", dayofweek("date"))

fuel_df = fuel_df.filter(~col("day_of_week").isin([1, 7]))

# -----------------------------
# BASELINE
# -----------------------------
baseline_df = fuel_df.groupBy("model") \
                    .agg(avg("kmpl").alias("baseline_kmpl"))

# -----------------------------
# FLAGGING
# -----------------------------
fuel_df = fuel_df.join(baseline_df, "model")

fuel_df = fuel_df.withColumn(
    "threshold",
    col("baseline_kmpl") * 0.88
)

fuel_df = fuel_df.withColumn(
    "status",
    when(col("kmpl") < col("threshold"), "FLAGGED").otherwise("OK")
)

# -----------------------------
# FINAL OUTPUT
# -----------------------------
final_df = fuel_df.select(
    "vin",
    "model",
    col("date").alias("audit_date"),
    "kmpl",
    "baseline_kmpl",
    "status"
)

# -----------------------------
# WRITE TO GOLD
# -----------------------------
final_df.write.mode("overwrite").parquet(
    "s3://ttn-de-bootcamp-gold-us-east-1/farhan_gold/fuel_efficiency_audit/"
)

job.commit()