import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from S3 (raw layer)
input_path = "s3://your-bucket/raw/transactions/"
df = spark.read.option("header", "true").csv(input_path)

# Data Cleansing
df_clean = df.dropna()

# Type Casting
df_transformed = df_clean.withColumn(
    "amount", F.col("amount").cast("double")
)

# Add Partition Columns
df_final = df_transformed \
    .withColumn("year", F.year(F.current_timestamp())) \
    .withColumn("month", F.month(F.current_timestamp())) \
    .withColumn("day", F.dayofmonth(F.current_timestamp()))

# Repartition for optimization
df_final = df_final.repartition("year", "month")

# Write to S3 (processed layer)
output_path = "s3://your-bucket/processed/transactions/"

df_final.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(output_path)

job.commit()
