# Example of PySpark BigQuery cleaning script
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, when
import os

project_id = "able-balm-454718-n8"
dataset_id = "battery_sandbox"
source_table = "battery_data_sp1_initial_capacity_10_16_2015"
target_table = "battery_data_sp1_cleaned"

spark = SparkSession.builder \
    .appName("BatteryETL") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.30.0") \
    .getOrCreate()

df = spark.read.format("bigquery") \
    .option("project", project_id) \
    .option("parentProject", project_id) \
    .option("table", f"{dataset_id}.{source_table}") \
    .load()

# Drop nulls and constants
phys_cols = df.columns
df_cleaned = df.dropna(subset=phys_cols)

n_unique = df_cleaned.select([
    countDistinct(col(c)).alias(c) for c in df_cleaned.columns
]).collect()[0].asDict()

constant_cols = [k for k, v in n_unique.items() if v <= 1]
df_cleaned = df_cleaned.drop(*constant_cols)

# Upload to BigQuery
df_cleaned.write \
    .format("bigquery") \
    .option("writeMethod", "direct") \
    .option("writeDisposition", "WRITE_TRUNCATE") \
    .option("project", project_id) \
    .option("parentProject", project_id) \
    .option("table", f"{dataset_id}.{target_table}") \
    .mode("overwrite") \
    .save()

