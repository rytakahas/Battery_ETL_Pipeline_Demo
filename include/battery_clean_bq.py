# include/battery_clean_bq.py
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

# ----------------------------
# CLI Argument Parsing
# ----------------------------
parser = argparse.ArgumentParser(description="Battery Data Cleaner for BigQuery")
parser.add_argument("--input", required=True, help="Fully-qualified input table (e.g. dataset.table)")
parser.add_argument("--output", required=True, help="Fully-qualified output table (e.g. dataset.cleaned_table)")
parser.add_argument("--project", default="able-balm-454718-n8", help="GCP Project ID")
args = parser.parse_args()

project_id = args.project
input_table = args.input
output_table = args.output

# ----------------------------
# Start SparkSession
# ----------------------------
spark = SparkSession.builder \
    .appName("BatteryETL_Cleaning") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.30.0") \
    .getOrCreate()

# ----------------------------
# Load from BigQuery
# ----------------------------
df = spark.read.format("bigquery") \
    .option("project", project_id) \
    .option("parentProject", project_id) \
    .option("table", input_table) \
    .load()

# ----------------------------
# Drop all-null and constant columns
# ----------------------------
non_null_df = df.dropna(how='all')
unique_counts = non_null_df.select([
    countDistinct(col(c)).alias(c) for c in non_null_df.columns
]).collect()[0].asDict()

constant_cols = [k for k, v in unique_counts.items() if v <= 1]
df_cleaned = non_null_df.drop(*constant_cols)

# ----------------------------
# Write cleaned data back to BigQuery
# ----------------------------
df_cleaned.write \
    .format("bigquery") \
    .option("writeMethod", "direct") \
    .option("writeDisposition", "WRITE_TRUNCATE") \
    .option("project", project_id) \
    .option("parentProject", project_id) \
    .option("table", output_table) \
    .mode("overwrite") \
    .save()

print(f"✅ Successfully cleaned and uploaded to {output_table}")

