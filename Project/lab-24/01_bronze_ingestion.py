# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Auto Loader Ingestion
# MAGIC Ingests raw CSV files from S3 into the Bronze Delta table using Auto Loader (cloudFiles).
# MAGIC Full fidelity: no transformations, no filtering — raw data preserved as-is.

# COMMAND ----------

# Cell 1 — Imports and S3 path configuration
import dlt
from pyspark.sql import functions as F

# Replace <your-bucket-name> with your actual S3 bucket name (e.g., orch-lab-784266215529)
S3_BUCKET = "<your-bucket-name>"
RAW_PATH  = f"s3://{S3_BUCKET}/raw/sales/"

# COMMAND ----------

# Cell 2 — Bronze DLT table using Auto Loader
@dlt.table(
    name    = "bronze_sales",
    comment = "Raw sales orders ingested from S3 via Auto Loader — full fidelity, no transformations."
)
def bronze_sales():
    return (
        spark.readStream.format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .option("cloudFiles.schemaLocation",
                     f"s3://{S3_BUCKET}/dlt_checkpoints/bronze_schema/")
             .load(RAW_PATH)
             .withColumn("_ingested_at", F.current_timestamp())
             # Unity Catalog requires _metadata.file_path instead of input_file_name()
             .withColumn("_source_file", F.col("_metadata.file_path"))
    )
