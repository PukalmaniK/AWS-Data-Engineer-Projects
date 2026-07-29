# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Layer: Auto Loader Ingestion
# MAGIC
# MAGIC This notebook defines the **Bronze DLT table** (`bronze_orders`).
# MAGIC
# MAGIC - Reads raw CSV files from S3 using **Auto Loader** (`cloudFiles` format).
# MAGIC - Stores data at **full fidelity** — no cleansing, no filtering.
# MAGIC - Adds **audit columns** (`_ingested_at`, `_source_file`) for traceability.
# MAGIC
# MAGIC > ⚠️ **Do not run this notebook directly.** It is a source file for the DLT pipeline.
# MAGIC > Attach it via the DLT pipeline configuration UI and run the pipeline from there.

# COMMAND ----------

# Cell 1 — Pipeline configuration
import dlt
from pyspark.sql import functions as F

# ── UPDATE THIS VALUE ──────────────────────────────────────────────────────────
S3_BUCKET = "dlt-lab-<your-aws-account-id>"   # Replace with your actual bucket name
# ──────────────────────────────────────────────────────────────────────────────

RAW_PATH = f"s3://{S3_BUCKET}/raw/orders/"

print(f"S3 Bucket : {S3_BUCKET}")
print(f"Raw Path  : {RAW_PATH}")

# COMMAND ----------

# Cell 2 — Bronze DLT table using Auto Loader
@dlt.table(
    name    = "bronze_orders",
    comment = "Raw orders ingested from S3 via Auto Loader — full fidelity, no transformations."
)
def bronze_orders():
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
