# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion Architecture — Delta Live Tables Pipeline
# MAGIC
# MAGIC This notebook defines the **Bronze → Silver → Gold** pipeline declaratively
# MAGIC using Delta Live Tables (DLT). It is uploaded to the Databricks workspace
# MAGIC and referenced as the source code in the `medallion_dlt_pipeline` DLT pipeline.
# MAGIC
# MAGIC **Pipeline name:** `medallion_dlt_pipeline`
# MAGIC **Pipeline mode:** Triggered
# MAGIC **Destination schema:** `medallion_lab`
# MAGIC
# MAGIC > Before creating the pipeline, ensure the path variables below match
# MAGIC > the S3 bucket you created in Activity 1.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Update `bucket` to your actual S3 bucket name before running the pipeline.

# COMMAND ----------
import dlt
from pyspark.sql.functions import (
    col,
    current_timestamp,
    upper,
    trim,
    to_timestamp,
    to_date,
    sum as _sum,
    count,
    countDistinct,
    row_number,
)
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Paths — replace <your-initials>-<random-4-digits> with your actual suffix
# ---------------------------------------------------------------------------
bucket            = "dbx-medallion-<your-initials>-<random-4-digits>"
raw_orders_path   = f"s3://{bucket}/raw/orders/"
bronze_checkpoint = f"s3://{bucket}/checkpoints/bronze_orders/"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Layer
# MAGIC
# MAGIC Incrementally ingests CSV files from the S3 raw landing path using Auto Loader.
# MAGIC Adds `source_file` and `ingestion_ts` metadata columns so every record is
# MAGIC traceable back to its origin file and load time.

# COMMAND ----------
@dlt.table(
    name="dlt_bronze_orders",
    comment="Bronze ingestion from S3 raw/orders/ using Auto Loader (cloudFiles). "
            "Preserves raw fidelity and captures ingestion metadata.",
)
def dlt_bronze_orders():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("cloudFiles.inferColumnTypes", "true")
             .option("cloudFiles.schemaLocation", bronze_checkpoint + "_dlt_schema")
             .option("header", "true")
             .load(raw_orders_path)
             .withColumn("source_file", col("_metadata.file_path"))
             .withColumn("ingestion_ts", current_timestamp())
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Layer
# MAGIC
# MAGIC Cleanses and deduplicates Bronze records.
# MAGIC
# MAGIC **Data Quality Expectations (enforce via `expect_or_drop`):**
# MAGIC | Rule | Column | Condition |
# MAGIC |------|--------|-----------|
# MAGIC | valid_order_id | order_id | IS NOT NULL |
# MAGIC | valid_customer_id | customer_id | IS NOT NULL |
# MAGIC | valid_amount | order_amount | > 0 |
# MAGIC | valid_status | order_status | IN ('PLACED','SHIPPED','DELIVERED','CANCELLED') |
# MAGIC
# MAGIC Rows that fail any expectation are **dropped** before reaching the Silver table.
# MAGIC A window function retains only the **latest** record per `order_id`.

# COMMAND ----------
@dlt.table(
    name="dlt_silver_orders",
    comment="Silver cleansed and deduplicated orders. "
            "Invalid rows dropped by DLT expectations. "
            "One row per order_id (latest by order_ts + ingestion_ts).",
)
@dlt.expect_or_drop("valid_order_id",    "order_id IS NOT NULL AND order_id != ''")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL AND customer_id != ''")
@dlt.expect_or_drop("valid_amount",      "order_amount > 0")
@dlt.expect_or_drop(
    "valid_status",
    "upper(trim(order_status)) IN ('PLACED','SHIPPED','DELIVERED','CANCELLED')",
)
def dlt_silver_orders():
    df = (
        dlt.read("dlt_bronze_orders")
           .withColumn("order_status", upper(trim(col("order_status"))))
           .withColumn("order_ts",     to_timestamp(col("order_ts")))
    )

    dedup_window = (
        Window.partitionBy("order_id")
              .orderBy(col("order_ts").desc(), col("ingestion_ts").desc())
    )

    return (
        df.withColumn("rn", row_number().over(dedup_window))
          .filter(col("rn") == 1)
          .drop("rn")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Layer
# MAGIC
# MAGIC Aggregates Silver records into **daily business metrics** grouped by
# MAGIC `order_date` and `order_status`. Produces three KPI columns:
# MAGIC - `total_sales_amount` — sum of order_amount
# MAGIC - `total_orders`       — count of orders
# MAGIC - `distinct_customers` — count of unique customers

# COMMAND ----------
@dlt.table(
    name="dlt_gold_daily_sales",
    comment="Gold business metrics aggregated by order_date and order_status. "
            "Ready for Databricks SQL dashboards and reporting.",
)
def dlt_gold_daily_sales():
    return (
        dlt.read("dlt_silver_orders")
           .withColumn("order_date", to_date(col("order_ts")))
           .groupBy("order_date", "order_status")
           .agg(
               _sum("order_amount").alias("total_sales_amount"),
               count("order_id").alias("total_orders"),
               countDistinct("customer_id").alias("distinct_customers"),
           )
    )