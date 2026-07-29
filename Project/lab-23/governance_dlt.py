# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog — Governance, Lineage & Sharing
# MAGIC ## Delta Live Tables Pipeline: Bronze → Silver → Gold
# MAGIC
# MAGIC This notebook defines the **Bronze → Silver → Gold** pipeline declaratively
# MAGIC using Delta Live Tables (DLT). It is imported into the Databricks workspace
# MAGIC and referenced as the source code in the `governance_dlt_pipeline` DLT pipeline.
# MAGIC
# MAGIC **Pipeline name:** `governance_dlt_pipeline`
# MAGIC **Pipeline mode:** Triggered
# MAGIC **Default schema:** `governance_lab`
# MAGIC
# MAGIC ---
# MAGIC ### ⚠️ CRITICAL — Update the bucket variable before running the pipeline
# MAGIC
# MAGIC Replace `dbx-governance-<your-initials>-<random-4-digits>` below with your
# MAGIC actual S3 bucket name. **Do NOT run this notebook interactively** — it must be
# MAGIC executed via a DLT Pipeline (Jobs & Pipelines → ETL pipeline).
# MAGIC
# MAGIC Also add the same AWS credential key-value pairs to the pipeline Configuration
# MAGIC section that you used on the `dbx-governance-cluster`.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col,
    current_timestamp,
    upper,
    trim,
    to_timestamp,
    sum as _sum,
    count,
    expr,
    row_number,
)
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Paths — replace the placeholder with your actual S3 bucket name
# ---------------------------------------------------------------------------
bucket             = "dbx-governance-<your-initials>-<random-4-digits>"   # ← UPDATE THIS
raw_customers_path = f"s3://{bucket}/raw/customers/"
bronze_checkpoint  = f"s3://{bucket}/checkpoints/bronze_customers/"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Layer
# MAGIC
# MAGIC Incrementally ingests CSV files from the S3 raw landing path using Auto Loader.
# MAGIC Captures `source_file` (via `_metadata.file_path`) and `ingestion_ts` so every
# MAGIC record is traceable back to its origin file and load time.
# MAGIC Raw fidelity is preserved — no filtering occurs at this layer.

# COMMAND ----------

@dlt.table(
    name="dlt_bronze_customers",
    comment="Bronze ingestion from s3://<bucket>/raw/customers/ using Auto Loader (cloudFiles). "
            "Preserves raw fidelity and captures ingestion metadata for traceability.",
)
def dlt_bronze_customers():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("cloudFiles.inferColumnTypes", "true")
             .option("cloudFiles.schemaLocation", bronze_checkpoint + "_dlt_schema")
             .option("header", "true")
             .load(raw_customers_path)
             .withColumn("source_file", col("_metadata.file_path"))
             .withColumn("ingestion_ts", current_timestamp())
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Layer
# MAGIC
# MAGIC Cleanses and deduplicates Bronze records.
# MAGIC
# MAGIC **Data Quality Expectations (enforced via `expect_or_drop`):**
# MAGIC
# MAGIC | Rule | Column | Condition |
# MAGIC |------|--------|-----------|
# MAGIC | valid_customer_id | customer_id | IS NOT NULL AND trim != '' |
# MAGIC | valid_name | full_name | IS NOT NULL AND trim != '' |
# MAGIC | valid_lifetime_value | lifetime_value | >= 0 |
# MAGIC | valid_tier | customer_tier | IN ('BRONZE','SILVER','GOLD','PLATINUM') |
# MAGIC
# MAGIC Rows that fail **any** expectation are **dropped** before reaching Silver.
# MAGIC A window function retains only the **latest** record per `customer_id`
# MAGIC (ordered by `record_ts` DESC, then `ingestion_ts` DESC).

# COMMAND ----------

@dlt.table(
    name="dlt_silver_customers",
    comment="Silver cleansed and deduplicated customers. "
            "Invalid rows dropped by DLT expectations. "
            "One row per customer_id (latest by record_ts + ingestion_ts).",
)
@dlt.expect_or_drop("valid_customer_id",    "customer_id IS NOT NULL AND trim(customer_id) != ''")
@dlt.expect_or_drop("valid_name",           "full_name IS NOT NULL AND trim(full_name) != ''")
@dlt.expect_or_drop("valid_lifetime_value", "lifetime_value >= 0")
@dlt.expect_or_drop(
    "valid_tier",
    "upper(trim(customer_tier)) IN ('BRONZE','SILVER','GOLD','PLATINUM')",
)
def dlt_silver_customers():
    df = (
        dlt.read("dlt_bronze_customers")
           .withColumn("customer_tier", upper(trim(col("customer_tier"))))
           .withColumn("record_ts",     to_timestamp(col("record_ts")))
    )

    dedup_window = (
        Window.partitionBy("customer_id")
              .orderBy(col("record_ts").desc(), col("ingestion_ts").desc())
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
# MAGIC Aggregates Silver records into **customer metrics** grouped by `state` and
# MAGIC `customer_tier`. Produces three KPI columns consumed by governed Unity Catalog
# MAGIC objects and Delta Sharing:
# MAGIC
# MAGIC - `customer_count`        — total customers in each state/tier group
# MAGIC - `active_customer_count` — count where `is_active = true`
# MAGIC - `total_lifetime_value`  — sum of lifetime_value per group

# COMMAND ----------

@dlt.table(
    name="dlt_gold_customer_metrics",
    comment="Gold customer metrics aggregated by state and customer_tier. "
            "Ready for Unity Catalog governance, persona-based access, "
            "Delta Sharing, and Databricks SQL dashboards.",
)
def dlt_gold_customer_metrics():
    return (
        dlt.read("dlt_silver_customers")
           .groupBy("state", "customer_tier")
           .agg(
               count("customer_id").alias("customer_count"),
               _sum(expr("CASE WHEN is_active = 'true' OR is_active = true THEN 1 ELSE 0 END"))
                   .alias("active_customer_count"),
               _sum("lifetime_value").alias("total_lifetime_value"),
           )
    )
