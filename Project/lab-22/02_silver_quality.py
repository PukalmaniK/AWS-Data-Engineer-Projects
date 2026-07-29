# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Layer: Data Quality Expectations
# MAGIC
# MAGIC This notebook defines the **Silver DLT table** (`silver_orders`).
# MAGIC
# MAGIC | Expectation | Rule | Mode | Behaviour on Violation |
# MAGIC |---|---|---|---|
# MAGIC | `order_id_not_null` | `order_id IS NOT NULL` | **Warn** | Row passes through; violation counted in event log |
# MAGIC | `quantity_positive` | `quantity > 0` | **Drop** | Violating row is silently removed; pipeline continues |
# MAGIC | `unit_price_positive` | `unit_price > 0` | **Fail** | Pipeline halts immediately; forces investigation |
# MAGIC
# MAGIC Transformations applied:
# MAGIC - Casts `quantity` → `IntegerType`, `unit_price` → `DoubleType`
# MAGIC - Derives `revenue = quantity * unit_price`
# MAGIC - Parses `order_date` string → `DateType`
# MAGIC - Filters rows to approved statuses only (`completed`, `pending`, `cancelled`)
# MAGIC - Drops the `_source_file` audit column (no longer needed at Silver)
# MAGIC
# MAGIC > ⚠️ **Do not run this notebook directly.** It is a source file for the DLT pipeline.

# COMMAND ----------

# Cell 1 — Imports
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# COMMAND ----------

# Cell 2 — Silver DLT table with data quality expectations

@dlt.expect("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect_or_drop("quantity_positive", "quantity > 0")
@dlt.expect_or_fail("unit_price_positive", "unit_price > 0")
@dlt.table(
    name    = "silver_orders",
    comment = "Cleansed and validated orders. Bad records handled per expectation policy."
)
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
           # Cast to correct types
           .withColumn("quantity",   F.col("quantity").cast(IntegerType()))
           .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
           # Derive revenue
           .withColumn("revenue",    F.col("quantity") * F.col("unit_price"))
           # Parse date
           .withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
           # Keep only valid statuses
           .filter(F.col("status").isin("completed", "pending", "cancelled"))
           # Drop Bronze-only audit column
           .drop("_source_file")
    )
