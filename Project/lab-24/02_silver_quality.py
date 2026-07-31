# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Data Quality & Cleansing
# MAGIC Reads from Bronze, applies data quality expectations (Warn / Drop / Drop),
# MAGIC casts types, derives revenue, and filters to known-good status values.

# COMMAND ----------

# Cell 1 — Imports
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# COMMAND ----------

# Cell 2 — Silver DLT table with data quality expectations
@dlt.expect("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect_or_drop("quantity_positive", "quantity > 0")
@dlt.expect_or_drop("unit_price_positive", "unit_price > 0")   # Changed from expect_or_fail to expect_or_drop
@dlt.table(
    name    = "silver_sales",
    comment = "Cleansed and validated sales orders. Bad records handled per expectation policy."
)
def silver_sales():
    return (
        dlt.read_stream("bronze_sales")
           .withColumn("quantity",   F.col("quantity").cast(IntegerType()))
           .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
           .withColumn("revenue",    F.col("quantity") * F.col("unit_price"))
           .withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
           .filter(F.col("status").isin("completed", "pending", "cancelled"))
           .drop("_source_file")
    )
