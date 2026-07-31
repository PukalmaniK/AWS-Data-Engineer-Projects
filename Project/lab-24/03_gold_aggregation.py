# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Business Aggregation
# MAGIC Aggregates clean Silver data into a business-ready revenue summary by product and region.
# MAGIC This table is the final output consumed by SQL Warehouse dashboards.

# COMMAND ----------

# Cell 1 — Imports
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# Cell 2 — Gold DLT table: revenue summary by product and region
@dlt.table(
    name    = "gold_revenue_summary",
    comment = "Revenue aggregated by product and region. Business-ready for dashboards and BI tools."
)
def gold_revenue_summary():
    return (
        dlt.read("silver_sales")
           .groupBy("product", "region", "status")
           .agg(
               F.sum("revenue").alias("total_revenue"),
               F.count("order_id").alias("order_count"),
               F.avg("unit_price").alias("avg_unit_price"),
               F.sum("quantity").alias("total_units_sold")
           )
           .orderBy("product", "region")
    )
