# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Layer: Revenue Aggregation
# MAGIC
# MAGIC This notebook defines the **Gold DLT table** (`gold_revenue_by_product`).
# MAGIC
# MAGIC - Reads from the clean **Silver layer** using `dlt.read()` (batch, not streaming).
# MAGIC - Aggregates revenue, order count, and average unit price **by product and status**.
# MAGIC - Produces a compact, business-ready table suitable for BI dashboards and scheduled reports.
# MAGIC
# MAGIC **Output schema:**
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `product` | String | Product name |
# MAGIC | `status` | String | Order status (`completed`, `pending`, `cancelled`) |
# MAGIC | `total_revenue` | Double | Sum of `quantity * unit_price` for the group |
# MAGIC | `order_count` | Long | Number of orders in the group |
# MAGIC | `avg_unit_price` | Double | Average unit price for the group |
# MAGIC
# MAGIC > ⚠️ **Do not run this notebook directly.** It is a source file for the DLT pipeline.

# COMMAND ----------

# Cell 1 — Imports
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# Cell 2 — Gold DLT table: revenue summary by product and status
@dlt.table(
    name    = "gold_revenue_by_product",
    comment = "Revenue aggregated by product and order status. Business-ready for dashboards."
)
def gold_revenue_by_product():
    return (
        dlt.read("silver_orders")
           .groupBy("product", "status")
           .agg(
               F.sum("revenue").alias("total_revenue"),
               F.count("order_id").alias("order_count"),
               F.avg("unit_price").alias("avg_unit_price")
           )
           .orderBy("product", "status")
    )
