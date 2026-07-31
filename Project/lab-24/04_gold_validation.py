# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Validation
# MAGIC Post-pipeline validation notebook — confirms row counts and revenue totals
# MAGIC after the DLT pipeline completes. Used as Task 3 in the Databricks Workflow.

# COMMAND ----------

# Cell 1 — Set catalog and schema context
# Replace <your-catalog> with your actual Unity Catalog catalog name
CATALOG = "<your-catalog>"
SCHEMA  = "orch_lab_db"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# Cell 2 — Validate Bronze row count
bronze_count = spark.sql("SELECT COUNT(*) AS cnt FROM bronze_sales").collect()[0]["cnt"]
print(f"Bronze row count : {bronze_count}")
assert bronze_count >= 25, f"FAIL: Expected >= 25 bronze rows, got {bronze_count}"
print("Bronze validation PASSED")

# COMMAND ----------

# Cell 3 — Validate Silver row count
silver_count = spark.sql("SELECT COUNT(*) AS cnt FROM silver_sales").collect()[0]["cnt"]
print(f"Silver row count : {silver_count}")
assert silver_count >= 20, f"FAIL: Expected >= 20 silver rows, got {silver_count}"
print("Silver validation PASSED")

# COMMAND ----------

# Cell 4 — Validate Gold aggregation
gold_df = spark.sql("""
    SELECT product, region, SUM(total_revenue) AS revenue
    FROM gold_revenue_summary
    GROUP BY product, region
    ORDER BY revenue DESC
""")
gold_df.show()

gold_count = gold_df.count()
assert gold_count > 0, "FAIL: Gold table is empty"
print(f"Gold validation PASSED — {gold_count} product/region combinations found")

# COMMAND ----------

# Cell 5 — Print summary
print("=" * 50)
print("PIPELINE VALIDATION SUMMARY")
print("=" * 50)
print(f"  Bronze rows   : {bronze_count}")
print(f"  Silver rows   : {silver_count}")
print(f"  Dropped rows  : {bronze_count - silver_count}")
print(f"  Gold groups   : {gold_count}")
print("  Status        : ALL CHECKS PASSED")
