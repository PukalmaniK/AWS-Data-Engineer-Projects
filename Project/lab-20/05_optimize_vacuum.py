# Databricks notebook source
# Lab 20 | Notebook 05 — OPTIMIZE, Z-ORDER & VACUUM
# Purpose: Compact small files, enable data skipping, and reclaim storage.
# Prerequisite: All previous notebooks must have been run successfully.

# COMMAND ----------

from delta.tables import DeltaTable

DB_NAME = "delta_lab_db"
TABLE   = f"{DB_NAME}.customers_managed"

# COMMAND ----------

# CELL 1 — Check physical file count and size BEFORE optimization
dt = DeltaTable.forName(spark, TABLE)
detail_before = dt.detail().select("numFiles", "sizeInBytes", "location").collect()[0]

print("=== Before OPTIMIZE ===")
print(f"  Number of files : {detail_before['numFiles']}")
print(f"  Total size      : {detail_before['sizeInBytes']} bytes")
print(f"  Location        : {detail_before['location']}")

# COMMAND ----------

# CELL 2 — Run OPTIMIZE to compact small Parquet files
print("Running OPTIMIZE...")
optimize_result = spark.sql(f"OPTIMIZE {TABLE}")
display(optimize_result)

# COMMAND ----------

# CELL 3 — Check file count AFTER basic OPTIMIZE
detail_after_opt = dt.detail().select("numFiles", "sizeInBytes").collect()[0]

print("=== After OPTIMIZE ===")
print(f"  Number of files : {detail_after_opt['numFiles']}")
print(f"  Total size      : {detail_after_opt['sizeInBytes']} bytes")
print(f"  Files reduced by: {detail_before['numFiles'] - detail_after_opt['numFiles']}")

# COMMAND ----------

# CELL 4 — Run OPTIMIZE with Z-ORDER on the 'city' column
# Z-ORDER co-locates rows with the same city into the same files,
# enabling data skipping for WHERE city = '...' predicates.
print("Running OPTIMIZE with Z-ORDER on city...")
zorder_result = spark.sql(f"OPTIMIZE {TABLE} ZORDER BY (city)")
display(zorder_result)

# COMMAND ----------

# CELL 5 — View OPTIMIZE + Z-ORDER metrics from DESCRIBE HISTORY
print("OPTIMIZE entries in transaction history:")
display(spark.sql(f"""
    SELECT version, timestamp, operation, operationMetrics
    FROM   (DESCRIBE HISTORY {TABLE})
    WHERE  operation = 'OPTIMIZE'
"""))

# Key operationMetrics fields:
#   numFilesAdded   — new compacted files written
#   numFilesRemoved — old small files logically deleted
#   numBytesAdded   — bytes written by compacted files

# COMMAND ----------

# CELL 6 — VACUUM DRY RUN: list files that would be deleted (no actual deletion)
print("Running VACUUM DRY RUN (7-day / 168-hour retention):")
display(spark.sql(f"VACUUM {TABLE} RETAIN 168 HOURS DRY RUN"))

# On a freshly created lab table this may return 0 rows — expected.

# COMMAND ----------

# CELL 7 — VACUUM with 0-hour retention override (LAB ONLY — NOT for production)
# This removes ALL old file versions immediately.
# After this, Time Travel to old versions will NOT be available.

print("Disabling retention safety check for lab purposes...")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

print("Running VACUUM with RETAIN 0 HOURS...")
spark.sql(f"VACUUM {TABLE} RETAIN 0 HOURS")
print("VACUUM complete.")

# Restore the safety check after the lab operation
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
print("Retention safety check restored.")

# COMMAND ----------

# CELL 8 — Verify file count after VACUUM
detail_after_vacuum = dt.detail().select("numFiles", "sizeInBytes").collect()[0]

print("=== After VACUUM ===")
print(f"  Number of files : {detail_after_vacuum['numFiles']}")
print(f"  Total size      : {detail_after_vacuum['sizeInBytes']} bytes")

# COMMAND ----------

# CELL 9 — Confirm Time Travel is no longer available for vacuumed versions
print("Attempting to time-travel to version 0 after VACUUM (expected to FAIL)...")
try:
    display(spark.sql(f"SELECT COUNT(*) FROM {TABLE} VERSION AS OF 0"))
    print("Unexpected: time travel succeeded — old files may still be present.")
except Exception as e:
    print("Expected error — old version files have been vacuumed.")
    print(f"   Error: {str(e)[:300]}")

# COMMAND ----------

# CELL 10 — Final summary of the table state
print("=== Final Table State ===")
display(spark.sql(f"SELECT * FROM {TABLE} ORDER BY customer_id"))
final_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}").collect()[0]["cnt"]
print(f"\nFinal row count: {final_count}")
