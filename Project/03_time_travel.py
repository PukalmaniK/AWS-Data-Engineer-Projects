# Databricks notebook source
# Lab 20 | Notebook 03 — Time Travel & Restore
# Purpose: Query historical table versions and restore from an accidental delete.
# Prerequisite: Notebooks 01 and 02 must have been run successfully.

# COMMAND ----------

DB_NAME = "delta_lab_db"
TABLE   = f"{DB_NAME}.customers_managed"

# COMMAND ----------

# CELL 1 — View full transaction history
history_df = spark.sql(f"DESCRIBE HISTORY {TABLE}")
display(history_df.select("version", "timestamp", "operation", "operationParameters", "operationMetrics"))

# Note your version numbers:
#   Version 0 = CREATE TABLE + initial seed load
#   Version 1 = INSERT (2 rows)
#   Version 2 = UPDATE (pending → active)
#   Version 3 = DELETE (customer_id = 5)
#   Version 4 = MERGE

# COMMAND ----------

# CELL 2 — Time Travel by VERSION: compare row count at version 1 vs current
display(spark.sql(f"""
    SELECT COUNT(*) AS row_count, 'version_1 (post-INSERT)' AS label
    FROM   {TABLE} VERSION AS OF 1

    UNION ALL

    SELECT COUNT(*) AS row_count, 'current (post-MERGE)' AS label
    FROM   {TABLE}
"""))


# COMMAND ----------

# CELL 3 — View table at version 2 (before DELETE — Emma Wilson should be visible)
print("Table state at version 2 (after UPDATE, before DELETE):")
display(spark.sql(f"""
    SELECT * FROM {TABLE} VERSION AS OF 2
    ORDER  BY customer_id
"""))

# COMMAND ----------

# CELL 4 — Time Travel by TIMESTAMP: auto-read from DESCRIBE HISTORY
history_rows = spark.sql(f"DESCRIBE HISTORY {TABLE}").collect()
v1_ts = [r["timestamp"] for r in history_rows if r["version"] == 1]

if v1_ts:
    ts_str = str(v1_ts[0])
    print(f"Querying table TIMESTAMP AS OF: {ts_str}")
    display(spark.sql(f"""
        SELECT * FROM {TABLE} TIMESTAMP AS OF '{ts_str}'
        ORDER BY customer_id
    """))
else:
    print("Version 1 not found — ensure Notebook 02 was run.")

# COMMAND ----------

# CELL 5 — SIMULATED DISASTER: accidental full-table delete
print("Simulating accidental full-table DELETE...")
spark.sql(f"DELETE FROM {TABLE}")

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}").collect()[0]["cnt"]
print(f"Row count after accidental DELETE: {count}")   # Expected: 0

# COMMAND ----------

# CELL 6 — Identify the last good version programmatically
print("Transaction history after accidental delete:")
display(spark.sql(f"DESCRIBE HISTORY {TABLE}"))

all_versions = spark.sql(f"DESCRIBE HISTORY {TABLE}").collect()
accidental_delete_version = all_versions[0]["version"]
last_good_version = accidental_delete_version - 1
print(f"\nAccidental delete version   : {accidental_delete_version}")
print(f"Last good version to restore: {last_good_version}")

# COMMAND ----------

# CELL 7 — RESTORE the table to the last good version
spark.sql(f"RESTORE TABLE {TABLE} TO VERSION AS OF {last_good_version}")
print(f"Table restored to version {last_good_version}.")

# COMMAND ----------

# CELL 8 — Verify recovery
count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}").collect()[0]["cnt"]
print(f"Row count after RESTORE: {count}")   

display(spark.sql(f"SELECT * FROM {TABLE} ORDER BY customer_id"))

# COMMAND ----------

# CELL 9 — Confirm RESTORE appears in history (auditable recovery)
display(spark.sql(f"DESCRIBE HISTORY {TABLE} LIMIT 5"))
