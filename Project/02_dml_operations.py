# Databricks notebook source
# Lab 20 | Notebook 02 — Full DML Operations & Transaction Log Inspection
# Purpose: Run INSERT, UPDATE, DELETE, MERGE and inspect the _delta_log.
# Prerequisite: Notebook 01 must have been run successfully.
#
# NOTES FROM LAB EXECUTION:
#   - The managed table delta log cannot be read via dbutils.fs.ls() due to
#     Unity Catalog managed storage overlap restriction (LOCATION_OVERLAP error).
#   - Use DESCRIBE HISTORY SQL command instead — it reads the same _delta_log
#     and presents it as a structured table. This is the recommended approach.
#   - MERGE row count: updates CSV contains customer_ids 2,3,7,9 (UPDATE) and
#     11,12 (INSERT). Since 11 and 12 already exist from the INSERT cell,
#     only 4 updates occur — no new rows are inserted. Final count = 11.

# COMMAND ----------

import json

S3_BUCKET        = "delta-lab-<your-aws-account-id>"   # e.g. "delta-lab-784266215529"
DB_NAME          = "delta_lab_db"
TABLE            = f"{DB_NAME}.customers_managed"
UPDATES_CSV_PATH = f"s3://{S3_BUCKET}/raw/customers_updates.csv"

print(f"Working table    : {TABLE}")
print(f"Updates CSV path : {UPDATES_CSV_PATH}")

# COMMAND ----------

# CELL 1 — INSERT two new customer rows
spark.sql(f"""
    INSERT INTO {TABLE}
    VALUES (11, 'Priya Nair',    'Bengaluru',  52000.00, 'active'),
           (12, 'Carlos Rivera', 'São Paulo',  31500.75, 'pending')
""")

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}").collect()[0]["cnt"]
print(f"Row count after INSERT: {count}")   # Expected: 12
display(spark.sql(f"SELECT * FROM {TABLE} WHERE customer_id >= 11"))

# COMMAND ----------

# CELL 2 — UPDATE: activate all pending customers and apply a 10% balance increase
spark.sql(f"""
    UPDATE {TABLE}
    SET    account_balance = account_balance * 1.10,
           status          = 'active'
    WHERE  status = 'pending'
""")

print("Rows updated — verifying:")
display(spark.sql(f"SELECT customer_id, name, account_balance, status FROM {TABLE} ORDER BY customer_id"))

# COMMAND ----------

# CELL 3 — DELETE: remove customer_id = 5 (simulating a GDPR erasure request)
spark.sql(f"DELETE FROM {TABLE} WHERE customer_id = 5")

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}").collect()[0]["cnt"]
print(f"Row count after DELETE: {count}")   # Expected: 11

# COMMAND ----------

# CELL 4 — MERGE (UPSERT): load updates from S3 and merge into managed table
# The updates CSV has:
#   - customer_ids 2, 3, 7, 9  → MATCHED rows → UPDATE
#   - customer_ids 11, 12      → already inserted above → UPDATE (not new INSERT)
# Therefore final row count stays at 11 (not 13) — correct behaviour.

updates_df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(UPDATES_CSV_PATH))

updates_df.createOrReplaceTempView("customer_updates_stage")
print(f"Update stage rows: {updates_df.count()}")
display(updates_df)

spark.sql(f"""
    MERGE INTO {TABLE} AS target
    USING customer_updates_stage AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            target.name            = source.name,
            target.city            = source.city,
            target.account_balance = source.account_balance,
            target.status          = source.status
    WHEN NOT MATCHED THEN
        INSERT (customer_id, name, city, account_balance, status)
        VALUES (source.customer_id, source.name, source.city,
                source.account_balance, source.status)
""")

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}").collect()[0]["cnt"]
print(f"Row count after MERGE: {count}")
display(spark.sql(f"SELECT * FROM {TABLE} ORDER BY customer_id"))

# COMMAND ----------

# CELL 5 — Get the managed table location (for reference)
# NOTE: dbutils.fs.ls() on the managed table _delta_log path raises an
# AnalysisException (LOCATION_OVERLAP) because Unity Catalog managed storage
# restricts direct file system access to managed table paths.
# We use DESCRIBE HISTORY instead — it reads the same _delta_log internally.

from delta.tables import DeltaTable

dt       = DeltaTable.forName(spark, TABLE)
location = dt.detail().select("location").collect()[0]["location"]
print(f"Table location : {location}")
print(f"Delta log path : {location}/_delta_log/")
print()
print("NOTE: Direct dbutils.fs.ls() access to managed table _delta_log is")
print("restricted in Unity Catalog. Using DESCRIBE HISTORY instead (Cell 6).")

# COMMAND ----------

# CELL 6 — DESCRIBE HISTORY: full audit trail of all DML operations
# This reads the _delta_log internally and returns one row per committed transaction.
# Look for: CREATE OR REPLACE TABLE AS SELECT, WRITE, UPDATE, DELETE, MERGE

display(spark.sql(f"DESCRIBE HISTORY {TABLE}"))

# Expected versions (most recent first):
#   version 4 → MERGE
#   version 3 → DELETE
#   version 2 → UPDATE
#   version 1 → WRITE (initial seed load)
#   version 0 → CREATE OR REPLACE TABLE AS SELECT
#
# NOTE DOWN your version numbers — you will use them in Notebook 03 (Time Travel).