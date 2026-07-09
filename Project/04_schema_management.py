# Databricks notebook source
# Lab 20 | Notebook 04 — Schema Enforcement & Schema Evolution
# Purpose: Demonstrate Delta Lake schema protection and controlled evolution.
# Prerequisite: Notebooks 01 and 02 must have been run successfully.
#
# NOTE: This notebook uses an explicit StructType schema when creating the
# test DataFrame to avoid a type conflict between PySpark's default 'long'
# type and the Delta table's 'int' type for customer_id. Using Row() without
# a schema causes a "Failed to merge fields" error even with mergeSchema=true.

# COMMAND ----------

# CELL 1 — Imports and config
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

DB_NAME = "delta_lab_db"
TABLE   = f"{DB_NAME}.customers_managed"

# COMMAND ----------

# CELL 2 — Show the current schema of the managed table
print("Current schema of customers_managed:")
spark.sql(f"DESCRIBE TABLE {TABLE}").show(truncate=False)


# COMMAND ----------

# CELL 3 — SCHEMA ENFORCEMENT: attempt to append a DataFrame with an extra column
# This should FAIL with an AnalysisException.
# NOTE: We use an explicit StructType schema to match the Delta table's int type
# for customer_id. Using Row() defaults customer_id to long, causing a type
# conflict error instead of the expected schema mismatch error.

schema = StructType([
    StructField("customer_id",     IntegerType(), True),
    StructField("name",            StringType(),  True),
    StructField("city",            StringType(),  True),
    StructField("account_balance", DoubleType(),  True),
    StructField("status",          StringType(),  True),
    StructField("loyalty_tier",    StringType(),  True)   # <-- extra column not in Delta schema
])

bad_schema_df = spark.createDataFrame(
    [(99, "Test User", "Delhi", 1000.00, "active", "gold")],
    schema=schema
)

print("Attempting to append a DataFrame with an extra column (loyalty_tier)...")
print("Expecting an AnalysisException — schema enforcement is ON by default.\n")

try:
    (bad_schema_df.write
     .format("delta")
     .mode("append")
     .saveAsTable(TABLE))
    print("ERROR: Write succeeded — schema enforcement was NOT triggered (unexpected).")
except AnalysisException as e:
    print("Schema enforcement triggered — write was REJECTED as expected.")
    print(f"\nError message:\n{str(e)[:500]}")

# Expected output:
# Attempting to append a DataFrame with an extra column (loyalty_tier)...
# Expecting an AnalysisException — schema enforcement is ON by default.
#
# Schema enforcement triggered — write was REJECTED as expected.
#
# Error message:
# A schema mismatch detected when writing to the Delta table (Table ID: xxxxxxxx).
# To enable schema migration using DataFrameWriter or DataStreamWriter, please set:
# '.option("mergeSchema", "true")'.

# COMMAND ----------

# CELL 4 — SCHEMA EVOLUTION: enable mergeSchema and retry the same write
# This should SUCCEED and add loyalty_tier as a new column.
print("Retrying the write with mergeSchema=true...")

schema = StructType([
    StructField("customer_id",     IntegerType(), True),
    StructField("name",            StringType(),  True),
    StructField("city",            StringType(),  True),
    StructField("account_balance", DoubleType(),  True),
    StructField("status",          StringType(),  True),
    StructField("loyalty_tier",    StringType(),  True)
])

bad_schema_df = spark.createDataFrame(
    [(99, "Test User", "Delhi", 1000.00, "active", "gold")],
    schema=schema
)

(bad_schema_df.write
 .format("delta")
 .mode("append")
 .option("mergeSchema", "true")
 .saveAsTable(TABLE))

print("Write succeeded with mergeSchema enabled.")

# Expected output:
# Retrying the write with mergeSchema=true...
# Write succeeded with mergeSchema enabled.

# COMMAND ----------

# CELL 5 — Verify the schema now includes the new column
print("Updated schema after mergeSchema:")
spark.sql(f"DESCRIBE TABLE {TABLE}").show(truncate=False)


# COMMAND ----------

# CELL 6 — Verify existing rows have NULL for the new column; new row has value
print("Data in customers_managed — showing loyalty_tier column:")
display(spark.sql(f"""
    SELECT customer_id, name, loyalty_tier
    FROM   {TABLE}
    ORDER  BY customer_id
"""))


#
# NOTE: If this notebook was run multiple times, customer_id=99 may appear
# more than once. This is expected — each run appends a new row for Test User.
# The schema enforcement and evolution behaviour is correct regardless.

# COMMAND ----------

# CELL 7 — DESCRIBE HISTORY to see the schema change in the transaction log
print("Transaction history showing schema evolution event:")
display(spark.sql(f"DESCRIBE HISTORY {TABLE} LIMIT 5"))


