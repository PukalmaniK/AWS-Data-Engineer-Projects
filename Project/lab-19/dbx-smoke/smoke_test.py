# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks <-> AWS smoke test
# MAGIC Verifies the cluster's instance profile can read/write the data bucket.

# COMMAND ----------
# Cell 1 — Define the data bucket manually
import os

# REPLACE THIS with your actual bucket name from AWS S3!
DATA_BUCKET = "dbx-data-put-your-actual-suffix-here" 

print("Data bucket:", DATA_BUCKET)

# COMMAND ----------
# Cell 2 — Confirm the instance profile is active using Boto3
import boto3

try:
    sts = boto3.client('sts')
    identity = sts.get_caller_identity()
    print("Success! Cluster is running with Assumed Role:")
    print(identity['Arn'])
except Exception as e:
    print("Error getting IAM Identity. Is the instance profile attached to the cluster?")
    print(e)

# COMMAND ----------
# Cell 3 — List raw/ prefix in the data bucket (confirms S3 read access)
dbutils.fs.ls(f"s3a://{DATA_BUCKET}/raw/")

# COMMAND ----------
# Cell 4 — Read sample.csv and display it
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"s3a://{DATA_BUCKET}/raw/sample.csv")
)
df.show()

# COMMAND ----------
# Cell 5 — Add temperature_f column and write Parquet back to S3 (confirms write access)
out = df.selectExpr(
    "id",
    "city",
    "temperature_c",
    "temperature_c * 9/5 + 32 AS temperature_f"
)

(
    out.write
    .mode("overwrite")
    .parquet(f"s3a://{DATA_BUCKET}/processed/temperature_f/")
)

print("Wrote:", f"s3a://{DATA_BUCKET}/processed/temperature_f/")
dbutils.fs.ls(f"s3a://{DATA_BUCKET}/processed/temperature_f/")