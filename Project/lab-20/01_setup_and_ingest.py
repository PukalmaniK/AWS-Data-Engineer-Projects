# Databricks notebook source
# Lab 20 | Notebook 01 — Setup & Ingest
# Purpose: Create the lab database, managed Delta table, and load seed data.
#
# PRE-REQUISITES (already configured on the cluster):
#   - Cluster: delta-lab-cluster (Runtime 13.3 LTS, Single node)
#   - Spark config on cluster:
#       spark.hadoop.fs.s3n.awsAccessKeyId     <YOUR-ACCESS-KEY>
#       spark.hadoop.fs.s3n.awsSecretAccessKey <YOUR-SECRET-KEY>
#       spark.hadoop.fs.s3.awsAccessKeyId      <YOUR-ACCESS-KEY>
#       spark.hadoop.fs.s3.awsSecretAccessKey  <YOUR-SECRET-KEY>
#   - Environment variables on cluster:
#       AWS_ACCESS_KEY_ID=<YOUR-ACCESS-KEY>
#       AWS_SECRET_ACCESS_KEY=<YOUR-SECRET-KEY>
#
# NOTE: External Delta tables on S3 are skipped in this lab because Unity Catalog
# requires External Location registration for custom S3 paths. All activities use
# the managed table (delta_lab_db.customers_managed).

# COMMAND ----------

# CELL 1 — Header comment cell (no code needed)
# Lab 20 | Notebook 01 — Setup & Ingest
# Purpose: Create the lab database, managed Delta table, and load seed data.
# Update S3_BUCKET below before running.

# COMMAND ----------

# CELL 2 — CONFIGURATION (update S3_BUCKET before running)
S3_BUCKET           = "delta-lab-<your-aws-account-id>"   # e.g. "delta-lab-784266215529"
EXTERNAL_TABLE_PATH = f"s3://{S3_BUCKET}/delta/customers_external/"
SEED_CSV_PATH       = f"s3://{S3_BUCKET}/raw/customers_seed.csv"
DB_NAME             = "delta_lab_db"

print(f"S3 Bucket          : {S3_BUCKET}")
print(f"External Table Path: {EXTERNAL_TABLE_PATH}")
print(f"Seed CSV Path      : {SEED_CSV_PATH}")

# COMMAND ----------

# CELL 3 — Drop and recreate the lab database to ensure a clean state
spark.sql(f"DROP DATABASE IF EXISTS {DB_NAME} CASCADE")
spark.sql(f"CREATE DATABASE {DB_NAME} COMMENT 'Lab 20 — Delta Lake Operations'")
print(f"Database '{DB_NAME}' created.")

# COMMAND ----------

# CELL 4 — Read the seed CSV from S3
# The cluster Spark config handles AWS credentials via:
#   spark.hadoop.fs.s3n.awsAccessKeyId / awsSecretAccessKey
#   spark.hadoop.fs.s3.awsAccessKeyId  / awsSecretAccessKey

seed_df = (spark.read
           .option("header", "true")
           .option("inferSchema", "true")
           .csv(SEED_CSV_PATH))

print(f"Seed row count: {seed_df.count()}")
seed_df.printSchema()
display(seed_df)

# COMMAND ----------

# CELL 5 — Create MANAGED Delta table and load seed data
# A managed table stores data inside the Databricks warehouse location.
# Dropping a managed table also deletes its data files.

spark.sql(f"DROP TABLE IF EXISTS {DB_NAME}.customers_managed")

(seed_df.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{DB_NAME}.customers_managed"))

count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DB_NAME}.customers_managed").collect()[0]["cnt"]
print(f"customers_managed loaded — row count: {count}")

# COMMAND ----------

# CELL 6 — External table creation skipped
# Unity Catalog requires External Location registration for custom S3 paths.
# All lab activities use the managed table instead.
print("Skipping external table - using managed table for all lab activities.")
print(f"Managed table: {DB_NAME}.customers_managed - already loaded with 10 rows")

# COMMAND ----------

# CELL 7 — Final verification — display the managed table
print("=== customers_managed ===")
display(spark.sql(f"SELECT * FROM {DB_NAME}.customers_managed ORDER BY customer_id"))
