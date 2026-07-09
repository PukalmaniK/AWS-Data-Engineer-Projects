# Lab 20 — Delta Lake Operations: Full DML & Time Travel

## Overview

This folder contains all notebooks and data files for **Lab 20** of the Databricks on AWS module. The lab covers the full operational surface of Delta Lake: DML, transaction log inspection, Time Travel, schema management, and storage optimization.

## Files

| File | Purpose |
|---|---|
| `01_setup_and_ingest.py` | Create lab database, managed and external Delta tables, load seed data |
| `02_dml_operations.py` | INSERT, UPDATE, DELETE, MERGE, transaction log inspection, DESCRIBE HISTORY |
| `03_time_travel.py` | VERSION AS OF, TIMESTAMP AS OF, disaster recovery via RESTORE TABLE |
| `04_schema_management.py` | Schema enforcement failure, mergeSchema evolution, verification |
| `05_optimize_vacuum.py` | OPTIMIZE, Z-ORDER by city, VACUUM dry run and 0-hour override |
| `../customers_seed.csv` | 10-row initial customer dataset |
| `../customers_updates.csv` | 6-row update file (4 updates + 2 new rows) for MERGE |

## Before You Begin

1. Update `S3_BUCKET` in **Notebooks 01, 02, and 04** with your actual AWS account ID.
2. Ensure `customers_seed.csv` and `customers_updates.csv` are uploaded to `s3://delta-lab-<account-id>/raw/` before running Notebook 01.
3. Run notebooks **in order** — each notebook depends on the state created by the previous one.

## Module Reference

This lab covers topics from **Module 5, Section 2: Delta Lake Deep Dive**:
- Full DML support (INSERT, UPDATE, DELETE, MERGE)
- ACID transactions and the transaction log
- Time travel and table restore
- Schema enforcement and evolution
- OPTIMIZE, Z-ORDER, and VACUUM
