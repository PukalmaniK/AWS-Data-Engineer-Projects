# Lab 18 — Delta Live Tables Pipeline with Data Quality
## Project Folder README

---

## Folder Contents

```text
~/Desktop/Project/
├── orders_raw.csv              ← 20 valid order records (upload to S3)
├── orders_bad.csv              ← 8 bad records to trigger DQ expectations (upload to S3)
└── dlt_lab/
    ├── README.md               ← This file
    ├── 01_bronze_ingestion.py  ← Bronze DLT table (Auto Loader from S3)
    ├── 02_silver_quality.py    ← Silver DLT table (data quality expectations)
    └── 03_gold_aggregation.py  ← Gold DLT table (revenue aggregation)
```

---

## What Each File Does

### Data Files

| File | Rows | Purpose |
|---|---|---|
| `orders_raw.csv` | 20 | Clean, valid orders across 3 products and 3 statuses |
| `orders_bad.csv` | 8 | Intentionally bad records: null order_ids, negative quantities, zero quantities, invalid statuses |

**Upload both files to:** `s3://dlt-lab-<your-account-id>/raw/orders/`

### Notebook Files

| File | Layer | DLT Table | Key Concepts |
|---|---|---|---|
| `01_bronze_ingestion.py` | Bronze | `bronze_orders` | Auto Loader, `cloudFiles`, schema inference, audit columns |
| `02_silver_quality.py` | Silver | `silver_orders` | `@dlt.expect` (Warn), `@dlt.expect_or_drop` (Drop), `@dlt.expect_or_fail` (Fail) |
| `03_gold_aggregation.py` | Gold | `gold_revenue_by_product` | `dlt.read()` batch aggregation, business-ready output |

---

## Data Quality Expectations Summary

Expectations are defined on the **Silver layer** (`02_silver_quality.py`):

| Expectation Name | Rule | Mode | What Happens |
|---|---|---|---|
| `order_id_not_null` | `order_id IS NOT NULL` | Warn | Row kept; violation recorded in event log |
| `quantity_positive` | `quantity > 0` | Drop | Row removed silently; pipeline continues |
| `unit_price_positive` | `unit_price > 0` | Fail | Pipeline halts; requires investigation |

---

## Expected Record Counts After Pipeline Run

| Layer | Table | Expected Rows | Notes |
|---|---|---|---|
| Bronze | `bronze_orders` | 28 | All records from both CSVs, no filtering |
| Silver | `silver_orders` | ~22 | After dropping negative-qty rows and invalid-status rows |
| Gold | `gold_revenue_by_product` | ~9 | One row per product-status combination |

> Exact counts depend on which records pass the Silver `.filter()` for valid statuses.

---

## S3 Upload Checklist

Before running the DLT pipeline, confirm the following files exist in S3:

- [ ] `s3://dlt-lab-<your-account-id>/raw/orders/orders_raw.csv`
- [ ] `s3://dlt-lab-<your-account-id>/raw/orders/orders_bad.csv`

---

## Before Running the Pipeline

1. Open `01_bronze_ingestion.py` in Databricks and update:
   ```python
   S3_BUCKET = "dlt-lab-<your-aws-account-id>"
   ```
2. Import all 3 `.py` notebooks into your `dlt_lab` Databricks workspace folder.
3. Create a new DLT pipeline in the **Delta Live Tables** UI, add all 3 notebooks as source files, set **Pipeline mode** to `Triggered`, and set **Target schema** to `dlt_lab_db`.
4. Click **Start** and monitor the pipeline graph.

---

## Troubleshooting

| Issue | Likely Cause | Fix |
|---|---|---|
| Pipeline fails at Bronze | Wrong S3 path or missing IAM credentials | Check `S3_BUCKET` value in `01_bronze_ingestion.py` and Spark config on cluster |
| `unit_price_positive` Fail fires unexpectedly | `orders_bad.csv` contains a zero or negative `unit_price` | Review `orders_bad.csv` — this expectation uses Fail mode and will halt the pipeline |
| Silver row count is 0 | Status filter too strict or wrong column name | Check `.filter(F.col("status").isin(...))` in `02_silver_quality.py` |
| Gold table is empty | Silver is empty | Fix Silver first; Gold reads from Silver |
| Auto Loader schema location error | `cloudFiles.schemaLocation` path does not exist or no write permission | Ensure the cluster IAM role has write access to the `dlt_checkpoints/` prefix in S3 |
