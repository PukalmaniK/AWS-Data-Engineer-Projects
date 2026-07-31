# Lab 24 — End-to-End Pipeline Orchestration & SQL Analytics

## Notebooks in this folder

| File | Layer | Description |
|---|---|---|
| `01_bronze_ingestion.py` | Bronze | Auto Loader ingestion from S3 raw CSV files |
| `02_silver_quality.py` | Silver | DLT expectations: Warn / Drop / Fail cleansing |
| `03_gold_aggregation.py` | Gold | Revenue aggregation by product, region, and status |

## S3 Upload Path

Upload `sales_raw.csv` and `sales_bad.csv` (from `~/Desktop/Project/`) to:

```
s3://<your-bucket>/raw/sales/
```

## Workflow Task Order

```
Task 1: auto_loader_ingestion  (DLT Pipeline — Bronze)
    ↓
Task 2: dlt_silver_gold        (DLT Pipeline — Silver + Gold)
    ↓
Task 3: gold_validation_query  (Notebook — SQL validation)
```

## S3 Bucket Variable

Update `S3_BUCKET` in `01_bronze_ingestion.py` to match your actual bucket name before importing into Databricks.
