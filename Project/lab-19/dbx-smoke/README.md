# dbx-smoke

A minimal smoke-test repository for validating a production Databricks workspace deployment on AWS.

## Purpose

This repo is used as part of the **Databricks Workspace Deployment on AWS** lab. It is cloned into Databricks Repos and the `notebooks/smoke_test` notebook is run both interactively (on the all-purpose cluster) and as a scheduled job (on the job cluster) to confirm end-to-end wiring across:

- Custom VPC networking (private subnets, NAT egress, self-referencing security group)
- Cross-account IAM role (`dbx-cross-account-role`)
- EC2 instance profile (`dbx-s3-access-role`) for S3 access via short-lived credentials
- Databricks Repos ↔ GitHub integration

## Repository Structure

```
dbx-smoke/
├── notebooks/
│   └── smoke_test.py   # PySpark notebook — reads sample.csv, writes temperature_f Parquet
└── README.md
```

## What the Smoke Test Does

| Cell | Action | Validates |
|------|--------|-----------|
| 1 | Resolves `dbx-data-<account-id>` bucket name from cluster tags | Cluster is running in the correct AWS account |
| 2 | Calls `aws sts get-caller-identity` | Instance profile (`dbx-s3-access-role`) is actively attached |
| 3 | `dbutils.fs.ls(s3a://.../raw/)` | S3 read access via instance profile |
| 4 | Reads `raw/sample.csv` with PySpark | Schema inference + data load |
| 5 | Writes `processed/temperature_f/` as Parquet | S3 write access via instance profile |

## Prerequisites

- Databricks workspace deployed in customer-managed VPC mode (`dbx-lab-ws`)
- `dbx-all-purpose` cluster (autoscaling 2–4, mixed on-demand + spot) is running
- `dbx-smoke-job` workflow created with a job cluster (autoscaling 1–3, 100% spot)
- `sample.csv` uploaded to `s3://dbx-data-<your-aws-account-id>/raw/sample.csv`

## Setup

1. Push this repo to GitHub (`dbx-smoke` — public).
2. In Databricks workspace UI → **Workspace → Repos → Add → Git folder**.
3. Paste your repo URL and clone on branch `main`.
4. Open `notebooks/smoke_test` and attach to `dbx-all-purpose`.
5. Click **Run all** — all cells should succeed.

## Running as a Job

Navigate to **Workflows → `dbx-smoke-job` → Run now**.  
The job cluster provisions automatically, runs the notebook, then terminates — keeping cloud spend minimal.
