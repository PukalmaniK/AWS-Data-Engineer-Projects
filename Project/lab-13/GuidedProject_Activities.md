# Title: Glue ETL Pipeline — Raw to Curated with Bookmarks

---

## 🌟 Overview: What is the purpose of this Lab?

Welcome to your AWS Glue ETL Pipeline project! In modern data lake architectures, transforming raw data into clean, structured, and queryable formats is essential for analytics and downstream consumption. This lab will guide you through building a robust ETL pipeline using AWS Glue, S3, and the Glue Data Catalog, with a specific focus on incremental processing using Glue Job Bookmarks.

**The Purpose of this Lab:**
You are part of a data engineering team at a fast-growing IoT company. Raw telemetry data arrives continuously in the Bronze (Raw) layer of your data lake. Your task is to build an automated AWS Glue PySpark job to transform, deduplicate, and cast this data into an optimized format (Parquet) in the Silver (Curated) layer. Crucially, you must ensure the pipeline supports **incremental processing** using Glue Job Bookmarks so it only processes new files on subsequent runs, avoiding costly reprocessing of old data.

By the end of this project, you will have:

- Designed a partitioned data lake (Raw and Curated zones) on S3 with lifecycle policies.
- Built a multi-step Glue PySpark ETL job handling data type casting and deduplication.
- Enabled and verified **Job Bookmarks** to ensure idempotent, incremental data loads.
- Handled schema evolution by processing a new file with an added column.
- Queried the curated data efficiently using Amazon Athena, leveraging SQL JOINs and partition pruning.

### Learning Path:

1. **Authentication:** Configure your AWS Console access and credentials.
2. **S3 Data Lake Foundations:** Design and provision a production data lake with best practices.
3. **Metadata Cataloging:** Use the Glue Data Catalog to make reference data available for ETL joins.
4. **ETL Pipeline & Bookmarks:** Build, run, and verify a chunked PySpark Glue job with incremental state tracking.
5. **Analytics & Validation:** Query the optimized curated data using Athena.

---

## 📊 Dataset / Knowledge Source Used

The following files are provided in the `~/Desktop/Project/` folder on your lab desktop. You will upload these to S3 as part of Activity 1. We are using an IoT Telemetry scenario for this build.

**Local File Structure:**

```text
~/Desktop/Project/
├── telemetry_20240801.csv
├── telemetry_20240802.csv
├── telemetry_20240803_v2.csv
└── device_mapping.json
```

| File                        | Zone                   | Description                                                                |
| --------------------------- | ---------------------- | -------------------------------------------------------------------------- |
| `telemetry_20240801.csv`    | `raw/date=2024-08-01/` | 500 raw device telemetry transactions for Aug 1, 2024                      |
| `telemetry_20240802.csv`    | `raw/date=2024-08-02/` | 500 raw device telemetry transactions for Aug 2, 2024                      |
| `telemetry_20240803_v2.csv` | `raw/date=2024-08-03/` | 500 transactions with new `battery_voltage` column (schema evolution demo) |
| `device_mapping.json`       | `raw/reference/`       | 50 device reference records in JSONL format (for SQL JOINs)                |

> **Note on JSONL format:** `device_mapping.json` uses one JSON object per line (not a JSON array). This is the format Athena and Glue natively read using the JSON SerDe.

---

## 🛠️ Prerequisites & AWS Setup

Ensure the following are available:

- An **AWS account** with permissions for S3, Glue, and Athena.
- Access to the AWS Console (Lab credentials provided).
- The local data files located at `~/Desktop/Project/`:
  - `~/Desktop/Project/telemetry_20240801.csv`
  - `~/Desktop/Project/telemetry_20240802.csv`
  - `~/Desktop/Project/telemetry_20240803_v2.csv`
  - `~/Desktop/Project/device_mapping.json`
- No local Jupyter notebook is required; all activities are performed via the AWS Console and Athena web UI.

---

## Configure AWS Credentials

First, log in to the AWS Web Console to set up our destination.

- Login to AWS Console:
  - Click on the `Lab Access` icon on the desktop.

![Images](images/lab-image1.png)

- Click on `Access Lab` and using the given credentials login to AWS Console. This will allow you to access the AWS resources from the Console.

![Images](images/lab-image2.png)

- Once logged in, set the region to `us-east-1`. Create all resources in the **US-EAST-1 (N. Virginia) Region**.

- Steps to select the AWS region in the AWS Console:
  - Locate the region selector at the top-right corner of the console (next to your Account Name, which is painted in red).
  - Click on the dropdown, and a list of available AWS Regions will appear.

![Images](images/lab-image3.png)

    - Choose us-east-1 (N.Virginia) Region.

![Images](images/lab-image4.png)

---

# Activity 1: Design and Provision a Production Data Lake on S3

**Purpose of this Activity:** To create a robust, secure, and cost-effective S3-based data lake with best practices for folder structure, versioning, encryption, and lifecycle management — and to upload the provided project files into the correct zones.

### Step 1.1: Create the S3 Bucket

1. In the AWS Management Console search bar, type **S3** and select it from the services menu.

![Images](images/1search_click_s3.png)

2. Click the **Create bucket** button.

![Images](images/2create_bucket.png)

3. **Bucket Name Configuration:**
   - Provide a globally unique name, e.g., `datalake-<your-unique-id>`.
   - Set the AWS Region to **us-east-1 (N. Virginia)**.

4. **Bucket Settings:**
   - **Object Ownership:** Leave as ACLs disabled (recommended).
   - **Block Public Access:** Keep the "Block _all_ public access" setting checked (default).
   - **Bucket Versioning:** Select **Enable** to protect against accidental deletions.
   - **Default Encryption:** Leave as Server-side encryption with Amazon S3 managed keys (SSE-S3), which is now enabled by default.

5. Click **Create bucket**.

![Images](images/3bucket_name_region.jpeg)

6. **Lifecycle Policy:**

- Click into your newly created bucket.
- Navigate to the **Management** tab.

- Click **Create lifecycle rule**.

![Images](images/click_management_tab.png)

- **Lifecycle rule name:** Enter `TransitionToGlacier`.
  - **Choose a rule scope:** Select **Apply to all objects in the bucket** and check the acknowledgment box.

![Images](images/lifecycle_rule_name.png)

- **Lifecycle rule actions:** Check the box for **Transition current versions of objects between storage classes**.

![Images](images/lifecycle_rule_actions.png)

- **Transition current versions of objects between storage classes:**
  - Choose **Glacier Flexible Retrieval** from the Storage class transitions dropdown.
  - Set **Days after object creation** to `30`.
- Scroll down and click **Create rule**.

![Images](images/storage_class_days.png)

---

### Step 1.2: Create Data Lake Folder Structure

**Purpose:** To organize your data lake into logical zones for raw and curated data following best practices for partitioning.

#### Phase A: Navigate to Your Bucket

1. In the AWS Management Console, ensure you are in the **Amazon S3** service.
2. Under the **Buckets** list, locate and click directly on the name of the bucket you just created (e.g., `datalake-<your-unique-id>`).

![Images](images/select_your_bucket.png)

3. Ensure you are viewing the **Objects** tab.

![Images](images/object_tab.png)

#### Phase B: Create Top-Level Folders

You will create three main folders at the root level of your bucket.

1. Click the **Create folder** button.

![Images](images/craete_folder.png)

2. In the **Folder name** field, type `raw`.

![Images](images/folder_name.png)

3. Scroll down and click the **Create folder** button at the bottom of the page.

![Images](images/create_folder_button.png)

4. Repeat steps 1-3 two more times to create the remaining top-level folders:
   - `curated`
   - `athena-results`

![Images](images/folders_created.png)

#### Phase C: Create Hive-Partitioned Subfolders in 'raw'

Now, you will navigate _inside_ the `raw` folder to create subfolders. Hive partitioning (`key=value`) allows Glue to automatically detect partitions.

1. From the **Objects** tab, click directly on the **`raw`** folder name to open it.

![Images](images/click_folder_raw.png)

2. **Create the first subfolder:**
   - Click the **Create folder** button.
   - In the **Folder name** field, type exactly: `date=2024-08-01`
   - Click **Create folder**.

![Images](images/curated_date_2024_06_01_folder.png)

3. Repeat this process to create two more folders inside `raw`:
   - `date=2024-08-02`
   - `date=2024-08-03`
   - `reference`

![Images](images/curated_folders_created.png)

### Step 1.3: Upload Project Files via AWS Console

**Purpose:** To load the sample data from your local `~/Desktop/Project/` folder into the `raw` data lake zones.

#### 1. Upload the Telemetry Data

1. Click into the `raw/date=2024-08-01` subfolder.

![Images](images/click_date1_folder.png)

2. Click the orange **Upload** button, then **Add files**.

![Images](images/date1_click_upload.png)
![Images](images/date1_click_addfiles.png)

3. Browse to `~/Desktop/Project/`, select `telemetry_20240801.csv`, and click **Open**.

![Images](images/date1_csv_open.png)

4. Scroll to the bottom and click **Upload**. Close the window when successful.

![Images](images/date1_csv_upload.png)
![Images](images/date1_csv_close.png)

5. Repeat the process for the other dates:
   - Upload `telemetry_20240802.csv` to `raw/date=2024-08-02/`

![Images](images/date2_csv_file.png)

- Upload `telemetry_20240803_v2.csv` to `raw/date=2024-08-03/`

![Images](images/date3_csv_file.png)

#### 2. Upload the Reference Data

1. Navigate to the `raw/reference/` subfolder.
2. Click **Upload**, then **Add files**.

![Images](images/reference_folder_upload.png)

3. Select `device_mapping.json` from your Project folder.

![Images](images/json_file_open.png)

4. Scroll down, click **Upload**, and then **Close** once it succeeds.

![Images](images/json_file_close.png)

---

# Activity 2: Catalog Reference Data with Glue

**Purpose of this Activity:** Before building the ETL pipeline, we need to register the reference data (JSON) in the AWS Glue Data Catalog. This step creates a database and a metadata table that you can query later.

### Step 2.1: Create a Glue Database

1. In the AWS Console, search for **Glue** and select it.

![Images](images/select_glue.png)

2. In the left navigation pane, under **Data Catalog**, click **Databases** > **Add database**.

![Images](images/click_add_database.png)

3. Name the database `datalake_db` and click **Create database**.

![Images](images/glue_database_name.png)

### Step 2.2: Create a Glue Crawler for Reference Data

1. In the left navigation, go to **Crawlers** > **Create crawler**.

![Images](images/create_crawler_button.png)

2. **Name:** `crawler_device_mapping`. Click **Next**.

![Images](images/crawler_name_next.png)

3. **Data Source:** Click **Add a data source**.

![Images](images/click_add_datasource.png)

- Select **S3**.
- Browse to `s3://<your-bucket>/raw/reference/`.

![Images](images/select_browse_s3.png)

- Click `datalake-<your-bucket>` name.

![Images](images/click_s3_bucket.png)

- Click `raw` folder name.

![Images](images/click_raw_folder.png)

- Select `reference/` folder and click **Choose**

![Images](images/select_reference_folder.png)

- Click **Add an S3 data source**. Click **Next**.

![Images](images/click_add_s3_datasource.png)

4. **IAM Role:** Click **Create new IAM role**. Type `ReferenceCrawler` in the box (creating `AWSGlueServiceRole-ReferenceCrawler`). Click **Next**.

![Images](images/crawler_create_IAM.png)
![Images](images/crawler_IAM_name.png)
![Images](images/crawler_IAM_next.png)

5. **Output configuration:** Select `datalake_db` as the target database, scroll down and click **Next**.

![Images](images/output_config.png)

6. Review and click **Create crawler**.

![Images](images/click_create_crawler.png)

7. Once created, select the crawler and click **Run**. Wait for it to complete. Note: Glue crawlers may take 1–2 minutes to finish.

![Images](images/select_run_crawler.png)

8. Click **Tables** in left side navigation.

![Images](images/click_tables.png)

9.  You will now have a `reference` table in your `datalake_db` database.

![Images](images/reference_table.png)

**Important IAM S3 Permissions Verification**
Before you proceed with the script, you must ensure your Glue job has permission to read and write to your S3 bucket to avoid a `Forbidden (Status Code: 403)` error.

1. Open a new AWS Console tab and navigate to the **IAM** service.

![Images](images/serach_iam.png)

2. Click on **Roles** in the left menu and find the role attached to your Glue job (e.g., `AWSGlueServiceRole-ReferenceCrawler`).

![Images](images/sreach_role.png)

3. Click the role name, then click **Add permissions** -> **Attach policies**.

![Images](images/add_permission.png)

4. Search for **`AmazonS3FullAccess`**, check the box next to it, and click **Add permissions**.

![Images](images/s3_permision_add.png)
![Images](images/s3_permision_added.png)

---

# Activity 3: Bronze to Silver Zone ETL — Build and Run the PySpark Job

**Purpose of this Activity:** This is the core data engineering task. You will write a Glue PySpark script that extracts data from the **Bronze zone (Raw)**, applies type casting, deduplication, and null handling, and writes optimized Parquet to the **Silver zone (Curated)**. You will explicitly enable Job Bookmarks to track processed files.

### Step 3.1: Configure the Glue ETL Job Properties

1. In the AWS Glue Console, navigate to **ETL jobs** under Data Integration and ETL.

![Images](images/click_ETL_jobs.png)

2. Select **Visual ETL** (or **Spark script editor** if available) and switch to the **Script** tab.

![Images](images/select_visual_ETL.png)
![Images](images/switch_script.png)

3. Notice that the script says **Script (Locked)**. To write our custom code, click the **Edit script** button on the right side of the screen. Confirm any warning prompts that appear (unlocking it means the visual canvas will no longer auto-update, which is exactly what we want).

![Images](images/edit_script.png)

4. Click on the **Job details** tab at the top.
   - **Name:** `telemetry_raw_to_curated`
   - **IAM Role:** Choose the `AWSGlueServiceRole-ReferenceCrawler` (or create a new role with S3 and Glue permissions).

![Images](images/job_details_tab.png)

- **Number of workers:** Set to `2` to save lab resources.

![Images](images/number_worker.png)

- **Job bookmark:** Change the dropdown from Disable to **Enable**. _(Critical step for incremental processing!)_

![Images](images/job_bookmark_enable.png)

---

### Step 3.2: Writing the PySpark ETL Code (Bronze to Silver)

**Purpose:** This unified PySpark script handles the end-to-end ETL process. It initializes the AWS Glue environment, extracts raw data from the **Bronze zone** using Job Bookmarks (to skip previously processed files), and seamlessly handles empty incremental runs using an `if` statement. It then transforms the data (applying deduplication, null handling, and type casting) and finally loads the optimized Parquet files into the **Silver zone** before committing the job state.

**Important Script Navigation Guidance:**

- Open the **Script** tab.
- Remove the default generated script completely.
- Paste the provided code below into the editor.

**What You Need to Modify:**
Users ONLY need to modify:

- `source_path`
- `target_path`

Replace the placeholder bucket name (`datalake-your-unique-id`) with your own bucket name. Do not modify the remaining ETL logic.

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame

# Initialize contexts and job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# WARNING: Replace 'datalake-your-unique-id' with your actual S3 bucket name
source_path = "s3://datalake-your-unique-id/raw/"      # Bronze Zone
target_path = "s3://datalake-your-unique-id/curated/"  # Silver Zone

# Read Raw CSV Data using DynamicFrame from the Bronze Zone
# The transformation_ctx is what Glue uses to link this read operation to the Bookmark state
raw_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [source_path],
        "recurse": True,
        "exclusions": ["**reference**"]
    },
    transformation_ctx="raw_dyf"
)

# Check if there is any new data to process (this handles the 0-byte Bookmark run)
if raw_dyf.count() > 0:

    # Convert DynamicFrame to Spark DataFrame for complex transformations
    df = raw_dyf.toDF()

    # Bronze to Silver Transformations:
    # 1. Deduplication based on transaction ID
    # 2. Null Handling: Drop rows missing critical IDs
    # 3. Type Casting: String to Double
    df_clean = df.dropDuplicates(["telemetry_id"]) \
                 .na.drop(subset=["telemetry_id", "device_id"]) \
                 .withColumn("temperature", col("temperature").cast("double"))

    # Convert the cleaned DataFrame back to a Glue DynamicFrame
    curated_dyf = DynamicFrame.fromDF(df_clean, glueContext, "curated_dyf")

    # Write the data to the Silver zone as Parquet files partitioned by date
    glueContext.write_dynamic_frame.from_options(
        frame=curated_dyf,
        connection_type="s3",
        format="glueparquet",
        connection_options={"path": target_path, "partitionKeys": ["date"]},
        format_options={"compression": "snappy"},
        transformation_ctx="datasink"
    )
    print("New data processed and written to Silver (curated) zone.")

else:
    print("Job Bookmark detected no new files. Skipping transformation and load.")

# CRITICAL: Commit the job state. This updates the Bookmark so old files aren't re-processed next time.
job.commit()
```

![Images](images/change_s3_name.png)

### Step 3.3: Execute and Verify Bookmarks

**Purpose:** To run the ETL job and explicitly prove that the bookmark system prevents the reprocessing of old files.

**Understanding Glue Job Bookmarks:**

- Glue Job Bookmarks help AWS Glue remember which files were already processed.
- Prevents duplicate processing.
- Enables incremental ETL pipelines.

_Always click **Save** before running the Glue job, otherwise your latest code changes will not execute._

![Images](images/save_code.png)

1. **The Initial Run (Full Load):**
   - Click the orange **Run** button in the top right corner.

![Images](images/run_button.png)

- Navigate to the **Runs** tab to monitor the progress. Note: Glue job startup may take 2–5 minutes.
- **Verify Storage:** Once the job status says "Succeeded", open a new console tab, go to S3, and check your **Silver (`curated/`) zone**. You should verify that `date=2024-08-01/`, `date=2024-08-02/`, and `date=2024-08-03/` exist inside this zone and contain snappy-compressed Parquet files.

![Images](images/run_tab.png)
![Images](images/curated_folder_click.png)
![Images](images/curated_date_folders.png)

2. **The Bookmark Run (Incremental Load):**
   - Go back to the AWS Glue console. Without changing any files in S3, click the **Run** button a _second time_.

- Wait for the job to complete. It will likely finish much faster.

- **Verify via Logs (The Proof):** Click on the successful second Run ID to select it.

![Images](images/code_run_success_again.png)

- Look at the tabs below the run list and click on the **Logs** tab (next to "Run insights" and "Metrics").

![Images](images/select_log_tab.png)

- Click the link on the right side that says **Driver and executor log streams** (this will open Amazon CloudWatch in a new tab).

![Images](images/log_stream_link.png)

- _Important:_ In CloudWatch, ensure you are looking at the **output** logs. If the top breadcrumb says `/aws-glue/jobs/error`, click "Log Management" on the left menu and select **`/aws-glue/jobs/output`** instead.

![Images](images/click_output_log.png)

- Click the **Search all log streams** button.

![Images](images/search_log_button.png)

- Type **"Job Bookmark detected"** into the search bar to find the exact message we programmed: **`"Job Bookmark detected no new files. Skipping transformation and load."`**

![Images](images/result_output.png)

**Troubleshooting Note:** If the Glue job fails with AccessDenied errors, ensure the IAM role has `AmazonS3FullAccess` and `AWSGlueServiceRole` permissions.

---

# Activity 4: Query Data with Amazon Athena (Silver Zone)

**Purpose of this Activity:** To analyze your optimized **Silver zone (curated)** data using serverless SQL queries, perform SQL JOINs across tables, and observe how schema evolution is handled seamlessly in Parquet format.

### Step 4.1: Catalog the Curated Data

Before querying, Athena needs to know the schema of your newly created Parquet files.

1. Go back to Glue **Crawlers** and click **Create crawler**

![Images](images/create_new_crawler.png)

2. Named it `crawler_telemetry_curated` and click Next

![Images](images/new_crawler_name_next.png)

2. Click **Add a data source** then Set the data source to your S3 path: `s3://<your-bucket>/curated/` and click **Add an S3 data soucre**. Click Next

![Images](images/new_s3_path.png)

3. Select your existing IAM Role (`AWSGlueServiceRole-ReferenceCrawler`).

![Images](images/select_same_iam.png)

4. Set the target database to `datalake_db`.

![Images](images/output_config_again.png)

5. Review and click **Create crawler**.

![Images](images/click_create_crawler-again.png)

### Step 4.1a: Run the Crawler and Verify Table Creation

**Purpose:** To execute the crawler so it can scan your optimized Parquet files in S3, infer the schema (including the partitions), and automatically create the `curated` metadata table in your Glue Data Catalog.

1. **Select the Crawler:**
   - Locate the `crawler_telemetry_curated` crawler you just created in the list.
   - Check the empty box to the left of its name to select it.

2. **Run the Crawler:**
   - Click the **Run** button located near the top right of the Crawlers list.

![Images](images/select_nw_crawler_run.png)

3. **Monitor the Status:**
   - Watch the **State** column for your crawler. It will transition through several phases: _Starting_ -> _Running_ -> _Stopping_ -> _Ready_.
   - _Note: This scanning process typically takes 1 to 2 minutes to complete._

4. **Verify the Table Creation:**
   - Once the crawler state returns to _Ready_, look at the **Table changes** column. It should indicate that 1 table was added.
   - To definitively verify your data is ready for Athena, look at the left navigation pane again and click on **Databases**.
   - Click on your `datalake_db` database.
   - Click on the **Tables** link under the database details (or click "Tables" in the left navigation menu).
   - You should now see a new table named **`curated`** in the list. This table holds the schema and partition metadata required for your SQL queries!

![Images](images/table_created_curated.png)

### Step 4.2: Set Up Athena

**Note:** Athena queries will fail until an S3 query result location is configured.

1. In the AWS Console, search for **Athena** and select it.

![Images](images/search_select_athena.png)

2. On the Athena welcome screen, select **Query your data in Athena console** click **Launch query editor**.

![Images](images/launch_query_editor.png)

3. On the Athena **Editor** tab, you will see a blue notification banner stating "Before you run your first query, you need to set up a query result location in Amazon S3." Click the **Edit settings** button directly inside that banner.

![Images](images/edit_settings.png)

4. You will be taken to the **Query settings** tab. Under the **Query result encryption** section, click the **Manage** button on the right side.

![Images](images/manage_settings.png)

5. In the configuration window that opens, set the **Query result location** to point to the results folder you created in your S3 bucket (e.g., `s3://datalake-your-unique-id/athena-results/`). Scroll down and click **Save**.

![Images](images/manage_query.png)

6. Click the **Editor** tab at the top to return to the query interface. Look at the **Data** pane on the left side. Ensure the **Data source** is set to `AwsDataCatalog`, and select your **`datalake_db`** from the **Database** dropdown right below it.

![Images](images/editor_tab_config.png)

### Step 4.3: SQL JOIN — Enrich Telemetry with Device Metadata

Athena uses partition pruning to scan only matching folders instead of the entire dataset, improving performance and reducing query cost.

Use the reference table to join hardware version information into your telemetry queries:
Copy the below code and run it in the Editor.

![Images](images/sql1_code.png)

```sql
SELECT
    c.telemetry_id,
    c.device_id,
    c.temperature,
    c.date,
    r.hardware_version,
    r.firmware_rev
FROM curated AS c
JOIN reference AS r
    ON c.device_id = r.device_id
WHERE c.date = '2024-08-01'
ORDER BY c.temperature DESC
LIMIT 10;
```

![Images](images/sql1_result.png)

### Step 4.4: Schema Evolution — Querying the New Column

The Aug 3 dataset introduces the `battery_voltage` column. This simulates real-world schema evolution where incoming data structures change over time.

The `battery_voltage` column only exists in the `date=2024-08-03` partition (our simulated v2 schema). Query it directly:

![Images](images/sql2_code.png)

```sql
SELECT
    telemetry_id,
    device_id,
    temperature,
    battery_voltage
FROM curated
WHERE date = '2024-08-03'
  AND CAST(battery_voltage AS double) < 20.0
ORDER BY CAST(battery_voltage AS double) ASC
LIMIT 10;
```

![Images](images/sql2_result.png)

For partitions without this column (Aug 1 and Aug 2), Athena handles the schema evolution gracefully and simply returns `NULL`.

---

## 🎓 Conclusion

This guided project demonstrated how to design and provision a production-grade data lake on Amazon S3, automate metadata discovery with Glue Crawlers, and analyze data efficiently with Athena. More importantly, you built an enterprise-grade Glue PySpark ETL job that utilizes **Job Bookmarks** for stateful, incremental processing.

You practiced real-world data engineering patterns, including:

- Architecting a multi-tier data lake pipeline (**Bronze to Silver zones**).
- Applying critical data quality transformations (deduplication, **null handling**, and type casting).
- Implementing Hive-style S3 partitioning.
- Handling schema evolution gracefully.
- Enriching transactional data with reference lookups using SQL JOINs.

These are essential skills for any modern AWS Data Engineer!
