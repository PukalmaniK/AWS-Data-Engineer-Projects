# Title: Data Quality Gates in Glue Pipelines

---

## 🌟 Overview: What is the purpose of this Lab?

Welcome to your AWS Glue Data Quality Pipeline project! In modern data engineering, preventing bad data from entering your data warehouse or analytics layer is just as critical as the transformations themselves. This lab will guide you through building a robust ETL pipeline using AWS Glue, S3, and Amazon CloudWatch, with a specific focus on evaluating data health in transit using native AWS Glue Data Quality rules.

**The Purpose of this Lab:**
You are part of a data engineering team responsible for the ingestion of daily e-commerce transaction data. Raw financial transactions arrive continuously in the Raw layer of your data lake. Your task is to build an automated AWS Glue PySpark job that acts as a gatekeeper. You will validate incoming data against strict Data Quality Definition Language (DQDL) rules (ensuring completeness and uniqueness). Clean data will be passed to the Curated zone for analytics, while invalid records will be quarantined for engineering review. Finally, you will publish quality metrics to CloudWatch and set up automated alerts so your team can proactively monitor pipeline health.

By the end of this project, you will have:

- Designed an S3 data lake with logical routing zones (Raw, Curated, and Quarantine).
- Configured robust IAM permissions to seamlessly integrate Glue, S3, and CloudWatch.
- Built an AWS Glue PySpark ETL pipeline with embedded Data Quality checks.
- Implemented Row-Level Outcomes to natively split and route passing vs. failing records.
- Monitored data quality metrics natively inside Amazon CloudWatch.
- Queried trusted data using Amazon Athena.
- Configured automated email alerts via Amazon SNS for data quality breaches.

### Learning Path:

1. **Authentication:** Configure your AWS Console access and credentials.
2. **Architecture & Proactive Security:** Provision S3 zones and create IAM roles ahead of time.
3. **Metadata Cataloging:** Use the Glue Data Catalog to discover the raw dataset schemas.
4. **Data Quality ETL Pipeline:** Build, run, and verify a Glue PySpark job with DQDL rules and conditional S3 routing.
5. **Observability & Metrics:** Verify routing execution and analyze pipeline health in CloudWatch.
6. **Analytics:** Catalog the Curated zone and query trusted data using Amazon Athena.
7. **Automated Alerting:** Set up an Amazon SNS topic and CloudWatch Alarm to notify your team of bad data spikes.

---

## 📊 Dataset / Knowledge Source Used

The following files are provided in the `~/Desktop/Project/` folder on your lab desktop. You will upload these to S3 as part of Activity 1. We are using an E-Commerce Transactions scenario for this build.

**Local File Structure:**

```text
~/Desktop/Project/
├── transactions_20260510.csv
└── transactions_20260511_corrupted.csv
```

| File                                  | Zone                   | Description                                                                 |
| ------------------------------------- | ---------------------- | --------------------------------------------------------------------------- |
| `transactions_20260510.csv`           | `raw/date=2026-05-10/` | 1,000 perfectly clean, unique transaction records for May 10, 2026.         |
| `transactions_20260511_corrupted.csv` | `raw/date=2026-05-11/` | 1,000 transactions containing null `transaction_id`s and duplicate amounts. |

---

## 🛠️ Prerequisites & AWS Setup

Ensure the following are available:

- An **AWS account** with permissions for S3, Glue, IAM, Athena, SNS, and CloudWatch.
- Access to the AWS Console (Lab credentials provided).
- The local data files located at `~/Desktop/Project/`:
  - `~/Desktop/Project/transactions_20260510.csv`
  - `~/Desktop/Project/transactions_20260511_corrupted.csv`
- No local Jupyter notebook is required; all activities are performed via the AWS Console.

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

# Activity 1: Provision the Data Lake and Proactive IAM Security

**Purpose of this Activity:** To set up the foundational storage architecture (Raw, Curated, and Quarantine zones) and to configure the required IAM Role ahead of time. Proactively configuring IAM ensures your Glue job has the exact permissions required to read/write to S3 and publish metrics to CloudWatch, guaranteeing a flawless pipeline execution later.

### Step 1.1: Create the S3 Bucket Architecture

1. In the AWS Management Console search bar at the very top of the screen, type **S3** and select **S3** from the Services dropdown menu.

![Images](images/1search_click_s3.png)

2. Click the orange **Create bucket** button on the right side of the screen.

3. **Bucket Name Configuration:**
   - Under **General purpose**, provide a globally unique name in the **Bucket name** field (e.g., `dq-datalake-<your-unique-id>`).
   - Ensure the AWS Region is set to **US East (N. Virginia) us-east-1**.

4. Scroll all the way down to the bottom of the page, leaving all other default settings exactly as they are (Block all public access enabled), and click the orange **Create bucket** button.

![Images](images/3bucket_name_region.jpeg)

### Step 1.2: Create Data Lake Folder Structure

Now, we will create the logical zones inside your new bucket.

1. In the S3 Buckets list, click directly on the **name** of your newly created bucket (e.g., `dq-datalake-...`) to open it. Ensure you are on the **Objects** tab.

![Images](images/click_s3_bucket.png)

2. Click the **Create folder** button.

![Images](images/create_folder_button.png)

3. In the **Folder name** field, type exactly: `raw`

![Images](images/type_raw.png)

4. Scroll to the bottom and click the **Create folder** button.

![Images](images/create_raw_folder_dq.png)

5. Repeat steps 2-4 three more times to create the following top-level folders. You should now have four folders in your bucket:
   - `curated`
   - `quarantine`
   - `athena-results`

![Images](images/create_three_folders_dq.png)

6. Next, we will create partition subfolders inside the raw zone. Click directly on the `raw` folder to navigate inside it.

![Images](images/open_raw_folder.png)

7. Click the **Create folder** button.

![Images](images/raw_create_folder.png)

8. In the **Folder name** field, type exactly: `date=2026-05-10`

![Images](images/date1_name.png)

9. Scroll down and click **Create folder**.

![Images](images/date1_create_folder.png)

10. Click the **Create folder** button again.

![Images](images/click_create_folder_again.png)

11. In the **Folder name** field, type exactly: `date=2026-05-11`

![Images](images/date2_name.png)

12. Scroll down and click **Create folder**.

![Images](images/date2_create.png)
![Images](images/folders_created.png)

### Step 1.3: Upload Project Files via AWS Console

We will now load the sample data from your local desktop into the respective S3 raw partitions.

#### 1. Upload the Clean Data

1. Ensure you are inside the `raw` folder. Click directly on the `date=2026-05-10` folder to open it.

![Images](images/open_date1.png)

2. Click the orange **Upload** button.

![Images](images/date1_upload.png)

3. Click the **Add files** button. A file browser window will open.

![Images](images/date1_add_file.png)

4. Navigate to your `~/Desktop/Project/` folder, select `transactions_20260510.csv`, and click **Open**.

![Images](images/open_csv.png)

5. Scroll to the very bottom of the AWS screen and click the orange **Upload** button.

![Images](images/csv_upload.png)

6. Wait for the green "Upload succeeded" banner, then click the **Close** button in the top right.

![Images](images/click_close.png)

#### 2. Upload the Corrupted Data

1. Look at the breadcrumb trail at the top of the S3 console (e.g., `Amazon S3 > Buckets > dq-datalake-... > raw > date=2026-05-10`). Click on **raw** to go back up one level.

![Images](images/go_back_raw.png)

2. Click directly on the `date=2026-05-11` folder to open it.

![Images](images/open_date2.png)

3. Click the orange **Upload** button, then click **Add files**.

![Images](images/date2_upload_button.png)

4. Navigate to your `~/Desktop/Project/` folder, select `transactions_20260511_corrupted.csv`, and click **Open**.

![Images](images/date2_csv_open.png)

5. Scroll to the bottom and click **Upload**.
6. Wait for the success message, then click **Close**.

![Images](images/date2_close.png)

### Step 1.4: Proactive IAM Role Creation

Before we configure Glue, we must grant it permission to access our newly created S3 bucket and to write logs and metrics to CloudWatch.

1. In the AWS Console search bar at the very top, type **IAM** and select it from the dropdown.

![Images](images/select_iam.png)

2. On the left-hand navigation menu, click on **Roles**.

![Images](images/click_roles.png)

3. Click the orange **Create role** button on the right side.

![Images](images/create_role_button.png)

4. Under **Trusted entity type**, ensure **AWS service** is selected.

![Images](images/trusted_entity.png)

5. Under the **Service or use case** dropdown, select **Glue**. Look below and select the **Glue** radio button that appears.

![Images](images/trusted_entity.png)

6. Click the **Next** button in the bottom right corner.

![Images](images/select_glue.png)

7. You are now on the "Add permissions" screen. You need to attach three specific policies.
   - Click into the **Search** box, type `AmazonS3FullAccess`, and press Enter. Click the **checkbox** next to the policy name in the list below.

![Images](images/s3full_access.png)

- Clear the search box. Type `AWSGlueServiceRole` and press Enter. Click the **checkbox** next to the policy name.

![Images](images/glue_service_role.png)

- Clear the search box again. Type `CloudWatchFullAccess` and press Enter. Click the **checkbox** next to the policy name.

![Images](images/cloudwatch_access.png)

8. Look at the top right of the permissions list. It should say "3 policies selected". Click the **Next** button at the bottom right.

![Images](images/3policies_attached.png)

9. In the **Role name** field at the top, type exactly: `GlueDataQualityAdminRole`

![Images](images/role_name.png)

10. Scroll to the very bottom of the page and click the blue **Create role** button.

![Images](images/iam_name_and_create_role_dq.png)

---

# Activity 2: Catalog Raw Data with Glue

**Purpose of this Activity:** AWS Glue needs a metadata table to understand the schema and partitions of our incoming S3 transactions before the ETL job can process them. We will create a database and use a crawler to scan and register our raw CSV files.

### Step 2.1: Create the Glue Database

1. Search for **Glue** in the top AWS Console search bar and select **AWS Glue**.

![Images](images/select_glue_service.png)

2. Look at the left navigation pane. Under the **Data Catalog** section, click on **Databases**.
3. Click the orange **Add database** button.

![Images](images/add_database_button.png)

4. In the **Name** field, type exactly: `sales_db`

5. Click the orange **Create database** button at the bottom right.

![Images](images/name_create_db.png)

### Step 2.2: Crawl the Raw Zone

1. Look back at the left navigation pane. Under the **Data Catalog** section, click on **Crawlers**.
2. Click the orange **Create crawler** button.

![Images](images/crawler_create_button.png)

3. On the Set crawler properties page, type `crawler_raw_transactions` in the **Name** field. Click **Next**.

![Images](images/crawler_name.png)

4. On the Choose data sources and classifiers page, click the **Add a data source** button. A side panel will open.

![Images](images/add_datasource.png)

- Ensure **Data source** is set to **S3**.
- Under **S3 path**, click the **Browse S3** button.

![Images](images/s3_path.png)

- A window appears. Click the bucket (`dq-datalake-...`).

![Images](images/open_s3_bucket.png)

- Then click the radio button next to the `raw/` folder. Click the **Choose** button.

![Images](images/choose_raw.png)

- Click the orange **Add an S3 data source** button at the bottom of the side panel.

![Images](images/add_s3_data_source.png)

5. You should now see your S3 path listed. Click **Next**.

![Images](images/listed_s3_path.png)

6. On the Configure security settings page, under **Existing IAM role**, click the dropdown and select the exact role you just created: `GlueDataQualityAdminRole`. Click **Next**.

![Images](images/select_role.png)

7. On the Set output and scheduling page, under **Target database**, click the dropdown and select `sales_db`. Scroll down and click **Next**.

![Images](images/sales_db_selected.png)
![Images](images/db_next.png)

8. Review your settings on the final page and click the orange **Create crawler** button.

![Images](images/create_crawler.png)

9. Return to the Crawlers list. Select the checkbox next to `crawler_raw_transactions`, and click the **Run** button near the top right.

![Images](images/run_crawler.png)

10. The state will change to _Starting_, then _Running_. Wait roughly 1-2 minutes for the state to return to _Ready_.

![Images](images/run_completed.png)

11. To verify success, go to the left navigation pane, click the **Tables** link under it. You should see a new table named `raw` automatically created!

![Images](images/table_created.png)

---

# Activity 3: Build the Glue ETL Pipeline with Data Quality Gates

**Purpose of this Activity:** This is where the core logic occurs. You will author a PySpark script that evaluates incoming data using strict Data Quality Definition Language (DQDL) rules. Utilizing row-level outcomes, the pipeline will physically separate valid data into the Curated S3 bucket and drop invalid data into the Quarantine S3 bucket.

### Step 3.1: Configure the Glue ETL Job

1. In the AWS Glue Console left navigation pane, scroll down to the **Data Integration and ETL** section and click on **ETL jobs**.
2. In the center of the screen, click on the **Visual ETL** card (or Spark script editor if prompted).

![Images](images/etl_jobs.png)

3. You will see a visual canvas. Look directly above the canvas and click on the **Script** tab.
4. The script editor will be locked. Look to the right side of the screen and click the **Edit script** button. A warning box will appear; click **Confirm** to unlock it.

![Images](images/unlock_script.png)

5. Now, click on the **Job details** tab located at the top center of the screen. We need to configure the job environment:
   - **Name:** Type `transactions_quality_gate_job`

![Images](images/job_name.png)

- **IAM Role:** Click the dropdown and select `GlueDataQualityAdminRole`.

![Images](images/job_iam.png)

- **Worker type:** Leave as G.1X.
- **Requested number of workers:** Change the default value to `2` (to save lab resources).

![Images](images/number_worker.png)

- **Job bookmark:** Scroll down to find this option. Click the dropdown and ensure it is set to **Disable** (we want to process all partitions simultaneously to witness the data routing split in action).

![Images](images/bookmark_disabled.png)

### Step 3.2: Writing the PySpark Data Quality Code

1. Navigate back to the **Script** tab at the top.
2. Click inside the code editor box. Select all the default text (`Ctrl+A` or `Cmd+A`) and hit **Delete** or **Backspace** to clear the editor completely. It must be totally blank.

![Images](images/empty_script.png)

3. Copy the entire block of Python code below and paste it into the editor.

**Code Explanation:**

- **DQDL Rules (`dq_rules`):** Defines enterprise data quality requirements natively. `IsComplete` ensures no null values exist for the specified columns, and `IsUnique` verifies there are no duplicate transaction IDs.
- **`EvaluateDataQuality().process_rows` & Routing:** Evaluates the dataset and enables CloudWatch metrics publishing. It attaches a `rowLevelOutcomes` array to every record, allowing the script to cleanly filter "Passed" records to `curated_path` and "Failed" records to `quarantine_path`.

**CRITICAL STEP:** Look at lines 17 and 18 in the code editor. You MUST replace `dq-datalake-your-unique-id` with your actual, exact S3 bucket name, or the job will fail.

![Images](images/replace_bucket_name.png)

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

# Initialize contexts
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# WARNING: Replace 'dq-datalake-your-unique-id' with your actual S3 bucket name
curated_path = "s3://dq-datalake-your-unique-id/curated/"
quarantine_path = "s3://dq-datalake-your-unique-id/quarantine/"

# Read raw data from the Glue Catalog
raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="sales_db",
    table_name="raw",
    transformation_ctx="raw_dyf"
)

# Define Data Quality Definition Language (DQDL) Rules
dq_rules = (
    "Rules = [\n"
    "    IsComplete \"transaction_id\",\n"
    "    IsUnique \"transaction_id\",\n"
    "    IsComplete \"amount\"\n"
    "]"
)

# Apply the Data Quality Evaluation with Row-Level Routing and CloudWatch Publishing Enabled
dq_evaluation = EvaluateDataQuality().process_rows(
    frame=raw_dyf,
    ruleset=dq_rules,
    publishing_options={
        "dataQualityEvaluationContext": "transactions_quality_eval",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"}
)

# Extract row-level outcomes to separate Passed vs. Failed records
outcomes_dyf = dq_evaluation.select("rowLevelOutcomes")

# Filter Good Data (Passed) and clean up temporary DQ columns
good_records = Filter.apply(frame=outcomes_dyf, f=lambda x: x["DataQualityEvaluationResult"] == "Passed")
clean_good_records = DropFields.apply(
    frame=good_records,
    paths=["DataQualityRulesPass", "DataQualityRulesFail", "DataQualityRulesSkip", "DataQualityEvaluationResult"]
)

# Filter Bad Data (Failed) and clean up temporary DQ columns
bad_records = Filter.apply(frame=outcomes_dyf, f=lambda x: x["DataQualityEvaluationResult"] == "Failed")
clean_bad_records = DropFields.apply(
    frame=bad_records,
    paths=["DataQualityRulesPass", "DataQualityRulesFail", "DataQualityRulesSkip", "DataQualityEvaluationResult"]
)

# Route Good Records to Curated Zone as partitioned Parquet
if clean_good_records.count() > 0:
    glueContext.write_dynamic_frame.from_options(
        frame=clean_good_records,
        connection_type="s3",
        format="glueparquet",
        connection_options={"path": curated_path, "partitionKeys": ["date"]}
    )

# Route Bad Records to Quarantine Zone as partitioned Parquet
if clean_bad_records.count() > 0:
    glueContext.write_dynamic_frame.from_options(
        frame=clean_bad_records,
        connection_type="s3",
        format="glueparquet",
        connection_options={"path": quarantine_path, "partitionKeys": ["date"]}
    )

job.commit()
```

4. Once the paths are updated, look at the top right corner of the AWS Console and click the **Save** button.

![Images](images/click_save.png)

### Step 3.3: Run the Pipeline and Verify Data Routing

1. After saving, click the orange **Run** button (located right next to the Save button) to begin the pipeline execution.

![Images](images/click_run_button.png)

2. Navigate to the **Runs** tab (located near the top center, next to Job details) to monitor the execution. Wait until the Run Status updates from _Running_ to **Succeeded**. (This usually takes 2-3 minutes).

![Images](images/run_succeed.png)

3. **Verify Physical Routing in S3:**
   - Leave this tab open. Right-click on the AWS logo at the top left of the screen and open a new tab. In the new tab, search for and open **S3**.

![Images](images/open_new_window.png)

- Click on your bucket (`dq-datalake-...`).

![Images](images/click_your_bucket.png)

- Click the **`curated`** folder. You should see a partition folder for `date=2026-05-10/` (and potentially a few clean records from May 11). Click into it, and you will see newly generated `.parquet` files. These are your validated, trusted records.

![Images](images/click_curated.png)
![Images](images/curated_date1.png)
![Images](images/parquet_files.png)

- Go back to the root of your bucket and click the **`quarantine`** folder. You will see a partition populated with `.parquet` files. These are the records that explicitly failed your DQ rules (the duplicate and null transaction IDs), safely isolated from downstream analytics!

![Images](images/click_quarantine.png)
![Images](images/quarantine_date.png)
![Images](images/quarantine_parquet.png)

---

# Activity 4: Monitor Quality Metrics via CloudWatch

**Purpose of this Activity:** Because we explicitly set `enableDataQualityCloudWatchMetrics: True` in our PySpark code, AWS Glue automatically emitted telemetry regarding the pass/fail rate of our dataset. We will now visualize this in Amazon CloudWatch for enterprise observability.

### Step 4.1: Navigate to CloudWatch Metrics

1. In the AWS Management Console search bar at the very top, type **CloudWatch** and select it from the dropdown.

![Images](images/open_cloudwatch.png)

2. Look at the left navigation pane. Scroll down to the **Metrics** section, click to expand it, and then click on **All metrics**.

![Images](images/click_all_metrics.png)

### Step 4.2: View Glue Data Quality Metrics

1. In the center of the screen under the "Custom namespaces" section, look for a card labeled **`Glue Data Quality`** and click on it. _(Note: If you do not see it immediately, ensure your AWS region in the top right is still set to N. Virginia)._

![Images](images/glue_data_quality.png)

2. Based on your pipeline's telemetry grouping, click on the box labeled **`EvaluationContext, JobName`** to drill down into the specific metrics for your job.

![Images](images/click_jobname.png)

3. You will now see a list of metrics logged that correspond to the overall evaluation context we defined in our script:
   - `glue.data.quality.rules.passed`
   - `glue.data.quality.rules.failed`

4. Click the **checkboxes** next to these metric names in the list. Look at the visual graph generated at the top of the screen; you will see distinct data points (or lines) representing the exact volume of rules that passed and failed against your defined quality gates.

![Images](images/select_checkboxes.png)

---

# Activity 5: Analyze Curated Data with Amazon Athena

**Purpose of this Activity:** The ultimate goal of a data lake is to make data accessible for analytics. We will use a Glue Crawler to catalog our trusted, Curated Parquet files and then query them using serverless SQL via Amazon Athena.

### Step 5.1: Catalog the Curated Data

1. Return to the **AWS Glue** console. In the left navigation pane, click **Crawlers** and then **Create crawler**.

![Images](images/create_crawler_again.png)

2. **Name:** Type `crawler_curated_transactions`. Click **Next**.

![Images](images/crawler_name_again.png)

3. **Data Source:** Click **Add a data source**.

![Images](images/add_datasource_again.png)

- Ensure **Data source** is set to **S3**.
- Browse S3 and select your `curated/` folder inside your `dq-datalake-...` bucket. Click **Choose**.
- Click **Add an S3 data source**. Click **Next**.

![Images](images/choose_curated.png)

4. **IAM Role:** Select your existing `GlueDataQualityAdminRole`. Click **Next**.

![Images](images/glue_role.png)

5. **Output configuration:** Select `sales_db` as the target database. Click **Next**.

![Images](images/output_config.png)

6. Review and click **Create crawler**.

![Images](images/new_crawler_create.png)

7. Select the new `crawler_curated_transactions` and click **Run**. Wait 1-2 minutes until the status returns to _Ready_.

![Images](images/select_crawler_run.png)

8. Go to the left navigation pane, click the **Tables** link under it. You should see a new table named `curated`.

![Images](images/curated_table.png)

### Step 5.2: Set Up Athena Query Location

_Athena queries will fail if an S3 query result location is not configured._

1. In the AWS Console search bar, type **Athena** and select it.

![Images](images/select_athena.png)

2. On the Athena welcome screen, select **Query your data in Athena console** click **Launch query editor**.

![Images](images/launch_query_editor.png)

3. You will see a blue banner asking to set up a query result location. Click the **Edit settings** button inside that banner, then click on **Manage**.

![Images](images/edit_settings.png)
![Images](images/manage_button.png)

4. Under **Query result location**, click **Browse S3**, select your bucket (`dq-datalake-...`), and select the `athena-results/` folder we created in Activity 1. Click **Choose**.

![Images](images/browse_s3.png)

5. Scroll down and click **Save**.

![Images](images/save_path.png)

### Step 5.3: Execute SQL Query on Trusted Data

1. Click the **Editor** tab at the top.
2. Ensure the **Database** dropdown on the left is set to `sales_db`.

![Images](images/sales_db_select.png)

3. In the main query window, paste the following SQL code:

**Code Explanation:**

- This query selects data specifically from our trusted `curated` table.
- It leverages partition pruning (`WHERE date = '2026-05-10'`) to only scan the necessary folders, reducing costs.
- We aggregate the `amount` column to find the total trusted revenue for the day.

```sql
SELECT
    date,
    COUNT(transaction_id) as total_valid_transactions,
    SUM(CAST(amount AS DOUBLE)) as total_daily_revenue
FROM curated
WHERE date = '2026-05-10'
GROUP BY date;
```

4. Click the blue **Run** button. View your results in the pane below to see your clean, trusted analytics data!

![Images](images/run_query.png)

---

# Activity 6: Configure Automated Alerts via SNS and CloudWatch

**Purpose of this Activity:** In a real-world production environment, you need to be notified instantly when data quality drops below acceptable thresholds. We will create an Amazon SNS Topic to send an email and trigger a CloudWatch Alarm based on the metrics emitted by our Glue job.

### Step 6.1: Create an SNS Topic and Subscribe

1. In the AWS Console search bar, type **SNS** and select **Simple Notification Service**.

![Images](images/select_sns.png)

2. On the left menu, click **Topics**, then click the orange **Create topic** button.

![Images](images/create_topic.png)

3. Under **Type**, select **Standard**.
4. **Name:** Type `DataQualityAlerts`. Scroll to the bottom and click **Create topic**.

![Images](images/sns_topic_name.png)

![Images](images/sns_create_button.png)

5. You are now on the Topic details page. Click the orange **Create subscription** button.

![Images](images/create_subscription.png)

6. **Protocol:** Select **Email** from the dropdown.

![Images](images/select_email.png)

7. **Endpoint:** Type in your actual email address.

![Images](images/email_endpoint.png)

8. Click **Create subscription**.

![Images](images/create_sub_button.png)

9. _Important:_ Check your email inbox. You will receive an "AWS Notification - Subscription Confirmation" email. Open it and click the **Confirm subscription** link.

![Images](images/email_inbox.png)
![Images](images/confirm_subcription.png)

### Step 6.2: Create a CloudWatch Alarm

1. Navigate back to **CloudWatch** via the top search bar.

![Images](images/select_cloudwatch.png)

2. On the left menu, click **Alarms**.
3. Click the orange **Create alarm** button.

![Images](images/create_alarm.png)

4. Click **Select metric**.

![Images](images/select_metric.png)

5. Navigate to **Glue Data Quality** > **JobName**. Find the row for your `transactions_quality_gate_job` where the metric is the failing rule count (e.g., records failing `IsUnique`). Check the box and click **Select metric**.

![Images](images/metric_glue_data_quality.png)
![Images](images/jobname.png)
![Images](images/failed_metric.png)

6. Scroll down Under the **Conditions** section:
   - Threshold type: **Static**
   - Whenever [Metric] is...: **Greater/Equal**
   - than: `1` (Meaning if even 1 record fails our quality gate, trigger the alarm).
7. Click **Next**.

![Images](images/conditions.png)

8. On the Configure actions page, under **Send a notification to the following SNS topic**, select the `DataQualityAlerts` topic you just created. Scroll down click **Next**.

![Images](images/notification.png)

9. **Alarm name:** Type `High-Quarantine-Volume-Alarm`. Click **Next**.

![Images](images/alarm_name.png)

10. Review your settings and click **Create alarm**.

![Images](images/create_alarm_button.png)

_Enterprise Note:_ The next time your Glue pipeline runs and shunts corrupted data into the quarantine bucket, this CloudWatch metric will spike, breaching the threshold of 1, and immediately firing an email alert to your data engineering team!

---

## 🎓 Conclusion

This guided project demonstrated how to enforce rigorous data reliability natively within an AWS pipeline. By combining AWS Glue ETL with integrated Data Quality (DQDL) rules, you successfully built an automated, enterprise data gatekeeper.

You practiced real-world data engineering patterns, including:

- Architecting an S3 Data Lake with dedicated logical routing zones (**Raw, Curated, and Quarantine**).
- Applying proactive **IAM security policies** to ensure seamless pipeline permissions.
- Writing native **Data Quality Definition Language (DQDL)** rules to automatically govern schema integrity.
- Utilizing **Row-Level Outcomes** in PySpark to logically bifurcate passing and failing data streams during an active job.
- Querying optimized, trusted data seamlessly using **Amazon Athena**.
- Emitting pipeline health telemetry to **Amazon CloudWatch** and establishing automated email alerting via **Amazon SNS**.

These are advanced, highly sought-after skills for any modern AWS Data Engineer tasked with maintaining trusted data lakes!
