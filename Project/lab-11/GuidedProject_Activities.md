# Title: Glue Crawler, Data Catalog & Athena Analytics

---

## 🌟 Overview: What is the purpose of this Lab?

Welcome to your AWS Data Lake Analytics project! In modern data architectures, making data discoverable, queryable, and governed is essential for analytics at scale. This lab will guide you through the foundational steps of provisioning a production-grade data lake on Amazon S3, cataloging data with AWS Glue, and running analytics using Amazon Athena.

**The Purpose of this Lab:**
You are part of a data team managing a data lake on S3. Your task is to make the data queryable by creating metadata using Glue Crawlers, adjust schemas as needed, and run Athena queries to analyze the data. You will also observe how partitioning improves query performance and reduces cost.

By the end of this project, you will have:

- Designed a three-zone data lake (Raw/Bronze, Curated/Silver, Consumption/Gold) on S3.
- Cataloged partitioned CSV and JSONL data using Glue Crawlers and the Glue Data Catalog.
- Queried the data efficiently with Athena, leveraging partition pruning and SQL JOINs.
- Demonstrated schema evolution by adding a new column in a new data file.

### Learning Path:

1. **Authentication:** Configure your AWS Console access and credentials.
2. **S3 Data Lake Foundations:** Design and provision a production data lake with best practices.
3. **Metadata & Analytics:** Use Glue Crawlers and the Glue Data Catalog to make data queryable in Athena.

---

## 📊 Dataset / Knowledge Source Used

The following files are provided in the `~/Desktop/Project/` folder on your lab desktop. You will upload these to S3 as part of Activity 1.

| File                    | Zone                       | Description                                                                 |
| ----------------------- | -------------------------- | --------------------------------------------------------------------------- |
| `sales_20240601.csv`    | `curated/date=2024-06-01/` | 200 sales transactions for June 1, 2024                                     |
| `sales_20240602.csv`    | `curated/date=2024-06-02/` | 200 sales transactions for June 2, 2024                                     |
| `sales_20240603_v2.csv` | `curated/date=2024-06-03/` | 200 transactions with new `discount_applied` column (schema evolution demo) |
| `store_mapping.json`    | `raw/reference/`           | 50 customer reference records in JSONL format (for SQL JOINs)               |

> **Note on JSONL format:** `store_mapping.json` uses one JSON object per line (not a JSON array). This is the format Athena natively reads using the `org.apache.hive.hcatalog.data.JsonSerDe` SerDe. Do not reformat it as a standard JSON array.

---

## 🛠️ Prerequisites & AWS Setup

Ensure the following are available:

- An **AWS account** with permissions for S3, Glue, and Athena.
- Access to the AWS Console (Lab credentials provided).
- The local data files located at `~/Desktop/Project/`:
  - `~/Desktop/Project/sales_20240601.csv`
  - `~/Desktop/Project/sales_20240602.csv`
  - `~/Desktop/Project/sales_20240603_v2.csv`
  - `~/Desktop/Project/store_mapping.json`
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

![Images](images/click_management_tab.png)

- Click **Create lifecycle rule**.

![Images](images/create_lifecycle.png)

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
![Images](images/create_rule.png)

---

### Step 1.2: Create Data Lake Folder Structure

**Purpose:** To organize your data lake into logical zones for raw, curated, and consumption-ready data following the Medallion Architecture.

#### Phase A: Navigate to Your Bucket

1. In the AWS Management Console, ensure you are in the **Amazon S3** service.
2. Under the **Buckets** list, locate and click directly on the name of the bucket you just created (e.g., `datalake-<your-unique-id>`).

![Images](images/select_your_bucket.png)

3. Ensure you are viewing the **Objects** tab.

![Images](images/object_tab.png)

#### Phase B: Create Top-Level Folders

You will create four main folders at the root level of your bucket.

1. Click the **Create folder** button.

![Images](images/craete_folder.png)

2. In the **Folder name** field, type `raw`.

![Images](images/folder_name.png)

3. Scroll down and click the **Create folder** button at the bottom of the page.

![Images](images/create_folder_button.png)

4. Repeat steps 1-3 three more times to create the remaining top-level folders:
   - `curated`
   - `consumption`
   - `athena-results`

![Images](images/folders_created.png)

#### Phase C: Create Hive-Partitioned Subfolders in 'curated'

Now, you will navigate _inside_ the curated folder to create subfolders.

1. From the **Objects** tab, click directly on the **`curated`** folder name to open it. You should now see that the folder is empty.

![Images](images/select_curated.png)
![Images](images/empty_curated.png)

2. **Create the first subfolder:**
   - Click the **Create folder** button.

![Images](images/curated_create_folder.png)

- In the **Folder name** field, type exactly: `date=2024-06-01`
  _(Note: This `key=value` naming convention is called Hive partitioning. It is required so AWS Glue can detect your partitions automatically)._

![Images](images/curated_date_2024_06_01_folder.png)

- Scroll down to the bottom and click **Create folder**.

![Images](images/curated_create_folder_button.png)

3. **Create the second subfolder:**
   - You will be returned to the inside of the `curated` folder. Click the **Create folder** button again.

![Images](images/curated_create_folder_again.png)

- In the **Folder name** field, type exactly: `date=2024-06-02`

![Images](images/curated_date_2024_06_02.png)

- Scroll down to the bottom and click **Create folder**.

![Images](images/curated_create_folder_button.png)

4. Repeat the same process for creating `date=2024-06-03` folder in `curated` folder.

![Images](images/curated_folders_created.png)

5. **Return to the main bucket level:**
   - Look at the breadcrumb navigation links near the top left of the screen (it will look like: _Amazon S3 > Buckets > your-bucket-name > curated_).
   - Click directly on your **bucket name** in that breadcrumb trail to return to the main root level where your top-level folders are.

![Images](images/return_bucket.png)

#### Phase D: Create Reference Subfolder in 'raw'

1. From the root level of your bucket, click on the **`raw`** folder name to open it.

![Images](images/click_folder_raw.png)

2. Click the **Create folder** button.

![Images](images/raw_create_folder_button.png)

3. In the **Folder name** field, type `reference`.

![Images](images/raw_folder_name.png)

4. Scroll down and Click **Create folder**.

### Step 1.3: Upload Project Files via AWS Console

**Purpose:** To load the provided sample data from your local `~/Desktop/Project/` folder into the correct data lake zones in S3.

#### 1. Upload the June 1 Sales Data

1. In the S3 console, navigate inside your bucket, click the `curated` folder, and then click into the `date=2024-06-01` subfolder.

![Images](images/date1_upload_data.png)

2. Click the orange **Upload** button.

![Images](images/date1_click_upload.png)

3. Click the **Add files** button.

![Images](images/date1_addfiles_button.png)

4. Browse your local computer to `~/Desktop/Project/`, select `sales_20240601.csv`, and click **Open**.

![Images](images/date1_upload_csv.png)

5. Scroll to the bottom of the page and click the orange **Upload** button.

![Images](images/date1_click_upload_file.png)

6. Once the green "Upload succeeded" banner appears, click the **Close** button in the top right.

![Images](images/date1_upload_status.png)

#### 2. Upload the June 2 Sales Data

1. Look at the breadcrumb links at the top (e.g., _Amazon S3 > Buckets > your-bucket > curated > date=2024-06-01_). Click on **`curated`** to go back up one level.

![Images](images/back_to_curated.png)

2. Click into the `date=2024-06-02` folder.

![Images](images/raw_date2_folder.png)

3. Click **Upload**, then **Add files**.

![Images](images/date2_click_upload.png)

4. Select `sales_20240602.csv` from your Project folder.

![Images](images/date2_open_csv_file.png)

5. Scroll down, click **Upload**, and then click **Close** once it succeeds.

![Images](images/date2_click_upload_csv.png)

#### 3. Upload the June 3 Sales Data

1. Use the breadcrumb links again to go back to the **`curated`** folder.
2. Click into the `date=2024-06-03` folder.

![Images](images/raw_date3.png)

3. Click **Upload**, then **Add files**.
4. Select `sales_20240603_v2.csv` from your Project folder.

![Images](images/date3_csv_open.png)

5. Scroll down, click **Upload**, and then click **Close** once it succeeds.

![Images](images/date3_close.png)

#### 4. Upload the Reference Data

1. Use the breadcrumb links to return to the root level of your bucket by clicking your **bucket name**.

![Images](images/back_to_bucket.png)

2. Click into the `raw` folder, and then click into the `reference` subfolder.

![Images](images/click_raw_folder.png)

3. Click **Upload**, then **Add files**.

![Images](images/reference_click_upload.png)

4. Select `store_mapping.json` from your Project folder.

![Images](images/select_json_file.png)

5. Scroll down, click **Upload**, and then click **Close** once it succeeds.

![Images](images/json_upload_sucess.png)

### Step 1.4: (Optional) Configure S3 Event Notifications

**Purpose:** To trigger Glue Crawlers or Lambda functions automatically when new data arrives.

1. In the S3 bucket **Properties** tab, scroll down to **Event notifications** and create an event for the `curated/` prefix.
2. Set the destination to trigger a Lambda function or an SQS queue designed to start your Glue Crawler.

---

# Activity 2: Catalog Data with Glue Crawler & Glue Data Catalog

**Purpose of this Activity:** To automate metadata discovery and schema management for your data lake using AWS Glue. You will create two crawlers — one for the partitioned sales CSV data and one for the JSONL reference data.

### Step 2.1: Create a Glue Database

1. In the AWS Console, search for **Glue** and select it.

![Images](images/select_glue.png)

2. In the left navigation pane, under **Data Catalog**, click **Databases** > **Add database**.

![Images](images/select_database.png)
![Images](images/click_add_database.png)

3. Name the database `datalake_db` and click **Create database**.

![Images](images/database_name_create.png)

---

### Step 2.2: Create a Glue Crawler for Sales Data (Curated Zone)

1. In the left navigation, under **Data Catalog**, click **Crawlers** > **Create crawler**.

![Images](images/select_crawlers.png)
![Images](images/create_crawler.png)

2. **Step 1: Set crawler properties**
   - Name: `crawler_sales_curated`. Click **Next**.

![Images](images/crawler_name.png)

3. **Step 2: Choose data sources and classifiers**
   - Click **Add a data source**.

![Images](images/add_resource_data.png)

- Data source: **S3**.
- S3 path: Browse to select `s3://<bucket>/curated/` (select the parent folder to detect partitions).
- Subsequent crawler runs: **Crawl all sub-folders**. Click **Add an S3 data source**. Click **Next**.

![Images](images/s3_data_source_crawl_subfolder.png)
![Images](images/crwaler_click_next.png)

4. **Step 3: Configure security settings (Create IAM Role)**
   - Under the **IAM role** section, look for the option to set up your role.
   - Click the **Create new IAM role** button.

![Images](images/create_new_IAM_role.png)

- A text field will appear that already contains the prefix `AWSGlueServiceRole-`.
- In that text box, type `DataLake` (so the full name becomes `AWSGlueServiceRole-DataLake`).

![Images](images/IAM_role_name.png)

- _(Note: By using this button, AWS automatically creates the role in the background, attaches the required `AWSGlueServiceRole` permissions, and generates a policy allowing Glue to read the specific S3 path you provided in the previous step)._
- Scroll down and click **Next**.

![Images](images/IAM_role_next_button.png)

5. **Step 4: Set output and scheduling**
   - Target database: Select `datalake_db`.
   - Table name prefix: Enter `sales_`.
   - Crawler schedule: **On demand**. Click **Next**.

![Images](images/crawler_output_config.png)

6. Review the settings and click **Create crawler**.

![Images](images/click_create_crawler.png)

---

### Step 2.3: Create a Custom JSON Classifier and Reference Crawler

First, create the custom classifier for the JSONL format:

1. In the left navigation, under **Data Catalog**, click **Classifiers** > **Add classifier**.

![Images](images/add_clasifier.png)

2. Classifier name: `jsonl_classifier`.
3. Classifier type: **JSON**.
4. JSON path: `$[*]`
5. Click **Create**.

![Images](images/clasifier_details.png)

Now, build the crawler:

1. Go back to **Crawlers** > **Create crawler**.

![Images](images/create_crawler_again.png)

2. **Step 1:** Name: `crawler_store_mapping`. Click **Next**.

![Images](images/crawler_name_again.png)

3. **Step 2:**
   - Click **Add a data source**. Data source: **S3**. Path: `s3://<bucket>/raw/reference/`. Add the data source and click **Next**.

![Images](images/add_s3_data_source.png)

- Under _Custom classifiers_, click **Add** and select `jsonl_classifier`.

![Images](images/json_clasifier.png)

4. **Step 3:** click the **Create new IAM role** button again. In the text box (which has **AWSGlueServiceRole-**), type `ReferenceData` (so the full name is AWSGlueServiceRole-ReferenceData).
   ![Images](images/create_IAM_again.png)
   ![Images](images/crawler_create_IAM_role.png)

5. **Step 4:** Target database: `datalake_db`. Leave table prefix blank. Schedule: **On demand**. Click **Next**.

![Images](images/crawler_output_config_again.png)

6. Review and click **Create crawler**.

![Images](images/create_crawler_again_button.png)

---

### Step 2.4: Run Both Crawlers and Inspect the Catalog

1. In the Crawlers list, select both `crawler_sales_curated` and `crawler_store_mapping`, then click **Run**.

![Images](images/run_both_crawler.png)

2. Wait for the state to cycle through _Running_, _Stopping_, and back to _Ready_ (takes 1–2 minutes).

![Images](images/different_states_crawler.png)

3. Navigate to **Databases** > `datalake_db` > **Tables** and look for the two newly created tables:

- `sales_curated` — for the sales transactions
- `reference` (or `store_mapping`) — for the customer lookup data

![Images](images/click_dbs.png)

4. **Schema Observation for `sales_curated`:**
   - Click into the table and check that the following columns exist: `transaction_id` (string), `customer_id` (string), `product_category` (string), `amount` (double)
   - Ensure the partition key is recognized: `date` (string) — mapped from the folder names.

![Images](images/curated_sales_table.png)

5. **Schema Observation for the reference table:**
   - Click into the table and check that these columns exist: `customer_id` (string), `region` (string), `loyalty_tier` (string).

![Images](images/reference_table.png)

---

### Step 2.5: Demonstrate Schema Evolution

**Purpose:** The file `sales_20240603_v2.csv` contains an extra column `discount_applied` (boolean) that was not present in the June 1 and June 2 files. This simulates a real-world scenario where upstream systems add new fields.

1. Inspect the schema of the `sales_curated` table in the Data Catalog.
2. Observe that the crawler automatically detected the `discount_applied` column from the June 3 partition and appended it to the table schema. When queried, partitions without this data will return `null` for this field.

---

# Activity 3: Query Data with Amazon Athena

**Purpose of this Activity:** To analyze your data lake using serverless SQL queries, perform SQL JOINs across tables, and observe the impact of partitioning on performance and cost.

### Step 3.1: Set Up Athena

1. In the AWS Console, search for **Athena** and select it.

![Images](images/search_athena.png)

2. On the Athena welcome screen, under the **Get started** section, ensure the radio button for **Query your data in Athena console** is selected, and then click the orange **Launch query editor** button. Before running your first query, you must set the result location.

![Images](images/launch_query_editor.png)

3. Click the **Settings** tab at the top of the Query editor, then click **Manage**.

![Images](images/edit_settiings.png)

4. Set the **Query result location and encryption** to point to your S3 bucket:

![Images](images/manage_query.png)

```text
s3://<bucket-name>/athena-results/
```

Click **Save**.

5. Back in the **Editor** tab, ensure the Data source is set to `AwsDataCatalog` and select `datalake_db` from the Database dropdown.

![Images](images/athena_editor_config.png)

---

### Step 3.2: Explore the Sales Data

Run a basic row count query to confirm the data loaded correctly:
Copy the below code and run it on Editor

```sql
SELECT COUNT(*) AS total_rows
FROM sales_curated;
```

![Images](images/sql1_command_run.png)
![Images](images/sql1_command_result.png)

Preview the first 10 rows:
Replace with the below code and run it on Editor

```sql
SELECT *
FROM sales_curated
LIMIT 10;
```

![Images](images/sql2_command_result.png)

---

### Step 3.3: Partition Pruning for Cost Optimization

**Full table scan (no filter):**
Replace with the below code and run it on Editor

```sql
SELECT COUNT(*) FROM sales_curated;
```

![Images](images/sql3_command_result.png)

Note the **Data scanned** value in the query results panel.

**Partition-filtered query:**
Replace with the below code and run it on Editor

```sql
SELECT COUNT(*) FROM sales_curated
WHERE date = '2024-06-01';
```

![Images](images/sql4_command_result.png)

Compare the **Data scanned** value. Because Athena only reads the `date=2024-06-01/` folder, the bytes scanned will be roughly one-third of the full scan — directly reducing query cost.

---

### Step 3.4: SQL JOIN — Enrich Sales Data with Customer Regions

Use the reference table to join customer region and loyalty tier into your sales queries:
Replace with the below code and run it on Editor

```sql
SELECT
    s.transaction_id,
    s.customer_id,
    s.product_category,
    s.amount,
    s.date,
    m.region,
    m.loyalty_tier
FROM sales_curated AS s
JOIN reference AS m
    ON s.customer_id = m.customer_id
WHERE s.date = '2024-06-01'
ORDER BY s.amount DESC
LIMIT 20;
```

![Images](images/sql5_command_result.png)

---

### Step 3.5: Aggregation — Revenue by Region and Category

Replace with the below code and run it on Editor

```sql
SELECT
    m.region,
    s.product_category,
    COUNT(*)          AS transaction_count,
    ROUND(SUM(s.amount), 2) AS total_revenue,
    ROUND(AVG(s.amount), 2) AS avg_order_value
FROM sales_curated AS s
JOIN reference AS m
    ON s.customer_id = m.customer_id
GROUP BY m.region, s.product_category
ORDER BY total_revenue DESC;
```

![Images](images/sql6_command_result.png)

---

### Step 3.6: Schema Evolution — Query the Discount Column

The `discount_applied` column only exists in the `date=2024-06-03` partition. Query it directly:
Replace with the below code and run it on Editor

```sql
SELECT
    transaction_id,
    customer_id,
    amount,
    discount_applied
FROM sales_curated
WHERE date = '2024-06-03'
  AND discount_applied = true
ORDER BY amount DESC
LIMIT 10;
```

![Images](images/sql7_command_result.png)

For partitions without this column (June 1 and June 2), the returned values will be `NULL`. Calculate a summary to see this dynamic:
Replace with the below code and run it on Editor

```sql
SELECT
    date,
    COUNT(*)                                        AS total_transactions,
    COUNT(CASE WHEN discount_applied = true THEN 1 END) AS discounted_transactions
FROM sales_curated
GROUP BY date;
```

![Images](images/sql8_command_result.png)

---

### Step 3.7: Data Types and Schema Check

```sql
DESCRIBE sales_curated;
```

Check that the Glue Catalog types match these expectations:

- `transaction_id` → `string`
- `customer_id` → `string`
- `product_category` → `string`
- `amount` → `double`
- `discount_applied` → `boolean`
- `date` → `string` (partition key)

![Images](images/schema_check.png)

---

## 🎓 Conclusion

This guided project demonstrated how to design and provision a production-grade data lake on Amazon S3, automate metadata discovery with Glue Crawlers (for both CSV and JSONL formats), and analyze data efficiently with Athena using partition pruning and SQL JOINs. You practiced real-world patterns including Hive-style partitioning, schema evolution when new columns are added, and enriching transactional data with reference lookups — skills essential for any modern AWS Data Engineer.
