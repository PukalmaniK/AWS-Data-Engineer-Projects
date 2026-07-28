# Title: Redshift Ingestion, Distribution & Spectrum

---

## 🌟 Overview: What is the purpose of this Lab?

Welcome to your Amazon Redshift Data Warehousing project! In modern analytics architectures, your warehouse must store curated, conformed data in the most query-efficient layout possible **and** reach back into the data lake when raw or semi-structured data needs to be analyzed alongside it. This lab will guide you through building a production-grade Redshift workload using S3, IAM, the Glue Data Catalog, and Redshift Spectrum.

**The Purpose of this Lab:**
You are part of a data engineering team optimizing data warehouse performance for an e-commerce company. The business team needs fast analytical queries on order history but also wants to blend in raw clickstream events that currently live only in the data lake. Your task is to load curated dimensional data into Redshift using the **COPY command with a manifest file**, design tables with different **distribution styles (KEY, ALL, EVEN)** and a **compound sort key**, compare query plans with **EXPLAIN**, query S3 data directly using **Redshift Spectrum**, and finally export aggregated results back to S3 as **partitioned Parquet** via **UNLOAD**.

By the end of this project, you will have:

- Provisioned a Redshift Serverless namespace and workgroup with the correct IAM role for S3 and Glue access.
- Built a small star schema using deliberate distribution and sort key choices.
- Loaded data from S3 using the COPY command driven by a manifest file.
- Compared EXPLAIN plans across KEY, ALL, and EVEN distribution styles.
- Queried external S3 Parquet data through Redshift Spectrum using a Glue Data Catalog database.
- Exported aggregated results back to S3 as partitioned Parquet using UNLOAD.

### Learning Path:

1. **Authentication:** Configure your AWS Console access and credentials.
2. **S3 Warehouse Foundations:** Provision the S3 bucket, folder layout, and manifest file.
3. **IAM & Glue Setup:** Create a Spectrum-ready IAM role and Glue database, then crawl the lake.
4. **Redshift Serverless:** Stand up the namespace and workgroup.
5. **Modeling & Ingestion:** Design fact and dimension tables with distribution and sort keys, then COPY data in.
6. **Spectrum & UNLOAD:** Query the data lake from Redshift and export aggregates back as partitioned Parquet.

---

## Difficulty Level

Practitioner

---

## 📊 Dataset / Knowledge Source Used

The following files are provided in the `~/Desktop/Project/` folder on your lab desktop. You will upload these to S3 as part of Activity 1. We are using a small e-commerce analytics scenario so the lab stays fast and easy to verify.

**Local File Structure:**

```text
~/Desktop/Project/
├── customers.csv
├── products.csv
├── orders_part1.csv
├── orders_part2.csv
├── orders_manifest.json
└── web_events/
    ├── event_date=2024-06-01/events.parquet
    └── event_date=2024-06-02/events.parquet
```

| File                   | Zone                             | Description                                                                 |
| ---------------------- | -------------------------------- | --------------------------------------------------------------------------- |
| `customers.csv`        | `warehouse/customers/`           | 500 customer master records (customer_id, name, city, signup_date)          |
| `products.csv`         | `warehouse/products/`            | 200 product master records (product_id, product_name, category, price)      |
| `orders_part1.csv`     | `warehouse/orders/`              | 150 order transactions (first half of the dataset)                          |
| `orders_part2.csv`     | `warehouse/orders/`              | 150 order transactions (second half — referenced together via the manifest) |
| `orders_manifest.json` | `manifest/`                      | Manifest file listing the two order CSVs for the COPY command               |
| `web_events/*.parquet` | `lake/web_events/event_date=...` | 200 clickstream events across 2 partitions (for Spectrum querying)          |

> **Note on manifest format:** `orders_manifest.json` follows the Redshift COPY manifest schema with a top-level `entries` array. Each entry points at one of the order CSV files in S3 and is marked `"mandatory": true`.

---

## 🛠️ Prerequisites & AWS Setup

Ensure the following are available:

- An **AWS account** with permissions for S3, IAM, Glue, and Redshift.
- Access to the AWS Console (Lab credentials provided).
- The local data files located at `~/Desktop/Project/`:
  - `~/Desktop/Project/customers.csv`
  - `~/Desktop/Project/products.csv`
  - `~/Desktop/Project/orders_part1.csv`
  - `~/Desktop/Project/orders_part2.csv`
  - `~/Desktop/Project/orders_manifest.json`
  - `~/Desktop/Project/web_events/` (folder containing two partitioned Parquet files)
- No local Jupyter notebook is required; all activities are performed via the AWS Console and Redshift Query Editor V2.

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

# Activity 1: Provision the S3 Warehouse and Upload Source Files

**Purpose of this Activity:** To create the S3 bucket and folder layout that Redshift will use for COPY ingestion, Spectrum querying, manifest files, and UNLOAD output — and to upload every source file to its correct location before any Redshift work begins.

### Step 1.1: Create the S3 Bucket

1. In the AWS Management Console search bar at the very top of the screen, type **S3** and select **S3** from the Services dropdown menu.

![Images](images/1search_click_s3.png)

2. Click the orange **Create bucket** button on the right side of the screen.

![Images](images/2create_bucket.png)

3. **Bucket Name Configuration:**
   - Under **General configuration**, type a globally unique name in the **Bucket name** field, e.g., `redshift-warehouse-<your-unique-id>`.
   - Ensure the AWS Region is set to **US East (N. Virginia) us-east-1**.

4. Scroll all the way down to the bottom of the page, leaving all other default settings exactly as they are (Block all public access enabled), and click the orange **Create bucket** button.

![Images](images/3bucket_name_region.jpg)

### Step 1.2: Create the S3 Folder Layout

1. In the S3 Buckets list, click directly on the **name** of your newly created bucket (e.g., `redshift-warehouse-...`) to open it. Ensure you are on the **Objects** tab at the top.

![Images](images/select_your_bucket.png)

2. Click the white **Create folder** button on the right side of the objects list.

![Images](images/craete_folder.png)

3. In the **Folder name** field, type exactly: `warehouse`

4. Scroll to the bottom and click the orange **Create folder** button.

![Images](images/create_folder_button.png)

5. Repeat steps 2–4 three more times to create the following top-level folders. You should now have four folders in your bucket:
   - `lake`
   - `manifest`
   - `unload`

![Images](images/folders_created.png)

6. Click directly on the `warehouse` folder name to navigate inside it.

![Images](images/select_warehouse.png)

- Click **Create folder**,

![Images](images/warehouse_create_folder.png)

- Type `customers`, and click **Create folder**.

![Images](images/customer_folder.png)

7. Repeat to create two more subfolders inside `warehouse`:
   - `products`
   - `orders`

![Images](images/warehouse_subfolders.png)

8. Use the breadcrumb at the top (e.g., `Amazon S3 > Buckets > redshift-warehouse-... > warehouse`) and click on your bucket name to go back to the root.

![Images](images/back_to_root.png)

- Click into the `lake` folder.

![Images](images/select_lake.png)

- Inside `lake`, click **Create folder**,

![Images](images/lake_create_folder.png)

- Type `web_events`, and click **Create folder**.

![Images](images/lake_web_events_folder.png)

9. Click into the `web_events` folder,

![Images](images/lake_web_events_folder.png)

- then click **Create folder**, type exactly `event_date=2024-06-01`, and click **Create folder**.

![Images](images/date1_create_button.png)
![Images](images/web_event_create_folder.png)

- Repeat once more for creating folder named as `event_date=2024-06-02`.

![Images](images/lake_event_date_folders.png)

### Step 1.3: Upload the Warehouse CSV Files

1. Use the breadcrumb to return to your bucket root, click into `warehouse`, and then click into the `customers` folder.

![Images](images/select_warehouse.png)

2. Click the orange **Upload** button on the right.

![Images](images/date1_click_upload.png)

3. Click the **Add files** button. A file browser window will open.

![Images](images/date1_click_addfiles.png)

4. Navigate to your `~/Desktop/Project/` folder, select `customers.csv`, and click **Open**.

![Images](images/open_customer_csv.png)

5. Scroll to the very bottom of the AWS screen and click the orange **Upload** button.

![Images](images/customers_upload.png)

6. Wait for the green "Upload succeeded" banner, then click the **Close** button in the top right corner.

![Images](images/customers_close.png)

7. **Upload the Products File:**
   - At the top of the S3 console, use the breadcrumb trail to navigate back to the **`warehouse`** folder.

![Images](images/back_to_warehouse.png)

- Click directly on the **`products`** folder to open it.

![Images](images/click_product.png)

- Click the orange **Upload** button on the right side of the screen, then click the **Add files** button.

![Images](images/product_add_files.png)

- In the file browser window, locate and click on **`products.csv`** to select it, then click **Open**.

![Images](images/open_product_csv.png)

- Scroll to the bottom of the AWS page and click the orange **Upload** button.

![Images](images/upload_product.png)

- Once the green "Upload succeeded" banner appears, click the **Close** button in the top right corner.

![Images](images/products_uploaded.png)

8. **Upload the Orders Files:**
   - Use the breadcrumb at the top to navigate back to `warehouse`.

![Images](images/click_warehouse.png)

- Click into the `orders` folder.

![Images](images/click_order.png)

- Click the orange **Upload** button, then click **Add files**.

![Images](images/order_add_files.png)

- In the file browser, click once on `orders_part1.csv` to select it.
- Hold down the `Ctrl` key (or `Cmd` on Mac) and click on `orders_part2.csv` so that **both** files are highlighted.
- Click **Open**.

![Images](images/order_open_csv.png)

9. Scroll to the bottom and click **Upload**. Once both files show success, click **Close**.

![Images](images/orders_two_files.png)

![Images](images/orders_uploaded.png)

### Step 1.4: Note Your Manifest Contents

Open `orders_manifest.json` from your desktop in any text editor — it should look like this. The two `url` values point to the exact S3 locations of the order CSVs you just uploaded.

![Images](images/open_json.png)

- The manifest file tells Redshift exactly which files to read during COPY, instead of relying on a prefix.
- Setting `"mandatory": true` forces the COPY to fail loudly if any listed file is missing — which is exactly what you want in production.

```json
{
  "entries": [
    {
      "url": "s3://redshift-warehouse-<your-unique-id>/warehouse/orders/orders_part1.csv",
      "mandatory": true
    },
    {
      "url": "s3://redshift-warehouse-<your-unique-id>/warehouse/orders/orders_part2.csv",
      "mandatory": true
    }
  ]
}
```

> **Important:** Before continuing, open `orders_manifest.json` on your desktop, replace `redshift-warehouse-<your-unique-id>` with your actual bucket name in BOTH `url` entries, save the file, and upload it into the `manifest/` folder in S3.

![Images](images/replace_bucket_name.png)

### Step 1.5: Upload the Manifest File

1. Use the breadcrumb at the top to return to the bucket root. Click into the `manifest` folder.

![Images](images/click_manifest.png)

2. Click **Upload** → **Add files**, select `orders_manifest.json` from `~/Desktop/Project/`, click **Open**, then click **Upload** at the bottom and **Close**.

![Images](images/open_json_file.png)

![Images](images/manifest_uploaded.png)

### Step 1.6: Upload the Parquet Files for Spectrum

1. Use the breadcrumb to return to the bucket root, click into `lake`, then click into `web_events`, then click into `event_date=2024-06-01`.

![Images](images/click_lake.png)
![Images](images/click_date1.png)

2. Click **Upload** → **Add files**, navigate into `~/Desktop/Project/web_events/event_date=2024-06-01/`, select `events.parquet`, click **Open**, then click **Upload** and **Close**.

![Images](images/parquet1_open.png)

![Images](images/event1_upload.png)

![Images](images/event_date1_uploaded.png)

3. Use the breadcrumb to return to `web_events`, then click into `event_date=2024-06-02`. Click **Upload** → **Add files**, select `events.parquet` from `~/Desktop/Project/web_events/event_date=2024-06-02/`, click **Open**, then click **Upload** and **Close**.

![Images](images/date2_upload.png)
![Images](images/parquet2_open.png)
![Images](images/event_date2_uploaded.png)

---

# Activity 2: Create the IAM Role and Catalog the Data Lake

**Purpose of this Activity:** Before Redshift can read from S3, write to S3, or use Spectrum, it needs an IAM role with the right permissions. We will also create a Glue database and run a crawler over the `web_events` Parquet data so Redshift Spectrum has a catalog to point at.

### Step 2.1: Create the Redshift IAM Role

1. In the AWS Console search bar at the very top, type **IAM** and select **IAM** from the dropdown.

![Images](images/serach_iam.png)

2. On the left-hand navigation menu, click on **Roles**.

3. Click the orange **Create role** button on the right side.

![Images](images/create_role_button.png)

4. Under **Trusted entity type**, ensure **AWS service** is selected.

5. Under **Use case**, click the **Service or use case** dropdown and select **Redshift**. Below that, select the **Redshift - Customizable** radio button.

![Images](images/redshift_customizable.png)

6. Click the **Next** button in the bottom right corner.

7. You are now on the "Add permissions" screen. Attach two policies:
   - Click the **Search** box, type `AmazonS3FullAccess`, and press Enter. Click the **checkbox** next to the policy name.

![Images](images/s3_permision_add.png)

- Clear the search box, type `AWSGlueConsoleFullAccess`, and press Enter. Click the **checkbox** next to that policy.

![Images](images/glue_policy_selected.png)

8. The top of the page should read "2 policies selected". Click **Next** in the bottom right.

9. In the **Role name** field at the top, type exactly: `RedshiftSpectrumRole`

![Images](images/iam_role_name.png)

10. Scroll all the way down and click the orange **Create role** button.

![Images](images/iam_role_created.png)

11. After creation, the IAM console will show your role in the list. Click directly on `RedshiftSpectrumRole`, and on the **Summary** page at the top, **copy the full ARN** (e.g., `arn:aws:iam::123456789012:role/RedshiftSpectrumRole`) into a text note — you will paste this into Redshift later.

![Images](images/click_your_role.png)
![Images](images/iam_role_arn.png)

12. While still on the `RedshiftSpectrumRole` page in the IAM console, click the **Trust relationships** tab.

13. Click the **Edit trust policy** button.

![Images](images/edit_tursted_entity.png)

14. By default, the policy only allows Redshift to assume this role. We must add AWS Glue so your crawler can use it later. Update the JSON document to look exactly like the code block below, ensuring `"glue.amazonaws.com"` is added to the `Service` array:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": ["redshift.amazonaws.com", "glue.amazonaws.com"]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

![Images](images/glue_added_policy.png)

15. Click the **Update policy** button to save your changes.

![Images](images/update_policy.png)

### Step 2.2: Create the Glue Database

1. In the AWS Console search bar, type **Glue** and select **AWS Glue** from the dropdown.

![Images](images/select_glue.png)

2. On the left navigation pane, under **Data Catalog**, click **Databases**.

3. Click the orange **Add database** button on the right.

![Images](images/click_add_database.png)

4. In the **Name** field, type exactly: `ecom_lake_db`

5. Scroll down and click the orange **Create database** button.

![Images](images/glue_database_name.png)

### Step 2.3: Crawl the Web Events Parquet Data

1. On the left navigation pane, under **Data Catalog**, click **Crawlers**.

2. Click the orange **Create crawler** button.

![Images](images/create_crawler_button.png)

3. **Name:** Type `crawler_web_events`. Click **Next**.

![Images](images/crawler_name_next.png)

4. On the Choose data sources page, click the **Add a data source** button. A side panel will open.

![Images](images/click_add_datasource.png)

- Ensure **Data source** is set to **S3**.
- Under **S3 path**, click **Browse S3**.

![Images](images/select_browse_s3.png)

- In the popup, click your bucket name (`redshift-warehouse-...`).

![Images](images/click_s3_bucket.png)

- Click the `lake/` folder.

![Images](images/click_lake_folder.png)

- Select the radio button next to `web_events/` and click the **Choose** button at the bottom right.

![Images](images/select_web_events_folder.png)

- Click the orange **Add an S3 data source** button at the bottom of the side panel.

![Images](images/added_s3_datasource.png)

5. You should now see your S3 path listed. Click **Next**.

6. On the Configure security settings page, under **Existing IAM role**, click the dropdown and select `RedshiftSpectrumRole`. Click **Next**.

![Images](images/crawler_use_iam.png)

7. On the Set output and scheduling page, under **Target database**, click the dropdown and select `ecom_lake_db`. Scroll down and click **Next**.

![Images](images/output_config.png)

8. Review your settings on the final page and click **Create crawler**.

![Images](images/click_create_crawler.png)

9. Return to the Crawlers list. Select the checkbox next to `crawler_web_events`, and click the **Run** button near the top right.

![Images](images/select_run_crawler.png)

10. The state will change to _Starting_, then _Running_. Wait roughly 1–2 minutes for the state to return to **Ready**.

11. Click **Tables** in the left navigation pane. You should now see a new table named `web_events` under the `ecom_lake_db` database.

![Images](images/web_events_table.png)

### Step 2.4: Fix the Duplicate Partition Column in Glue

Because the Parquet files have an internal `event_date` column AND are stored in an S3 folder path named `event_date=...`, the crawler registers the column twice. We must remove the duplicate before Redshift Spectrum can query it.

1. Open the **AWS Glue Console**.
2. On the left navigation pane under **Data Catalog**, click **Tables**.
3. Click on your `web_events` table to open its details.

![Images](images/open_web_table.png)

4. Scroll down to the list of columns. You will see `event_date` listed twice: once in the main list, and once at the very bottom marked as a **Partition key**.

![Images](images/two_event_date.png)

5. Click the **Edit schema** button on the right side of that section.

![Images](images/open_web_table.png)

6. Check the box next to the regular `event_date` column (the one at the top in the main list, _not_ the partition key).
7. Click the **Delete** button.

![Images](images/select_regular_event.png)

8. Click **Save as new version** (or **Save**, depending on your UI version).

![Images](images/save_table.png)

---

# Activity 3: Provision Redshift Serverless

**Purpose of this Activity:** To stand up a Redshift Serverless environment (namespace + workgroup) where you will load your warehouse data and run analytical queries, and to attach the IAM role you created so Redshift can read from S3 and query the Glue catalog.

### Step 3.1: Create the Namespace and Workgroup

1. In the AWS Console search bar at the very top, type **Redshift** and select **Amazon Redshift** from the dropdown.

![Images](images/select_redshift.png)

![Images](images/serverless.png)

2. On the left navigation pane, click **Serverless dashboard**.

3. In the main panel, click the orange **Create workgroup** button on the right.

![Images](images/create_workgroup_button.png)

4. On the **Workgroup settings** page, Fill it out as follows:
   - **Workgroup name:** Type exactly `analytics-wg`.

![Images](images/wg_name.png)

- **Performance and cost controls:** Select the **Base capacity** radio button (instead of the default Price-performance target) and ensure it is set to **8** RPU (this is perfect for the lab).

![Images](images/wg_base_capacity.png)

- **Limits & Track:** Leave these sections at their default values.
- **Network and security:** Scroll down and leave the IP address type, VPC, Security groups, and Subnets at their default selections. Ensure SSL remains Enabled.

5. Click **Next** at the bottom right.

![Images](images/wg_ssl_next.png)

6. On the **Namespace** page, under **Choose a namespace**, select the **Create a new namespace** radio button.
   - **Namespace:** Type exactly `analytics-ns`.

![Images](images/wg_ns_name.png)

7. Under **Database name and password**, leave the **Database name** as `dev`. Under **Admin user credentials**, select **Customize admin user credentials** and:
   - **Admin user name:** `admin`
   - **Admin user password:** Choose **Generate a password** (Redshift will create one automatically) or type a strong password and store it safely.

![Images](images/admin_creds.png)

8. Scroll to **Permissions**. Click **Associate IAM roles**

![Images](images/manage_iam_roles.png)

9. In the popup, click the **checkbox** next to `RedshiftSpectrumRole`, then click **Associate IAM roles**.

![Images](images/associate_role.png)

10. Click **Next** at the bottom right.

![Images](images/wg_ns_click_next.png)

11. Review all settings on the final page and click the orange **Create** button.

![Images](images/create_workgroup_final.png)

12. The workgroup will take 2–4 minutes to provision. Refresh the **Serverless dashboard** until the **analytics-wg** workgroup shows **Status: Available**.

![Images](images/workgroup_available.png)

### Step 3.2: Open Query Editor V2

1. On the left navigation pane in the Redshift console, click **Query editor v2**.

![Images](images/click_query_editor.png)

2. Query Editor V2 will open in a new tab. On the left-hand tree, locate the **Serverless: analytics-wg** entry and click it.

![Images](images/locate_wg.png)

3. A connection dialog will appear in the center. Under **Authentication**, select **Federated user** (or "Temporary credentials using your IAM identity"), confirm the **Database** is `dev`, and click **Create connection**.

![Images](images/create_connection.png)

4. You should now see the `dev` database expand on the left, with a query tab open on the right.

![Images](images/qev2_connected.png)

---

# Activity 4: Build the Star Schema and COPY Data from S3

**Purpose of this Activity:** To create the dimension and fact tables with deliberate distribution styles and sort keys, and to load data into them using the COPY command — including a COPY driven by a manifest file for the orders table.

### Step 4.1: Create the Dimension Tables (DISTSTYLE ALL)

1. In the Query Editor V2 query tab on the right, paste the SQL below and click the **Run** button (or press `Ctrl+Enter`).

- The `dim_customers` table is small (only 50 rows), so we replicate it to every compute node using `DISTSTYLE ALL` — this guarantees joins to the fact table happen locally on each slice.
- Sorting on `customer_id` helps merge joins between the fact and dimension stay efficient.

```sql
CREATE TABLE dim_customers (
    customer_id   INTEGER NOT NULL,
    name          VARCHAR(100),
    city          VARCHAR(50),
    signup_date   DATE
)
DISTSTYLE ALL
SORTKEY (customer_id);

CREATE TABLE dim_products (
    product_id    INTEGER NOT NULL,
    product_name  VARCHAR(100),
    category      VARCHAR(50),
    price         DECIMAL(10,2)
)
DISTSTYLE ALL
SORTKEY (product_id);
```

![Images](images/create_dims_result.png)

### Step 4.2: Create the Fact Table (DISTKEY + Compound SORTKEY)

1. Open a new query tab by clicking the **+** icon at the top of the editor, paste the SQL below, and click **Run**.

![Images](images/plus_button.png)

- `DISTKEY(customer_id)` co-locates each order row on the same slice as its matching customer, so the join with `dim_customers` does not have to redistribute data.
- The **compound** `SORTKEY (order_date, customer_id)` accelerates date-range filters first and customer lookups second — exactly the access pattern analytics teams use.

```sql
CREATE TABLE fact_orders (
    order_id      BIGINT     NOT NULL,
    order_date    DATE       NOT NULL,
    customer_id   INTEGER    NOT NULL,
    product_id    INTEGER    NOT NULL,
    quantity      INTEGER,
    amount        DECIMAL(12,2)
)
DISTSTYLE KEY
DISTKEY (customer_id)
COMPOUND SORTKEY (order_date, customer_id);
```

![Images](images/create_fact_result.png)

### Step 4.3: COPY the Dimension Data from S3

- **Reuse the current tab:** You can simply highlight and delete the `CREATE TABLE` SQL you ran in Step 4.2, paste the `COPY` statements for Step 4.3 into that same space, and click **Run**.

1. Replace `<your-unique-id>` in both statements below with the suffix you used in your bucket name, and replace `<account-id>` in the IAM role with your AWS account ID (you can copy the full ARN you saved earlier).

- `COPY` is the fastest way to load data into Redshift — it loads in parallel across all slices.
- `IGNOREHEADER 1` skips the CSV header row, and `REGION 'us-east-1'` makes sure Redshift looks in the right region.

```sql
COPY dim_customers
FROM 's3://redshift-warehouse-<your-unique-id>/warehouse/customers/'
IAM_ROLE 'arn:aws:iam::<account-id>:role/RedshiftSpectrumRole'
FORMAT AS CSV
IGNOREHEADER 1
REGION 'us-east-1';

COPY dim_products
FROM 's3://redshift-warehouse-<your-unique-id>/warehouse/products/'
IAM_ROLE 'arn:aws:iam::<account-id>:role/RedshiftSpectrumRole'
FORMAT AS CSV
IGNOREHEADER 1
REGION 'us-east-1';
```

2. Run each statement and verify success messages.

![Images](images/copy1_dims_success.png)
![Images](images/copy2_dims_success.png)

### Step 4.4: COPY the Fact Data Using a Manifest File

1. In a new query tab, paste the SQL below (substituting your bucket name and account ID) and click **Run**.

- The `MANIFEST` keyword tells COPY to read `orders_manifest.json` and load every file listed inside it — perfect for handling multiple part files atomically.
- Because the manifest has `"mandatory": true`, the COPY fails loudly if either part file is missing.

```sql
COPY fact_orders
FROM 's3://redshift-warehouse-<your-unique-id>/manifest/orders_manifest.json'
IAM_ROLE 'arn:aws:iam::<account-id>:role/RedshiftSpectrumRole'
FORMAT AS CSV
IGNOREHEADER 1
MANIFEST
REGION 'us-east-1';
```

![Images](images/copy_fact_manifest_success.png)

2. Verify the row counts with a quick sanity check:

```sql
SELECT 'dim_customers' AS tbl, COUNT(*) FROM dim_customers
UNION ALL
SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL
SELECT 'fact_orders', COUNT(*) FROM fact_orders;
```

![Images](images/row_counts_result.png)

---

# Activity 5: Compare Distribution Styles with EXPLAIN

**Purpose of this Activity:** To see directly how a poor distribution choice forces Redshift to redistribute or broadcast data between compute nodes. You will build a second copy of the fact table with `DISTSTYLE EVEN` and compare EXPLAIN plans for the same join.

### Step 5.1: Create a Comparison Fact Table with DISTSTYLE EVEN

1. In a new query tab, paste and run the SQL below.

- `DISTSTYLE EVEN` distributes rows round-robin across slices with no awareness of join keys — convenient, but it forces redistribution at join time.
- We then copy data over with `INSERT INTO ... SELECT` so the row counts match the original fact table.

```sql
CREATE TABLE fact_orders_even (
    order_id      BIGINT     NOT NULL,
    order_date    DATE       NOT NULL,
    customer_id   INTEGER    NOT NULL,
    product_id    INTEGER    NOT NULL,
    quantity      INTEGER,
    amount        DECIMAL(12,2)
)
DISTSTYLE EVEN
COMPOUND SORTKEY (order_date, customer_id);

INSERT INTO fact_orders_even
SELECT * FROM fact_orders;
```

![Images](images/even_table_loaded.png)

### Step 5.2: Run EXPLAIN on Both Joins

1. Paste each EXPLAIN below into the editor, highlight one statement at a time, and click **Run** so you can see each plan in isolation.

- The first EXPLAIN should show **DS_DIST_NONE** (no redistribution) for the join, because the fact and dimension are co-located via `DISTKEY` + `DISTSTYLE ALL`.
- The second EXPLAIN should show **DS_BCAST_INNER** or **DS_DIST_INNER**, indicating that Redshift had to broadcast/redistribute data because `fact_orders_even` is not co-located on `customer_id`.

```sql
-- Optimal: DISTKEY fact + ALL dim
EXPLAIN
SELECT c.city, SUM(f.amount) AS revenue
FROM fact_orders f
JOIN dim_customers c
  ON f.customer_id = c.customer_id
WHERE f.order_date BETWEEN DATE '2024-01-01' AND DATE '2024-06-30'
GROUP BY c.city;
```

![Images](images/explain_distkey.png)

```sql
-- Suboptimal: EVEN fact + ALL dim
EXPLAIN
SELECT c.city, SUM(f.amount) AS revenue
FROM fact_orders_even f
JOIN dim_customers c
  ON f.customer_id = c.customer_id
WHERE f.order_date BETWEEN DATE '2024-01-01' AND DATE '2024-06-30'
GROUP BY c.city;
```

![Images](images/explain_even.png)

2. Read the **Operation** column in each plan. Note how the `DS_DIST_*` annotations differ — this is the visible cost of a bad distribution choice.

---

# Activity 6: Query the Data Lake with Redshift Spectrum

**Purpose of this Activity:** To register the Glue catalog database as an external schema inside Redshift, then run queries that join warehouse fact data with raw Parquet data still sitting in S3.

### Step 6.1: Create the External Schema

1. In a new query tab, paste the SQL below (replace `<account-id>` with your AWS account ID) and click **Run**.

- `CREATE EXTERNAL SCHEMA` registers your Glue database as a schema inside Redshift — every table Glue knows about in `ecom_lake_db` becomes queryable under `spectrum_lake`.
- The `IAM_ROLE` is the same role you attached to the workgroup, giving Spectrum permission to read your Parquet files.

```sql
CREATE EXTERNAL SCHEMA spectrum_lake
FROM DATA CATALOG
DATABASE 'ecom_lake_db'
IAM_ROLE 'arn:aws:iam::<account-id>:role/RedshiftSpectrumRole'
REGION 'us-east-1';
```

![Images](images/external_schema_created.png)

2. Expand the **Schemas** tree on the left in Query Editor V2. You should see `spectrum_lake` listed under the `dev` database, and inside it the `web_events` table from the Glue crawler.

![Images](images/spectrum_tree.png)

### Step 6.2: Run a Spectrum Query

1. Paste the SQL below into a new query tab and click **Run**.

- This query joins the warehouse `fact_orders` table with the external `spectrum_lake.web_events` Parquet data on `customer_id`, blending data that lives in two completely different storage layers.
- The filter on `event_date` exercises **partition pruning** — Spectrum only reads the matching Parquet partition, keeping scan costs down.

```sql
SELECT
    e.event_date,
    c.city,
    COUNT(DISTINCT e.session_id) AS sessions,
    SUM(f.amount)               AS revenue
FROM spectrum_lake.web_events e
JOIN fact_orders f
  ON e.customer_id = f.customer_id
JOIN dim_customers c
  ON c.customer_id = f.customer_id
WHERE e.event_date IN (DATE '2024-06-01', DATE '2024-06-02')
GROUP BY e.event_date, c.city
ORDER BY revenue DESC;
```

![Images](images/spectrum_join_result.png)

---

# Activity 7: UNLOAD Aggregated Results to S3 as Partitioned Parquet

**Purpose of this Activity:** To export query results back into the data lake in an analytics-ready format so downstream consumers (Athena, EMR, BI tools) can use them — using Redshift's `UNLOAD` command with Parquet output and partitioning.

### Step 7.1: Run the UNLOAD Statement

1. In a new query tab, paste the SQL below (replace your bucket and account ID) and click **Run**.

- `UNLOAD` writes the result of a SELECT directly to S3, in parallel across slices.
- `FORMAT AS PARQUET` produces columnar files, and `PARTITION BY (order_date)` creates Hive-style partition folders under the target prefix — exactly what Athena and Glue expect.

```sql
UNLOAD ('
    SELECT order_date, customer_id, SUM(amount) AS daily_revenue, COUNT(*) AS order_count
    FROM fact_orders
    GROUP BY order_date, customer_id
')
TO 's3://redshift-warehouse-<your-unique-id>/unload/order_summary/'
IAM_ROLE 'arn:aws:iam::<account-id>:role/RedshiftSpectrumRole'
FORMAT AS PARQUET
PARTITION BY (order_date)
ALLOWOVERWRITE;
```

![Images](images/unload_success.png)

### Step 7.2: Verify the Output in S3

1. Right-click the AWS logo in the top left and open a new tab. In the new tab, search for and open **S3**.

2. Click your bucket (`redshift-warehouse-...`).

3. Click into the `unload` folder.

![Images](images/open_unload.png)

4. Click into the `order_summary/` folder. You should see partition subfolders named like `order_date=2024-01-15/`, `order_date=2024-02-03/`, etc. Click into any of them.

![Images](images/folders.png)

5. Inside the partition, you will see one or more `.parquet` files. These are your downstream-consumable, columnar, partitioned outputs!

![Images](images/unload_partitions.png)

---

## 🎓 Conclusion

This guided project demonstrated how to build a production-grade Redshift workload that loads, models, and serves analytical data while also reaching back into the data lake when needed. You stood up Redshift Serverless, designed tables with deliberate distribution and sort key choices, and used Spectrum to blend warehouse and lake data in a single query.

You practiced real-world data engineering patterns, including:

- Provisioning a **Redshift Serverless** namespace and workgroup with an IAM role wired up for both S3 and Glue.
- Designing a small **star schema** with intentional **DISTKEY**, **DISTSTYLE ALL**, and **compound SORTKEY** choices.
- Loading data via the **COPY command** — including a manifest-driven load for multi-file ingestion.
- Reading **EXPLAIN plans** to see the impact of distribution choices on join behavior.
- Querying S3 Parquet data directly through **Redshift Spectrum** using an external schema backed by the **Glue Data Catalog**.
- Exporting analytical results back to S3 as **partitioned Parquet** using **UNLOAD**.

These are essential skills for any modern AWS Data Engineer working with Redshift!
