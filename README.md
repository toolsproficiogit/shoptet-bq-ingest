# 🌐 CSV → BigQuery Cloud Run Ingest

This solution runs entirely in **Google Cloud Platform (GCP)** — no local installation is needed. It should run fast and with low costs, and offer good scalability and flexibility. 
It downloads your **CSV exports** from a remote link (such as Shoptet CSVs) and loads them into **BigQuery**, automatically and securely.

Everything runs **within your own GCP project**, there is no external data processing - this solution only provides the codebase to set up your own service.

> **Important:** this tool was developed to be compatible with **Shoptet CSVs**. However, it should work with any CSV as long as its schema is properly configured.

---

## 🚀 Getting Started on Google Cloud

### 🪪 Step 1. Log in to Google Cloud Platform
1. Open [https://console.cloud.google.com/](https://console.cloud.google.com/)
2. Sign in using your **Google Workspace** or **GCP user account**.
3. Make sure you have access to the correct **project** (shown in the top navigation bar).

### 🧭 Step 2. Select the correct project
In the top GCP navigation bar, click the **project selector** and choose the project you want to use for deployment.

### 💻 Step 3. Start a Cloud Shell session
1. In the top-right corner of the GCP Console, click the **Cloud Shell icon (›_ )**.
2. Wait for the terminal to initialize — you’ll see a prompt like:
   ```bash
   Welcome to Cloud Shell! Type “help” to get started.
   user@cloudshell:~$
   ```
3. From here, follow  the steps below to **create a BigQuery dataset** (if needed) and **deploy** the service. For the included commands, just copy/paste them into the Cloud Shell.

---

## 📦 Create a BigQuery Dataset (required)

You must create the dataset that your tables will live in. By default, it will create dataset **csv_export** in the active project, in location **EU**. If you want to change the dataset name and/or location, switch these fields in the command.

```bash
PROJECT_ID="$(gcloud config get-value project)"

bq --project_id="$PROJECT_ID" --location=EU mk -d csv_export || true
```

> *For a more advanced implementation, you can change the name and location of the dataset. You can even create multiple datasets by running the command repeatedly. However, make sure to remember the names and the corresponding locations as they will be used later.*

---

## 🚦 Prerequisites & Permissions

Before starting, ensure your account has the roles below. Without them, deployment or scheduling may fail.

### 🧩 Required roles for the deploying user

| Role | Why it's needed |
|------|------------------|
| `roles/run.admin` | Deploy and manage Cloud Run services |
| `roles/cloudscheduler.admin` | Create scheduled jobs |
| `roles/iam.serviceAccountAdmin` | Create and manage service accounts |
| `roles/iam.serviceAccountUser` | Allow using service accounts in deploys |
| `roles/cloudbuild.builds.editor` | Build and push Docker images |
| `roles/artifactregistry.admin` | Create and manage Artifact Registry repositories |
| `roles/serviceusage.serviceUsageAdmin` | Enable required APIs |
| `roles/storage.admin` | Create buckets & upload YAML (remote config) |
| `roles/editor` | For Google Sheets API access |

### ✅ Grant the roles (run yourself you have a project owner/admin role, otherwise contact project owner/admin)

```bash
# Clone repo and make scripts executable
git clone https://github.com/toolsproficiogit/shoptet-bq-ingest.git
cd shoptet-bq-ingest
chmod +x scripts/*.sh

# Grant roles to a specific user (change project ID and email as needed)
PROJECT_ID="<YOUR_PROJECT_ID>"
USER_EMAIL="<YOUR_EMAIL>"
PROJECT_ID="$PROJECT_ID" USER_EMAIL="$USER_EMAIL" ./scripts/grant_permissions.sh
```

### 🔍 Verify your permissions

```bash
gcloud projects get-iam-policy "$PROJECT_ID" \\
  --flatten="bindings[].members" \\
  --filter="bindings.members:user:${USER_EMAIL}" \\
  --format="table(bindings.role)"
```

Ask a **Project Owner** or **Organization Admin** to grant roles if any are missing.

---

## Quick Start — Multi‑Pipeline

A single service that runs multiple pipelines (CSV links and target tables) defined in a remote **YAML** file or **Google Sheet**, using a custom schema config. You define settings individually for each pipeline.

### 1) Prepare Your Configuration

Choose one of the following configuration sources:

#### Option A: Google Sheets (Recommended)

1. **Create a Google Sheet** with two tabs: `Pipeline_Config` and `Schema_Config`.
2. **Populate the tabs** with your configuration data (see templates below).
3. **Get your service account email:**

```bash
PROJECT_NUMBER=$(gcloud projects describe $(gcloud config get-value project) --format='value(projectNumber)')
echo "${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
```

4. **Share the sheet** with this email address and give it **Editor** permissions.

#### Option B: YAML Files in GCS

1. **Create `config.yaml` and `schemas.yaml`** files (see templates below).
2. **Upload the files** to a GCS bucket.

### 2) Deploy the Service

Run the `deploy_multi.sh` script to deploy the service to Cloud Run. This script handles both Google Sheets and YAML configurations.

```bash
chmod +x scripts/*.sh
./scripts/deploy_multi.sh
```

The script will prompt you to choose your configuration source and guide you through the deployment process.

### 3) Manually run at once (all pipelines)

```bash
./scripts/trigger.sh
```

### 4) Schedule runs with default (daily at 6 AM) (all pipelines)

```bash
./scripts/schedule_multi.sh
```

---

## Configuration Templates

### Google Sheets: `Pipeline_Config` Tab

| pipeline_id | export_type | csv_url | bq_table_id | delimiter | encoding | skip_leading_rows | load_mode | window_days | dedupe_mode | timeout_sec | retries | active |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| pipe_1 | orders | https://... | project.dataset.table | ; | utf-8 | 1 | auto | 30 | auto_dedupe | 300 | 3 | TRUE |

### Google Sheets: `Schema_Config` Tab

| export_type | name | type | mode | description | source | parse_logic |
|---|---|---|---|---|---|---|
| orders | id | STRING | NULLABLE | Order ID | id | string |
| orders | amount | FLOAT | NULLABLE | Order Amount | amount | float |

### YAML: `config.yaml`

```yaml
pipelines:
  - id: pipeline_1
    export_type: orders
    csv_url: https://...
    bq_table_id: project.dataset.table
    delimiter: ";"
    encoding: utf-8
    skip_leading_rows: 1
    load_mode: auto
    window_days: 30
    dedupe_mode: auto_dedupe
    timeout_sec: 300
    retries: 3
    active: true
```

### YAML: `schemas.yaml`

```yaml
schemas:
  orders:
    - name: id
      type: STRING
      mode: NULLABLE
      description: Order ID
      source: id
      parse: string
    - name: amount
      type: FLOAT
      mode: NULLABLE
      description: Order Amount
      source: amount
      parse: float
```

# Configuration and Schema Field Explanations

Below is the full reference for the supported `Pipeline_Config` and `Schema_Config` fields.

---

## Pipeline Configuration Fields

These fields define how a pipeline operates: where to fetch the CSV, where to load it, and how to process it.

### `pipeline_id` (Required)

**Type:** String  
**Example:** `pipe_1`, `client1_orders`, `shoptet_customers`

A unique identifier for this pipeline.

---

### `export_type` (Required)

**Type:** String  
**Example:** `orders`, `customers`, `products`

The type of export this pipeline represents. It has to be one of the export types in the `Schema_Config`, select one that matches your CSV columns. You can add new/custom schemas in the `Schema_Config` if necessary.

---

### `csv_url` (Required)

**Type:** URL  
**Example:** `https://api.shoptet.cz/export/orders.csv`, `https://example.com/shoptet.csv`

The full URL where the CSV file is located. The URL must be publicly accessible or have appropriate authentication embedded in the URL.

---

### `bq_table_id` (Required)

**Type:** BigQuery Table ID  
**Example:** `project-id.dataset_name.table_name`

BigQuery destination table where the data will be loaded. Format: `PROJECT_ID.DATASET_NAME.TABLE_NAME`. The table will be created automatically if it doesn't exist. As a best practice each pipeline should have its own destination table.

---

### `delimiter` (Optional)

**Type:** String  
**Default:** `;`  
**Example:** `;`, `,`, `\t`

The character used to separate columns in the CSV file. Most CSVs use semicolon (`;`), but comma (`,`) and Tab (`\t`) are also supported.

---

### `encoding` (Optional)

**Type:** String  
**Default:** `utf-8`  
**Valid values:** `utf-8`, `utf-8-sig`, `cp1250`, `windows-1250`, `iso-8859-2`, `latin-1`, `auto`

The character encoding of the CSV file. The service will attempt to auto-detect if set to `auto`. For Shoptet CSVs from Czech systems, `cp1250` or `windows-1250` are common. UTF-8 is the standard for most modern systems.

---

### `skip_leading_rows` (Optional)

**Type:** Integer  
**Default:** `1`  
**Example:** `1`, `2`, `0`

Number of rows to skip at the beginning of the CSV file before processing. Use `1` to skip the header row (most common), `2` to skip header and one additional row, or `0` to process all rows including the header.

---

### `load_mode` (Optional)

**Type:** String  
**Default:** `auto`  
**Valid values:** `auto`, `full`, `window`

Determines how much historical data to keep:

- **`auto`**: On first run, loads all data (`full`). On subsequent runs, upserts only the last `window_days` of data.
- **`full`**: Always loads all available data from the CSV.
- **`window`**: Always upserts only the last `window_days` of data (see `window_days` field).

---

### `window_days` (Optional)

**Type:** Integer  
**Default:** `30`  
**Example:** `30`, `7`, `90`

Number of days of historical data that will be processed using `load_mode: window` or `load_mode: auto` (after the first run). Data older than this many days will be kept as-is. This is useful for updating latest retroactive data changes without having to rewrite full history.

---

### `dedupe_mode` (Optional)

**Type:** String  
**Default:** `auto_dedupe`  
**Valid values:** `no_dedupe`, `auto_dedupe`, `full_dedupe`

Determines how duplicate rows are handled:

- **`no_dedupe`**: No deduplication. All rows are loaded as-is, even if they're duplicates.
- **`auto_dedupe`**: Removes duplicates based on key fields (ID, product_id, order_id, etc.). Keeps the last occurrence.
- **`full_dedupe`**: Removes rows that are completely identical across all fields.

---

### `timeout_sec` (Optional)

**Type:** Integer  
**Default:** `300`  
**Example:** `300`, `600`, `900`

Maximum time (in seconds) to wait for the CSV download to complete. If the download takes longer than this, it will timeout and the pipeline will fail. Increase this for very large CSV files.

---

### `retries` (Optional)

**Type:** Integer  
**Default:** `3`  
**Example:** `1`, `3`, `5`

Number of times to retry downloading the CSV if the download fails. Each retry uses exponential backoff. Useful for handling temporary network issues.

---

### `active` (Optional, Google Sheets only)

**Type:** Boolean  
**Default:** `TRUE`  
**Valid values:** `TRUE`, `FALSE`

Whether this pipeline should be executed. Set to `FALSE` to temporarily disable a pipeline without deleting it. This field is only used in Google Sheets.

### `key_fields` (Optional, Google Sheets only)

**Type:** Comma-separated field names  
**Default:** Auto-detected (looks for id, product_id, order_id, customer_id, sku)  
**Example:** `date,email,code`

Controls deduplication logic; when multiple rows have the same key combination, only the last occurrence is kept. Make sure all the key fields are configured in the Schema.

---

## Schema Configuration Fields

These fields define the structure of the CSV data and how each column should be processed and stored in BigQuery. Be very careful when changing these values, incorrect configuration can break the pipeline!

### `export_type` (Required)

**Type:** String  
**Example:** `orders`, `customers`, `products`

The type of export this schema applies to. The `export_type` field in the corresponding pipeline must match one of the defined export types. 1 row = 1 field/column in a CSV export. You can add new rows to add fields to existing export types, or create new types.

---

### `name` (Required)

**Type:** String  
**Example:** `id`, `order_date`, `customer_name`, `amount`

The name of the column in BigQuery. This is the name that will appear in your BigQuery table. Use snake_case for consistency.

---

### `type` (Required)

**Type:** String  
**Valid values:** `STRING`, `INTEGER`, `FLOAT`, `BOOLEAN`, `TIMESTAMP`, `DATE`, `NUMERIC`

The data type of the column in BigQuery:

- **`STRING`**: Text data (default for most CSV columns)
- **`INTEGER`**: Whole numbers (no decimals)
- **`FLOAT`**: Decimal numbers
- **`BOOLEAN`**: True/False values
- **`TIMESTAMP`**: Date and time (automatically normalized from DATETIME)
- **`DATE`**: Date only (YYYY-MM-DD)
- **`NUMERIC`**: High-precision decimal numbers

---

### `mode` (Optional)

**Type:** String  
**Default:** `NULLABLE`  
**Valid values:** `NULLABLE`, `REQUIRED`, `REPEATED`

Determines whether the column can be empty (usually best kept at NULLABLE, only change if you know what you are doing):

- **`NULLABLE`**: Column can be empty (NULL values allowed)
- **`REQUIRED`**: Column must have a value (NULL values not allowed)
- **`REPEATED`**: Column contains an array of values (rarely used)

---

### `description` (Optional)

**Type:** String  
**Example:** `Order ID`, `Customer name`, `Total amount in EUR`

A human-readable description of what this column contains. This helps with documentation and understanding the data later. Appears in BigQuery table schema documentation.

---

### `source` (Optional)

**Type:** String  
**Default:** Same as `name`  
**Example:** `id`, `order_id`, `customer_name`

The name of the column in the CSV file. If different from the BigQuery column name (`name`), use this to map CSV columns to BigQuery columns. For example, if your CSV has a column called `order_id` but you want it named `id` in BigQuery, set `source: order_id` and `name: id`.

---

### `parse` (Optional)

**Type:** String  
**Default:** `string`  
**Valid values:** `string`, `int`, `float`, `datetime`, `date_only`, `bool`, `decimal_comma`

How to parse the value from the CSV before storing in BigQuery:

- **`string`**: (Default) Reads the value as a string and strips leading/trailing whitespace and quotes.
- **`int`**: Converts a string to an integer.
- **`float`**: Converts a string to a standard float.
- **`datetime`**: Parses a date/time string into a standard datetime object. Supports common formats like YYYY-MM-DD HH:MM:SS, YYYY-MM-DDTHH:MM:SS, and ISO 8601.
- **`date_only`**: Parses a date/time string and returns only the date part in YYYY-MM-DD format.
- **`bool`**: Converts a string to a boolean. true, 1, t, yes, y are treated as True.
- **`decimal_comma`**: Converts a string with a comma as the decimal separator (e.g., "123,45") to a standard float (123.45). This is common in European number formats.

---

## Special Auto-Generated Columns

The service automatically adds two columns to every pipeline:

### `identifier`

**Type:** STRING  
**Content:** The `pipeline_id` value

This column is added to every row and contains the pipeline ID. Useful for identifying which source the data came from when multiple pipelines load into the same table.

---

### `date_only`

**Type:** DATE  
**Content:** Date extracted from the `date` column

If your schema includes a `date` column (with type `TIMESTAMP` or `DATE`), the service automatically creates a `date_only` column containing just the date portion (without time). Useful for daily aggregations and grouping.

---

## Manage pipelines and schemas on deployed service

### A) Edit Pipeline Configs

#### If using Google Sheets:

1. Open your Google Sheet.
2. Edit the `Pipeline_Config` tab.
3. The changes will be applied on the next run.

#### If using YAML:

```bash
SERVICE="csv-bq-multi"; REGION="europe-west1"

CONFIG_URL=$(gcloud run services describe "$SERVICE" --region "$REGION" --format=json \
  | jq -r ".spec.template.spec.containers[0].env[] | select(.name==\"CONFIG_URL\") | .value")
BUCKET=$(echo "$CONFIG_URL" | sed -E "s#https?://storage.googleapis.com/([^/]+)/.*#\\1#")
OBJECT=$(echo "$CONFIG_URL" | sed -E "s#https?://storage.googleapis.com/[^/]+/(.*)#\\1#")

mkdir -p config && gsutil cp "gs://${BUCKET}/${OBJECT}" config/config.yaml
cloudshell edit config/config.yaml

gsutil cp config/config.yaml "gs://${BUCKET}/${OBJECT}"
```

### B) Edit Schemas

#### If using Google Sheets:

1. Open your Google Sheet.
2. Edit the `Schema_Config` tab.
3. The changes will be applied on the next run.

#### If using YAML:

```bash
SERVICE="csv-bq-multi"; REGION="europe-west1"

SCHEMA_URL=$(gcloud run services describe "$SERVICE" --region "$REGION" --format=json \
  | jq -r ".spec.template.spec.containers[0].env[] | select(.name==\"SCHEMA_URL\") | .value")
SBUCKET=$(echo "$SCHEMA_URL" | sed -E "s#https?://storage.googleapis.com/([^/]+)/.*#\\1#")
SOBJECT=$(echo "$SCHEMA_URL" | sed -E "s#https?://storage.googleapis.com/[^/]+/(.*)#\\1#")

mkdir -p config && gsutil cp "gs://${SBUCKET}/${SOBJECT}" config/schemas.yaml
cloudshell edit config/schemas.yaml

gsutil cp config/schemas.yaml "gs://${SBUCKET}/${SOBJECT}"
```

### Retrigger run manually

```bash
./scripts/trigger.sh
```

---

## Updating Environment Variables

To update environment variables on a deployed service without redeploying, use the `update_env.sh` script.

```bash
# Run in interactive mode
./scripts/update_env.sh
```

```bash
# Run with command-line arguments: set allow unknown columns to true and switch to google sheets source
./scripts/update_env.sh --allow-unknown true
./scripts/update_env.sh --use-sheets true --sheet-id YOUR_SHEET_ID
```

---

## Teardown (clean-up)

```bash
./scripts/teardown.sh
```

## Schema Validation and Migration

The service now includes a schema validation and migration strategy to handle changes to your BigQuery table schemas. This is controlled by the `SCHEMA_MIGRATION_MODE` environment variable.

**strict** - (default) always return errors when changing schema, requires action (like creating a new table)

**auto_migrate** - adds new columns safely, but returns errors when removing or changing existing ones

**recreate** - recreates the table anytime the schema is changed

### How to Set

**During deployment:**

The `deploy_multi.sh` script will prompt you to choose the schema migration mode during deployment.

**On a deployed service:**

You can update the `SCHEMA_MIGRATION_MODE` on a deployed service without redeploying using the `update_env.sh` script:

```bash
# Interactive mode
./scripts/update_env.sh
```

```bash
# Command-line mode
./scripts/update_env.sh --set SCHEMA_MIGRATION_MODE=auto_migrate
```

### Recommendations

- **Production:** Use `strict` mode to prevent accidental schema changes.
- **Development:** Use `auto_migrate` or `recreate` to iterate quickly.
- **Always back up your data** before making significant schema changes.

---

## 📊 Verify BigQuery Data

After a successful run, check your tables in BigQuery:

```sql
SELECT * FROM `<PROJECT>.csv_export.<TABLE>`
ORDER BY date DESC
LIMIT 20;
```

---

**Repository:** [https://github.com/toolsproficiogit/shoptet-bq-ingest](https://github.com/toolsproficiogit/shoptet-bq-ingest)
