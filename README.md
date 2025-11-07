# 🌐 Shoptet → BigQuery Cloud Run Ingest

This solution runs entirely in **Google Cloud Platform (GCP)** — no local installation is needed. It should run fast and with low costs, and offers good scalability. 
It downloads your **Shoptet CSV exports** and loads them into **BigQuery**, automatically and securely.

> **Important:** This service **does not create BigQuery datasets for you**. You must create the **dataset(s)** ahead of time and choose/remember their **location** (e.g., `EU`). The service will create **tables** if they are missing.

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

You must create the dataset that your tables will live in. By default, it will create dataset **shoptet_export** in the active project, in location **EU**.

```bash
PROJECT_ID="$(gcloud config get-value project)"

bq --project_id="$PROJECT_ID" --location=EU mk -d shoptet_export || true
```

> Save/remember the `BQ_LOCATION` you pick here — you will reuse it later.

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

### ✅ Grant the roles (admin or owner runs this)

```bash
# Clone repo and make scripts executable
git clone https://github.com/toolsproficiogit/shoptet-bq-ingest.git
cd shoptet-bq-ingest
chmod +x scripts/*.sh

# Grant roles to a specific user (run as Project Owner/Admin)
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

## 🚀 Quick Start: Single Pipeline

This mode loads exactly one CSV → one BigQuery table. Use this if you have just a single export for a single project. If you plan to ingest multiple CSVs in a single project, use the Multi-Pipeline setup instructions below.

### ⚙️ Deploy via Cloud Shell

Open Cloud Shell and run:

```bash
git clone https://github.com/toolsproficiogit/shoptet-bq-ingest.git
cd shoptet-bq-ingest
chmod +x scripts/*.sh
./scripts/deploy_single.sh
```

During the prompt-based setup, you’ll provide:

| Prompt | What to enter |
|--------|----------------|
| **Project ID** | Your GCP project ID (e.g., `my-gcp-project`) |
| **Region** | Cloud Run region (e.g., `europe-west1`) |
| **Service name** | The Cloud Run service name (default `shoptet-bq-ingest`). You can deploy multiple services with different names if needed. |
| **CSV URL** | The direct Shoptet CSV export link. |
| **BigQuery table (project.dataset.table)** | Use your **existing dataset**. Example: `my-gcp-project.shoptet_data.orders` |
| **Window days** | Days to upsert for incremental loads; default `30`. This reduces the number of processed rows for each scheduled run. |
| **Load mode** | `auto` (default; full if table empty, then window), `full` (always all rows), or `window` (always last N days). |
| **BQ Location** | Must match the dataset’s location (e.g., `EU`). |

> 🧠 First run loads **full history** automatically if the table is empty (when `LOAD_MODE=auto`). After that, only the last 30 days will be appended/updated.

After the service is deployed, you can **trigger it manually**, if you want to test the connection and populate data immediately before setting up schedule. Use **this command**:

```bash
SERVICE_URL="<YOUR_SERVICE_URL>"
ID_TOKEN=$(gcloud auth print-identity-token)
curl -s -H "Authorization: Bearer $ID_TOKEN" "${SERVICE_URL}/run" | jq
```

You will see the **Service URL** printed in the Cloud Shell. Alternatively, you can check it in Cloud Console, if you navigate to Cloud Run > Services > load-csv-to-bigquery (your chosen service name) > URL is displayed at the top.

Response example:
```json
{
  "status": "ok",
  "message": "Ingest complete",
  "parsed_rows": 400,
  "kept_rows": 398,
  "mode": "auto"
}
```

You can now verify BigQuery by running the command at the end of this README, or by simply looking in the UI.

---

## Quick Start — Multi‑Pipeline

A single service that runs multiple pipelines (CSV links and target tables) defined in a remote **YAML** file. You define settings individually for each pipeline.

### 1) Prepare YAML locally
```bash
git clone https://github.com/toolsproficiogit/shoptet-bq-ingest.git
cd shoptet-bq-ingest
cp config/config.example.yaml config/config.yaml
cloudshell edit config/config.yaml     # visual editor opens
# edit pipelines, Save
```

YAML example:
```yaml
pipelines:
  - id: advertiser1_orders
    export_type: orders
    csv_url: https://example.com/shoptet.csv
    bq_table_id: <PROJECT>.shoptet_export.advertiser1_orders
    load_mode: auto
    window_days: 30

  - id: advertiser2_customers
    export_type: customers  
    csv_url: https://example.com/shoptet.csv
    bq_table_id: <PROJECT>.shoptet_export.advertiser2_customers
    load_mode: auto
    window_days: 30
```

### 3) Schema library (define columns and formats in the CSV)

You can edit or add schemas that the service will expect from incoming CSVs. You can control the processing of each CSV pipeline in the export_type field. There are 3 premade export configurations, **If your export matches one of these, you do not need to create new ones.**

```yaml
export_types:
  basic:
    - name: date
      source: date
      type: DATETIME
      parse: datetime
    - name: orderItemType
      source: orderItemType
      type: STRING
      parse: string
    - name: orderItemTotalPriceWithoutVat
      source: orderItemTotalPriceWithoutVat
      type: FLOAT
      parse: decimal_comma

  orders:
    - name: date
      source: date
      type: DATETIME
      parse: datetime
    - name: orderItemTotalPriceWithoutVat
      source: orderItemTotalPriceWithoutVat
      type: FLOAT
      parse: decimal_comma
    - name: orderItemType
      source: orderItemType
      type: STRING
      parse: string
    - name: statusName
      source: statusName
      type: STRING
      parse: string
    - name: orderItemName
      source: orderItemName
      type: STRING
      parse: string
    - name: orderItemAmount
      source: orderItemAmount
      type: INT64
      parse: int
    - name: orderItemUnitPurchasePrice
      source: orderItemUnitPurchasePrice
      type: FLOAT
      parse: decimal_comma

  customers:
    - name: date
      source: date
      type: DATETIME
      parse: datetime
    - name: email
      source: email
      type: STRING
      parse: string
    - name: statusName
      source: statusName
      type: STRING
      parse: string
    - name: code
      source: code
      type: STRING
      parse: string
    - name: totalPriceWithoutVat
      source: totalPriceWithoutVat
      type: FLOAT
      parse: decimal_comma
    - name: orderPurchasePrice
      source: orderPurchasePrice
      type: FLOAT
      parse: decimal_comma
```

### 2) Deploy (uploads YAML to GCS, sets CONFIG_URL, deploys service)
```bash
chmod +x scripts/*.sh
./scripts/deploy_multi.sh
```
The script will choose defaults for the following options. to confirm default, press **ENTER**, otherwise input your own values:

| Setting | Default |
|--------|----------------|
| **Region** | europe-west1 |
| **Service name** | shoptet-bq-multi |
| **Pipelines file** | config/cinfig.yaml |
| **Schemas file** | config/schemas.yaml |
| **BigQuery location** | EU|
| **GCS bucket** | Shoptet-config-<PROJECT_ID> |
| **Scheduler job name** | daily-shoptet-bq |
| **Mode** | auto (full history on first run, then only last <Window days>) |
| **Window days** | 30 |

It will:
1) Validate both YAMLs
2) Warn if multiple pipelines target the same table (must confirm "Y/N"
3) Upload both configs to Cloud Storage
4) Build and deploy Cloud Run service
5) Grant permissions
6) Print `Service URL` and save it to deploy_state

### 3) Manually run at once (all pipelines)

```bash
./scripts/trigger.sh
```
### 4) Schedule runs with default (daily at 6 AM) (all pipelines)

```bash
./scripts/schedule_multi.sh
```

---

## Manage pipelines and schemas on deployed service

You deployed previously, and the service is running. Now you want to change pipelines. Start a **new Cloud Shell session** (described at the start of the README). 

### A) Edit pipeline configs
```bash
SERVICE="shoptet-bq-multi"; REGION="europe-west1"

CONFIG_URL=$(gcloud run services describe "$SERVICE" --region "$REGION" --format=json \
  | jq -r '.spec.template.spec.containers[0].env[] | select(.name=="CONFIG_URL") | .value')
BUCKET=$(echo "$CONFIG_URL" | sed -E 's#https?://storage.googleapis.com/([^/]+)/.*#\1#')
OBJECT=$(echo "$CONFIG_URL" | sed -E 's#https?://storage.googleapis.com/[^/]+/(.*)#\1#')

mkdir -p config && gsutil cp "gs://${BUCKET}/${OBJECT}" config/config.yaml
cloudshell edit config/config.yaml
```

Edit the config file, add or remove pipelines, save, and:
```bash
gsutil cp config/config.yaml "gs://${BUCKET}/${OBJECT}"
```

### B) Edit schemas
```bash
SERVICE="shoptet-bq-multi"; REGION="europe-west1"

SCHEMA_URL=$(gcloud run services describe "$SERVICE" --region "$REGION" --format=json \
  | jq -r '.spec.template.spec.containers[0].env[] | select(.name=="SCHEMA_URL") | .value')
SBUCKET=$(echo "$SCHEMA_URL" | sed -E 's#https?://storage.googleapis.com/([^/]+)/.*#\1#')
SOBJECT=$(echo "$SCHEMA_URL" | sed -E 's#https?://storage.googleapis.com/[^/]+/(.*)#\1#')

mkdir -p config && gsutil cp "gs://${SBUCKET}/${SOBJECT}" config/schemas.yaml
cloudshell edit config/schemas.yaml
```

Edit the config file, add or remove pipelines, save, and:
```bash
gsutil cp config/schemas.yaml "gs://${SBUCKET}/${SOBJECT}"
```

### Retrigger run manually
```bash
SERVICE_URL=$(gcloud run services describe shoptet-bq-multi --region europe-west1 --format='value(status.url)')
ID_TOKEN=$(gcloud auth print-identity-token)
curl -s -H "Authorization: Bearer $ID_TOKEN" "${SERVICE_URL}/run" | jq
```

### ⏱ About cron expressions (set up flexible schedules)

You can find more about Cron expressions [https://docs.cloud.google.com/scheduler/docs/configuring/cron-job-schedules](here).

Examples of useful, ready-to-use Cron expressions for this deployment:

| Example | Meaning |
|----------|----------|
| `0 * * * *` | Every hour |
| `0 6 * * *` | Every day at 06:00 UTC |
| `*/15 * * * *` | Every 15 minutes |

> Cron is in **UTC**. Convert as needed for your local time.

---

## Teardown (clean-up)

```bash
REGION="europe-west1"
gcloud run services delete shoptet-bq-multi --region "$REGION" -q
gcloud scheduler jobs delete daily-shoptet-bq --location "$REGION" -q
```

---

## 📊 Verify BigQuery Data

After a successful run, check your tables in BigQuery:

```sql
SELECT * FROM `<PROJECT>.shoptet_export.<TABLE>`
ORDER BY date DESC
LIMIT 20;
```

---

**Repository:** [https://github.com/toolsproficiogit/shoptet-bq-ingest](https://github.com/toolsproficiogit/shoptet-bq-ingest)
