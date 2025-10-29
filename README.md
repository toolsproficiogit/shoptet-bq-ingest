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

You must create the dataset that your tables will live in. Remember the **dataset ID** and its **location** (e.g., `EU`).

```bash
# 🔧 Replace placeholders before running
PROJECT_ID="<YOUR_PROJECT_ID>"
DATASET_ID="<YOUR_DATASET_ID>"     # choose yourself, e.g. shoptet_data
BQ_LOCATION="<BQ_LOCATION>"         # choose a convenient location, e.g. EU

bq --project_id="${PROJECT_ID}" --location="${BQ_LOCATION}" mk -d "${PROJECT_ID}:${DATASET_ID}"
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

### ✅ Grant the roles (admin or owner runs this)

```bash
PROJECT_ID="<YOUR_PROJECT_ID>"
USER_EMAIL="<YOUR_EMAIL>"

for ROLE in \
  roles/run.admin \
  roles/cloudscheduler.admin \
  roles/iam.serviceAccountAdmin \
  roles/iam.serviceAccountUser \
  roles/cloudbuild.builds.editor \
  roles/artifactregistry.admin \
  roles/serviceusage.serviceUsageAdmin
do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="user:${USER_EMAIL}" \
    --role="$ROLE" \
    --condition=None
done
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

You will see the **Service URL** printed in the Cloud Shell. Alternatively, you can check it in Cloud Console, if you navigate to Cloud Run > Services > load-csv-to-bigquery (your chosen service name) > URL is displayed at the top. It should look like this: https://load-csv-to-bigquery-111111111111.europe-west1.run.app

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
  - id: orders
    csv_url: https://example.com/orders.csv
    bq_table_id: myproject.sales.orders
    load_mode: auto
    window_days: 30
  - id: shipping
    csv_url: https://example.com/shipping.csv
    bq_table_id: myproject.sales.shipping
    load_mode: window
    window_days: 14
```

### 2) Deploy (uploads YAML to GCS, sets CONFIG_URL, deploys service)
```bash
chmod +x scripts/*.sh
./scripts/deploy_multi.sh
```
The script will prompt for:
- Project, Region, Service name
- **Local YAML path** (e.g., `config/config.yaml`)
- **GCS bucket** (will be created if missing)
- **Object name** (e.g., `shoptet_config.yaml`)

It will:
1) Upload the YAML to `gs://<bucket>/<object>`  
2) Deploy Cloud Run with `MULTI_MODE=true` and `CONFIG_URL=https://storage.googleapis.com/<bucket>/<object>`  
3) Print the service URL and a test `curl`

### 3) Trigger test (all pipelines)
```bash
SERVICE_URL="<YOUR_SERVICE_URL>"
ID_TOKEN=$(gcloud auth print-identity-token)
curl -s -H "Authorization: Bearer $ID_TOKEN" "${SERVICE_URL}/run" | jq
```

Run a **single pipeline** by ID:
```bash
curl -s -H "Authorization: Bearer $ID_TOKEN" "${SERVICE_URL}/run?pipeline=<PIPELINE_ID>" | jq
```

You will see the **Service URL** printed in the Cloud Shell. Alternatively, you can check it in Cloud Console, if you navigate to Cloud Run > Services > load-csv-to-bigquery (your chosen service name) > URL is displayed at the top. It should look like this: https://load-csv-to-bigquery-111111111111.europe-west1.run.app

---

## Add/Remove Pipelines on an already-deployed service

You deployed previously, and the service is running. Now you want to change pipelines. Start a **new Cloud Shell session** (described at the start of the README). 

### A) Find the CONFIG_URL
```bash
SERVICE="<SERVICE_NAME>"     # e.g., shoptet-bq-multi
REGION="<REGION>"            # e.g., europe-west1
gcloud run services describe "$SERVICE" --region "$REGION" --format=json   | jq '.spec.template.spec.containers[0].env'

# Pull just the URL
CONFIG_URL=$(gcloud run services describe "$SERVICE" --region "$REGION"   --format="value(spec.template.spec.containers[0].env.list().filter(env, env.name='CONFIG_URL').0.value)")
echo "$CONFIG_URL"
```

Extract bucket/object (optional):
```bash
BUCKET=$(echo "$CONFIG_URL" | sed -E 's#https?://storage.googleapis.com/([^/]+)/.*#\1#')
OBJECT=$(echo "$CONFIG_URL" | sed -E 's#https?://storage.googleapis.com/[^/]+/(.*)#\1#')
echo "Bucket=$BUCKET  Object=$OBJECT"
```

### B) Download, edit visually, re‑upload
```bash
mkdir -p config
gsutil cp "gs://${BUCKET}/${OBJECT}" config/config.yaml

cloudshell edit config/config.yaml    # add/remove pipelines, Save

gsutil cp config/config.yaml "gs://${BUCKET}/${OBJECT}"
```

### C) Trigger and verify
```bash
SERVICE_URL="<YOUR_SERVICE_URL>"
ID_TOKEN=$(gcloud auth print-identity-token)
curl -s -H "Authorization: Bearer $ID_TOKEN" "${SERVICE_URL}/run" | jq
```

> Scheduler jobs do **not** need changes. Next scheduled run uses the new YAML.

---

## ⏰ Scheduling (Automation)

Here you can select refresh frequency with which the BigQuery table will be updated. 

### 🔹 Single-pipeline schedule

```bash
cd scripts
./schedule_single.sh
```

Prompts you for:
- Project ID & Region
- Job name (e.g. `daily-shoptet-bq`)
- Cron schedule (see below)

### 🔹 Multi-pipeline schedule

```bash
cd scripts
./schedule_multi.sh
```

You can schedule **all pipelines** or a single one by ID.

### ⏱ About cron expressions

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
cd scripts
./teardown.sh

gcloud run services list --region <REGION>
gcloud scheduler jobs list --location <REGION>
```

---

## 📊 Verify BigQuery Data

After a successful run, check your dataset:

```sql
SELECT * FROM `<PROJECT>.<DATASET>.<TABLE>`
ORDER BY date DESC
LIMIT 20;
```

---

**Repository:** [https://github.com/toolsproficiogit/shoptet-bq-ingest](https://github.com/toolsproficiogit/shoptet-bq-ingest)
