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

> **For a more advanced implementation, you can change the name and location of the dataset. You can even create multiple datasets by running the command repeatedly. However, make sure to remember the names and the corresponding locations as they will be used later.*

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

A single service that runs multiple pipelines (CSV links and target tables) defined in a remote **YAML** file, using a custom schema config. You define settings individually for each pipeline.

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
  - id: client1_orders
    export_type: orders
    csv_url: https://example.com/shoptet.csv
    bq_table_id: <PROJECT>.<DATASET>.<TABLE>
    load_mode: auto
    window_days: 30

  - id: client1_customers
    export_type: customers  
    csv_url: https://example.com/shoptet.csv
    bq_table_id: <PROJECT>.<DATASET>.<TABLE>
    load_mode: auto
    window_days: 30
```

You can add/remove and edit pipelines, but make sure to keep **correct formatting** (consistent indentation, spaces, number of enters)

Make sure the datasets for your tables (remember names from the first step)!

### 3) Schema library (define columns and formats in the CSV)

You can edit or add schemas that the service will expect from incoming CSVs. You can control the processing of each CSV pipeline in the export_type field. There are 3 premade CSV configurations (based on custom Shoptet exports), **If your export matches one of these, you do not need to create new ones.**

You can inspect the default schemas in the `config/schemas.yaml` file.

When editing these, make sure the column names and data types **exactly match** your CSVs (including hidden characters, trailing spaces etc.), otherwise the columns will be unprocessed. The **parse** refers to the data type as it appears in the CSV, The **type** refers to the data type this field will be saved as to BigQuery.

### 2) Deploy (uploads YAML to GCS, sets CONFIG_URL, deploys service)
```bash
chmod +x scripts/*.sh
./scripts/deploy_multi.sh
```
The script will choose defaults for the following options. To confirm default, press **ENTER**, otherwise input your own values (only if you know what you are doing):

| Setting | Default |
|--------|----------------|
| **Region** | europe-west1 |
| **Service name** | csv-bq-multi |
| **Pipelines file** | config/config.yaml |
| **Schemas file** | config/schemas.yaml |
| **BigQuery location** | EU|
| **GCS bucket** | csv-config-<PROJECT_ID> |
| **Mode** | auto (full history on first run, then only last <Window days>) |
| **Window days** | 30 |

It will:
1) Validate both YAMLs
2) Warn if multiple pipelines target the same table (must confirm "Y/N")
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

The script will choose defaults for the following options. To confirm default, press **ENTER**, otherwise input your own values (at your own risk, if you know what you are doing):

| Setting | Default |
|--------|----------------|
| **Region** | europe-west1 (needs to match service location) |
| **Service name** | csv-bq-multi (needs to match created service) |
| **Scheduler job name** | daily-csv-bq |
| **CRON** | 0 6 * * * (every day at 6 AM, change for different frequency - see more info at the end) |
| **Pipeline ID** | all pipelines (input IDs to only create for some pipelines) |

---

## Manage pipelines and schemas on deployed service

You deployed previously, and the service is running. Now you want to change pipelines on the fly. That's very easy, and doesn't require any redeployment. Start a **new Cloud Shell session** (described at the start of the README). 

### A) Edit pipeline configs
```bash
SERVICE="csv-bq-multi"; REGION="europe-west1"

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
SERVICE="csv-bq-multi"; REGION="europe-west1"

SCHEMA_URL=$(gcloud run services describe "$SERVICE" --region "$REGION" --format=json \
  | jq -r '.spec.template.spec.containers[0].env[] | select(.name=="SCHEMA_URL") | .value')
SBUCKET=$(echo "$SCHEMA_URL" | sed -E 's#https?://storage.googleapis.com/([^/]+)/.*#\1#')
SOBJECT=$(echo "$SCHEMA_URL" | sed -E 's#https?://storage.googleapis.com/[^/]+/(.*)#\1#')

mkdir -p config && gsutil cp "gs://${SBUCKET}/${SOBJECT}" config/schemas.yaml
cloudshell edit config/schemas.yaml
```

Edit the schema file, add or remove export types/fields, save, and:
```bash
gsutil cp config/schemas.yaml "gs://${SBUCKET}/${SOBJECT}"
```

### Retrigger run manually
```bash
SERVICE_URL=$(gcloud run services describe csv-bq-multi --region europe-west1 --format='value(status.url)')
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

## Updating a deployed service to allow unknown columns

If your CSV exports include unexpected or renamed fields and you don't want the pipeline to stop,  
you can enable a safe fallback mode so the service skips unrecognized columns. **Only enable this if you know what you are doing!** Otherwise it is preferable to edit the schema file to include the missing columns. 

### One-command setup (copy-paste)

Run this in Cloud Shell while in the repo directory:
```bash
./scripts/update_env_allow_unknown.sh
```

You can verify with:
```bash
gcloud run services describe csv-bq-multi \
  --region europe-west1 \
  --format="table(spec.template.spec.containers[].env[].name,spec.template.spec.containers[].env[].value)"
```

Once this is set, all scheduled and manual runs **automatically skip unknown columns**
(e.g., new or extra headers in the CSV).

## Teardown (clean-up)

```bash
REGION="europe-west1"
gcloud run services delete csv-bq-multi --region "$REGION" -q
gcloud scheduler jobs delete daily-csv-bq --location "$REGION" -q
```

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
