# 🌐 Shoptet → BigQuery Cloud Run Ingest

This solution runs entirely in **Google Cloud Platform (GCP)** — no local installation is needed.  
It downloads your **Shoptet CSV exports** and loads them into **BigQuery**, automatically and securely.

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
3. From here, follow the steps below to clone and deploy the service. For the included commands, just copy/paste them into the Cloud Shell.

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

This mode loads one CSV → one BigQuery table.

### ⚙️ Deploy via Cloud Shell

Open Cloud Shell and run:

```bash
git clone https://github.com/toolsproficiogit/shoptet-bq-ingest.git
cd shoptet-bq-ingest
chmod +x scripts/*.sh
./scripts/deploy_single.sh
```

During the prompt-based setup, you’ll provide:

| Prompt | Description |
|---------|--------------|
| **Project ID** | Your GCP project name |
| **Region** | Recommended: `europe-west1` |
| **CSV URL** | Direct Shoptet export URL |
| **BigQuery Table ID** | Format: `project.dataset.table` |
| **BigQuery Location** | e.g. `EU` |
| **Window Days** | Number of days for incremental updates (default: 30) |

🧠 **Tip:** First run loads full history automatically if the table is empty.

---

## 🧩 Multi-Pipeline Setup

Use one Cloud Run service for multiple CSVs → multiple tables.

### ⚙️ Step 1. Edit your configuration file

```bash
cp config/config.example.yaml config/config.yaml
nano config/config.yaml
```

Each entry defines one pipeline:

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

### ⚙️ Step 2. Deploy

```bash
chmod +x scripts/*.sh
./scripts/deploy_multi.sh
```

You’ll be asked whether to use a **baked** YAML (included in the container) or a **remote** YAML (hosted on GCS).

### 📡 Step 3. Trigger manually after deployment

```bash
SERVICE_URL="<YOUR_SERVICE_URL>"
ID_TOKEN=$(gcloud auth print-identity-token)
curl -s -H "Authorization: Bearer $ID_TOKEN" "${SERVICE_URL}/run" | jq
```

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

---

## ⏰ Scheduling (Automation)

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

| Example | Meaning |
|----------|----------|
| `0 * * * *` | Every hour |
| `0 6 * * *` | Every day at 06:00 UTC |
| `*/15 * * * *` | Every 15 minutes |

---

## 🔁 Managing Pipelines (Multi-mode)

### ➕ Add a pipeline
Edit `config/config.yaml` or upload an updated YAML using:

```bash
cd scripts
./upload_config.sh
```

This uploads your local YAML to a GCS bucket and links it to the service.

### 🗑 Remove or modify a pipeline
Just edit the YAML (locally or remotely hosted) and re-run `upload_config.sh` — the next scheduled or manual run will apply changes.

### 🔁 Force reload of all data
Temporarily set:
```bash
gcloud run services update <SERVICE_NAME>   --region <REGION>   --set-env-vars LOAD_MODE=full
```
Then run `/run` manually once, and revert to `LOAD_MODE=auto`.

---

## 🧾 Verify & Trigger Manually

```bash
SERVICE_URL="<YOUR_SERVICE_URL>"
ID_TOKEN=$(gcloud auth print-identity-token)
curl -s -H "Authorization: Bearer $ID_TOKEN" "${SERVICE_URL}/run" | jq
```

To run a single pipeline in multi-mode:
```bash
curl -s -H "Authorization: Bearer $ID_TOKEN" "${SERVICE_URL}/run?pipeline=<PIPELINE_ID>" | jq
```

---

## 🧹 Teardown (Clean up)

### 🔻 Delete Scheduler job and Cloud Run service

```bash
cd scripts
./teardown.sh
```

Prompts for:
- Project ID
- Region
- Service name (e.g., `shoptet-bq-ingest`)
- Scheduler job name (optional)

Manually verify cleanup:
```bash
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

## 💡 Summary of Key Commands

| Action | Command |
|---------|----------|
| Test service | `curl -s -H "Authorization: Bearer $(gcloud auth print-identity-token)" "<SERVICE_URL>/run" | jq` |
| Deploy single | `./scripts/deploy_single.sh` |
| Deploy multi | `./scripts/deploy_multi.sh` |
| Schedule single | `./scripts/schedule_single.sh` |
| Schedule multi | `./scripts/schedule_multi.sh` |
| Upload YAML config | `./scripts/upload_config.sh` |
| Delete service/schedule | `./scripts/teardown.sh` |

---

**Repository:** [https://github.com/toolsproficiogit/shoptet-bq-ingest](https://github.com/toolsproficiogit/shoptet-bq-ingest)
