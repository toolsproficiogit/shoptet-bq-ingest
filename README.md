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

| **<YOUR-SERVICE-URL>** | You will see the URL printed in the Cloud Shell. Alternatively, you can check it in Cloud Console, if you navigate to Cloud Run > Services > load-csv-to-bigquery (your chosen service name) > URL is displayed at the top. It should have this format: https://load-csv-to-bigquery-111111111111.europe-west1.run.app |

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

You can now go to BigQuery UI and check that a table was created within the defined dataset, and that it was populated with data from the CSV  (click on the table and select "Schema" to check that fields and their types match, "Details" to check if the number of rows is correct, and "Preview" to see values).

---

## 🧩 Multi-Pipeline Setup (many CSVs → many tables)

A single service runs multiple pipelines defined in a **YAML** file.

### 📘 YAML access modes (choose one)

| Mode | What it means | How you edit later |
|------|----------------|--------------------|
| **Baked** | `config/config.yaml` is bundled **inside the container** at build time. | Edit local YAML → **rebuild & redeploy** (or switch to Remote mode). |
| **Remote** | Service reads YAML from a **URL** (usually GCS) via `CONFIG_URL`. | Edit local YAML → **upload** to GCS (overwrite) → **no redeploy**. |

Check which mode you’re using:
```bash
SERVICE="<SERVICE_NAME>"; REGION="<REGION>"
gcloud run services describe "$SERVICE" --region "$REGION" --format=json  | jq '.spec.template.spec.containers[0].env'
# If you see CONFIG_URL -> Remote mode; if absent -> Baked mode
```

### ✍️ Edit YAML in **Cloud Shell Editor** (non-programmer friendly)

1. Open the Editor (top-right “<>” icon) or run:
   ```bash
   cloudshell edit config/config.yaml
   ```
2. Add/remove pipelines in the YAML.
3. Save file.

Then follow the steps below based on your mode.

### 🛠 Change pipelines **after deployment** — full process

#### A) If you deployed in **Baked** mode
1. **Edit YAML**: open `config/config.yaml` in the Editor and save.
2. **Rebuild & redeploy** (bakes the new YAML):
   ```bash
   PROJECT_ID="<PROJECT_ID>"
   REGION="<REGION>"
   SERVICE="<SERVICE_NAME>"
   REPO="containers"
   IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE}:v2"

   gcloud builds submit --tag "$IMAGE"
   gcloud run deploy "$SERVICE"      --image "$IMAGE"      --region "$REGION"      --platform managed      --no-allow-unauthenticated      --set-env-vars MULTI_MODE=true,BQ_LOCATION=<BQ_LOCATION>
   ```
3. **Trigger** to verify:
   ```bash
   SERVICE_URL="<YOUR_SERVICE_URL>"
   ID_TOKEN=$(gcloud auth print-identity-token)
   curl -s -H "Authorization: Bearer $ID_TOKEN" "${SERVICE_URL}/run" | jq
   ```
4. **Tip:** To avoid rebuilds going forward, switch to **Remote** once by running `./scripts/upload_config.sh` (see next case).

#### B) If you deployed in **Remote** mode
1. **Edit YAML**: open `config/config.yaml` in the Editor and save (or edit any local copy).
2. **Upload & link** (overwrites GCS object and sets `CONFIG_URL` if needed):
   ```bash
   cd scripts
   ./upload_config.sh
   ```
   Prompts for:
   - Project/Region/Service
   - Local YAML path (e.g., `config/config.yaml`)
   - GCS bucket + object name (creates the bucket if missing)
3. **Trigger** to verify:
   ```bash
   SERVICE_URL="<YOUR_SERVICE_URL>"
   ID_TOKEN=$(gcloud auth print-identity-token)
   curl -s -H "Authorization: Bearer $ID_TOKEN" "${SERVICE_URL}/run" | jq
   ```

> You can also upload directly with `gsutil cp config/config.yaml gs://<BUCKET>/<OBJECT>` if `CONFIG_URL` is already set on the service.

### 📦 Deploy multi for the first time

```bash
chmod +x scripts/*.sh
./scripts/deploy_multi.sh
# Choose "baked" or "url" (remote). If "url", you'll provide CONFIG_URL later via upload_config.sh.
```

Trigger all pipelines:
```bash
SERVICE_URL="<YOUR_SERVICE_URL>"
ID_TOKEN=$(gcloud auth print-identity-token)
curl -s -H "Authorization: Bearer $ID_TOKEN" "${SERVICE_URL}/run" | jq
```

Run a **single pipeline** by ID:
```bash
curl -s -H "Authorization: Bearer $ID_TOKEN" "${SERVICE_URL}/run?pipeline=<PIPELINE_ID>" | jq
```

---

| **<YOUR-SERVICE-URL>** | An URL that will trigger the run, you will see it printed in the Cloud Shell after the service is deployed. Alternatively, you can check it in the UI, if you go Cloud Run > Services > load-csv-to-bigquery (or a different name you entered for the service name) > URL is displayed at the top |

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

You can now go to BigQuery UI and check that tables were created within the defined datasets, and that they were populated with data from the CSVs (click on the table and select "Schema" to check that fields and their types match, "Details" to check if the number of rows is correct, and "Preview" to see values).

---

## ⏰ Scheduling (Automation)

Here you can select refresh frequency with which the BigQuery table will be updated. You can choose anywhere between 

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

## 🔁 Managing Pipelines After Deployment (Multi-mode)

### ➕ Add a pipeline
Edit `config/config.yaml` or upload an updated YAML using:

```bash
cd scripts
./upload_config.sh
```

This uploads your local YAML to a Google Cloud Storage bucket and links it to the service.

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
