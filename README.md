# Service Plan Document Intelligence Platform

> **AI-powered document processing for Home Based Support Services (HBSS)**
> Built on Databricks with Unity Catalog, AI Functions, Lakeflow Declarative Pipelines, and Databricks Apps — packaged as a Databricks Asset Bundle.

Insurance/care providers' coordinators submit **Service Plan** PDFs (client details,
funder, care package, conditions, risks, goals). Auto Loader ingests them, AI Functions
parse and structure them, Gold materialized views power a **Genie** space, and a sharp
3-tab **Databricks App** lets users submit, monitor, and query the data in natural language.

---

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌───────────────────────────────┐
│ Coordinators│     │  UC Volume     │     │         BRONZE LAYER            │
│  (via App)  │───▶│   InputPDFs    │───▶│  Auto Loader (Streaming Tables) │
│  or Setup   │     │  Landing Zone  │     │  raw_documents                  │
│    Job      │     └──────────────┘     │  document_submissions           │
└─────────────┘                           └───────────────┬───────────────┘
                                                        │
                                          ┌─────────────▼─────────────────┐
                                          │         SILVER LAYER            │
                                          │  ai_parse_document() ─▶ text    │
                                          │  ai_extract() ─▶ typed fields   │
                                          │  parsed_documents               │
                                          │  service_plan_extracted         │
                                          └───────────────┬───────────────┘
                                                        │
                                          ┌─────────────▼─────────────────┐
                                          │          GOLD LAYER (MVs)       │
                                          │  service_plans                  │
                                          │  care_hours_by_region           │
                                          │  conditions_summary             │
                                          │  services_demand                │
                                          │  risk_register                  │
                                          │  processing_overview            │
                                          └───────────────┬───────────────┘
                                                        │
                                          ┌─────────────▼─────────────────┐
                                          │       GENIE SPACE               │
                                          │  "Doc Processing Helper"        │
                                          └───────────────────────────────┘
```

## Components

| Component | Description |
|-----------|-------------|
| **Bronze** | Auto Loader ingests PDFs as binary; `document_submissions` parses the filename convention into submitter email + submission id |
| **Silver** | `ai_parse_document()` extracts text; `ai_extract()` structures it into typed Service Plan fields |
| **Gold** | Materialized views for care hours by region, conditions, services demand, risk register, and processing overview |
| **Databricks App** | 3-tab Dash portal: Submit & Track, Pipeline Ops, Genie Assistant |
| **Genie Space** | Natural-language Q&A over Gold tables — "Doc Processing Helper" |
| **Scheduled Job** | Runs the pipeline every 4 hours; also triggerable on-demand from the app |
| **Setup Job** | Creates catalog/schemas/volume/users and generates 20 sample Service Plan PDFs |

## Project Structure

```
Docprocessing/
├── databricks.yml              # Bundle definition + variables + targets
├── config.yml                  # Central config: env, catalog, schemas, genie, app names
├── README.md
├── resources/
│   ├── pipeline.yml            # Lakeflow pipeline (Bronze→Silver→Gold, serverless)
│   ├── job.yml                 # Scheduled pipeline job + one-off setup job
│   └── app.yml                 # Databricks App resource (job wired in)
└── src/
    ├── pipeline/
    │   ├── 01_bronze_ingestion.sql
    │   ├── 02_silver_processing.sql
    │   └── 03_gold_aggregations.sql
    ├── app/
    │   ├── app.py              # Dash app (dark glassmorphism, 3 tabs, Genie via SDK)
    │   ├── app.yaml            # App runtime config + env wiring
    │   └── requirements.txt
    └── setup/
        └── generate_sample_data.py   # Catalog/schemas/volume/users + sample PDFs
```

## Configuration

All environment-specific names live in **`config.yml`** (environment name, workspace host,
CLI profile, catalog, bronze/silver/gold schemas, volume, Genie space name, app name,
sample-data counts, schedule). The deployable defaults are mirrored in `databricks.yml`
under `variables:` — edit both when retargeting.

| Setting | Default |
|---------|---------|
| Catalog | `DocProcessing` |
| Bronze schema | `DocProcess_Bronze` |
| Silver schema | `DocProcess_Silver` |
| Gold schema | `DocProcess_Gold` |
| Volume | `InputPDFs` |
| Genie space | `Doc Processing Helper` |
| Workspace | `https://dbc-dedf1927-d4f5.cloud.databricks.com` (profile `docprocessing`) |

## Prerequisites

- Databricks workspace with **Unity Catalog** + **Serverless** enabled
- **AI Functions** available (`ai_parse_document`, `ai_extract`)
- Databricks CLI v0.240+ (tested with v1.0.0)
- A SQL Warehouse (for the app's queries)

## Deploy

```bash
# 1. Validate
databricks bundle validate -t dev -p docprocessing

# 2. Deploy (creates pipeline, jobs, and the app)
databricks bundle deploy -t dev -p docprocessing

# 3. Generate sample data (catalog, schemas, volume, users, 20 PDFs)
databricks bundle run doc_processing_setup -t dev -p docprocessing

# 4. Run the processing pipeline (Bronze → Silver → Gold)
databricks bundle run doc_processing_job -t dev -p docprocessing
```

## Post-deploy wiring

Two ids aren't known until after deploy — set them so the app is fully live:

1. **SQL Warehouse** — copy a warehouse id and set `DATABRICKS_WAREHOUSE_ID` in the
   App's *Environment* settings (or in `src/app/app.yaml`).
2. **Genie space** — create a Genie space named **"Doc Processing Helper"** with these
   Gold tables, then set `GENIE_SPACE_ID`:
   - `DocProcessing.DocProcess_Gold.service_plans`
   - `DocProcessing.DocProcess_Gold.care_hours_by_region`
   - `DocProcessing.DocProcess_Gold.conditions_summary`
   - `DocProcessing.DocProcess_Gold.services_demand`
   - `DocProcessing.DocProcess_Gold.risk_register`
   - `DocProcessing.DocProcess_Gold.processing_overview`

The app degrades gracefully while these are empty (tabs render; Genie/queries activate
once ids are set).

## The App (3 tabs)

1. **Submit & Track** — detects the logged-in user from their email, shows a welcome
   message, lets them upload Service Plan PDFs to the volume, and lists their own
   submissions with processing status.
2. **Pipeline Ops** — trigger the pipeline on demand; shows the last 5 runs with
   status, failure reason, and the count + names of files processed per run.
3. **Genie Assistant** — natural-language Q&A over the Gold tables.

## Sample Questions for Genie

- "What are the total weekly care hours by region?"
- "Which regions have the most clients funded by DHB?"
- "How many clients have a fall risk but no completed manual handling plan?"
- "What are the most in-demand services?"
- "Show the count of clients per primary condition."

## Filename Convention

PDFs land as `{email_slug}__{submission_id}__service_plan.pdf`, where `email_slug`
encodes the submitter's email (`@`→`_at_`, `.`→`_dot_`). Bronze parses this back into
`submitter_email` so each submission is attributed to a user.

## Notes

- All sample data is **synthetic** — no real PII.
- The attached `ServicePlan_example` PDF is the **visual/layout reference**; generated
  PDFs mirror its sectioned, professional government-care-form look.
