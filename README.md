# Service Plan Document Intelligence Platform

> **AI-powered document processing for Home Based Support Services (HBSS)**
> Built on Databricks with Unity Catalog, AI Functions, Lakeflow Declarative Pipelines, and Databricks Apps — packaged as a **Databricks Asset Bundle (DAB)**.

Insurance/care providers' coordinators submit **Service Plan** PDFs (client details,
funder, care package, conditions, risks, goals). Auto Loader ingests them, AI Functions
parse and structure them, Gold materialized views power a **Genie Agent**, and a sharp
3-tab **Databricks App** lets users submit, monitor, and query the data in natural language.

Everything is **config-driven and self-provisioning**: one file (`config.yml`) defines all
names, and a single `deploy` + two `run` commands stand up the catalog, schemas, volume,
sample data, pipeline, app, and the Genie Agent — no manual id wiring.

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
                                          │  dates normalized ─▶ real DATE  │
                                          │  parsed_documents               │
                                          │  service_plan_extracted         │
                                          └───────────────┬───────────────┘
                                                        │
                                          ┌─────────────▼─────────────────┐
                                          │       GOLD LAYER (dim/fact MVs) │
                                          │  dim_client                     │
                                          │  dim_care_coordinator           │
                                          │  fact_service_plan              │
                                          │  fact_service_demand_by_region  │
                                          │  fact_risk_profile              │
                                          │  agg_intake_funnel              │
                                          └───────────────┬───────────────┘
                                                        │
                                          ┌─────────────▼─────────────────┐
                                          │       GENIE AGENT (auto)        │
                                          │  space named per config.yml     │
                                          │  provisioned after the pipeline  │
                                          └───────────────────────────────┘
```

## Components

| Component | Description |
|-----------|-------------|
| **Bronze** | Auto Loader ingests PDFs as binary; `document_submissions` parses the folder path into submitter email + submission id |
| **Silver** | `ai_parse_document()` extracts text; `ai_extract()` structures it into typed Service Plan fields; dates are normalized to real `DATE` |
| **Gold** | Dimensional model (`dim_*` / `fact_*` / `agg_*`) powering analytics and Genie |
| **Databricks App** | 3-tab Dash portal: Submit & Track, Pipeline Ops, Genie Assistant |
| **Genie Agent** | Genie space auto-created after the pipeline; the app resolves it **by name** |
| **Scheduled Job** | Runs the pipeline every 4 hours, then provisions/refreshes the Genie Agent; also triggerable on-demand from the app |
| **Setup Job** | Creates catalog/schemas/volume/users, grants the app's service principal, and generates sample Service Plan PDFs |

## Project Structure

```
Docprocessing/
├── databricks.yml              # Bundle root: name, include, targets
├── config.yml                  # ★ SINGLE SOURCE OF TRUTH (bundle variables)
├── README.md
├── resources/
│   ├── pipeline.yml            # Lakeflow pipeline (Bronze→Silver→Gold, serverless)
│   ├── job.yml                 # Scheduled pipeline+Genie job; one-off setup job
│   ├── app.yml                 # Databricks App (env + warehouse/job resources)
│   └── service_plan_intelligence.dashboard.yml
└── src/
    ├── pipeline/
    │   ├── 01_bronze_ingestion.sql     # ${catalog}/${bronze_schema}/${volume_name}
    │   ├── 02_silver_processing.sql    # ${...} + date normalization
    │   └── 03_gold_aggregations.sql    # ${...} dimensional model
    ├── app/
    │   ├── app.py              # Dash app; resolves Genie by name
    │   ├── app.yaml            # minimal runtime manifest (command only)
    │   └── requirements.txt
    ├── setup/
    │   ├── generate_sample_data.py     # catalog/schemas/volume/users/grants + PDFs
    │   └── provision_genie.py          # idempotent Genie Agent creation
    └── service_plan_intelligence.lvdash.json
```

## Configuration — one file

All environment-specific values live in **`config.yml`** as Databricks Asset Bundle
**variables**. It is `include`d by `databricks.yml`, so the bundle, resources, pipeline
SQL, setup/genie notebooks, and the app all read from it — nothing is duplicated.

| Variable | Default | Used by |
|----------|---------|---------|
| `catalog` | `DocProcessing` | everything |
| `bronze_schema` | `DocProcess_Bronze` | pipeline, setup, app |
| `silver_schema` | `DocProcess_Silver` | pipeline, app |
| `gold_schema` | `DocProcess_Gold` | pipeline, genie, app |
| `volume_name` | `InputPDFs` | pipeline, setup, app |
| `genie_space_name` | `Doc Processing Helper` | genie provisioning, app (resolve-by-name) |
| `app_name` | `doc-processing-app` | app resource, grants |
| `warehouse_id` | `29b33b8b6a20b116` | app, genie, dashboard |
| `num_users` / `num_documents` | `10` / `20` | setup job |
| `processing_cron` / `timezone` | every 4h / `UTC` | scheduled job |

To retarget: edit the defaults in `config.yml` (or override per target under
`targets.<name>.variables` in `databricks.yml`), then `validate` + `deploy`.

## Prerequisites

- Databricks workspace with **Unity Catalog** + **Serverless** enabled
- **AI Functions** available (`ai_parse_document`, `ai_extract`)
- Databricks CLI v0.240+ (tested with v1.0.0)
- A SQL Warehouse (id set in `config.yml` → `warehouse_id`)

## Deploy

```bash
# 1. Validate
databricks bundle validate -t dev -p docprocessing

# 2. Deploy (creates pipeline, jobs, app, dashboard)
databricks bundle deploy -t dev -p docprocessing

# 3. Setup: catalog, schemas, volume, users, app-SP grants, and 20 sample PDFs
databricks bundle run doc_processing_setup -t dev -p docprocessing

# 4. Process: Bronze → Silver → Gold, THEN auto-provision the Genie Agent
databricks bundle run doc_processing_job -t dev -p docprocessing
```

That's it — no manual warehouse/Genie id wiring. The app gets its warehouse and job ids
from bundle app-resources, and finds the Genie Agent by its configured name at runtime.

> **Note on the Genie Agent:** it is created by the `provision_genie` task, which runs
> *after* the pipeline (Gold tables must exist first). Because the app resolves the space
> by name, it works on the next page load once the job completes. If the space is private,
> the provisioning step best-effort grants the app's service principal access and prints a
> one-line manual fallback if it can't.

## The App (3 tabs)

1. **Submit & Track** — detects the logged-in user, lets them upload Service Plan PDFs to
   the volume, and lists their submissions with a derived Processed/Pending status.
2. **Pipeline Ops** — trigger the pipeline on demand; shows the last 5 runs with status,
   failure reason, and the count + names of files processed per run.
3. **Genie Assistant** — natural-language Q&A over the Gold tables via the Genie Agent.

## Sample Questions for Genie

- "What are the total weekly care hours by region?"
- "Which regions have the most clients funded by DHB?"
- "How many clients have a fall risk but no completed manual handling plan?"
- "What are the most in-demand services?"
- "Show the count of clients per primary condition."
- "What is the average number of days from referral to service start by region?"

## Filename / Folder Convention

PDFs land as `InputPDFs/{email_slug}/{submission_id}/{original_name}`, where `email_slug`
encodes the submitter's email (`@`→`_at_`, `.`→`_dot_`). Bronze parses the folder path back
into `submitter_email` + `submission_id` so each submission is attributed to a user.
(Legacy flat names `{email_slug}__{submission_id}__service_plan.pdf` are still supported.)

## Roadmap

- **Unity Catalog Metric Views** — governed business metrics (care capacity & hours, intake
  funnel, risk & compliance, demographics) for use in Genie and AI/BI dashboards. Planned
  as the next iteration, once processed data has been reviewed.

## Notes

- All sample data is **synthetic** — no real PII.
- Silver normalizes NZ day-first dates (`dd.mm.yyyy` / `dd/mm/yyyy`) and ISO to real
  `DATE` values, so Gold's date math (time-to-start, monthly trends) is correct.
