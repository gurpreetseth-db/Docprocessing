# Service Plan Document Intelligence Platform

> **AI-powered document processing for Home Based Support Services (HBSS)**
> Built on Databricks with Unity Catalog, AI Functions, Lakeflow Declarative Pipelines, and Databricks Apps — packaged as a **Databricks Asset Bundle (DAB)**.

Insurance/care providers' coordinators submit **Service Plan** PDFs (client details,
funder, care package, conditions, risks, goals). Auto Loader ingests them, AI Functions
parse and structure them, the pipeline **validates mandatory fields and quarantines any
document that fails**, Gold materialized views power a **Genie Agent**, and a sharp
4-tab **Databricks App** lets users submit, monitor, review data-quality rejections, and
query the data in natural language.

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
                                          │  ai_query() ─▶ typed fields     │
                                          │  dates normalized ─▶ real DATE  │
                                          │  parsed_documents               │
                                          │  service_plan_candidates        │
                                          │   ├─ valid ─▶ service_plan_extracted
                                          │   └─ invalid ─▶ service_plan_quarantine ─▶ App "Data Quality" tab
                                          └───────────────┬───────────────┘
                                                        │ (valid only)
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
| **Silver** | `ai_parse_document()` extracts text; a single `ai_query()` (Claude) structures it into typed fields in `service_plan_candidates`; dates normalized to real `DATE`. Each document is **validated for mandatory fields** and fanned out: valid → `service_plan_extracted`, invalid → `service_plan_quarantine` |
| **Validation & Quarantine** | Documents missing **client first name, gender, date of birth, address, GP name, or NHI** (or that error during extraction) are withheld from Gold and written to `service_plan_quarantine` with a human-readable `rejection_reason`. Warn-only DLT expectations also publish per-field violation rates to the pipeline's Data Quality view |
| **Gold** | Dimensional model (`dim_*` / `fact_*` / `agg_*`) built only from **valid** plans, powering analytics and Genie |
| **Databricks App** | 4-tab Dash portal: Submit & Track, Pipeline Ops, **Data Quality**, Genie Assistant |
| **Genie Agent** | Genie space auto-created after the pipeline; the app resolves it **by name** |
| **Scheduled Job** | Runs the pipeline every 4 hours, then provisions/refreshes the Genie Agent; also triggerable on-demand from the app |
| **Setup Job** | Creates catalog/schemas/volume/users, grants the app's service principal, and generates sample Service Plan PDFs — including deliberate **anomalous** PDFs (missing fields) when `generate_anomalies` is on |

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
| `generate_anomalies` | `true` | setup job (inject missing-field PDFs) |
| `anomaly_document_count` | `5` | setup job (how many bad PDFs; rotates the 5 fields) |
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

## The App (4 tabs)

1. **Submit & Track** — detects the logged-in user, lets them upload Service Plan PDFs to
   the volume, and lists their submissions with a derived Processed/Pending status. Click a
   row to view the PDF.
2. **Pipeline Ops** — trigger the pipeline on demand; shows the last 5 runs with status,
   failure reason, and the count + names of files processed per run.
3. **Data Quality** — every PDF the pipeline **rejected**, with the reason(s) it failed
   (missing first name / gender / DOB / address / GP name / NHI). Includes a live
   reason-breakdown bar chart, a free-text search, a reason filter, and clickable rows that
   open the offending PDF. The KPI ribbon gains a **Rejected** count.
4. **Genie Assistant** — natural-language Q&A over the Gold tables via the Genie Agent.

### Data Quality & Validation

The Silver pipeline validates every document against six mandatory fields (client first
name, gender, date of birth, address, GP name, NHI). A document missing any of them — or
one whose AI extraction errors — is **withheld from Gold** and written to
`${silver_schema}.service_plan_quarantine` with a semicolon-joined `rejection_reason`. Only
valid plans reach `service_plan_extracted` and the Gold model, so analytics and Genie are
never polluted by incomplete records.

To demo this end-to-end, the setup job can inject deliberately anomalous PDFs
(`generate_anomalies: true`, `anomaly_document_count: 5`). Each bad PDF blanks one mandatory
field, rotating through the five so **every rejection reason appears at least once**. Set
`generate_anomalies: false` to generate only clean documents. Editing the validation rules
after data already exists? Re-run with `--full-refresh-all` (see below) so the whole corpus
is re-validated.

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

## Performance & incremental processing

The pipeline only ever processes **new** files — the expensive AI functions never re-run
over the whole corpus:

- **Bronze** streaming tables use Auto Loader, which checkpoints processed files and
  ingests only new ones.
- **Silver** `parsed_documents` and `service_plan_extracted` are **streaming tables**, so
  `ai_parse_document` and the `ai_query` (Claude) extraction run **once per new document**
  and append — not on every run.
- **Gold** materialized views refresh incrementally (Enzyme) over the appended rows.
- **Delta layout:** liquid clustering (`CLUSTER BY`) on common filter/join columns
  (region, referral_month, nhi, submitter_email) across Bronze/Silver/Gold; the setup job
  enables **Predictive Optimization** on the catalog so `OPTIMIZE`/`VACUUM`/clustering
  maintenance is automatic.

**Re-processing everything on purpose.** Because Silver is incremental, changing the
extraction prompt/schema only affects *new* documents. To re-run the AI over **all**
existing documents (e.g. after editing `02_silver_processing.sql`), do a full refresh:

```bash
# 1) Full refresh of the pipeline — re-parses + re-extracts EVERY document, rebuilds Gold
databricks bundle run doc_processing_pipeline -t dev -p docprocessing --full-refresh-all
# 2) Rebuild the metric views + refresh the Genie agent afterwards
databricks bundle run doc_processing_job -t dev -p docprocessing
```

## Roadmap

- **Unity Catalog Metric Views** — governed business metrics (care capacity & hours, intake
  funnel, risk & compliance, demographics) for use in Genie and AI/BI dashboards. Planned
  as the next iteration, once processed data has been reviewed.

## Notes

- All sample data is **synthetic** — no real PII.
- Silver normalizes NZ day-first dates (`dd.mm.yyyy` / `dd/mm/yyyy`) and ISO to real
  `DATE` values, so Gold's date math (time-to-start, monthly trends) is correct.
