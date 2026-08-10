# Insurance Document Intelligence Platform

> **AI-powered document processing for Pacific Shield Insurance Group**  
> Built on Databricks with Unity Catalog, AI Functions, Lakeflow Pipelines, and Databricks Apps

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                    DATA FLOW ARCHITECTURE                         │
└──────────────────────────────────────────────────────────────────────┘

┌─────────────┐     ┌──────────────┐     ┌───────────────────────────────┐
│  Insurance  │     │    UC Volume   │     │         BRONZE LAYER            │
│   Agents    │───▶│   InputPDFs    │───▶│  Auto Loader (Streaming Table)  │
│  (via App)  │     │  Landing Zone  │     │  raw_documents                  │
└─────────────┘     └──────────────┘     │  document_submissions           │
                                          └───────────────┬───────────────┘
                                                        │
                                                        ▼
                                          ┌───────────────────────────────┐
                                          │         SILVER LAYER            │
                                          │  ai_parse_document() ─▶ parse   │
                                          │  ai_extract() ─▶ structure      │
                                          │  parsed_documents               │
                                          │  extracted_insurance_data       │
                                          └───────────────┬───────────────┘
                                                        │
                                                        ▼
                                          ┌───────────────────────────────┐
                                          │          GOLD LAYER             │
                                          │  sales_summary                  │
                                          │  claims_processed               │
                                          │  outstanding_claims             │
                                          │  processing_overview            │
                                          └───────────────┬───────────────┘
                                                        │
                                          ┌─────────────▼─────────────────┐
                                          │       GENIE AGENT               │
                                          │  "Doc Processing Helper"        │
                                          │  Natural Language Queries       │
                                          └───────────────────────────────┘
```

## Components

| Component | Description |
|-----------|-------------|
| **Bronze Layer** | Auto Loader ingests PDFs as binary from UC Volume |
| **Silver Layer** | `ai_parse_document()` extracts text; `ai_extract()` structures it into typed fields |
| **Gold Layer** | Materialized views for sales, claims processed, outstanding claims, and processing overview |
| **Databricks App** | 3-tab portal: document submission, pipeline ops, Genie chat |
| **Genie Space** | Natural language Q&A over Gold tables |
| **Scheduled Job** | Runs pipeline every 4 hours |

## Prerequisites

- Databricks workspace with **Unity Catalog** enabled
- **Serverless compute** enabled
- **AI Functions** enabled (`ai_parse_document`, `ai_extract`)
- Databricks CLI >= 0.239.0
- Python 3.10+

## Quick Start

### 1. Deploy the Bundle

```bash
# Install Databricks CLI if not already
pip install databricks-cli

# Authenticate
databricks auth login --host <your-workspace-url>

# Validate the bundle
databricks bundle validate --target dev

# Deploy
databricks bundle deploy --target dev
```

### 2. Run Setup Notebook

Open and run `src/setup/generate_sample_data.py` in your workspace. This creates:
- Catalog and schemas
- UC Volume for PDF landing
- 10 sample users
- 20 realistic insurance PDFs

### 3. Run the Pipeline

```bash
databricks bundle run doc_processing_pipeline --target dev
```

Or trigger from the App's Pipeline Ops tab.

### 4. Create Genie Space

In the Databricks UI:
1. Navigate to **Genie** > **New Space**
2. Name: "Doc Processing Helper"
3. Add Gold layer tables:
   - `DocProcessing.DocProcess_Gold.sales_summary`
   - `DocProcessing.DocProcess_Gold.claims_processed`
   - `DocProcessing.DocProcess_Gold.outstanding_claims`
   - `DocProcessing.DocProcess_Gold.processing_overview`
4. Copy the Space ID for app configuration

### 5. Configure and Deploy App

Set environment variables in the App configuration:
- `GENIE_SPACE_ID`: From step 4
- `JOB_ID`: From bundle deploy output
- `PIPELINE_ID`: From bundle deploy output
- `DATABRICKS_WAREHOUSE_ID`: Your SQL warehouse ID

## Project Structure

```
Docprocessing/
├── databricks.yml              # Bundle configuration
├── README.md                   # This file
├── resources/
│   ├── pipeline.yml            # SDP pipeline + schemas + volumes
│   ├── job.yml                 # Scheduled processing job
│   └── app.yml                 # Databricks App resource
└── src/
    ├── pipeline/
    │   ├── 01_bronze_ingestion.sql     # Auto Loader
    │   ├── 02_silver_processing.sql    # AI parsing & extraction
    │   └── 03_gold_aggregations.sql    # Business MVs
    ├── app/
    │   ├── app.py                      # Dash application
    │   ├── app.yaml                    # App runtime config
    │   └── requirements.txt            # Python dependencies
    └── setup/
        └── generate_sample_data.py     # Data bootstrapper
```

## Tech Stack

- **Databricks Unity Catalog** - Data governance & catalog
- **Lakeflow Spark Declarative Pipelines** - ETL orchestration
- **Auto Loader** - Incremental file ingestion
- **AI Functions** - `ai_parse_document()`, `ai_extract()`
- **Materialized Views** - Pre-computed Gold aggregations
- **Databricks Apps** - Hosted web application
- **Dash + Plotly** - Interactive web UI framework
- **Genie API** - Natural language data Q&A
- **Declarative Automation Bundles** - Infrastructure as code

## Sample Questions for Genie

- "What are the total sales by region for July 2026?"
- "Which agents have the most outstanding claims?"
- "Show me claims pending due to fraud investigation"
- "What is the average processing amount per product line?"
- "Compare sales performance between Northeast and West regions"

## License

Internal use - Pacific Shield Insurance Group
