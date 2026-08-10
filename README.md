# Insurance Document Intelligence Platform

> AI-powered document processing for Pacific Shield Insurance Group
> Built on Databricks with Unity Catalog, AI Functions, Lakeflow Pipelines, and Databricks Apps

## Architecture

```
PDFs (Agents) --> UC Volume (InputPDFs) --> Auto Loader --> BRONZE (raw_documents)
                                                              |
                                                              v
                                          ai_parse_document() --> SILVER (parsed_documents)
                                                              |
                                                              v
                                          ai_extract() --> SILVER (extracted_insurance_data)
                                                              |
                                                              v
                                          Materialized Views --> GOLD (sales_summary,
                                                                       claims_processed,
                                                                       outstanding_claims)
                                                              |
                                                              v
                                          Genie Space --> Natural Language Q&A
```

## Components

| Component | Description |
|-----------|-------------|
| Bronze Layer | Auto Loader ingests PDFs as binary from UC Volume |
| Silver Layer | ai_parse_document() + ai_extract() structures insurance data |
| Gold Layer | Materialized views for sales, claims, outstanding claims |
| Databricks App | 3-tab portal: submission, pipeline ops, Genie chat |
| Genie Space | Natural language Q&A over Gold tables |
| Scheduled Job | Runs pipeline every 4 hours |

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Serverless compute enabled
- AI Functions enabled (ai_parse_document, ai_extract)
- Databricks CLI >= 0.239.0

## Quick Start

### 1. Deploy the Bundle
```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

### 2. Run Setup Notebook
Open and run `src/setup/generate_sample_data.py` to create catalog, schemas, users, and sample PDFs.

### 3. Run the Pipeline
```bash
databricks bundle run doc_processing_pipeline --target dev
```

### 4. Create Genie Space
In Databricks UI: Genie > New Space > "Doc Processing Helper" > Add Gold tables.

### 5. Configure App
Set env vars: GENIE_SPACE_ID, JOB_ID, PIPELINE_ID, DATABRICKS_WAREHOUSE_ID

## Project Structure
```
Docprocessing/
├── databricks.yml
├── README.md
├── resources/
│   ├── pipeline.yml
│   ├── job.yml
│   └── app.yml
└── src/
    ├── pipeline/
    │   ├── 01_bronze_ingestion.sql
    │   ├── 02_silver_processing.sql
    │   └── 03_gold_aggregations.sql
    ├── app/
    │   ├── app.py
    │   ├── app.yaml
    │   └── requirements.txt
    └── setup/
        └── generate_sample_data.py
```

## Tech Stack

- Databricks Unity Catalog, Lakeflow SDP, Auto Loader
- AI Functions (ai_parse_document, ai_extract)
- Materialized Views, Databricks Apps (Dash + Plotly)
- Genie API, Declarative Automation Bundles