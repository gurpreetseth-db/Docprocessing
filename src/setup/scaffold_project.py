# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Scaffold DAB Project - All Files
import os

BASE = "/Workspace/Users/gurpreet.sethi@databricks.com/Docprocessing"

def write_file(rel_path, content):
    full_path = os.path.join(BASE, rel_path)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    with open(full_path, "w") as f:
        f.write(content)
    print(f"  ✓ {rel_path}")

print("🚀 Scaffolding Insurance Document Intelligence Platform...\n")

# ============================================================
# 1. databricks.yml
# ============================================================
write_file("databricks.yml", '''bundle:
  name: doc-processing-bundle

variables:
  catalog:
    default: DocProcessing
  bronze_schema:
    default: DocProcess_Bronze
  silver_schema:
    default: DocProcess_Silver
  gold_schema:
    default: DocProcess_Gold
  volume_name:
    default: InputPDFs
  genie_space_name:
    default: "Doc Processing Helper"

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: ${workspace.host}
  prod:
    mode: production
    workspace:
      host: ${workspace.host}
''')

# ============================================================
# 2. resources/pipeline.yml
# ============================================================
write_file("resources/pipeline.yml", '''resources:
  pipelines:
    doc_processing_pipeline:
      name: "Doc Processing Pipeline"
      catalog: ${var.catalog}
      target: ${var.bronze_schema}
      serverless: true
      continuous: false
      libraries:
        - notebook:
            path: ../src/pipeline/01_bronze_ingestion.sql
        - notebook:
            path: ../src/pipeline/02_silver_processing.sql
        - notebook:
            path: ../src/pipeline/03_gold_aggregations.sql
      configuration:
        "spark.databricks.sql.aiFunctions.enabled": "true"

  schemas:
    bronze_schema:
      catalog_name: ${var.catalog}
      name: ${var.bronze_schema}
      comment: "Bronze layer - raw document ingestion"
    silver_schema:
      catalog_name: ${var.catalog}
      name: ${var.silver_schema}
      comment: "Silver layer - parsed and structured documents"
    gold_schema:
      catalog_name: ${var.catalog}
      name: ${var.gold_schema}
      comment: "Gold layer - aggregated business metrics"

  volumes:
    input_pdfs:
      catalog_name: ${var.catalog}
      schema_name: ${var.bronze_schema}
      name: ${var.volume_name}
      volume_type: MANAGED
      comment: "Landing zone for incoming PDF documents"
''')

# ============================================================
# 3. resources/job.yml
# ============================================================
write_file("resources/job.yml", '''resources:
  jobs:
    doc_processing_job:
      name: "Document Processing Job"
      description: "Scheduled job to process incoming insurance documents"
      tasks:
        - task_key: run_doc_pipeline
          pipeline_task:
            pipeline_id: ${resources.pipelines.doc_processing_pipeline.id}
            full_refresh: false
      schedule:
        quartz_cron_expression: "0 0 */4 * * ?"
        timezone_id: "UTC"
        pause_status: UNPAUSED
      tags:
        project: "doc-processing"
        team: "insurance-ops"
''')

# ============================================================
# 4. resources/app.yml
# ============================================================
write_file("resources/app.yml", '''resources:
  apps:
    doc_processing_app:
      name: doc-processing-${workspace.current_user.domain_friendly_name}
      description: "Insurance Document Processing Portal"
      source_code_path: ../src/app
      resources:
        - name: doc-processing-sql-warehouse
          sql_warehouse: {}
        - name: doc-processing-serving-endpoint
          serving_endpoint: {}
      permissions:
        - user_name: users
          level: CAN_USE
''')

print("\n✅ Bundle config and resource files created!")

# COMMAND ----------

# DBTITLE 1,Pipeline SQL Files (Bronze, Silver, Gold)
# ============================================================
# 5. src/pipeline/01_bronze_ingestion.sql
# ============================================================
write_file("src/pipeline/01_bronze_ingestion.sql", '''-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze Layer - Document Ingestion
-- MAGIC Auto Loader ingests PDF files from the InputPDFs volume

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE raw_documents
COMMENT "Raw binary PDF documents ingested via Auto Loader"
AS SELECT
  path AS file_path,
  length AS file_size,
  modificationTime AS file_modification_time,
  content,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  \'/Volumes/DocProcessing/DocProcess_Bronze/InputPDFs\',
  format => \'binaryFile\'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE document_submissions
COMMENT "Tracks document submissions and their processing status"
AS SELECT
  path AS file_path,
  regexp_extract(path, \'.*/([^/]+)$\', 1) AS file_name,
  regexp_extract(path, \'.*/([^_]+)_.*\', 1) AS submitter_email,
  length AS file_size,
  modificationTime AS submission_time,
  current_timestamp() AS ingestion_timestamp,
  \'PROCESSED\' AS processing_status
FROM STREAM read_files(
  \'/Volumes/DocProcessing/DocProcess_Bronze/InputPDFs\',
  format => \'binaryFile\'
);
''')

# ============================================================
# 6. src/pipeline/02_silver_processing.sql
# ============================================================
write_file("src/pipeline/02_silver_processing.sql", '''-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Layer - Document Parsing & Extraction
-- MAGIC Uses ai_parse_document() and ai_extract() to structure insurance documents

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Silver.parsed_documents
COMMENT "Documents parsed by AI document intelligence"
AS SELECT
  file_path,
  file_size,
  file_modification_time,
  ingestion_timestamp,
  ai_parse_document(content, MAP(\'version\', \'2.0\')) AS parsed_content
FROM LIVE.raw_documents;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Silver.extracted_insurance_data
COMMENT "Structured insurance data extracted from parsed documents"
AS SELECT
  file_path,
  ingestion_timestamp,
  ai_extract(
    parsed_content,
    \'{\n      "document_type": {"type": "string", "description": "Type: sales_report, claim_processed, claim_outstanding"},\n      "report_period": {"type": "string", "description": "Month/Year of the report"},\n      "agent_name": {"type": "string", "description": "Insurance agent or submitter name"},\n      "agent_email": {"type": "string", "description": "Agent email address"},\n      "total_sales_amount": {"type": "number", "description": "Total sales/premium amount in dollars"},\n      "number_of_policies_sold": {"type": "integer", "description": "Number of new policies sold"},\n      "claims_processed_count": {"type": "integer", "description": "Number of claims processed"},\n      "claims_processed_amount": {"type": "number", "description": "Total dollar amount of processed claims"},\n      "claims_outstanding_count": {"type": "integer", "description": "Number of outstanding/pending claims"},\n      "claims_outstanding_amount": {"type": "number", "description": "Total dollar amount of outstanding claims"},\n      "outstanding_reasons": {\n        "type": "array",\n        "description": "Reasons for outstanding claims",\n        "items": {\n          "type": "object",\n          "properties": {\n            "reason": {"type": "string"},\n            "count": {"type": "integer"},\n            "amount": {"type": "number"}\n          }\n        }\n      },\n      "region": {"type": "string", "description": "Geographic region or branch"},\n      "product_line": {"type": "string", "description": "Insurance product: auto, home, life, health, commercial"}\n    }\',
    MAP(\'version\', \'2.0\', \'instructions\', \'Extract insurance document data. Documents contain monthly sales reports, claim processing summaries, and outstanding claims with reasons. Extract all numerical values as numbers without currency symbols.\')
  ) AS extracted_data
FROM DocProcessing.DocProcess_Silver.parsed_documents
WHERE try_cast(parsed_content:error_status AS STRING) IS NULL;
''')

# ============================================================
# 7. src/pipeline/03_gold_aggregations.sql
# ============================================================
write_file("src/pipeline/03_gold_aggregations.sql", '''-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer - Business Aggregations
-- MAGIC Materialized views powering the Genie agent and dashboards

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.sales_summary
COMMENT "Monthly sales performance by agent and region"
AS SELECT
  extracted_data:agent_name::STRING AS agent_name,
  extracted_data:agent_email::STRING AS agent_email,
  extracted_data:report_period::STRING AS report_period,
  extracted_data:region::STRING AS region,
  extracted_data:product_line::STRING AS product_line,
  extracted_data:total_sales_amount::DOUBLE AS total_sales_amount,
  extracted_data:number_of_policies_sold::INT AS policies_sold,
  ingestion_timestamp
FROM DocProcessing.DocProcess_Silver.extracted_insurance_data
WHERE extracted_data:document_type::STRING = \'sales_report\';

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.claims_processed
COMMENT "Summary of processed insurance claims by agent and period"
AS SELECT
  extracted_data:agent_name::STRING AS agent_name,
  extracted_data:agent_email::STRING AS agent_email,
  extracted_data:report_period::STRING AS report_period,
  extracted_data:region::STRING AS region,
  extracted_data:product_line::STRING AS product_line,
  extracted_data:claims_processed_count::INT AS claims_count,
  extracted_data:claims_processed_amount::DOUBLE AS claims_amount,
  ingestion_timestamp
FROM DocProcessing.DocProcess_Silver.extracted_insurance_data
WHERE extracted_data:document_type::STRING = \'claim_processed\';

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.outstanding_claims
COMMENT "Outstanding claims with reasons for delay"
AS SELECT
  extracted_data:agent_name::STRING AS agent_name,
  extracted_data:agent_email::STRING AS agent_email,
  extracted_data:report_period::STRING AS report_period,
  extracted_data:region::STRING AS region,
  extracted_data:product_line::STRING AS product_line,
  extracted_data:claims_outstanding_count::INT AS outstanding_count,
  extracted_data:claims_outstanding_amount::DOUBLE AS outstanding_amount,
  explode(from_json(extracted_data:outstanding_reasons::STRING, \'ARRAY<STRUCT<reason:STRING, count:INT, amount:DOUBLE>>\')) AS reason_detail,
  ingestion_timestamp
FROM DocProcessing.DocProcess_Silver.extracted_insurance_data
WHERE extracted_data:document_type::STRING = \'claim_outstanding\';

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.processing_overview
COMMENT "Overall document processing metrics"
AS SELECT
  extracted_data:agent_name::STRING AS agent_name,
  extracted_data:document_type::STRING AS document_type,
  extracted_data:report_period::STRING AS report_period,
  extracted_data:region::STRING AS region,
  file_path,
  ingestion_timestamp AS processed_at
FROM DocProcessing.DocProcess_Silver.extracted_insurance_data;
''')

print("\n✅ Pipeline SQL files created!")

# COMMAND ----------

# DBTITLE 1,Dash App (app.py)
# ============================================================
# 8. src/app/app.py - Polished Dash Application
# ============================================================
app_py_content = '''
import os
import time
import json
from datetime import datetime, timezone
import base64

import dash
from dash import dcc, html, dash_table, callback, Input, Output, State, no_update
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

# --- Configuration ---
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")
JOB_ID = os.environ.get("JOB_ID", "")
PIPELINE_ID = os.environ.get("PIPELINE_ID", "")
CATALOG = "DocProcessing"
BRONZE_SCHEMA = "DocProcess_Bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/InputPDFs"

w = WorkspaceClient()

# --- Custom CSS ---
CUSTOM_CSS = """
@import url(\'https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap\');
body {
    font-family: \'Inter\', sans-serif !important;
    background: linear-gradient(135deg, #0a0e1a 0%, #1a1f3a 50%, #0d1525 100%) !important;
    min-height: 100vh;
}
.gradient-header {
    background: linear-gradient(90deg, #00b4d8 0%, #0077b6 40%, #6c63ff 100%);
    padding: 20px 40px;
    border-radius: 0 0 20px 20px;
    box-shadow: 0 4px 30px rgba(0, 180, 216, 0.3);
    margin-bottom: 30px;
}
.glass-card {
    background: rgba(255, 255, 255, 0.03) !important;
    border: 1px solid rgba(255, 255, 255, 0.08) !important;
    border-radius: 16px !important;
    backdrop-filter: blur(10px);
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3) !important;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}
.glass-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 12px 40px rgba(0, 180, 216, 0.15) !important;
}
.metric-value {
    font-size: 2.5rem;
    font-weight: 700;
    background: linear-gradient(135deg, #00b4d8, #6c63ff);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}
.status-badge-success {
    background: linear-gradient(135deg, #00c853, #00e676);
    color: #fff; padding: 4px 12px; border-radius: 20px;
    font-size: 0.75rem; font-weight: 600;
}
.status-badge-failed {
    background: linear-gradient(135deg, #ff1744, #ff5252);
    color: #fff; padding: 4px 12px; border-radius: 20px;
    font-size: 0.75rem; font-weight: 600;
}
.status-badge-pending {
    background: linear-gradient(135deg, #ff9100, #ffab40);
    color: #fff; padding: 4px 12px; border-radius: 20px;
    font-size: 0.75rem; font-weight: 600;
}
.chat-bubble-user {
    background: linear-gradient(135deg, #0077b6, #00b4d8);
    color: white; padding: 12px 18px;
    border-radius: 18px 18px 4px 18px;
    margin: 8px 0; max-width: 75%; margin-left: auto;
    word-wrap: break-word;
}
.chat-bubble-genie {
    background: rgba(255, 255, 255, 0.06);
    border: 1px solid rgba(255, 255, 255, 0.1);
    color: #e0e0e0; padding: 12px 18px;
    border-radius: 18px 18px 18px 4px;
    margin: 8px 0; max-width: 75%;
    word-wrap: break-word;
}
.nav-tabs .nav-link {
    color: rgba(255, 255, 255, 0.6) !important;
    border: none !important; font-weight: 500;
    padding: 12px 24px; transition: all 0.3s ease;
}
.nav-tabs .nav-link.active {
    color: #00b4d8 !important;
    background: rgba(0, 180, 216, 0.1) !important;
    border-bottom: 3px solid #00b4d8 !important;
    border-radius: 8px 8px 0 0;
}
.upload-zone {
    border: 2px dashed rgba(0, 180, 216, 0.4) !important;
    border-radius: 16px; padding: 40px; text-align: center;
    background: rgba(0, 180, 216, 0.03);
    transition: all 0.3s ease;
}
.upload-zone:hover {
    border-color: #00b4d8 !important;
    background: rgba(0, 180, 216, 0.08);
}
"""

# --- App Init ---
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.CYBORG,
        "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"
    ],
    suppress_callback_exceptions=True,
    title="DocProcess Portal"
)


def get_current_user():
    try:
        me = w.current_user.me()
        return {"email": me.user_name, "name": me.display_name or me.user_name.split("@")[0].replace(".", " ").title()}
    except Exception:
        return {"email": "demo.user@company.com", "name": "Demo User"}


def get_submitted_documents(user_email):
    try:
        results = w.statement_execution.execute_statement(
            warehouse_id=os.environ.get("DATABRICKS_WAREHOUSE_ID", ""),
            statement=f"SELECT file_name, submission_time, file_size, processing_status FROM {CATALOG}.{BRONZE_SCHEMA}.document_submissions WHERE submitter_email = \'{user_email}\' ORDER BY submission_time DESC LIMIT 50"
        )
        if results.result and results.result.data_array:
            return pd.DataFrame(results.result.data_array, columns=["File Name", "Submitted", "Size (bytes)", "Status"])
    except Exception as e:
        print(f"Error fetching documents: {e}")
    return pd.DataFrame(columns=["File Name", "Submitted", "Size (bytes)", "Status"])


def get_job_runs():
    try:
        if not JOB_ID:
            return []
        runs = w.jobs.list_runs(job_id=int(JOB_ID), limit=5)
        run_list = []
        for run in runs:
            duration = ""
            if run.start_time and run.end_time:
                dur_sec = (run.end_time - run.start_time) / 1000
                duration = f"{int(dur_sec // 60)}m {int(dur_sec % 60)}s"
            status = "Running"
            failure_reason = ""
            if run.state:
                if run.state.result_state == RunResultState.SUCCESS:
                    status = "Success"
                elif run.state.result_state == RunResultState.FAILED:
                    status = "Failed"
                    failure_reason = run.state.state_message or "Unknown error"
                elif run.state.life_cycle_state in [RunLifeCycleState.RUNNING, RunLifeCycleState.PENDING]:
                    status = "Running"
            run_list.append({"run_id": run.run_id, "start_time": datetime.fromtimestamp(run.start_time / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if run.start_time else "", "duration": duration, "status": status, "failure_reason": failure_reason})
        return run_list
    except Exception as e:
        print(f"Error fetching runs: {e}")
        return []


def poll_genie_response(space_id, conversation_id, message_id, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}")
            status = resp.get("status", "")
            if status == "COMPLETED":
                return resp
            elif status in ["FAILED", "CANCELLED"]:
                return {"error": resp.get("error", "Query failed")}
        except Exception as e:
            return {"error": str(e)}
        time.sleep(2)
    return {"error": "Timeout waiting for Genie response"}


def build_header():
    user = get_current_user()
    return html.Div([
        html.Div([
            html.Div([
                html.Div([html.I(className="bi bi-file-earmark-medical", style={"fontSize": "2.5rem"})], style={"marginRight": "20px"}),
                html.Div([html.H2("Insurance Document Intelligence", className="mb-0", style={"fontWeight": "700", "letterSpacing": "-0.5px"}), html.P("Pacific Shield Insurance Group - AI-Powered Document Processing", className="mb-0", style={"opacity": "0.85", "fontSize": "0.9rem"})])
            ], style={"display": "flex", "alignItems": "center"}),
            html.Div([
                html.Div([html.Span(user["name"], style={"fontWeight": "600", "fontSize": "0.95rem"}), html.Br(), html.Span(user["email"], style={"opacity": "0.7", "fontSize": "0.8rem"})], style={"textAlign": "right", "marginRight": "12px"}),
                html.Div(user["name"][0].upper(), style={"width": "44px", "height": "44px", "borderRadius": "50%", "background": "rgba(255,255,255,0.2)", "display": "flex", "alignItems": "center", "justifyContent": "center", "fontSize": "1.2rem", "fontWeight": "700"})
            ], style={"display": "flex", "alignItems": "center"})
        ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "color": "white"})
    ], className="gradient-header")


def build_tab1():
    user = get_current_user()
    return html.Div([
        dbc.Row([dbc.Col([dbc.Card([dbc.CardBody([html.H4(f"Welcome, {user[\'name\']}!", className="mb-2", style={"fontWeight": "600"}), html.P(f"Logged in as {user[\'email\']} | Upload insurance documents for AI processing", style={"color": "rgba(255,255,255,0.6)", "marginBottom": "0"})])], className="glass-card mb-4")])]),
        dbc.Row([
            dbc.Col([dbc.Card([dbc.CardBody([html.H5("Submit Documents", className="mb-3", style={"fontWeight": "600"}), dcc.Upload(id="upload-pdf", children=html.Div([html.I(className="bi bi-cloud-arrow-up", style={"fontSize": "3rem", "color": "#00b4d8"}), html.P("Drag & drop PDF files here", className="mt-2 mb-1", style={"fontWeight": "500"}), html.P("or click to browse", style={"color": "rgba(255,255,255,0.5)", "fontSize": "0.85rem"})]), className="upload-zone", multiple=True, accept=".pdf"), html.Div(id="upload-status", className="mt-3")])], className="glass-card mb-4")], md=5),
            dbc.Col([dbc.Card([dbc.CardBody([html.H5("Submission History", className="mb-3", style={"fontWeight": "600"}), html.Div(id="submissions-table")])], className="glass-card mb-4", style={"minHeight": "350px"})], md=7)
        ])
    ], style={"padding": "0 20px"})


def build_tab2():
    return html.Div([
        dbc.Row([dbc.Col([dbc.Card([dbc.CardBody([html.Div([html.Div([html.H5("Pipeline Control", className="mb-1", style={"fontWeight": "600"}), html.P("Trigger the document processing pipeline on-demand", style={"color": "rgba(255,255,255,0.5)", "fontSize": "0.85rem", "marginBottom": "0"})]), dbc.Button([html.I(className="bi bi-play-fill me-2"), "Run Pipeline Now"], id="run-pipeline-btn", color="info", size="lg", style={"borderRadius": "12px", "fontWeight": "600", "background": "linear-gradient(135deg, #0077b6, #00b4d8)", "border": "none"})], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"})])], className="glass-card mb-4")])]),
        html.Div(id="run-trigger-status", className="mb-3"),
        dbc.Row([dbc.Col([dbc.Card([dbc.CardBody([html.H5("Recent Pipeline Runs", className="mb-3", style={"fontWeight": "600"}), html.Div(id="runs-table")])], className="glass-card")])])
    ], style={"padding": "0 20px"})


def build_tab3():
    return html.Div([
        dbc.Card([dbc.CardBody([
            html.Div([html.H5([html.I(className="bi bi-stars me-2"), "Genie - Ask About Your Documents"], style={"fontWeight": "600", "marginBottom": "4px"}), html.P("Ask questions in natural language about sales, claims, and processing metrics", style={"color": "rgba(255,255,255,0.5)", "fontSize": "0.85rem", "marginBottom": "0"})], className="mb-3"),
            html.Div(id="chat-messages", style={"height": "450px", "overflowY": "auto", "padding": "20px", "borderRadius": "12px", "background": "rgba(0,0,0,0.2)", "border": "1px solid rgba(255,255,255,0.05)"}, children=[html.Div([html.Div("Hi! I\'m your Document Intelligence assistant. Ask me anything about your insurance documents - sales performance, claims status, outstanding items, and more!", className="chat-bubble-genie")])]),
            html.Div([dbc.InputGroup([dbc.Input(id="chat-input", type="text", placeholder="Ask about sales, claims, processing status...", style={"background": "rgba(255,255,255,0.05)", "border": "1px solid rgba(255,255,255,0.1)", "color": "white", "borderRadius": "12px 0 0 12px"}), dbc.Button(html.I(className="bi bi-send-fill"), id="send-btn", color="info", style={"borderRadius": "0 12px 12px 0", "background": "linear-gradient(135deg, #0077b6, #00b4d8)", "border": "none", "width": "50px"})], className="mt-3")])
        ])], className="glass-card", style={"height": "calc(100vh - 220px)"})
    ], style={"padding": "0 20px"})


app.layout = html.Div([
    html.Style(CUSTOM_CSS),
    dcc.Store(id="conversation-store", data={"conversation_id": None, "messages": []}),
    dcc.Store(id="user-store", data=get_current_user()),
    build_header(),
    dbc.Tabs([dbc.Tab(build_tab1(), label="Document Portal", tab_id="tab-1", label_style={"fontSize": "0.9rem"}), dbc.Tab(build_tab2(), label="Pipeline Ops", tab_id="tab-2", label_style={"fontSize": "0.9rem"}), dbc.Tab(build_tab3(), label="Genie Chat", tab_id="tab-3", label_style={"fontSize": "0.9rem"})], id="main-tabs", active_tab="tab-1", style={"padding": "0 20px"}),
    dcc.Interval(id="refresh-interval", interval=30000, n_intervals=0)
])


@callback(Output("upload-status", "children"), Input("upload-pdf", "contents"), State("upload-pdf", "filename"), State("user-store", "data"), prevent_initial_call=True)
def handle_upload(contents_list, filenames, user_data):
    if not contents_list:
        return no_update
    results = []
    for content, filename in zip(contents_list, filenames):
        try:
            content_type, content_string = content.split(",")
            decoded = base64.b64decode(content_string)
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            email_prefix = user_data["email"].replace("@", "_at_").replace(".", "_")
            dest_name = f"{email_prefix}_{timestamp}_{filename}"
            dest_path = f"{VOLUME_PATH}/{dest_name}"
            w.files.upload(dest_path, decoded, overwrite=True)
            results.append(dbc.Alert(f"Uploaded: {filename}", color="success", className="py-2 px-3", style={"borderRadius": "10px"}))
        except Exception as e:
            results.append(dbc.Alert(f"Failed: {filename} - {str(e)}", color="danger", className="py-2 px-3", style={"borderRadius": "10px"}))
    return results


@callback(Output("submissions-table", "children"), Input("refresh-interval", "n_intervals"), State("user-store", "data"))
def refresh_submissions(n, user_data):
    df = get_submitted_documents(user_data["email"])
    if df.empty:
        return html.Div([html.I(className="bi bi-inbox", style={"fontSize": "2.5rem", "color": "rgba(255,255,255,0.2)"}), html.P("No documents submitted yet", className="mt-2", style={"color": "rgba(255,255,255,0.4)"})], style={"textAlign": "center", "padding": "60px 0"})
    return dash_table.DataTable(data=df.to_dict("records"), columns=[{"name": c, "id": c} for c in df.columns], style_table={"overflowX": "auto"}, style_header={"background": "rgba(0,180,216,0.15)", "color": "white", "fontWeight": "600", "border": "none"}, style_cell={"background": "transparent", "color": "white", "border": "1px solid rgba(255,255,255,0.05)", "fontSize": "0.85rem", "padding": "10px"}, style_data_conditional=[{"if": {"filter_query": "{Status} = PROCESSED"}, "color": "#00e676", "fontWeight": "600"}, {"if": {"filter_query": "{Status} = PENDING"}, "color": "#ffab40", "fontWeight": "600"}], page_size=8)


@callback(Output("run-trigger-status", "children"), Input("run-pipeline-btn", "n_clicks"), prevent_initial_call=True)
def trigger_pipeline(n):
    if not JOB_ID:
        return dbc.Alert("JOB_ID not configured", color="warning", style={"borderRadius": "10px"})
    try:
        run = w.jobs.run_now(job_id=int(JOB_ID))
        return dbc.Alert(f"Pipeline triggered! Run ID: {run.run_id}", color="success", style={"borderRadius": "10px"}, duration=6000)
    except Exception as e:
        return dbc.Alert(f"Failed: {str(e)}", color="danger", style={"borderRadius": "10px"})


@callback(Output("runs-table", "children"), Input("refresh-interval", "n_intervals"), Input("run-trigger-status", "children"))
def refresh_runs(n, _):
    runs = get_job_runs()
    if not runs:
        return html.Div([html.I(className="bi bi-clock-history", style={"fontSize": "2.5rem", "color": "rgba(255,255,255,0.2)"}), html.P("No pipeline runs yet", className="mt-2", style={"color": "rgba(255,255,255,0.4)"})], style={"textAlign": "center", "padding": "60px 0"})
    cards = []
    for run in runs:
        badge_class = "status-badge-success" if run["status"] == "Success" else ("status-badge-failed" if run["status"] == "Failed" else "status-badge-pending")
        card = dbc.Card([dbc.CardBody([html.Div([html.Div([html.Span(f"Run #{run[\'run_id\']}", style={"fontWeight": "600"}), html.Span(run["status"], className=badge_class, style={"marginLeft": "12px"})]), html.Small(run["start_time"], style={"color": "rgba(255,255,255,0.5)"})], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}), html.Div([html.Small(f"Duration: {run[\'duration\']}", style={"color": "rgba(255,255,255,0.5)"}), html.Small(f" | {run[\'failure_reason\']}", style={"color": "#ff5252"}) if run["failure_reason"] else None], className="mt-1")])], className="glass-card mb-2")
        cards.append(card)
    return cards


@callback(Output("chat-messages", "children"), Output("conversation-store", "data"), Output("chat-input", "value"), Input("send-btn", "n_clicks"), Input("chat-input", "n_submit"), State("chat-input", "value"), State("conversation-store", "data"), State("chat-messages", "children"), prevent_initial_call=True)
def handle_chat(n_clicks, n_submit, message, conv_data, current_messages):
    if not message or not message.strip():
        return no_update, no_update, no_update
    if not GENIE_SPACE_ID:
        current_messages.append(html.Div([html.Div(message, className="chat-bubble-user"), html.Div("GENIE_SPACE_ID not configured.", className="chat-bubble-genie")]))
        return current_messages, conv_data, ""
    current_messages.append(html.Div(html.Div(message, className="chat-bubble-user"), style={"display": "flex", "justifyContent": "flex-end"}))
    try:
        conversation_id = conv_data.get("conversation_id")
        if not conversation_id:
            resp = w.api_client.do("POST", f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation", body={"content": message})
            conversation_id = resp["conversation_id"]
            message_id = resp["message_id"]
        else:
            resp = w.api_client.do("POST", f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages", body={"content": message})
            message_id = resp["message_id"]
        conv_data["conversation_id"] = conversation_id
        result = poll_genie_response(GENIE_SPACE_ID, conversation_id, message_id)
        if "error" in result:
            current_messages.append(html.Div(f"Error: {result[\'error\']}", className="chat-bubble-genie"))
        else:
            attachments = result.get("attachments", [])
            reply_parts = []
            text_content = result.get("content", "")
            if text_content:
                reply_parts.append(html.P(text_content))
            for att in attachments:
                if att.get("type") == "QUERY_RESULT":
                    query = att.get("query", {}).get("sql", "")
                    if query:
                        reply_parts.append(html.Details([html.Summary("SQL Query", style={"cursor": "pointer", "color": "#00b4d8"}), html.Code(query, style={"fontSize": "0.8rem", "whiteSpace": "pre-wrap"})], className="mt-2"))
                    columns = att.get("query", {}).get("columns", [])
                    rows = att.get("query", {}).get("rows", [])
                    if columns and rows:
                        df = pd.DataFrame(rows, columns=[c.get("name", "") for c in columns])
                        reply_parts.append(dash_table.DataTable(data=df.head(20).to_dict("records"), columns=[{"name": c, "id": c} for c in df.columns], style_header={"background": "rgba(0,180,216,0.15)", "color": "white", "fontWeight": "600"}, style_cell={"background": "transparent", "color": "white", "border": "1px solid rgba(255,255,255,0.05)", "fontSize": "0.8rem"}, style_table={"marginTop": "10px"}))
            if reply_parts:
                current_messages.append(html.Div(reply_parts, className="chat-bubble-genie"))
            else:
                current_messages.append(html.Div("I processed your question but received no detailed response. Try rephrasing.", className="chat-bubble-genie"))
    except Exception as e:
        current_messages.append(html.Div(f"Error: {str(e)}", className="chat-bubble-genie"))
    return current_messages, conv_data, ""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
'''

write_file("src/app/app.py", app_py_content.strip())
print("\n\u2705 Dash app file created!")


# COMMAND ----------

# DBTITLE 1,App Config & Requirements
# ============================================================
# 9. src/app/app.yaml
# ============================================================
write_file("src/app/app.yaml", '''command:
  - "python"
  - "app.py"
env:
  - name: DATABRICKS_HOST
    description: "Databricks workspace host URL"
  - name: GENIE_SPACE_ID
    description: "Genie Space ID for Doc Processing Helper"
  - name: JOB_ID
    description: "Job ID for the document processing pipeline job"
  - name: PIPELINE_ID
    description: "Pipeline ID for the SDP pipeline"
  - name: DATABRICKS_WAREHOUSE_ID
    description: "SQL Warehouse ID for querying submission data"
''')

# ============================================================
# 10. src/app/requirements.txt
# ============================================================
write_file("src/app/requirements.txt", '''dash>=2.14.0
dash-bootstrap-components>=1.5.0
plotly>=5.18.0
pandas>=2.0.0
databricks-sdk>=0.20.0
requests>=2.31.0
''')

print("\u2705 App config files created!")

# COMMAND ----------

# DBTITLE 1,Setup Notebook - generate_sample_data.py
# ============================================================
# 11. src/setup/generate_sample_data.py (Databricks notebook)
# ============================================================
generate_sample_data_content = '''# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Document Intelligence - Sample Data Generator
# MAGIC This notebook creates the catalog, schemas, volume, users table, and generates 20 realistic sample PDF documents.

# COMMAND ----------

# MAGIC %pip install reportlab
# MAGIC %restart_python

# COMMAND ----------

import random
from datetime import datetime, timedelta
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_RIGHT
import io

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Catalog, Schemas, and Volume

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS DocProcessing")
spark.sql("CREATE SCHEMA IF NOT EXISTS DocProcessing.DocProcess_Bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS DocProcessing.DocProcess_Silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS DocProcessing.DocProcess_Gold")
spark.sql("CREATE VOLUME IF NOT EXISTS DocProcessing.DocProcess_Bronze.InputPDFs")
print("Catalog, schemas, and volume created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Users Table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

users_data = [
    (1, "Sarah Chen", "sarah.chen@pacificshield.com", "Sales", "Senior Agent", "West"),
    (2, "Marcus Johnson", "marcus.johnson@pacificshield.com", "Claims", "Manager", "Southeast"),
    (3, "Priya Patel", "priya.patel@pacificshield.com", "Underwriting", "Agent", "Northeast"),
    (4, "James O\'Brien", "james.obrien@pacificshield.com", "Sales", "Director", "Midwest"),
    (5, "Maria Rodriguez", "maria.rodriguez@pacificshield.com", "Claims", "Senior Agent", "Southwest"),
    (6, "David Kim", "david.kim@pacificshield.com", "Operations", "Agent", "West"),
    (7, "Emily Watson", "emily.watson@pacificshield.com", "Sales", "Agent", "Northeast"),
    (8, "Robert Singh", "robert.singh@pacificshield.com", "Claims", "Senior Agent", "Southeast"),
    (9, "Jessica Martinez", "jessica.martinez@pacificshield.com", "Underwriting", "Manager", "Southwest"),
    (10, "Thomas Wright", "thomas.wright@pacificshield.com", "Sales", "Agent", "Midwest")
]

schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("full_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("department", StringType(), False),
    StructField("role", StringType(), False),
    StructField("region", StringType(), False)
])

df_users = spark.createDataFrame(users_data, schema)
df_users.write.mode("overwrite").saveAsTable("DocProcessing.DocProcess_Bronze.users")
print(f"Created users table with {df_users.count()} records")
display(df_users)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate Sample PDF Documents

# COMMAND ----------

def create_sales_report_pdf(agent_name, agent_email, period, region):
    """Generate a realistic monthly sales report PDF."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(\'Title\', parent=styles[\'Title\'], fontSize=18, textColor=colors.HexColor(\'#1a237e\'), spaceAfter=6)
    subtitle_style = ParagraphStyle(\'Subtitle\', parent=styles[\'Normal\'], fontSize=11, textColor=colors.HexColor(\'#455a64\'), alignment=TA_CENTER)
    header_style = ParagraphStyle(\'Header\', parent=styles[\'Heading2\'], fontSize=13, textColor=colors.HexColor(\'#0d47a1\'), spaceBefore=20)

    elements = []
    elements.append(Paragraph("PACIFIC SHIELD INSURANCE GROUP", title_style))
    elements.append(Paragraph("Monthly Sales Performance Report", subtitle_style))
    elements.append(Spacer(1, 6))
    elements.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor(\'#1565c0\')))
    elements.append(Spacer(1, 15))

    info_data = [["Agent:", agent_name, "Region:", region], ["Email:", agent_email, "Period:", period], ["Department:", "Sales", "Report Type:", "Monthly Sales"]]
    info_table = Table(info_data, colWidths=[60, 180, 70, 150])
    info_table.setStyle(TableStyle([("FONTSIZE", (0, 0), (-1, -1), 9), ("TEXTCOLOR", (0, 0), (0, -1), colors.grey), ("TEXTCOLOR", (2, 0), (2, -1), colors.grey)]))
    elements.append(info_table)
    elements.append(Spacer(1, 20))

    elements.append(Paragraph("Sales by Product Line", header_style))
    products = ["Auto", "Home", "Life", "Health", "Commercial"]
    sales_data = [["Product Line", "Policies Sold", "Premium Amount ($)", "Target ($)", "Achievement %"]]
    total_policies = 0
    total_premium = 0
    for prod in products:
        policies = random.randint(8, 45)
        premium = round(random.uniform(25000, 180000), 2)
        target = round(premium * random.uniform(0.85, 1.15), 2)
        achievement = round((premium / target) * 100, 1)
        sales_data.append([prod, str(policies), f"{premium:,.2f}", f"{target:,.2f}", f"{achievement}%"])
        total_policies += policies
        total_premium += premium
    sales_data.append(["TOTAL", str(total_policies), f"{total_premium:,.2f}", "", ""])

    table = Table(sales_data, colWidths=[100, 85, 110, 100, 95])
    table.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(\'#1565c0\')), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white), ("FONTSIZE", (0, 0), (-1, -1), 9), ("GRID", (0, 0), (-1, -1), 0.5, colors.grey), ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor(\'#e3f2fd\')), ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold")]))
    elements.append(table)
    elements.append(Spacer(1, 20))

    elements.append(Paragraph("Summary", header_style))
    elements.append(Paragraph(f"Total policies sold this period: {total_policies}", styles[\'Normal\']))
    elements.append(Paragraph(f"Total premium generated: ${total_premium:,.2f}", styles[\'Normal\']))
    elements.append(Spacer(1, 30))
    elements.append(HRFlowable(width="100%", thickness=1, color=colors.grey))
    elements.append(Paragraph(f"Report generated: {datetime.now().strftime(\'{0}\'.format(\'%Y-%m-%d %H:%M\'))}", ParagraphStyle(\'Footer\', parent=styles[\'Normal\'], fontSize=8, textColor=colors.grey)))

    doc.build(elements)
    return buffer.getvalue()


def create_claims_processed_pdf(agent_name, agent_email, period, region):
    """Generate a claims processing summary PDF."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(\'Title\', parent=styles[\'Title\'], fontSize=18, textColor=colors.HexColor(\'#1a237e\'), spaceAfter=6)
    subtitle_style = ParagraphStyle(\'Subtitle\', parent=styles[\'Normal\'], fontSize=11, textColor=colors.HexColor(\'#455a64\'), alignment=TA_CENTER)
    header_style = ParagraphStyle(\'Header\', parent=styles[\'Heading2\'], fontSize=13, textColor=colors.HexColor(\'#0d47a1\'), spaceBefore=20)

    elements = []
    elements.append(Paragraph("PACIFIC SHIELD INSURANCE GROUP", title_style))
    elements.append(Paragraph("Claims Processing Summary", subtitle_style))
    elements.append(Spacer(1, 6))
    elements.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor(\'#1565c0\')))
    elements.append(Spacer(1, 15))

    info_data = [["Processor:", agent_name, "Region:", region], ["Email:", agent_email, "Period:", period], ["Department:", "Claims", "Report Type:", "Claims Processed"]]
    info_table = Table(info_data, colWidths=[70, 180, 70, 150])
    info_table.setStyle(TableStyle([("FONTSIZE", (0, 0), (-1, -1), 9), ("TEXTCOLOR", (0, 0), (0, -1), colors.grey), ("TEXTCOLOR", (2, 0), (2, -1), colors.grey)]))
    elements.append(info_table)
    elements.append(Spacer(1, 20))

    elements.append(Paragraph("Processed Claims Detail", header_style))
    claims_data = [["Claim ID", "Product", "Amount ($)", "Processing Date", "Status"]]
    num_claims = random.randint(12, 30)
    total_amount = 0
    statuses = ["Approved", "Approved", "Approved", "Approved", "Partial", "Denied"]
    for i in range(num_claims):
        claim_id = f"CLM-{random.randint(100000, 999999)}"
        product = random.choice(["Auto", "Home", "Life", "Health", "Commercial"])
        amount = round(random.uniform(500, 75000), 2)
        proc_date = (datetime.now() - timedelta(days=random.randint(1, 28))).strftime(\'%Y-%m-%d\')
        status = random.choice(statuses)
        claims_data.append([claim_id, product, f"{amount:,.2f}", proc_date, status])
        total_amount += amount

    table = Table(claims_data[:16], colWidths=[95, 80, 95, 100, 70])  # Limit rows for PDF readability
    table.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(\'#2e7d32\')), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white), ("FONTSIZE", (0, 0), (-1, -1), 8), ("GRID", (0, 0), (-1, -1), 0.5, colors.grey)]))
    elements.append(table)
    elements.append(Spacer(1, 15))
    elements.append(Paragraph(f"Total claims processed: {num_claims}", styles[\'Normal\']))
    elements.append(Paragraph(f"Total amount processed: ${total_amount:,.2f}", styles[\'Normal\']))
    elements.append(Spacer(1, 30))
    elements.append(HRFlowable(width="100%", thickness=1, color=colors.grey))

    doc.build(elements)
    return buffer.getvalue()


def create_outstanding_claims_pdf(agent_name, agent_email, period, region):
    """Generate an outstanding claims report PDF."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(\'Title\', parent=styles[\'Title\'], fontSize=18, textColor=colors.HexColor(\'#1a237e\'), spaceAfter=6)
    subtitle_style = ParagraphStyle(\'Subtitle\', parent=styles[\'Normal\'], fontSize=11, textColor=colors.HexColor(\'#455a64\'), alignment=TA_CENTER)
    header_style = ParagraphStyle(\'Header\', parent=styles[\'Heading2\'], fontSize=13, textColor=colors.HexColor(\'#b71c1c\'), spaceBefore=20)

    elements = []
    elements.append(Paragraph("PACIFIC SHIELD INSURANCE GROUP", title_style))
    elements.append(Paragraph("Outstanding Claims Report", subtitle_style))
    elements.append(Spacer(1, 6))
    elements.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor(\'#c62828\')))
    elements.append(Spacer(1, 15))

    info_data = [["Reviewer:", agent_name, "Region:", region], ["Email:", agent_email, "Period:", period], ["Department:", "Claims", "Report Type:", "Outstanding Claims"]]
    info_table = Table(info_data, colWidths=[70, 180, 70, 150])
    info_table.setStyle(TableStyle([("FONTSIZE", (0, 0), (-1, -1), 9), ("TEXTCOLOR", (0, 0), (0, -1), colors.grey), ("TEXTCOLOR", (2, 0), (2, -1), colors.grey)]))
    elements.append(info_table)
    elements.append(Spacer(1, 20))

    reasons_list = ["Missing Documentation", "Under Investigation", "Awaiting Medical Records", "Pending Appraisal", "Legal Review Required", "Fraud Investigation"]
    elements.append(Paragraph("Outstanding Claims by Reason", header_style))
    reason_data = [["Reason", "Count", "Total Amount ($)", "Avg Days Pending"]]
    total_count = 0
    total_amount = 0
    for reason in random.sample(reasons_list, random.randint(3, 6)):
        count = random.randint(2, 15)
        amount = round(random.uniform(10000, 250000), 2)
        avg_days = random.randint(7, 90)
        reason_data.append([reason, str(count), f"{amount:,.2f}", str(avg_days)])
        total_count += count
        total_amount += amount
    reason_data.append(["TOTAL", str(total_count), f"{total_amount:,.2f}", ""])

    table = Table(reason_data, colWidths=[150, 60, 120, 100])
    table.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(\'#c62828\')), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white), ("FONTSIZE", (0, 0), (-1, -1), 9), ("GRID", (0, 0), (-1, -1), 0.5, colors.grey), ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor(\'#ffebee\')), ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold")]))
    elements.append(table)
    elements.append(Spacer(1, 20))
    elements.append(Paragraph(f"Total outstanding claims: {total_count}", styles[\'Normal\']))
    elements.append(Paragraph(f"Total outstanding amount: ${total_amount:,.2f}", styles[\'Normal\']))
    elements.append(Spacer(1, 30))
    elements.append(HRFlowable(width="100%", thickness=1, color=colors.grey))

    doc.build(elements)
    return buffer.getvalue()

# COMMAND ----------

# Generate and upload 20 PDFs
import os

users = [
    ("Sarah Chen", "sarah.chen@pacificshield.com", "West"),
    ("Marcus Johnson", "marcus.johnson@pacificshield.com", "Southeast"),
    ("Priya Patel", "priya.patel@pacificshield.com", "Northeast"),
    ("James O\'Brien", "james.obrien@pacificshield.com", "Midwest"),
    ("Maria Rodriguez", "maria.rodriguez@pacificshield.com", "Southwest"),
    ("David Kim", "david.kim@pacificshield.com", "West"),
    ("Emily Watson", "emily.watson@pacificshield.com", "Northeast"),
    ("Robert Singh", "robert.singh@pacificshield.com", "Southeast"),
    ("Jessica Martinez", "jessica.martinez@pacificshield.com", "Southwest"),
    ("Thomas Wright", "thomas.wright@pacificshield.com", "Midwest")
]

periods = ["June 2026", "July 2026", "May 2026", "April 2026"]
doc_types = ["sales_report", "sales_report", "claim_processed", "claim_processed", "claim_outstanding", "claim_outstanding", "sales_report", "claim_processed", "claim_outstanding", "sales_report"]

volume_path = "/Volumes/DocProcessing/DocProcess_Bronze/InputPDFs"
generated_files = []

for i in range(20):
    user_idx = i % len(users)
    name, email, region = users[user_idx]
    period = periods[i % len(periods)]
    doc_type = doc_types[i % len(doc_types)]
    date_str = (datetime.now() - timedelta(days=random.randint(0, 30))).strftime(\'%Y%m%d\')

    if doc_type == "sales_report":
        pdf_bytes = create_sales_report_pdf(name, email, period, region)
    elif doc_type == "claim_processed":
        pdf_bytes = create_claims_processed_pdf(name, email, period, region)
    else:
        pdf_bytes = create_outstanding_claims_pdf(name, email, period, region)

    filename = f"{email}_{date_str}_{doc_type}.pdf"
    file_path = f"{volume_path}/{filename}"

    # Write using dbutils
    dbutils.fs.put(file_path.replace("/Volumes/", "dbfs:/Volumes/"), "", overwrite=True)
    # Use python to write binary
    with open(f"/Volumes/DocProcessing/DocProcess_Bronze/InputPDFs/{filename}", "wb") as f:
        f.write(pdf_bytes)

    generated_files.append(filename)
    print(f"  Generated: {filename}")

print(f"\\nTotal PDFs generated: {len(generated_files)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("  SETUP COMPLETE - Insurance Document Intelligence Platform")
print("=" * 60)
print(f"\\n  Catalog: DocProcessing")
print(f"  Schemas: DocProcess_Bronze, DocProcess_Silver, DocProcess_Gold")
print(f"  Volume: DocProcessing.DocProcess_Bronze.InputPDFs")
print(f"  Users: 10 sample insurance agents")
print(f"  PDFs Generated: {len(generated_files)} documents")
print(f"    - Sales Reports: {sum(1 for f in generated_files if \'sales\' in f)}")
print(f"    - Claims Processed: {sum(1 for f in generated_files if \'claim_processed\' in f)}")
print(f"    - Outstanding Claims: {sum(1 for f in generated_files if \'outstanding\' in f)}")
print(f"\\n  Next Steps:")
print(f"  1. Deploy the bundle: databricks bundle deploy --target dev")
print(f"  2. Run the pipeline to process documents")
print(f"  3. Configure the Genie space with Gold tables")
print(f"  4. Deploy the app and set environment variables")
print("=" * 60)
'''

write_file("src/setup/generate_sample_data.py", generate_sample_data_content.strip())
print("\n\u2705 Sample data generator notebook created!")

# COMMAND ----------

# DBTITLE 1,README.md
# ============================================================
# 12. README.md
# ============================================================
write_file("README.md", '''# Insurance Document Intelligence Platform

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

Or trigger from the App\'s Pipeline Ops tab.

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
\u251c\u2500\u2500 databricks.yml              # Bundle configuration
\u251c\u2500\u2500 README.md                   # This file
\u251c\u2500\u2500 resources/
\u2502   \u251c\u2500\u2500 pipeline.yml            # SDP pipeline + schemas + volumes
\u2502   \u251c\u2500\u2500 job.yml                 # Scheduled processing job
\u2502   \u2514\u2500\u2500 app.yml                 # Databricks App resource
\u2514\u2500\u2500 src/
    \u251c\u2500\u2500 pipeline/
    \u2502   \u251c\u2500\u2500 01_bronze_ingestion.sql     # Auto Loader
    \u2502   \u251c\u2500\u2500 02_silver_processing.sql    # AI parsing & extraction
    \u2502   \u2514\u2500\u2500 03_gold_aggregations.sql    # Business MVs
    \u251c\u2500\u2500 app/
    \u2502   \u251c\u2500\u2500 app.py                      # Dash application
    \u2502   \u251c\u2500\u2500 app.yaml                    # App runtime config
    \u2502   \u2514\u2500\u2500 requirements.txt            # Python dependencies
    \u2514\u2500\u2500 setup/
        \u2514\u2500\u2500 generate_sample_data.py     # Data bootstrapper
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
''')

print("\n\u2705 README created!")
print("\n" + "=" * 60)
print("  ALL FILES SCAFFOLDED SUCCESSFULLY!")
print("=" * 60)