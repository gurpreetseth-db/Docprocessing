import os
import time
import json
from datetime import datetime, timezone
import base64
from urllib.parse import quote

import dash
from dash import dcc, html, dash_table, callback, Input, Output, State, no_update, ctx, ALL
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

try:
    from flask import request
    HAS_FLASK_REQUEST = True
except ImportError:
    HAS_FLASK_REQUEST = False

# Client logo (Geneva Healthcare), embedded as a self-contained base64 data URI so the
# app never depends on static-asset serving.
from brand_assets import GENEVA_LOGO_URI

# --- Configuration from Environment ---
# All of these are injected by the bundle app resource (resources/app.yml), which
# sources them from config.yml. The app never hardcodes ids.
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_APP_PORT = int(os.environ.get("DATABRICKS_APP_PORT", "8050"))
DATABRICKS_WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
# The Genie Agent is resolved BY NAME at runtime (see resolve_genie_space_id()).
# GENIE_SPACE_ID is an optional explicit override; normally it is empty.
GENIE_SPACE_NAME = os.environ.get("GENIE_SPACE_NAME", "Doc Processing Helper")
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")
JOB_ID = os.environ.get("JOB_ID", "")
# Pipeline id (resolved at deploy via ${resources.pipelines.*.id}). The app
# triggers processing through the JOB (which runs the pipeline task), so this is
# used for status/lineage display rather than for triggering.
PIPELINE_ID = os.environ.get("PIPELINE_ID", "")

CATALOG = os.environ.get("CATALOG", "DocProcessing")
BRONZE_SCHEMA = os.environ.get("BRONZE_SCHEMA", "DocProcess_Bronze")
SILVER_SCHEMA = os.environ.get("SILVER_SCHEMA", "DocProcess_Silver")
GOLD_SCHEMA = os.environ.get("GOLD_SCHEMA", "DocProcess_Gold")
VOLUME_NAME = os.environ.get("VOLUME_NAME", "InputPDFs")
VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}"

# Silver quarantine table: one row per PDF the pipeline REJECTED (missing mandatory
# field or AI extraction error), populated by src/pipeline/02_silver_processing.sql.
# The "Data Quality" tab reads this.
QUARANTINE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.service_plan_quarantine"

# The mandatory fields the Silver pipeline validates (drives the reason filter dropdown).
REJECTION_REASONS = [
    "Missing client first name",
    "Missing gender",
    "Missing date of birth",
    "Missing address",
    "Missing GP name",
    "Missing NHI number",
    "AI extraction failed",
]

w = WorkspaceClient()

# Cache the resolved Genie space id so we only hit the list API once per process.
_GENIE_SPACE_ID_CACHE = {"id": GENIE_SPACE_ID or None}


def resolve_genie_space_id():
    """Resolve the Genie Agent's space id BY NAME (GENIE_SPACE_NAME).

    Auto-provisioning creates the space post-deploy, so its id isn't known at
    build time. Resolving by name means the app needs no id wiring and survives
    redeploys. An explicit GENIE_SPACE_ID env var still wins if provided.
    """
    if _GENIE_SPACE_ID_CACHE["id"]:
        return _GENIE_SPACE_ID_CACHE["id"]
    try:
        page_token, wanted = None, GENIE_SPACE_NAME.strip()
        while True:
            query = {"page_token": page_token} if page_token else None
            resp = w.api_client.do("GET", "/api/2.0/genie/spaces", query=query) or {}
            for s in resp.get("spaces", []):
                if (s.get("title") or "").strip() == wanted:
                    _GENIE_SPACE_ID_CACHE["id"] = s.get("space_id")
                    return _GENIE_SPACE_ID_CACHE["id"]
            page_token = resp.get("next_page_token")
            if not page_token:
                break
    except Exception as e:
        print(f"Could not resolve Genie space '{GENIE_SPACE_NAME}': {e}")
    return None


def run_sql(statement, warehouse_id=None):
    """Execute SQL and BLOCK until it finishes, returning the StatementResponse.

    execute_statement defaults to a short wait_timeout and 'continue on timeout',
    so on a cold/busy warehouse it can return with state=RUNNING and result=None.
    We request the max synchronous wait (50s) and then poll get_statement until the
    statement reaches a terminal state, so callers always get the real result.
    """
    warehouse_id = warehouse_id or DATABRICKS_WAREHOUSE_ID
    # on_wait_timeout defaults to CONTINUE, so a statement that outlives the 50s
    # synchronous window keeps running and we pick it up via polling below.
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
    )
    deadline = time.time() + 120
    while time.time() < deadline:
        state = getattr(getattr(resp, "status", None), "state", None)
        state_str = getattr(state, "value", str(state)) if state is not None else ""
        if state_str in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
            break
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)
    return resp


def escape_sql_string(s):
    """
    Escape single quotes in SQL string literals by doubling them.
    Prevents SQL injection in f-string queries.
    """
    return s.replace("'", "''")


# --- Custom CSS (Dark glassmorphism theme) ---
CUSTOM_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
body {
    font-family: 'Inter', sans-serif !important;
    background: linear-gradient(135deg, #0a0e1a 0%, #0f2438 50%, #0d1525 100%) !important;
    min-height: 100vh;
    color: #e0e0e0;
}
.gradient-header {
    background: linear-gradient(90deg, #2FA9E0 0%, #1E88C7 40%, #3FB6A8 100%);
    padding: 20px 40px;
    border-radius: 0 0 20px 20px;
    box-shadow: 0 4px 30px rgba(47, 169, 224, 0.3);
    margin-bottom: 30px;
    color: white;
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
    box-shadow: 0 12px 40px rgba(47, 169, 224, 0.15) !important;
}
.welcome-banner {
    background: rgba(47, 169, 224, 0.1);
    border-left: 4px solid #2FA9E0;
    padding: 16px;
    border-radius: 8px;
    color: #e0e0e0;
}
.welcome-banner .welcome-title {
    font-size: 1.3rem;
    font-weight: 600;
    color: #2FA9E0;
    margin-bottom: 4px;
}
.metric-value {
    font-size: 2.5rem;
    font-weight: 700;
    background: linear-gradient(135deg, #2FA9E0, #3FB6A8);
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
.status-badge-running {
    background: linear-gradient(135deg, #ff9100, #ffab40);
    color: #fff; padding: 4px 12px; border-radius: 20px;
    font-size: 0.75rem; font-weight: 600;
}
.status-badge-ingested {
    background: linear-gradient(135deg, #2FA9E0, #1E88C7);
    color: #fff; padding: 4px 12px; border-radius: 20px;
    font-size: 0.75rem; font-weight: 600;
}
.chat-bubble-user {
    background: linear-gradient(135deg, #1E88C7, #2FA9E0);
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
    color: #2FA9E0 !important;
    background: rgba(47, 169, 224, 0.1) !important;
    border-bottom: 3px solid #2FA9E0 !important;
    border-radius: 8px 8px 0 0;
}
.upload-zone {
    border: 2px dashed rgba(47, 169, 224, 0.4) !important;
    border-radius: 16px; padding: 40px; text-align: center;
    background: rgba(47, 169, 224, 0.03);
    transition: all 0.3s ease;
}
.upload-zone:hover {
    border-color: #2FA9E0 !important;
    background: rgba(47, 169, 224, 0.08);
}
.collapsible-files {
    margin-top: 8px;
    padding: 8px;
    background: rgba(0, 0, 0, 0.2);
    border-radius: 8px;
    font-size: 0.85rem;
}
.collapsible-files summary {
    cursor: pointer;
    color: #2FA9E0;
    font-weight: 500;
}
.collapsible-files ul {
    margin-top: 8px;
    margin-left: 16px;
    color: rgba(255, 255, 255, 0.7);
}
.submission-row-btn {
    display: flex;
    align-items: center;
    width: 100%;
    background: rgba(255, 255, 255, 0.03);
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 12px;
    padding: 12px 16px;
    margin-bottom: 10px;
    color: #e0e0e0;
    cursor: pointer;
    transition: all 0.2s ease;
    text-align: left;
}
.submission-row-btn:hover {
    background: rgba(47, 169, 224, 0.1);
    border-color: rgba(47, 169, 224, 0.4);
    transform: translateY(-1px);
}
"""

# --- Brand + motion layer (Geneva Healthcare) ---
# Layered on top of CUSTOM_CSS: adds tasteful animation, the logo chip, the KPI
# ribbon, a live status dot, and loading/typing feedback. Respects reduced-motion.
BRAND_CSS = """
:root{
  --geneva-blue:#2FA9E0; --geneva-deep:#1E88C7; --geneva-teal:#3FB6A8;
  --geneva-amber:#F2994A; --geneva-heart:#E84C4C;
}
@keyframes fadeInUp{from{opacity:0;transform:translateY(14px)}to{opacity:1;transform:none}}
@keyframes auroraShift{0%{background-position:0% 50%}50%{background-position:100% 50%}100%{background-position:0% 50%}}
@keyframes livePulse{0%{box-shadow:0 0 0 0 rgba(63,182,168,.55)}70%{box-shadow:0 0 0 8px rgba(63,182,168,0)}100%{box-shadow:0 0 0 0 rgba(63,182,168,0)}}
@keyframes typingBlink{0%,80%,100%{opacity:.2;transform:translateY(0)}40%{opacity:1;transform:translateY(-3px)}}
@keyframes floatSoft{0%{transform:translateY(0)}50%{transform:translateY(-4px)}100%{transform:translateY(0)}}

/* Animated brand header */
.gradient-header{
  background:linear-gradient(120deg,#2FA9E0 0%,#3FB6A8 34%,#1E88C7 68%,#2FA9E0 100%) !important;
  background-size:220% 220% !important;
  animation:auroraShift 16s ease infinite;
}
/* Logo chip so the white-ground brand mark reads cleanly on the header */
.brand-logo{
  background:#ffffff; border-radius:14px; padding:10px 16px;
  box-shadow:0 6px 18px rgba(0,0,0,.18); display:inline-flex; align-items:center;
}
.brand-logo img{height:42px; width:auto; display:block}

/* Staggered entrance for cards / banners / KPI tiles */
.glass-card, .welcome-banner, .kpi-card{animation:fadeInUp .5s ease both}
.kpi-card:nth-child(2){animation-delay:.06s}
.kpi-card:nth-child(3){animation-delay:.12s}
.kpi-card:nth-child(4){animation-delay:.18s}

/* Live status dot */
.live-dot{width:9px;height:9px;border-radius:50%;background:#eafff8;display:inline-block;
  margin-right:7px;animation:livePulse 2.2s infinite}

/* KPI ribbon */
.kpi-strip{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;
  padding:0 20px;margin:-8px 0 26px}
.kpi-card{position:relative;overflow:hidden;background:rgba(255,255,255,.04);
  border:1px solid rgba(255,255,255,.08);border-radius:16px;padding:18px 20px;
  backdrop-filter:blur(10px);transition:transform .2s ease,box-shadow .2s ease}
.kpi-card:hover{transform:translateY(-3px);box-shadow:0 12px 34px rgba(47,169,224,.16)}
.kpi-card::before{content:"";position:absolute;left:0;top:0;height:100%;width:4px}
.kpi-card.k-blue::before{background:linear-gradient(#2FA9E0,#1E88C7)}
.kpi-card.k-teal::before{background:linear-gradient(#3FB6A8,#2FA9E0)}
.kpi-card.k-amber::before{background:linear-gradient(#F2994A,#F2C94C)}
.kpi-card.k-heart::before{background:linear-gradient(#E84C4C,#F2994A)}
.kpi-icon{font-size:1.35rem;opacity:.9;margin-bottom:6px}
.kpi-card.k-blue .kpi-icon{color:#5cc0ef}
.kpi-card.k-teal .kpi-icon{color:#5fd3c4}
.kpi-card.k-amber .kpi-icon{color:#f6b26b}
.kpi-card.k-heart .kpi-icon{color:#f2807e}
.kpi-value{font-size:2rem;font-weight:700;line-height:1.05;
  background:linear-gradient(135deg,#2FA9E0,#3FB6A8);-webkit-background-clip:text;
  -webkit-text-fill-color:transparent;font-variant-numeric:tabular-nums}
.kpi-label{color:rgba(255,255,255,.6);font-size:.8rem;font-weight:500;margin-top:2px;
  letter-spacing:.02em}

/* Buttons: gentle lift + press */
.btn{transition:transform .12s ease, box-shadow .2s ease, filter .2s ease !important}
.btn:hover{filter:brightness(1.06)}
.btn:active{transform:scale(.97)}

/* Upload zone: soft breathing icon */
.upload-zone .bi-cloud-arrow-up{animation:floatSoft 3.5s ease-in-out infinite}

/* Genie typing indicator */
.typing{display:inline-flex;gap:5px;padding:12px 18px}
.typing span{width:8px;height:8px;border-radius:50%;background:#5fd3c4;
  animation:typingBlink 1.3s infinite}
.typing span:nth-child(2){animation-delay:.18s}
.typing span:nth-child(3){animation-delay:.36s}

/* Loading spinner tint */
._dash-loading-callback,._dash-loading{color:#3FB6A8}

@media (prefers-reduced-motion: reduce){
  *{animation:none !important; transition:none !important}
}
"""

# --- App Initialization ---
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.CYBORG,
        "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"
    ],
    suppress_callback_exceptions=True,
    title="GENEVA HEALTHCARE Document Intelligence Portal"
)

# Inject custom CSS via index_string for reliable loading
app.index_string = f"""
<!DOCTYPE html>
<html>
    <head>
        {{%metas%}}
        <title>{{%title%}}</title>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        {{%favicon%}}
        {{%css%}}
        <style>
        {CUSTOM_CSS}
        {BRAND_CSS}
        </style>
    </head>
    <body>
        {{%app_entry%}}
        <footer>
            {{%config%}}
            {{%scripts%}}
            {{%renderer%}}
        </footer>
    </body>
</html>
"""

# --- User Detection ---
def get_current_user():
    """
    Detect logged-in user from request headers (X-Forwarded-Email, X-Forwarded-Preferred-Username),
    fall back to WorkspaceClient().current_user.me(), then demo default.
    Resolve friendly name from users table if available.
    """
    user_email = None
    user_name = None

    # Try to get from Flask request headers (Databricks Apps)
    if HAS_FLASK_REQUEST:
        try:
            user_email = request.headers.get("X-Forwarded-Email")
            user_name = request.headers.get("X-Forwarded-Preferred-Username")
        except Exception:
            pass

    # Fall back to WorkspaceClient
    if not user_email:
        try:
            me = w.current_user.me()
            user_email = me.user_name
            user_name = me.display_name or me.user_name.split("@")[0].replace(".", " ").title()
        except Exception:
            pass

    # Final fallback
    if not user_email:
        user_email = "demo.user@company.com"
        user_name = "Demo User"

    # Try to resolve friendly name from users table
    if user_name is None:
        try:
            email_escaped = escape_sql_string(user_email)
            results = run_sql(
                f"SELECT full_name FROM {CATALOG}.{BRONZE_SCHEMA}.users WHERE email = '{email_escaped}' LIMIT 1"
            )
            if results.result and results.result.data_array and len(results.result.data_array) > 0:
                user_name = results.result.data_array[0][0]
            else:
                user_name = user_email.split("@")[0].replace(".", " ").title()
        except Exception:
            user_name = user_email.split("@")[0].replace(".", " ").title()

    return {"email": user_email, "name": user_name}


def email_to_slug(email):
    """Convert email to slug format: aroha@geneva.co.nz -> aroha_at_geneva_dot_co_dot_nz"""
    return email.replace("@", "_at_").replace(".", "_dot_")


def slug_to_email(slug):
    """Reverse of email_to_slug"""
    return slug.replace("_at_", "@").replace("_dot_", ".")


def _run_submissions_query(where_clause):
    """Run the submissions query with a real, derived status.

    Bronze's processing_status is a constant ('INGESTED'), so it can't tell you
    whether a document was actually processed. We derive the real status by
    LEFT JOINing to the Silver extraction table: a file present there has been
    fully parsed + extracted ('Processed'); otherwise it is still 'Pending'.
    """
    query = f"""
    SELECT
      s.file_name,
      s.submission_time,
      s.file_size,
      CASE WHEN e.file_path IS NOT NULL THEN 'Processed' ELSE 'Pending' END AS status,
      s.file_path
    FROM {CATALOG}.{BRONZE_SCHEMA}.document_submissions s
    LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.service_plan_extracted e
      ON s.file_path = e.file_path
    {where_clause}
    ORDER BY s.submission_time DESC
    LIMIT 50
    """
    results = run_sql(query)
    if results.result and results.result.data_array:
        df = pd.DataFrame(
            results.result.data_array,
            columns=["File Name", "Submitted", "Size (bytes)", "Status", "file_path"]
        )
        df["Submitted"] = pd.to_datetime(df["Submitted"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        return df
    return pd.DataFrame(columns=["File Name", "Submitted", "Size (bytes)", "Status", "file_path"])


def get_submitted_documents(user_email):
    """Return the current user's submissions with a derived processing status.

    Returns (df, scope) where scope is 'user' when the rows belong to the logged-in
    user, or 'all' when we fell back to showing every recent submission (used when the
    logged-in user has none of their own — e.g. a workspace admin viewing the demo).
    """
    empty = pd.DataFrame(columns=["File Name", "Submitted", "Size (bytes)", "Status", "file_path"])
    try:
        if not DATABRICKS_WAREHOUSE_ID:
            return empty, "user"

        email_escaped = escape_sql_string(user_email)
        df = _run_submissions_query(f"WHERE s.submitter_email = '{email_escaped}'")
        if not df.empty:
            return df, "user"

        # Fallback: the logged-in user has no submissions of their own. Show all
        # recent submissions so the demo is never blank.
        df_all = _run_submissions_query("")
        return df_all, "all"
    except Exception as e:
        print(f"Error fetching documents: {e}")

    return empty, "user"


def get_job_runs_with_file_counts():
    """
    Fetch last 5 job runs and compute files processed per run.
    Returns list of dicts with: run_id, start_time, end_time, duration, status, failure_reason, files_processed, file_names.
    """
    try:
        if not JOB_ID or not DATABRICKS_WAREHOUSE_ID:
            return []

        runs = w.jobs.list_runs(job_id=int(JOB_ID), limit=5)
        run_list = []

        for run in runs:
            duration = ""
            start_ts = None
            end_ts = None

            if run.start_time and run.end_time:
                start_ts = run.start_time / 1000
                end_ts = run.end_time / 1000
                dur_sec = end_ts - start_ts
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

            # Query files processed within this run's time window
            files_processed = []
            files_count = 0
            if start_ts and end_ts:
                try:
                    # Query document_submissions for files ingested during this run window
                    from_ts = datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat()
                    to_ts = datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat()
                    query = f"""
                    SELECT file_name FROM {CATALOG}.{BRONZE_SCHEMA}.document_submissions
                    WHERE ingestion_timestamp >= '{from_ts}' AND ingestion_timestamp <= '{to_ts}'
                    ORDER BY ingestion_timestamp DESC
                    """
                    results = run_sql(query)
                    if results.result and results.result.data_array:
                        files_processed = [row[0] for row in results.result.data_array]
                        files_count = len(files_processed)
                except Exception as e:
                    print(f"Error querying files for run {run.run_id}: {e}")

            run_list.append({
                "run_id": run.run_id,
                "start_time": datetime.fromtimestamp(run.start_time / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if run.start_time else "",
                "duration": duration,
                "status": status,
                "failure_reason": failure_reason,
                "files_count": files_count,
                "file_names": files_processed
            })

        return run_list
    except Exception as e:
        print(f"Error fetching runs: {e}")
        return []


def get_genie_response(space_id, conversation_id, message_id, timeout=60):
    """
    Fetch Genie message response using SDK's get_message_query_result.
    Handles polling and returns parsed content + query results.
    """
    try:
        # Get message (already resolved by *_and_wait, but re-fetch for full attachments)
        message = w.genie.get_message(space_id=space_id, conversation_id=conversation_id, message_id=message_id)

        # Check message status. Compare by string value (the enum's module path has
        # moved between databricks-sdk versions, so importing MessageStatus is fragile).
        status_str = getattr(message.status, "value", str(message.status)).upper()
        if "FAILED" in status_str or "CANCELLED" in status_str or "CANCELED" in status_str:
            return {"error": "Message processing failed", "status": status_str}

        result = {"text": "", "tables": []}

        for attachment in (message.attachments or []):
            # Text answer lives on attachment.text.content
            text_att = getattr(attachment, "text", None)
            if text_att is not None:
                txt = getattr(text_att, "content", None)
                if txt:
                    result["text"] = (result["text"] + "\n\n" + txt).strip() if result["text"] else txt

            # Query answer: attachment.query holds the SQL Genie generated. Rather than
            # depend on the version-specific Genie query-result endpoint, we re-run the
            # SQL on the warehouse via the stable statement_execution API.
            query_att = getattr(attachment, "query", None)
            if query_att is not None:
                sql_text = getattr(query_att, "query", "") or ""
                table = {
                    "sql": sql_text,
                    "description": getattr(query_att, "description", "") or "",
                    "columns": [],
                    "rows": [],
                }
                if sql_text and DATABRICKS_WAREHOUSE_ID:
                    try:
                        stmt = w.statement_execution.execute_statement(
                            warehouse_id=DATABRICKS_WAREHOUSE_ID,
                            statement=sql_text,
                            wait_timeout="30s",
                        )
                        manifest = getattr(stmt, "manifest", None)
                        schema = getattr(manifest, "schema", None) if manifest else None
                        cols = getattr(schema, "columns", None) if schema else None
                        if cols:
                            table["columns"] = [getattr(c, "name", f"col_{i}") for i, c in enumerate(cols)]
                        data = getattr(stmt, "result", None)
                        if data is not None and getattr(data, "data_array", None):
                            table["rows"] = data.data_array
                    except Exception as e:
                        print(f"Error executing Genie SQL: {e}")
                result["tables"].append(table)

        return result
    except Exception as e:
        return {"error": str(e)}


def build_header():
    """Build gradient header with user info."""
    user = get_current_user()
    return html.Div([
        html.Div([
            html.Div([
                # Client logo in a white chip so the brand mark reads on the header.
                html.Div(
                    html.Img(src=GENEVA_LOGO_URI, alt="Geneva Healthcare"),
                    className="brand-logo",
                    style={"marginRight": "20px"}
                ),
                html.Div([
                    html.H2(
                        "Document Intelligence",
                        className="mb-0",
                        style={"fontWeight": "700", "letterSpacing": "-0.5px"}
                    ),
                    html.P(
                        [html.Span(className="live-dot"),
                         "Home Based Support Services · AI-powered document processing"],
                        className="mb-0",
                        style={"opacity": "0.9", "fontSize": "0.9rem", "display": "flex", "alignItems": "center"}
                    )
                ])
            ], style={"display": "flex", "alignItems": "center"}),
            html.Div([
                html.Div([
                    html.Span(user["name"], style={"fontWeight": "600", "fontSize": "0.95rem"}),
                    html.Br(),
                    html.Span(user["email"], style={"opacity": "0.7", "fontSize": "0.8rem"})
                ], style={"textAlign": "right", "marginRight": "12px"}),
                html.Div(
                    user["name"][0].upper(),
                    style={
                        "width": "44px", "height": "44px", "borderRadius": "50%",
                        "background": "rgba(255,255,255,0.2)", "display": "flex",
                        "alignItems": "center", "justifyContent": "center",
                        "fontSize": "1.2rem", "fontWeight": "700"
                    }
                )
            ], style={"display": "flex", "alignItems": "center"})
        ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "color": "white"})
    ], className="gradient-header")


def get_submission_stats():
    """Overview counts for the KPI ribbon: total documents, processed, pending, rate."""
    stats = {"total": 0, "processed": 0, "pending": 0, "rate": 0}
    try:
        if not DATABRICKS_WAREHOUSE_ID:
            return stats
        q = f"""
        SELECT
          (SELECT COUNT(DISTINCT file_path) FROM {CATALOG}.{BRONZE_SCHEMA}.document_submissions) AS total,
          (SELECT COUNT(DISTINCT file_path) FROM {CATALOG}.{SILVER_SCHEMA}.service_plan_extracted) AS processed
        """
        res = run_sql(q)
        if res.result and res.result.data_array:
            total = int(res.result.data_array[0][0] or 0)
            processed = int(res.result.data_array[0][1] or 0)
            stats = {
                "total": total,
                "processed": processed,
                "pending": max(total - processed, 0),
                "rate": round(100 * processed / total) if total else 0,
            }
    except Exception as e:
        print(f"Error fetching stats: {e}")
    return stats


def get_rejected_count():
    """Count of distinct PDFs the pipeline rejected into the quarantine table.

    Queried separately from get_submission_stats() so that if the quarantine table
    does not exist yet (pipeline never run), it degrades to 0 on its own without
    zeroing the other KPIs.
    """
    try:
        if not DATABRICKS_WAREHOUSE_ID:
            return 0
        res = run_sql(f"SELECT COUNT(DISTINCT file_path) FROM {QUARANTINE_TABLE}")
        if res.result and res.result.data_array:
            return int(res.result.data_array[0][0] or 0)
    except Exception as e:
        print(f"Error fetching rejected count: {e}")
    return 0


def get_rejected_documents():
    """Return rejected documents (from the quarantine table) with their reasons.

    Columns: File Name, Submitter, Detected, Reason, file_path. De-duplicated on
    file_path (a full-refresh could append a file more than once) so downstream
    pattern-matched component ids stay unique.
    """
    cols = ["File Name", "Submitter", "Detected", "Reason", "file_path"]
    empty = pd.DataFrame(columns=cols)
    try:
        if not DATABRICKS_WAREHOUSE_ID:
            return empty
        q = f"""
        SELECT file_name, submitter_email, ingestion_timestamp, rejection_reason, file_path
        FROM {QUARANTINE_TABLE}
        ORDER BY ingestion_timestamp DESC
        LIMIT 200
        """
        res = run_sql(q)
        if res.result and res.result.data_array:
            df = pd.DataFrame(res.result.data_array, columns=cols)
            df["Detected"] = pd.to_datetime(df["Detected"]).dt.strftime("%Y-%m-%d %H:%M:%S")
            return df.drop_duplicates(subset=["file_path"]).reset_index(drop=True)
    except Exception as e:
        print(f"Error fetching rejected documents: {e}")
    return empty


def _kpi_card(icon, value, label, tone):
    return html.Div([
        html.Div(html.I(className=f"bi {icon}"), className="kpi-icon"),
        html.Div(str(value), className="kpi-value"),
        html.Div(label, className="kpi-label"),
    ], className=f"kpi-card {tone}")


def build_kpi_cards():
    """The KPI ribbon cards (returned as a list so the refresh callback can swap them).

    'Awaiting Processing' now excludes rejected files (rejected = ingested but withheld
    from Gold), so total = processed + pending + rejected.
    """
    s = get_submission_stats()
    rejected = get_rejected_count()
    pending = max(s["total"] - s["processed"] - rejected, 0)
    return [
        _kpi_card("bi-files", s["total"], "Documents Submitted", "k-blue"),
        _kpi_card("bi-check2-circle", s["processed"], "Processed", "k-teal"),
        _kpi_card("bi-hourglass-split", pending, "Awaiting Processing", "k-amber"),
        _kpi_card("bi-exclamation-octagon", rejected, "Rejected (Data Quality)", "k-heart"),
    ]


def build_tab1():
    """Submit & Track tab."""
    user = get_current_user()
    return html.Div([
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.Div([
                        html.Span("Welcome back, ", style={"fontSize": "1.1rem"}),
                        html.Span(user['name'], style={"fontSize": "1.1rem", "fontWeight": "600", "color": "#2FA9E0"})
                    ], style={"marginBottom": "8px"}),
                    html.Span(
                        f"Submit GENEVA HEALTHCARE PDFs for processing. Logged in as {user['email']}",
                        style={"color": "rgba(255,255,255,0.6)", "fontSize": "0.9rem"}
                    )
                ], className="welcome-banner")
            ])
        ], className="mb-4"),
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Submit Documents", className="mb-3", style={"fontWeight": "600"}),
                        dcc.Upload(
                            id="upload-pdf",
                            children=html.Div([
                                html.I(className="bi bi-cloud-arrow-up", style={"fontSize": "3rem", "color": "#2FA9E0"}),
                                html.P("Drag & drop PDF files here", className="mt-2 mb-1", style={"fontWeight": "500"}),
                                html.P("or click to browse", style={"color": "rgba(255,255,255,0.5)", "fontSize": "0.85rem"})
                            ]),
                            className="upload-zone",
                            multiple=True,
                            accept=".pdf"
                        ),
                        html.Div(id="upload-status", className="mt-3")
                    ])
                ], className="glass-card mb-4")
            ], md=5),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("My Submissions", className="mb-3", style={"fontWeight": "600"}),
                        dcc.Loading(html.Div(id="submissions-table"), type="dot", color="#3FB6A8", delay_show=350)
                    ])
                ], className="glass-card mb-4", style={"minHeight": "350px"})
            ], md=7)
        ]),
        # PDF viewer modal — opened by clicking a submission row
        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle(id="pdf-modal-title")),
                dbc.ModalBody(html.Div(id="pdf-modal-body", style={"minHeight": "70vh"})),
                dbc.ModalFooter(
                    dbc.Button("Close", id="pdf-modal-close", className="ms-auto", color="secondary")
                ),
            ],
            id="pdf-modal",
            size="xl",
            is_open=False,
            scrollable=True,
        ),
    ], style={"padding": "0 20px"})


def build_tab2():
    """Pipeline Ops tab."""
    return html.Div([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.H5("Pipeline Control", className="mb-1", style={"fontWeight": "600"}),
                                html.P(
                                    "Trigger the Auto Loader pipeline on-demand",
                                    style={"color": "rgba(255,255,255,0.5)", "fontSize": "0.85rem", "marginBottom": "0"}
                                )
                            ]),
                            dbc.Button(
                                [html.I(className="bi bi-play-fill me-2"), "Run Pipeline Now"],
                                id="run-pipeline-btn",
                                color="info",
                                size="lg",
                                style={
                                    "borderRadius": "12px",
                                    "fontWeight": "600",
                                    "background": "linear-gradient(135deg, #1E88C7, #2FA9E0)",
                                    "border": "none"
                                }
                            )
                        ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"})
                    ])
                ], className="glass-card mb-4")
            ])
        ]),
        html.Div(id="run-trigger-status", className="mb-3"),
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Recent Pipeline Runs", className="mb-3", style={"fontWeight": "600"}),
                        dcc.Loading(html.Div(id="runs-table"), type="dot", color="#3FB6A8", delay_show=350)
                    ])
                ], className="glass-card")
            ])
        ])
    ], style={"padding": "0 20px"})


def build_tab_dq():
    """Data Quality tab — PDFs the pipeline rejected, with reasons.

    Interactive: a live reason-breakdown chart, a free-text search, a reason filter
    dropdown, and clickable rows that open the same PDF viewer modal as Submit & Track.
    """
    dark_input = {
        "background": "rgba(255,255,255,0.05)",
        "border": "1px solid rgba(255,255,255,0.1)",
        "color": "white",
        "borderRadius": "12px",
    }
    return html.Div([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5(
                            [html.I(className="bi bi-shield-exclamation me-2"),
                             "Data Quality — Rejected Documents"],
                            className="mb-1", style={"fontWeight": "600"}
                        ),
                        html.P(
                            "Documents that failed validation in the pipeline are withheld from "
                            "the Gold model and listed here with the reason. Click any file to view the PDF.",
                            style={"color": "rgba(255,255,255,0.5)", "fontSize": "0.85rem", "marginBottom": "0"}
                        ),
                        # Live count + per-reason breakdown chart (filled by callback).
                        dcc.Loading(html.Div(id="dq-summary", className="mt-3"),
                                    type="dot", color="#3FB6A8", delay_show=350),
                    ])
                ], className="glass-card mb-4")
            ])
        ]),
        dbc.Row([
            dbc.Col([
                dbc.Input(
                    id="dq-search", type="text", debounce=True,
                    placeholder="Search by file name or submitter…", style=dark_input,
                )
            ], md=7),
            dbc.Col([
                dcc.Dropdown(
                    id="dq-reason-filter",
                    options=[{"label": "All reasons", "value": "All"}]
                            + [{"label": r, "value": r} for r in REJECTION_REASONS],
                    value="All", clearable=False,
                    style={"borderRadius": "12px", "color": "#111"},
                )
            ], md=5),
        ], className="mb-3"),
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Flagged Files", className="mb-3", style={"fontWeight": "600"}),
                        dcc.Loading(html.Div(id="rejected-list"),
                                    type="dot", color="#3FB6A8", delay_show=350),
                    ])
                ], className="glass-card")
            ])
        ]),
    ], style={"padding": "0 20px"})


def build_tab3():
    """Genie Assistant tab."""
    return html.Div([
        dbc.Card([
            dbc.CardBody([
                html.Div([
                    html.H5(
                        [html.I(className="bi bi-stars me-2"), "Genie - Ask About Your Documents"],
                        style={"fontWeight": "600", "marginBottom": "4px"}
                    ),
                    html.P(
                        "Ask questions in natural language about GENEVA HEALTHCARE plans, care hours, funding, and client conditions",
                        style={"color": "rgba(255,255,255,0.5)", "fontSize": "0.85rem", "marginBottom": "0"}
                    )
                ], className="mb-3"),
                dcc.Loading(
                    type="dot", color="#3FB6A8", delay_show=350,
                    children=html.Div(
                        id="chat-messages",
                        style={
                            "height": "450px", "overflowY": "auto", "padding": "20px",
                            "borderRadius": "12px", "background": "rgba(0,0,0,0.2)",
                            "border": "1px solid rgba(255,255,255,0.05)"
                        },
                        children=[
                            html.Div([
                                html.Div(
                                    "Kia ora! I'm your Service Plan Intelligence assistant. Ask me anything about care hours, funding, conditions, risk and safety, or ADL dependency across your plans.",
                                    className="chat-bubble-genie"
                                )
                            ])
                        ]
                    )
                ),
                html.Div([
                    dbc.InputGroup([
                        dbc.Input(
                            id="chat-input",
                            type="text",
                            placeholder="Ask about clients, care hours, funders, conditions...",
                            style={
                                "background": "rgba(255,255,255,0.05)",
                                "border": "1px solid rgba(255,255,255,0.1)",
                                "color": "white",
                                "borderRadius": "12px 0 0 12px"
                            }
                        ),
                        dbc.Button(
                            html.I(className="bi bi-send-fill"),
                            id="send-btn",
                            color="info",
                            style={
                                "borderRadius": "0 12px 12px 0",
                                "background": "linear-gradient(135deg, #1E88C7, #2FA9E0)",
                                "border": "none",
                                "width": "50px"
                            }
                        )
                    ], className="mt-3")
                ])
            ])
        ], className="glass-card", style={"height": "calc(100vh - 220px)"})
    ], style={"padding": "0 20px"})


# --- Main Layout ---
# Defined as a FUNCTION so it is re-evaluated on every page load *within* a request
# context. That lets get_current_user() read the X-Forwarded-Email header for the
# actual logged-in user, instead of capturing a single identity at import time.
def serve_layout():
    return html.Div([
        dcc.Store(id="conversation-store", data={"conversation_id": None, "messages": []}),
        dcc.Store(id="user-store", data=get_current_user()),
        build_header(),
        # Live KPI ribbon (overview across all tabs; refreshes on the interval).
        dcc.Loading(
            html.Div(build_kpi_cards(), id="kpi-strip", className="kpi-strip"),
            type="dot", color="#3FB6A8", delay_show=350,
        ),
        dbc.Tabs([
            dbc.Tab(build_tab1(), label="Submit & Track", tab_id="tab-1", label_style={"fontSize": "0.9rem"}),
            dbc.Tab(build_tab2(), label="Pipeline Ops", tab_id="tab-2", label_style={"fontSize": "0.9rem"}),
            dbc.Tab(build_tab_dq(), label="Data Quality", tab_id="tab-dq", label_style={"fontSize": "0.9rem"}),
            dbc.Tab(build_tab3(), label="Genie Assistant", tab_id="tab-3", label_style={"fontSize": "0.9rem"})
        ], id="main-tabs", active_tab="tab-1", style={"padding": "0 20px"}),
        dcc.Interval(id="refresh-interval", interval=30000, n_intervals=0)
    ])

app.layout = serve_layout


# --- Callbacks ---

@callback(
    Output("kpi-strip", "children"),
    Input("refresh-interval", "n_intervals"),
)
def refresh_kpis(n):
    """Keep the KPI ribbon live as documents are submitted/processed."""
    return build_kpi_cards()


@callback(
    Output("upload-status", "children"),
    Input("upload-pdf", "contents"),
    State("upload-pdf", "filename"),
    State("user-store", "data"),
    prevent_initial_call=True
)
def handle_upload(contents_list, filenames, user_data):
    """Handle PDF uploads and save to volume with correct filename convention."""
    if not contents_list:
        return no_update

    results = []
    email_slug = email_to_slug(user_data["email"])

    for content, filename in zip(contents_list, filenames):
        try:
            content_type, content_string = content.split(",")
            decoded = base64.b64decode(content_string)
            submission_id = datetime.now().strftime("%Y%m%d%H%M%S")
            # Preserve the ORIGINAL filename. Submitter email + submission id are carried
            # in the folder path instead of being baked into the name:
            #   InputPDFs/{email_slug}/{submission_id}/{original_filename}
            safe_name = os.path.basename(filename)  # strip any path components
            dest_path = f"{VOLUME_PATH}/{email_slug}/{submission_id}/{safe_name}"
            w.files.upload(dest_path, decoded, overwrite=True)
            results.append(
                dbc.Alert(
                    [html.I(className="bi bi-check-circle me-2"), f"Uploaded: {filename}"],
                    color="success",
                    className="py-2 px-3",
                    style={"borderRadius": "10px"}
                )
            )
        except Exception as e:
            results.append(
                dbc.Alert(
                    [html.I(className="bi bi-exclamation-circle me-2"), f"Failed: {filename} - {str(e)}"],
                    color="danger",
                    className="py-2 px-3",
                    style={"borderRadius": "10px"}
                )
            )

    return results


@callback(
    Output("submissions-table", "children"),
    Input("refresh-interval", "n_intervals"),
    State("user-store", "data")
)
def refresh_submissions(n, user_data):
    """Render the current user's submissions as a clickable list.

    Each row is a button (pattern-matched id carries the file_path) so clicking it
    opens the PDF viewer modal. Rows show file name, submitted time, and a status pill.
    """
    email = (user_data or {}).get("email", "")
    df, scope = get_submitted_documents(email)
    if df.empty:
        return html.Div([
            html.I(className="bi bi-inbox", style={"fontSize": "2.5rem", "color": "rgba(255,255,255,0.2)"}),
            html.P("No documents submitted yet", className="mt-2", style={"color": "rgba(255,255,255,0.4)"})
        ], style={"textAlign": "center", "padding": "60px 0"})

    rows = []
    for _, r in df.iterrows():
        status = r["Status"]
        pill_class = "status-badge-success" if status == "Processed" else "status-badge-running"
        rows.append(
            html.Button(
                [
                    html.Div([
                        html.I(className="bi bi-file-earmark-pdf",
                               style={"fontSize": "1.3rem", "color": "#2FA9E0", "marginRight": "12px"}),
                        html.Div([
                            html.Div(r["File Name"], style={
                                "fontWeight": "600", "fontSize": "0.85rem", "wordBreak": "break-all"}),
                            html.Small(f"Submitted {r['Submitted']}",
                                       style={"color": "rgba(255,255,255,0.5)"})
                        ])
                    ], style={"display": "flex", "alignItems": "center", "flex": "1", "textAlign": "left"}),
                    html.Span(status, className=pill_class, style={"marginLeft": "12px", "whiteSpace": "nowrap"}),
                    html.I(className="bi bi-eye", style={"marginLeft": "12px", "color": "rgba(255,255,255,0.6)"}),
                ],
                id={"type": "submission-row", "path": r["file_path"]},
                n_clicks=0,
                className="submission-row-btn",
            )
        )

    listing = html.Div(rows, style={"maxHeight": "420px", "overflowY": "auto"})

    if scope == "all":
        note = html.P(
            [html.I(className="bi bi-info-circle me-2"),
             "You have no submissions yet — showing all recent submissions across users."],
            style={"color": "rgba(255,255,255,0.5)", "fontSize": "0.8rem", "marginBottom": "10px"}
        )
        return html.Div([note, listing])

    return listing


def _load_pdf_iframe(file_path):
    """Download a PDF from the UC volume and return an <iframe> data-URI viewer."""
    try:
        # Stored paths look like 'dbfs:/Volumes/...'; the Files API wants '/Volumes/...'.
        vol_path = file_path
        if vol_path.startswith("dbfs:"):
            vol_path = vol_path[len("dbfs:"):]
        resp = w.files.download(vol_path)
        # DownloadResponse.contents is a streaming file-like object
        raw = resp.contents.read() if hasattr(resp, "contents") else resp.read()
        b64 = base64.b64encode(raw).decode("utf-8")
        src = f"data:application/pdf;base64,{b64}"
        return html.Iframe(
            src=src,
            style={"width": "100%", "height": "75vh", "border": "none", "borderRadius": "8px"},
        )
    except Exception as e:
        return html.Div(
            [html.I(className="bi bi-exclamation-triangle me-2"),
             f"Could not load PDF: {str(e)}"],
            style={"color": "#ff5252", "padding": "20px"},
        )


@callback(
    Output("pdf-modal", "is_open"),
    Output("pdf-modal-title", "children"),
    Output("pdf-modal-body", "children"),
    Input({"type": "submission-row", "path": ALL}, "n_clicks"),
    Input({"type": "rejected-row", "path": ALL}, "n_clicks"),
    Input("pdf-modal-close", "n_clicks"),
    State("pdf-modal", "is_open"),
    prevent_initial_call=True,
)
def toggle_pdf_modal(submission_clicks, rejected_clicks, close_click, is_open):
    """Open the PDF viewer when a submission OR rejected-document row is clicked."""
    trigger = ctx.triggered_id
    if trigger == "pdf-modal-close":
        return False, no_update, no_update

    # A row was clicked. ctx.triggered_id is the dict id carrying the file_path.
    if isinstance(trigger, dict) and trigger.get("type") in ("submission-row", "rejected-row"):
        # Ignore phantom fires where no click actually happened (all n_clicks 0/None).
        relevant = submission_clicks if trigger["type"] == "submission-row" else rejected_clicks
        if not any(relevant or []):
            return no_update, no_update, no_update
        file_path = trigger.get("path")
        title = file_path.rsplit("/", 1)[-1] if file_path else "Document"
        return True, title, _load_pdf_iframe(file_path)

    return no_update, no_update, no_update


@callback(
    Output("run-trigger-status", "children"),
    Input("run-pipeline-btn", "n_clicks"),
    prevent_initial_call=True
)
def trigger_pipeline(n):
    """Trigger pipeline run via Jobs API."""
    if not JOB_ID:
        return dbc.Alert(
            "JOB_ID not configured",
            color="warning",
            style={"borderRadius": "10px"}
        )
    try:
        run = w.jobs.run_now(job_id=int(JOB_ID))
        return dbc.Alert(
            [html.I(className="bi bi-play-fill me-2"), f"Pipeline triggered! Run ID: {run.run_id}"],
            color="success",
            style={"borderRadius": "10px"},
            duration=6000
        )
    except Exception as e:
        return dbc.Alert(
            [html.I(className="bi bi-exclamation-circle me-2"), f"Failed: {str(e)}"],
            color="danger",
            style={"borderRadius": "10px"}
        )


@callback(
    Output("runs-table", "children"),
    Input("refresh-interval", "n_intervals"),
    Input("run-trigger-status", "children")
)
def refresh_runs(n, _):
    """Refresh job runs table with file counts."""
    runs = get_job_runs_with_file_counts()
    if not runs:
        return html.Div([
            html.I(className="bi bi-clock-history", style={"fontSize": "2.5rem", "color": "rgba(255,255,255,0.2)"}),
            html.P("No pipeline runs yet", className="mt-2", style={"color": "rgba(255,255,255,0.4)"})
        ], style={"textAlign": "center", "padding": "60px 0"})

    cards = []
    for run in runs:
        badge_class = {
            "Success": "status-badge-success",
            "Failed": "status-badge-failed",
            "Running": "status-badge-running"
        }.get(run["status"], "status-badge-running")

        # Build card with collapsible file list
        card_body = [
            html.Div([
                html.Div([
                    html.Span(f"Run #{run['run_id']}", style={"fontWeight": "600"}),
                    html.Span(run["status"], className=badge_class, style={"marginLeft": "12px"})
                ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),
                html.Small(run["start_time"], style={"color": "rgba(255,255,255,0.5)"})
            ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),
            html.Div([
                html.Small(f"Duration: {run['duration']}", style={"color": "rgba(255,255,255,0.5)"}),
                html.Small(f" | {run['failure_reason']}", style={"color": "#ff5252"}) if run["failure_reason"] else None
            ], className="mt-1")
        ]

        # Add file count and collapsible list
        if run["files_count"] > 0:
            file_items = [html.Li(fn, style={"marginBottom": "4px"}) for fn in run["file_names"]]
            card_body.append(
                html.Details([
                    html.Summary(
                        [html.I(className="bi bi-file-pdf me-2"), f"{run['files_count']} file(s) processed"],
                        style={"cursor": "pointer", "color": "#2FA9E0", "fontWeight": "500"}
                    ),
                    html.Ul(file_items, style={"marginTop": "8px"})
                ], className="collapsible-files")
            )
        else:
            card_body.append(
                html.Small("0 files processed", style={"color": "rgba(255,255,255,0.4)", "marginTop": "8px"})
            )

        card = dbc.Card([dbc.CardBody(card_body)], className="glass-card mb-2")
        cards.append(card)

    return cards


def _reason_badges(reason_str):
    """Split a semicolon-joined rejection_reason into individual red pills."""
    parts = [p.strip() for p in (reason_str or "").split(";") if p.strip()]
    return [
        html.Span(p, className="status-badge-failed",
                  style={"marginRight": "6px", "marginBottom": "4px", "display": "inline-block"})
        for p in parts
    ]


def _dq_summary(df):
    """Header count + a horizontal bar chart of how many files hit each reason."""
    if df.empty:
        return html.Div()

    # Count each individual reason across all rejected files.
    counts = {}
    for reason_str in df["Reason"]:
        for p in (reason_str or "").split(";"):
            p = p.strip()
            if not p:
                continue
            # Collapse "AI extraction failed: <detail>" into one bucket.
            key = "AI extraction failed" if p.startswith("AI extraction failed") else p
            counts[key] = counts.get(key, 0) + 1

    ordered = sorted(counts.items(), key=lambda kv: kv[1])
    labels = [k for k, _ in ordered]
    values = [v for _, v in ordered]

    fig = go.Figure(go.Bar(
        x=values, y=labels, orientation="h",
        marker=dict(color="#ff5252"),
        text=values, textposition="auto",
    ))
    fig.update_layout(
        height=max(140, 34 * len(labels) + 60),
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="rgba(255,255,255,0.8)", size=11),
        xaxis=dict(gridcolor="rgba(255,255,255,0.08)", zeroline=False, dtick=1),
        yaxis=dict(automargin=True),
        showlegend=False,
    )
    return html.Div([
        html.Div([
            html.Span(str(len(df)), style={"fontSize": "1.6rem", "fontWeight": "700", "color": "#ff5252"}),
            html.Span(" document(s) rejected — breakdown by reason:",
                      style={"color": "rgba(255,255,255,0.7)", "marginLeft": "8px"}),
        ], className="mb-2"),
        dcc.Graph(figure=fig, config={"displayModeBar": False}),
    ])


@callback(
    Output("rejected-list", "children"),
    Output("dq-summary", "children"),
    Input("refresh-interval", "n_intervals"),
    Input("dq-search", "value"),
    Input("dq-reason-filter", "value"),
)
def update_data_quality(n, search, reason):
    """Render the rejected-documents list (search + reason filtered) and the summary chart."""
    df = get_rejected_documents()
    summary = _dq_summary(df)

    if df.empty:
        empty_state = html.Div([
            html.I(className="bi bi-patch-check", style={"fontSize": "2.5rem", "color": "rgba(63,182,168,0.5)"}),
            html.P("No rejected documents — every processed file passed validation.",
                   className="mt-2", style={"color": "rgba(255,255,255,0.4)"})
        ], style={"textAlign": "center", "padding": "60px 0"})
        return empty_state, summary

    # Apply reason filter (substring so "AI extraction failed" matches the detailed message).
    if reason and reason != "All":
        df = df[df["Reason"].str.contains(reason, case=False, na=False)]

    # Apply free-text search across file name + submitter.
    if search and search.strip():
        s = search.strip().lower()
        mask = (df["File Name"].str.lower().str.contains(s, na=False)
                | df["Submitter"].str.lower().str.contains(s, na=False))
        df = df[mask]

    if df.empty:
        return html.Div(
            "No rejected documents match your filters.",
            style={"color": "rgba(255,255,255,0.4)", "padding": "30px 0", "textAlign": "center"}
        ), summary

    rows = []
    for _, r in df.iterrows():
        rows.append(
            html.Button(
                [
                    html.Div([
                        html.I(className="bi bi-file-earmark-pdf",
                               style={"fontSize": "1.3rem", "color": "#ff5252", "marginRight": "12px"}),
                        html.Div([
                            html.Div(r["File Name"], style={
                                "fontWeight": "600", "fontSize": "0.85rem", "wordBreak": "break-all"}),
                            html.Small(f"{r['Submitter']} · detected {r['Detected']}",
                                       style={"color": "rgba(255,255,255,0.5)"}),
                            html.Div(_reason_badges(r["Reason"]), className="mt-2"),
                        ]),
                    ], style={"display": "flex", "alignItems": "flex-start", "flex": "1", "textAlign": "left"}),
                    html.I(className="bi bi-eye", style={"marginLeft": "12px", "color": "rgba(255,255,255,0.6)"}),
                ],
                id={"type": "rejected-row", "path": r["file_path"]},
                n_clicks=0,
                className="submission-row-btn",
                style={"alignItems": "flex-start"},
            )
        )

    return html.Div(rows, style={"maxHeight": "520px", "overflowY": "auto"}), summary


@callback(
    Output("chat-messages", "children"),
    Output("conversation-store", "data"),
    Output("chat-input", "value"),
    Input("send-btn", "n_clicks"),
    Input("chat-input", "n_submit"),
    State("chat-input", "value"),
    State("conversation-store", "data"),
    State("chat-messages", "children"),
    prevent_initial_call=True
)
def handle_chat(n_clicks, n_submit, message, conv_data, current_messages):
    """Handle Genie chat interactions using SDK."""
    if not message or not message.strip():
        return no_update, no_update, no_update

    space_id = resolve_genie_space_id()
    if not space_id:
        current_messages.append(
            html.Div([
                html.Div(message, className="chat-bubble-user", style={"marginLeft": "auto"}),
                html.Div(
                    f"Genie Agent '{GENIE_SPACE_NAME}' not found yet. Run the processing "
                    f"job (Pipeline Ops tab) — it provisions the Genie space after the "
                    f"pipeline completes.",
                    className="chat-bubble-genie"
                )
            ])
        )
        return current_messages, conv_data, ""

    # Add user message to display
    current_messages.append(
        html.Div(html.Div(message, className="chat-bubble-user"), style={"display": "flex", "justifyContent": "flex-end"})
    )

    try:
        conversation_id = conv_data.get("conversation_id")

        if not conversation_id:
            # Start new conversation with SDK
            response = w.genie.start_conversation_and_wait(
                space_id=space_id,
                content=message
            )
            conversation_id = response.conversation_id
        else:
            # Continue existing conversation
            response = w.genie.create_message_and_wait(
                space_id=space_id,
                conversation_id=conversation_id,
                content=message
            )
        # start_conversation_and_wait / create_message_and_wait return a GenieMessage
        # whose message-id attribute is `id` (not `message_id`).
        message_id = getattr(response, "id", None) or getattr(response, "message_id", None)

        conv_data["conversation_id"] = conversation_id

        # Fetch detailed message with attachments
        result = get_genie_response(space_id, conversation_id, message_id)

        if "error" in result:
            current_messages.append(
                html.Div(f"Error: {result['error']}", className="chat-bubble-genie")
            )
        else:
            reply_parts = []
            text_content = result.get("text", "")

            # Add text response
            if text_content:
                reply_parts.append(html.P(text_content, style={"whiteSpace": "pre-wrap"}))

            # Process query result tables
            for tbl in result.get("tables", []):
                query_sql = tbl.get("sql", "")
                columns = tbl.get("columns", [])
                rows = tbl.get("rows", [])

                # Show a description line if Genie provided one
                if tbl.get("description"):
                    reply_parts.append(html.P(tbl["description"], style={"whiteSpace": "pre-wrap"}))

                # Show SQL if available
                if query_sql:
                    reply_parts.append(
                        html.Details([
                            html.Summary("SQL Query", style={"cursor": "pointer", "color": "#2FA9E0"}),
                            html.Code(query_sql, style={"fontSize": "0.8rem", "whiteSpace": "pre-wrap"})
                        ], className="mt-2")
                    )

                # Show results table if available
                if columns and rows:
                    try:
                        df = pd.DataFrame(rows, columns=columns)
                        reply_parts.append(
                            dash_table.DataTable(
                                data=df.head(20).to_dict("records"),
                                columns=[{"name": c, "id": c} for c in df.columns],
                                style_header={
                                    "background": "rgba(47,169,224,0.15)",
                                    "color": "white",
                                    "fontWeight": "600"
                                },
                                style_cell={
                                    "background": "transparent",
                                    "color": "white",
                                    "border": "1px solid rgba(255,255,255,0.05)",
                                    "fontSize": "0.8rem",
                                    "padding": "8px"
                                },
                                style_table={"marginTop": "10px"}
                            )
                        )
                    except Exception as e:
                        reply_parts.append(html.P(f"Could not render table: {str(e)}", style={"fontSize": "0.8rem", "color": "#ff5252"}))

            if reply_parts:
                current_messages.append(html.Div(reply_parts, className="chat-bubble-genie"))
            else:
                current_messages.append(
                    html.Div(
                        "I processed your question but received no detailed response. Try rephrasing.",
                        className="chat-bubble-genie"
                    )
                )

    except Exception as e:
        error_msg = f"Error: {str(e)}"
        print(f"Chat error: {error_msg}")
        current_messages.append(html.Div(error_msg, className="chat-bubble-genie"))

    return current_messages, conv_data, ""


# --- Server ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=DATABRICKS_APP_PORT, debug=False)
