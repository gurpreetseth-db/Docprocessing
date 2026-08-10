import os
import time
import json
from datetime import datetime, timezone
import base64
from urllib.parse import quote

import dash
from dash import dcc, html, dash_table, callback, Input, Output, State, no_update, ctx
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from databricks.sdk.service.genie import MessageStatus

try:
    from flask import request
    HAS_FLASK_REQUEST = True
except ImportError:
    HAS_FLASK_REQUEST = False

# --- Configuration from Environment ---
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_APP_PORT = int(os.environ.get("DATABRICKS_APP_PORT", "8050"))
DATABRICKS_WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")
JOB_ID = os.environ.get("JOB_ID", "")
PIPELINE_ID = os.environ.get("PIPELINE_ID", "")

CATALOG = os.environ.get("CATALOG", "DocProcessing")
BRONZE_SCHEMA = os.environ.get("BRONZE_SCHEMA", "DocProcess_Bronze")
SILVER_SCHEMA = os.environ.get("SILVER_SCHEMA", "DocProcess_Silver")
GOLD_SCHEMA = os.environ.get("GOLD_SCHEMA", "DocProcess_Gold")
VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/InputPDFs"

w = WorkspaceClient()


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
    background: linear-gradient(135deg, #0a0e1a 0%, #1a1f3a 50%, #0d1525 100%) !important;
    min-height: 100vh;
    color: #e0e0e0;
}
.gradient-header {
    background: linear-gradient(90deg, #00b4d8 0%, #0077b6 40%, #6c63ff 100%);
    padding: 20px 40px;
    border-radius: 0 0 20px 20px;
    box-shadow: 0 4px 30px rgba(0, 180, 216, 0.3);
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
    box-shadow: 0 12px 40px rgba(0, 180, 216, 0.15) !important;
}
.welcome-banner {
    background: rgba(0, 180, 216, 0.1);
    border-left: 4px solid #00b4d8;
    padding: 16px;
    border-radius: 8px;
    color: #e0e0e0;
}
.welcome-banner .welcome-title {
    font-size: 1.3rem;
    font-weight: 600;
    color: #00b4d8;
    margin-bottom: 4px;
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
.status-badge-running {
    background: linear-gradient(135deg, #ff9100, #ffab40);
    color: #fff; padding: 4px 12px; border-radius: 20px;
    font-size: 0.75rem; font-weight: 600;
}
.status-badge-ingested {
    background: linear-gradient(135deg, #00b4d8, #0077b6);
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
.collapsible-files {
    margin-top: 8px;
    padding: 8px;
    background: rgba(0, 0, 0, 0.2);
    border-radius: 8px;
    font-size: 0.85rem;
}
.collapsible-files summary {
    cursor: pointer;
    color: #00b4d8;
    font-weight: 500;
}
.collapsible-files ul {
    margin-top: 8px;
    margin-left: 16px;
    color: rgba(255, 255, 255, 0.7);
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
    title="Service Plan Document Intelligence"
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
            results = w.statement_execution.execute_statement(
                warehouse_id=DATABRICKS_WAREHOUSE_ID,
                statement=f"SELECT full_name FROM {CATALOG}.{BRONZE_SCHEMA}.users WHERE email = '{email_escaped}' LIMIT 1"
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


def get_submitted_documents(user_email):
    """Query bronze.document_submissions for current user's submissions."""
    try:
        if not DATABRICKS_WAREHOUSE_ID:
            return pd.DataFrame(columns=["File Name", "Submitted", "Size (bytes)", "Status"])

        email_escaped = escape_sql_string(user_email)
        query = f"""
        SELECT file_name, submission_time, file_size, processing_status
        FROM {CATALOG}.{BRONZE_SCHEMA}.document_submissions
        WHERE submitter_email = '{email_escaped}'
        ORDER BY submission_time DESC
        LIMIT 50
        """
        results = w.statement_execution.execute_statement(
            warehouse_id=DATABRICKS_WAREHOUSE_ID,
            statement=query
        )
        if results.result and results.result.data_array:
            df = pd.DataFrame(
                results.result.data_array,
                columns=["File Name", "Submitted", "Size (bytes)", "Status"]
            )
            df["Submitted"] = pd.to_datetime(df["Submitted"]).dt.strftime("%Y-%m-%d %H:%M:%S")
            return df
    except Exception as e:
        print(f"Error fetching documents: {e}")

    return pd.DataFrame(columns=["File Name", "Submitted", "Size (bytes)", "Status"])


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
                    results = w.statement_execution.execute_statement(
                        warehouse_id=DATABRICKS_WAREHOUSE_ID,
                        statement=query
                    )
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
        # Get message (with polling via SDK)
        message = w.genie.get_message(space_id=space_id, conversation_id=conversation_id, message_id=message_id)

        # Check message status
        if message.status == MessageStatus.FAILED or message.status == MessageStatus.CANCELLED:
            return {"error": "Message processing failed", "status": str(message.status)}

        # Extract text content
        result = {
            "content": message.content or "",
            "status": str(message.status),
            "attachments": []
        }

        # Process attachments (query results, etc.)
        if message.attachments:
            for attachment in message.attachments:
                att_dict = {}
                if hasattr(attachment, 'type'):
                    att_dict["type"] = str(attachment.type)

                # For QUERY_RESULT attachments, fetch the query result
                if hasattr(attachment, 'query_id') and attachment.query_id:
                    try:
                        query_result = w.genie.get_message_query_result(
                            space_id=space_id,
                            conversation_id=conversation_id,
                            message_id=message_id,
                            query_id=attachment.query_id
                        )
                        if query_result:
                            att_dict["query"] = {
                                "query": query_result.query or "",
                                "columns": [{"name": c} for c in (query_result.columns or [])],
                                "rows": query_result.rows or []
                            }
                    except Exception as e:
                        print(f"Error fetching query result: {e}")

                if att_dict:
                    result["attachments"].append(att_dict)

        return result
    except Exception as e:
        return {"error": str(e)}


def build_header():
    """Build gradient header with user info."""
    user = get_current_user()
    return html.Div([
        html.Div([
            html.Div([
                html.Div(
                    [html.I(className="bi bi-file-earmark-pdf", style={"fontSize": "2.5rem"})],
                    style={"marginRight": "20px"}
                ),
                html.Div([
                    html.H2(
                        "Service Plan Document Intelligence",
                        className="mb-0",
                        style={"fontWeight": "700", "letterSpacing": "-0.5px"}
                    ),
                    html.P(
                        "Home Based Support Services - AI-Powered Document Processing",
                        className="mb-0",
                        style={"opacity": "0.85", "fontSize": "0.9rem"}
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


def build_tab1():
    """Submit & Track tab."""
    user = get_current_user()
    return html.Div([
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.Div([
                        html.Span("Welcome back, ", style={"fontSize": "1.1rem"}),
                        html.Span(user['name'], style={"fontSize": "1.1rem", "fontWeight": "600", "color": "#00b4d8"})
                    ], style={"marginBottom": "8px"}),
                    html.Span(
                        f"Submit Service Plan PDFs for processing. Logged in as {user['email']}",
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
                                html.I(className="bi bi-cloud-arrow-up", style={"fontSize": "3rem", "color": "#00b4d8"}),
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
                        html.Div(id="submissions-table")
                    ])
                ], className="glass-card mb-4", style={"minHeight": "350px"})
            ], md=7)
        ])
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
                                    "background": "linear-gradient(135deg, #0077b6, #00b4d8)",
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
                        html.Div(id="runs-table")
                    ])
                ], className="glass-card")
            ])
        ])
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
                        "Ask questions in natural language about service plans, care hours, funding, and client conditions",
                        style={"color": "rgba(255,255,255,0.5)", "fontSize": "0.85rem", "marginBottom": "0"}
                    )
                ], className="mb-3"),
                html.Div(
                    id="chat-messages",
                    style={
                        "height": "450px", "overflowY": "auto", "padding": "20px",
                        "borderRadius": "12px", "background": "rgba(0,0,0,0.2)",
                        "border": "1px solid rgba(255,255,255,0.05)"
                    },
                    children=[
                        html.Div([
                            html.Div(
                                "Hi! I'm your Service Plan Intelligence assistant. Ask me anything about your documents - client details, care hours, conditions, funders, and more!",
                                className="chat-bubble-genie"
                            )
                        ])
                    ]
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
                                "background": "linear-gradient(135deg, #0077b6, #00b4d8)",
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
app.layout = html.Div([
    dcc.Store(id="conversation-store", data={"conversation_id": None, "messages": []}),
    dcc.Store(id="user-store", data=get_current_user()),
    build_header(),
    dbc.Tabs([
        dbc.Tab(build_tab1(), label="Submit & Track", tab_id="tab-1", label_style={"fontSize": "0.9rem"}),
        dbc.Tab(build_tab2(), label="Pipeline Ops", tab_id="tab-2", label_style={"fontSize": "0.9rem"}),
        dbc.Tab(build_tab3(), label="Genie Assistant", tab_id="tab-3", label_style={"fontSize": "0.9rem"})
    ], id="main-tabs", active_tab="tab-1", style={"padding": "0 20px"}),
    dcc.Interval(id="refresh-interval", interval=30000, n_intervals=0)
])


# --- Callbacks ---

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
            # Filename convention: {email_slug}__{submission_id}__service_plan.pdf
            dest_name = f"{email_slug}__{submission_id}__service_plan.pdf"
            dest_path = f"{VOLUME_PATH}/{dest_name}"
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
    """Refresh submissions table with user's documents."""
    df = get_submitted_documents(user_data["email"])
    if df.empty:
        return html.Div([
            html.I(className="bi bi-inbox", style={"fontSize": "2.5rem", "color": "rgba(255,255,255,0.2)"}),
            html.P("No documents submitted yet", className="mt-2", style={"color": "rgba(255,255,255,0.4)"})
        ], style={"textAlign": "center", "padding": "60px 0"})

    return dash_table.DataTable(
        data=df.to_dict("records"),
        columns=[{"name": c, "id": c} for c in df.columns],
        style_table={"overflowX": "auto"},
        style_header={
            "background": "rgba(0,180,216,0.15)",
            "color": "white",
            "fontWeight": "600",
            "border": "none"
        },
        style_cell={
            "background": "transparent",
            "color": "white",
            "border": "1px solid rgba(255,255,255,0.05)",
            "fontSize": "0.85rem",
            "padding": "10px"
        },
        style_data_conditional=[
            {"if": {"filter_query": "{Status} = INGESTED"}, "color": "#00e676", "fontWeight": "600"},
            {"if": {"filter_query": "{Status} = PENDING"}, "color": "#ffab40", "fontWeight": "600"}
        ],
        page_size=8
    )


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
                        style={"cursor": "pointer", "color": "#00b4d8", "fontWeight": "500"}
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

    if not GENIE_SPACE_ID:
        current_messages.append(
            html.Div([
                html.Div(message, className="chat-bubble-user", style={"marginLeft": "auto"}),
                html.Div("GENIE_SPACE_ID not configured.", className="chat-bubble-genie")
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
                space_id=GENIE_SPACE_ID,
                content=message
            )
            conversation_id = response.conversation_id
            message_id = response.message_id
        else:
            # Continue existing conversation
            response = w.genie.create_message_and_wait(
                space_id=GENIE_SPACE_ID,
                conversation_id=conversation_id,
                content=message
            )
            message_id = response.message_id

        conv_data["conversation_id"] = conversation_id

        # Fetch detailed message with attachments
        result = get_genie_response(GENIE_SPACE_ID, conversation_id, message_id)

        if "error" in result:
            current_messages.append(
                html.Div(f"Error: {result['error']}", className="chat-bubble-genie")
            )
        else:
            attachments = result.get("attachments", [])
            reply_parts = []
            text_content = result.get("content", "")

            # Add text response
            if text_content:
                reply_parts.append(html.P(text_content))

            # Process query result attachments
            for att in attachments:
                if att.get("type") == "QUERY_RESULT" and att.get("query"):
                    query_info = att.get("query", {})
                    query_sql = query_info.get("query", "")
                    columns = query_info.get("columns", [])
                    rows = query_info.get("rows", [])

                    # Show SQL if available
                    if query_sql:
                        reply_parts.append(
                            html.Details([
                                html.Summary("SQL Query", style={"cursor": "pointer", "color": "#00b4d8"}),
                                html.Code(query_sql, style={"fontSize": "0.8rem", "whiteSpace": "pre-wrap"})
                            ], className="mt-2")
                        )

                    # Show results table if available
                    if columns and rows:
                        try:
                            col_names = [c.get("name", f"Column {i}") for i, c in enumerate(columns)]
                            df = pd.DataFrame(rows, columns=col_names)
                            reply_parts.append(
                                dash_table.DataTable(
                                    data=df.head(20).to_dict("records"),
                                    columns=[{"name": c, "id": c} for c in df.columns],
                                    style_header={
                                        "background": "rgba(0,180,216,0.15)",
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
