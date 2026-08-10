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
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
body {
    font-family: 'Inter', sans-serif !important;
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
            statement=f"SELECT file_name, submission_time, file_size, processing_status FROM {CATALOG}.{BRONZE_SCHEMA}.document_submissions WHERE submitter_email = '{user_email}' ORDER BY submission_time DESC LIMIT 50"
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
        dbc.Row([dbc.Col([dbc.Card([dbc.CardBody([html.H4(f"Welcome, {user['name']}!", className="mb-2", style={"fontWeight": "600"}), html.P(f"Logged in as {user['email']} | Upload insurance documents for AI processing", style={"color": "rgba(255,255,255,0.6)", "marginBottom": "0"})])], className="glass-card mb-4")])]),
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
            html.Div(id="chat-messages", style={"height": "450px", "overflowY": "auto", "padding": "20px", "borderRadius": "12px", "background": "rgba(0,0,0,0.2)", "border": "1px solid rgba(255,255,255,0.05)"}, children=[html.Div([html.Div("Hi! I'm your Document Intelligence assistant. Ask me anything about your insurance documents - sales performance, claims status, outstanding items, and more!", className="chat-bubble-genie")])]),
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
        card = dbc.Card([dbc.CardBody([html.Div([html.Div([html.Span(f"Run #{run['run_id']}", style={"fontWeight": "600"}), html.Span(run["status"], className=badge_class, style={"marginLeft": "12px"})]), html.Small(run["start_time"], style={"color": "rgba(255,255,255,0.5)"})], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}), html.Div([html.Small(f"Duration: {run['duration']}", style={"color": "rgba(255,255,255,0.5)"}), html.Small(f" | {run['failure_reason']}", style={"color": "#ff5252"}) if run["failure_reason"] else None], className="mt-1")])], className="glass-card mb-2")
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
            current_messages.append(html.Div(f"Error: {result['error']}", className="chat-bubble-genie"))
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