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
    """Get the current logged-in user."""
    try:
        me = w.current_user.me()
        return {"email": me.user_name, "name": me.display_name or me.user_name.split("@")[0].replace(".", " ").title()}
    except Exception:
        return {"email": "demo.user@company.com", "name": "Demo User"}


def get_submitted_documents(user_email):
    """Fetch previously submitted documents for the user."""
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
    """Fetch last 5 job runs."""
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
    """Poll Genie API until response is ready."""
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


# --- Layout Builders ---
def build_header():
    user = get_current_user()
    return html.Div([
        html.Div([
            html.Div([
                html.Div([html.I(className="bi bi-file-earmark-medical", style={"fontSize": "2.5rem"})], style={"marginRight": "20px"}),
                html.Div([
                    html.H2("Insurance Document Intelligence", className="mb-0", style={"fontWeight": "700", "letterSpacing": "-0.5px"}),
                    html.P("Pacific Shield Insurance Group - AI-Powered Document Processing", className="mb-0", style={"opacity": "0.85", "fontSize": "0.9rem"})
                ])
            ], style={"display": "flex", "alignItems": "center"}),
            html.Div([
                html.Div([
                    html.Span(user["name"], style={"fontWeight": "600", "fontSize": "0.95rem"}),
                    html.Br(),
                    html.Span(user["email"], style={"opacity": "0.7", "fontSize": "0.8rem"})
                ], style={"textAlign": "right", "marginRight": "12px"}),
                html.Div(user["name"][0].upper(), style={"width": "44px", "height": "44px", "borderRadius": "50%", "background": "rgba(255,255,255,0.2)", "display": "flex", "alignItems": "center", "justifyContent": "center", "fontSize": "1.2rem", "fontWeight": "700"})
            ], style={"display": "flex", "alignItems": "center"})
        ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "color": "white"})
    ], className="gradient-header")


def build_tab1():
    user = get_current_user()
    return html.Div([
        dbc.Row([dbc.Col([dbc.Card([dbc.CardBody([
            html.H4(f"Welcome, {user['name']}!", className="mb-2", style={"fontWeight": "600"}),
            html.P(f"Logged in as {user['email']} | Upload insurance documents for AI processing", style={"color": "rgba(255,255,255,0.6)", "marginBottom": "0"})
        ])], className="glass-card mb-4")])]),
        dbc.Row([
            dbc.Col([dbc.Card([dbc.CardBody([
                html.H5("Submit Documents", className="mb-3", style={"fontWeight": "600"}),
                dcc.Upload(
                    id="upload-pdf",
                    children=html.Div([
                        html.I(className="bi bi-cloud-arrow-up", style={"fontSize": "3rem", "color": "#00b4d8"}),
                        html.P("Drag & Drop or Click to Upload PDF", className="mt-2 mb-1", style={"fontWeight": "500"}),
                        html.P("Supports .pdf files up to 50MB", style={"fontSize": "0.8rem", "color": "rgba(255,255,255,0.5)"})
                    ]),
                    className="upload-zone",
                    multiple=True,
                    accept=".pdf"
                ),
                html.Div(id="upload-status", className="mt-3")
            ])], className="glass-card")], md=6),
            dbc.Col([dbc.Card([dbc.CardBody([
                html.Div([
                    html.H5("Submission History", className="mb-0", style={"fontWeight": "600"}),
                    dbc.Button([html.I(className="bi bi-arrow-clockwise me-1"), "Refresh"], id="refresh-submissions", size="sm", color="info", outline=True)
                ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),
                html.Div(id="submissions-table", className="mt-3")
            ])], className="glass-card")], md=6)
        ])
    ], style={"padding": "0 20px"})


def build_tab2():
    runs = get_job_runs()
    run_cards = []
    for run in runs:
        badge_class = "status-badge-success" if run["status"] == "Success" else "status-badge-failed" if run["status"] == "Failed" else "status-badge-pending"
        run_cards.append(dbc.Card([dbc.CardBody([
            html.Div([
                html.Div([html.Span(f"Run #{run['run_id']}", style={"fontWeight": "600"}), html.Span(run["status"], className=badge_class, style={"marginLeft": "12px"})]),
                html.Small(run["start_time"], style={"color": "rgba(255,255,255,0.5)"})
            ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),
            html.Div([
                html.Small(f"Duration: {run['duration']}", style={"color": "rgba(255,255,255,0.6)"}) if run["duration"] else html.Span(),
                html.Small(run["failure_reason"], style={"color": "#ff5252", "marginLeft": "12px"}) if run["failure_reason"] else html.Span()
            ], className="mt-2")
        ])], className="glass-card mb-2"))

    if not run_cards:
        run_cards = [html.P("No pipeline runs found.", style={"color": "rgba(255,255,255,0.5)", "textAlign": "center", "padding": "20px"})]

    return html.Div([
        dbc.Row([dbc.Col([dbc.Card([dbc.CardBody([
            html.Div([
                html.H5("Pipeline Control", className="mb-0", style={"fontWeight": "600"}),
                dbc.Button([html.I(className="bi bi-play-fill me-2"), "Trigger Pipeline"], id="trigger-pipeline", color="info", size="sm")
            ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),
            html.Div(id="pipeline-status", className="mt-2")
        ])], className="glass-card mb-4")])]),
        dbc.Row([dbc.Col([dbc.Card([dbc.CardBody([
            html.Div([
                html.H5("Recent Runs", className="mb-0", style={"fontWeight": "600"}),
                dbc.Button([html.I(className="bi bi-arrow-clockwise me-1"), "Refresh"], id="refresh-runs", size="sm", color="info", outline=True)
            ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),
            html.Div(run_cards, id="runs-container", className="mt-3")
        ])], className="glass-card")])])
    ], style={"padding": "0 20px"})


def build_tab3():
    return html.Div([
        dbc.Row([dbc.Col([dbc.Card([dbc.CardBody([
            html.Div([
                html.Div([html.I(className="bi bi-stars me-2", style={"color": "#6c63ff"}), html.H5("Genie Chat", className="mb-0 d-inline", style={"fontWeight": "600"})]),
                html.Small("Ask questions about your insurance data", style={"color": "rgba(255,255,255,0.5)"})
            ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),
            html.Div(id="chat-messages", style={"height": "400px", "overflowY": "auto", "padding": "20px", "marginTop": "15px", "borderRadius": "12px", "background": "rgba(0,0,0,0.2)"}),
            html.Div([
                dbc.Input(id="chat-input", placeholder="Ask about sales, claims, or outstanding amounts...", type="text", style={"background": "rgba(255,255,255,0.05)", "border": "1px solid rgba(255,255,255,0.1)", "color": "white", "borderRadius": "12px"}),
                dbc.Button([html.I(className="bi bi-send-fill")], id="send-chat", color="info", className="ms-2", style={"borderRadius": "12px"})
            ], style={"display": "flex", "marginTop": "15px"})
        ])], className="glass-card")])])
    ], style={"padding": "0 20px"})


# --- App Layout ---
app.layout = html.Div([
    html.Style(CUSTOM_CSS),
    build_header(),
    dbc.Tabs([
        dbc.Tab(build_tab1(), label="Document Portal", tab_id="tab-1", active_label_style={"color": "#00b4d8"}),
        dbc.Tab(build_tab2(), label="Pipeline Ops", tab_id="tab-2", active_label_style={"color": "#00b4d8"}),
        dbc.Tab(build_tab3(), label="Genie Chat", tab_id="tab-3", active_label_style={"color": "#00b4d8"}),
    ], id="tabs", active_tab="tab-1", style={"padding": "0 20px"}),
    dcc.Store(id="chat-history", data=[])
], style={"maxWidth": "1400px", "margin": "0 auto", "padding": "0 20px 40px"})


# --- Callbacks ---
@callback(
    Output("upload-status", "children"),
    Input("upload-pdf", "contents"),
    State("upload-pdf", "filename"),
    prevent_initial_call=True
)
def handle_upload(contents_list, filenames):
    if not contents_list:
        return no_update
    user = get_current_user()
    results = []
    for content, filename in zip(contents_list, filenames):
        try:
            content_string = content.split(",")[1]
            decoded = base64.b64decode(content_string)
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            email_prefix = user["email"].split("@")[0].replace(".", "_")
            dest_name = f"{email_prefix}_{timestamp}_{filename}"
            dest_path = f"{VOLUME_PATH}/{dest_name}"
            w.files.upload(dest_path, decoded)
            results.append(dbc.Alert(f"Uploaded: {filename}", color="success", dismissable=True, className="mb-2"))
        except Exception as e:
            results.append(dbc.Alert(f"Failed: {filename} - {str(e)}", color="danger", dismissable=True, className="mb-2"))
    return html.Div(results)


@callback(
    Output("submissions-table", "children"),
    Input("refresh-submissions", "n_clicks"),
    prevent_initial_call=False
)
def refresh_submissions(n):
    user = get_current_user()
    df = get_submitted_documents(user["email"])
    if df.empty:
        return html.P("No submissions found.", style={"color": "rgba(255,255,255,0.5)", "textAlign": "center", "padding": "20px"})
    return dash_table.DataTable(
        data=df.to_dict("records"),
        columns=[{"name": c, "id": c} for c in df.columns],
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": "rgba(0,180,216,0.1)", "color": "white", "fontWeight": "600", "border": "none"},
        style_cell={"backgroundColor": "transparent", "color": "white", "border": "1px solid rgba(255,255,255,0.05)", "padding": "10px"},
        style_data_conditional=[{"if": {"filter_query": "{Status} = PROCESSED"}, "color": "#00e676"}],
        page_size=10
    )


@callback(
    Output("pipeline-status", "children"),
    Input("trigger-pipeline", "n_clicks"),
    prevent_initial_call=True
)
def trigger_pipeline(n):
    if not JOB_ID:
        return dbc.Alert("JOB_ID not configured", color="warning")
    try:
        run = w.jobs.run_now(job_id=int(JOB_ID))
        return dbc.Alert(f"Pipeline triggered! Run ID: {run.run_id}", color="success", dismissable=True)
    except Exception as e:
        return dbc.Alert(f"Error: {str(e)}", color="danger", dismissable=True)


@callback(
    Output("runs-container", "children"),
    Input("refresh-runs", "n_clicks"),
    prevent_initial_call=True
)
def refresh_runs(n):
    runs = get_job_runs()
    if not runs:
        return [html.P("No runs found.", style={"color": "rgba(255,255,255,0.5)", "textAlign": "center"})]
    run_cards = []
    for run in runs:
        badge_class = "status-badge-success" if run["status"] == "Success" else "status-badge-failed" if run["status"] == "Failed" else "status-badge-pending"
        run_cards.append(dbc.Card([dbc.CardBody([
            html.Div([
                html.Div([html.Span(f"Run #{run['run_id']}", style={"fontWeight": "600"}), html.Span(run["status"], className=badge_class, style={"marginLeft": "12px"})]),
                html.Small(run["start_time"], style={"color": "rgba(255,255,255,0.5)"})
            ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}),
            html.Div([
                html.Small(f"Duration: {run['duration']}", style={"color": "rgba(255,255,255,0.6)"}) if run["duration"] else html.Span(),
                html.Small(run["failure_reason"], style={"color": "#ff5252", "marginLeft": "12px"}) if run["failure_reason"] else html.Span()
            ], className="mt-2")
        ])], className="glass-card mb-2"))
    return run_cards


@callback(
    Output("chat-messages", "children"),
    Output("chat-history", "data"),
    Output("chat-input", "value"),
    Input("send-chat", "n_clicks"),
    State("chat-input", "value"),
    State("chat-history", "data"),
    prevent_initial_call=True
)
def handle_chat(n_clicks, user_input, history):
    if not user_input or not user_input.strip():
        return no_update, no_update, no_update
    if not GENIE_SPACE_ID:
        history = history or []
        history.append({"role": "user", "content": user_input})
        history.append({"role": "genie", "content": "GENIE_SPACE_ID is not configured. Please set it in the app environment variables."})
        bubbles = []
        for msg in history:
            css_class = "chat-bubble-user" if msg["role"] == "user" else "chat-bubble-genie"
            bubbles.append(html.Div(msg["content"], className=css_class))
        return bubbles, history, ""

    history = history or []
    history.append({"role": "user", "content": user_input})

    # Start Genie conversation
    try:
        conv_resp = w.api_client.do("POST", f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation", body={"content": user_input})
        conversation_id = conv_resp.get("conversation_id", "")
        message_id = conv_resp.get("message_id", "")
        if not conversation_id or not message_id:
            history.append({"role": "genie", "content": "Failed to start conversation with Genie."})
        else:
            result = poll_genie_response(GENIE_SPACE_ID, conversation_id, message_id)
            if "error" in result:
                history.append({"role": "genie", "content": f"Error: {result['error']}"})
            else:
                # Extract response text
                attachments = result.get("attachments", [])
                answer_text = ""
                for att in attachments:
                    if att.get("text", {}).get("content"):
                        answer_text += att["text"]["content"] + "\n"
                    elif att.get("query", {}).get("description"):
                        answer_text += att["query"]["description"] + "\n"
                if not answer_text:
                    answer_text = "Query completed. Check the Genie Space for detailed results."
                history.append({"role": "genie", "content": answer_text.strip()})
    except Exception as e:
        history.append({"role": "genie", "content": f"Error: {str(e)}"})

    bubbles = []
    for msg in history:
        css_class = "chat-bubble-user" if msg["role"] == "user" else "chat-bubble-genie"
        bubbles.append(html.Div(msg["content"], className=css_class))
    return bubbles, history, ""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
