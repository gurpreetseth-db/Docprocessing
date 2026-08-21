# Databricks notebook source
# MAGIC %md
# MAGIC # Provision the Genie Agent (Genie Space)
# MAGIC
# MAGIC Idempotently creates the **Genie Agent** — a Genie space named `${genie_space_name}` —
# MAGIC attached to the Gold dimensional tables, seeded with sample questions.
# MAGIC
# MAGIC **Why this runs AFTER the pipeline:** a Genie space references Gold tables, so those
# MAGIC materialized views must already exist. This notebook is therefore wired as a task in
# MAGIC `doc_processing_job` that depends on the pipeline task (see resources/job.yml).
# MAGIC
# MAGIC **Resolve-by-name contract:** the app looks up the space by this exact title at
# MAGIC runtime, so no space-id ever needs to be wired anywhere. Re-running is safe — if a
# MAGIC space with this title already exists, we leave it in place (and refresh its warehouse).

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# --- Read configuration from widgets (fed by the job base_parameters) ---
def _widget(name, default=""):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default

catalog          = _widget("catalog", "DocProcessing")
gold_schema      = _widget("gold_schema", "DocProcess_Gold")
genie_space_name = _widget("genie_space_name", "Doc Processing Helper")
warehouse_id     = _widget("warehouse_id", "")
app_name         = _widget("app_name", "")   # optional: to grant the app's SP access

print("Genie provisioning configuration:")
print(f"  Catalog:        {catalog}")
print(f"  Gold schema:    {gold_schema}")
print(f"  Genie name:     {genie_space_name}")
print(f"  Warehouse id:   {warehouse_id}")
print(f"  App name:       {app_name or '(not provided)'}")

w = WorkspaceClient()

if not warehouse_id:
    raise ValueError(
        "warehouse_id is required to create a Genie space. Pass it via the job "
        "base_parameters (it comes from config.yml -> var.warehouse_id)."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold tables + sample questions the space is built from

# COMMAND ----------

GOLD_TABLES = [
    f"{catalog}.{gold_schema}.dim_client",
    f"{catalog}.{gold_schema}.dim_care_coordinator",
    f"{catalog}.{gold_schema}.fact_service_plan",
    f"{catalog}.{gold_schema}.fact_service_demand_by_region",
    f"{catalog}.{gold_schema}.fact_risk_profile",
    f"{catalog}.{gold_schema}.agg_intake_funnel",
]

SAMPLE_QUESTIONS = [
    "What are the total weekly care hours by region?",
    "Which regions have the most clients funded by DHB?",
    "How many clients have a fall risk but no completed manual handling plan?",
    "What are the most in-demand services?",
    "Show the count of clients per primary condition.",
    "What is the average number of days from referral to service start by region?",
]

DESCRIPTION = (
    "Service Plan Document Intelligence — natural-language Q&A over Home Based "
    "Support Services (HBSS) care plans.\n\n"
    "Gold dimensional model:\n"
    "- dim_client: one row per client (NHI) with demographics, region, conditions.\n"
    "- dim_care_coordinator: coordinator workload and capacity.\n"
    "- fact_service_plan: one row per plan (funder, hours, dates, risk plans).\n"
    "- fact_service_demand_by_region: service demand by type/region/funder/month.\n"
    "- fact_risk_profile: risk flags per client with vulnerability + region.\n"
    "- agg_intake_funnel: monthly referrals vs starts, time-to-start, hours.\n\n"
    "Facts join to dim_client on nhi_number and to dim_care_coordinator on "
    "care_coordinator. All data is synthetic."
)

# Genie serialized-space payload (schema version 2).
# API requires: json-encoded string, sample_questions as objects with 32-char hex id,
# and collections sorted by their key.
_questions = sorted(
    [{"id": uuid.uuid4().hex, "question": [q]} for q in SAMPLE_QUESTIONS],
    key=lambda x: x["id"],
)
_tables = sorted(
    [{"identifier": t} for t in GOLD_TABLES],
    key=lambda x: x["identifier"],
)
serialized_space = json.dumps({
    "version": 2,
    "config": {"sample_questions": _questions},
    "data_sources": {"tables": _tables},
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the space if one with this title does not already exist (idempotent)

# COMMAND ----------

def list_spaces():
    """Return all Genie spaces (handles pagination)."""
    spaces, page_token = [], None
    while True:
        query = {"page_token": page_token} if page_token else None
        resp = w.api_client.do("GET", "/api/2.0/genie/spaces", query=query) or {}
        spaces.extend(resp.get("spaces", []))
        page_token = resp.get("next_page_token")
        if not page_token:
            break
    return spaces

existing = next(
    (s for s in list_spaces() if (s.get("title") or "").strip() == genie_space_name.strip()),
    None,
)

if existing:
    space_id = existing.get("space_id")
    print(f"Genie space '{genie_space_name}' already exists (space_id={space_id}). "
          f"Leaving it in place — the app resolves it by name.")
else:
    # parent_path: register the space in the deploying identity's workspace home.
    me = w.current_user.me()
    parent_path = f"/Users/{me.user_name}"
    body = {
        "warehouse_id": warehouse_id,
        "parent_path": parent_path,
        "serialized_space": serialized_space,
        "title": genie_space_name,
        "description": DESCRIPTION,
    }
    created = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body) or {}
    space_id = created.get("space_id")
    print(f"Created Genie space '{genie_space_name}' (space_id={space_id}) "
          f"under {parent_path} on warehouse {warehouse_id}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best-effort: grant the app's service principal access to the space
# MAGIC The Databricks App runs as its own service principal. If the space is private it
# MAGIC must be shared with that SP (CAN RUN) for the app's Genie tab to work. The space
# MAGIC permissions endpoint is not always available, so this is wrapped defensively — the
# MAGIC job never fails here; if it can't grant automatically it prints exactly what to do.

# COMMAND ----------

app_sp = None
if app_name:
    try:
        app = w.apps.get(name=app_name)
        # The app's service principal client id (varies by SDK version).
        app_sp = (
            getattr(app, "service_principal_client_id", None)
            or getattr(app, "service_principal_id", None)
        )
    except Exception as e:
        print(f"Could not look up app '{app_name}': {e}")

if space_id and app_sp:
    granted = False
    acl = {"access_control_list": [{"service_principal_name": str(app_sp),
                                     "permission_level": "CAN_RUN"}]}
    for method, path in [
        ("PATCH", f"/api/2.0/permissions/genie/{space_id}"),
        ("PUT",   f"/api/2.0/permissions/genie/{space_id}"),
    ]:
        try:
            w.api_client.do(method, path, body=acl)
            print(f"Granted CAN_RUN on space {space_id} to app SP {app_sp} via {method} {path}.")
            granted = True
            break
        except Exception as e:
            print(f"  (permission attempt {method} {path} did not apply: {e})")
    if not granted:
        print(
            "\nACTION NEEDED: grant the app access manually — open the Genie space "
            f"'{genie_space_name}', Share -> add service principal '{app_sp}' with 'Can run'."
        )
else:
    print("Skipping app-SP grant (no app_name or space_id). If the app's Genie tab shows "
          "no space, share the space with the app's service principal in the Genie UI.")

# COMMAND ----------

print(f"\nGenie Agent ready: '{genie_space_name}' (space_id={space_id}). "
      f"The app resolves it by name — no id wiring required.")
