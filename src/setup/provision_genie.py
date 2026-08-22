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
import uuid
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

# De-identified Gold star schema: conformed dimensions + central plan-grain fact +
# atomic bridge-facts, plus governed metric views (KPIs). Genie natively supports
# metric views, so we attach both the facts (flexible questions) and the metric
# views (governed KPIs).
GOLD_TABLES = [
    # Dimensions
    f"{catalog}.{gold_schema}.dim_client",
    f"{catalog}.{gold_schema}.dim_care_coordinator",
    f"{catalog}.{gold_schema}.dim_region",
    f"{catalog}.{gold_schema}.dim_funder",
    f"{catalog}.{gold_schema}.dim_date",
    # Central fact
    f"{catalog}.{gold_schema}.fact_service_plan",
    # Bridge-facts (one per multi-valued attribute)
    f"{catalog}.{gold_schema}.fact_plan_condition",
    f"{catalog}.{gold_schema}.fact_plan_risk_flag",
    f"{catalog}.{gold_schema}.fact_home_safety_hazard",
    f"{catalog}.{gold_schema}.fact_support_task",
    f"{catalog}.{gold_schema}.fact_care_domain",
    f"{catalog}.{gold_schema}.fact_plan_service",
    f"{catalog}.{gold_schema}.fact_plan_equipment",
    f"{catalog}.{gold_schema}.fact_provider_linkage",
    # Governed metric views (KPIs)
    f"{catalog}.{gold_schema}.metric_capacity",
    f"{catalog}.{gold_schema}.metric_intake_funnel",
    f"{catalog}.{gold_schema}.metric_home_safety",
    f"{catalog}.{gold_schema}.metric_risk_flags",
    f"{catalog}.{gold_schema}.metric_dependency",
    f"{catalog}.{gold_schema}.metric_clinical",
]

SAMPLE_QUESTIONS = [
    # Capacity & workforce
    "What are the total weekly care hours by region?",
    "Which care coordinators have the highest active client caseload?",
    # Intake & funnel
    "What is the referral-to-service-start conversion rate by funder?",
    "What is the average number of days from referral to service start by region?",
    # Risk, safety & compliance
    "Which home-safety hazards are most often rated high risk?",
    "How many plans have a fall risk flag but an incomplete manual handling plan?",
    # Clinical & dependency
    "Which activities-of-daily-living tasks most often require full dependency support?",
    "What are the most common primary conditions among clients?",
]

DESCRIPTION = (
    "Service Plan Document Intelligence — natural-language Q&A over Home Based "
    "Support Services (HBSS) care plans. De-identified star schema (clients keyed by "
    "pseudonymous client_key; no names/NHI/DOB — use age_band).\n\n"
    "Dimensions: dim_client (age_band, gender, ethnicity, region, vulnerability_tier, "
    "interrai_score), dim_care_coordinator (workload), dim_region, dim_funder, dim_date.\n"
    "Central fact: fact_service_plan — one row per plan (plan_key), with weekly_care_hours, "
    "package_of_care_hours, interrai_score, days_referral_to_start, manual_handling/"
    "pressure_area completion flags, and referral/service/review dates.\n"
    "Bridge-facts (one row per plan and item): fact_plan_condition, fact_plan_risk_flag, "
    "fact_home_safety_hazard (is_present, risk_rating H/M/L), fact_support_task "
    "(support_type, dependency_level, is_dependent), fact_care_domain, fact_plan_service, "
    "fact_plan_equipment, fact_provider_linkage.\n"
    "Metric views (governed KPIs, query with MEASURE()): metric_capacity, "
    "metric_intake_funnel, metric_home_safety, metric_risk_flags, metric_dependency, "
    "metric_clinical.\n\n"
    "Join keys: facts join on client_key -> dim_client, coordinator_key -> "
    "dim_care_coordinator, and any date column -> dim_date.date_key. Every fact also "
    "carries region, funder, referral_month and vulnerability_tier for direct slicing. "
    "All data is synthetic."
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
