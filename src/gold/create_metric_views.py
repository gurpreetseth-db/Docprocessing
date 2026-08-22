# Databricks notebook source
# MAGIC %md
# MAGIC # Create Unity Catalog Metric Views (governed KPIs)
# MAGIC
# MAGIC Metric views sit on top of the Gold facts and define **one canonical version** of each
# MAGIC KPI (start rate, avg weekly hours, % dependent, …). Both the **Genie Agent** and the
# MAGIC **AI/BI dashboards** consume them, so numbers cannot silently diverge.
# MAGIC
# MAGIC Metric views are DBSQL DDL (`CREATE OR REPLACE VIEW … WITH METRICS LANGUAGE YAML`),
# MAGIC not pipeline datasets — this notebook runs as a job task **after** the Lakeflow
# MAGIC pipeline has materialized the Gold facts. Requires DBR 17.2+ (serverless default).

# COMMAND ----------

def _widget(name, default=""):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default

catalog     = _widget("catalog", "DocProcessing")
gold_schema = _widget("gold_schema", "DocProcess_Gold")
G = f"{catalog}.{gold_schema}"
print(f"Creating metric views in {G}")

# COMMAND ----------

# Each entry: view_name -> YAML body (source + dimensions + measures). One source table
# per view (no joins) for robustness; dimensions are the common slice attributes carried
# onto every fact, so these views compose cleanly with the dashboards and Genie.
METRIC_VIEWS = {

    # ---- Capacity & workforce (+ compliance flags) ----
    "metric_capacity": f"""
version: 1.1
source: {G}.fact_service_plan
comment: "Care capacity, package utilisation and plan-completion compliance, by region/funder/coordinator/time."
dimensions:
  - name: Region
    expr: region
  - name: Funder
    expr: funder
  - name: Contract Type
    expr: contract_type
  - name: Vulnerability Tier
    expr: vulnerability_tier
  - name: Age Band
    expr: age_band
  - name: Referral Month
    expr: referral_month
  - name: Coordinator Key
    expr: coordinator_key
measures:
  - name: Plan Count
    expr: COUNT(1)
  - name: Client Count
    expr: COUNT(DISTINCT client_key)
  - name: Total Weekly Hours
    expr: SUM(weekly_care_hours)
  - name: Avg Weekly Hours
    expr: AVG(weekly_care_hours)
  - name: Avg Package Hours
    expr: AVG(package_of_care_hours)
  - name: Avg InterRAI Score
    expr: AVG(interrai_score)
  - name: Plans Incomplete Manual Handling
    expr: COUNT_IF(NOT manual_handling_plan_completed)
  - name: Plans Incomplete Pressure Plan
    expr: COUNT_IF(NOT pressure_area_plan_completed)
""",

    # ---- Intake & funnel ----
    "metric_intake_funnel": f"""
version: 1.1
source: {G}.fact_service_plan
comment: "Referral-to-service-start funnel: volumes, conversion, time-to-start and upcoming reviews."
dimensions:
  - name: Referral Month
    expr: referral_month
  - name: Region
    expr: region
  - name: Funder
    expr: funder
measures:
  - name: Plans Referred
    expr: COUNT(1)
  - name: Plans Started
    expr: COUNT_IF(service_start_date IS NOT NULL)
  - name: Start Rate
    expr: COUNT_IF(service_start_date IS NOT NULL) / COUNT(1)
  - name: Avg Days To Start
    expr: AVG(days_referral_to_start)
  - name: Reviews Due Next 90 Days
    expr: COUNT_IF(review_date BETWEEN current_date() AND date_add(current_date(), 90))
""",

    # ---- Risk, safety & compliance: home safety hazards ----
    "metric_home_safety": f"""
version: 1.1
source: {G}.fact_home_safety_hazard
comment: "Home Safety Risk Assessment: hazard prevalence and H/M/L severity by region/hazard."
dimensions:
  - name: Region
    expr: region
  - name: Hazard
    expr: hazard
  - name: Risk Rating
    expr: risk_rating
  - name: Vulnerability Tier
    expr: vulnerability_tier
  - name: Referral Month
    expr: referral_month
measures:
  - name: Hazard Assessments
    expr: COUNT(1)
  - name: Hazards Present
    expr: COUNT_IF(is_present)
  - name: High Risk Hazards
    expr: COUNT_IF(is_present AND risk_rating = 'H')
  - name: Pct Hazards Present
    expr: COUNT_IF(is_present) / COUNT(1)
""",

    # ---- Risk flags prevalence ----
    "metric_risk_flags": f"""
version: 1.1
source: {G}.fact_plan_risk_flag
comment: "Risk-flag prevalence and co-occurrence with incomplete manual-handling plans."
dimensions:
  - name: Risk Flag
    expr: risk_flag
  - name: Region
    expr: region
  - name: Vulnerability Tier
    expr: vulnerability_tier
  - name: Referral Month
    expr: referral_month
measures:
  - name: Plans With Flag
    expr: COUNT(DISTINCT plan_key)
  - name: Flag Occurrences
    expr: COUNT(1)
  - name: Flags With Incomplete Manual Handling
    expr: COUNT_IF(NOT manual_handling_plan_completed)
""",

    # ---- ADL dependency ----
    "metric_dependency": f"""
version: 1.1
source: {G}.fact_support_task
comment: "Activities-of-daily-living dependency: share of tasks needing full support, by task/region."
dimensions:
  - name: Support Type
    expr: support_type
  - name: Action
    expr: action
  - name: Region
    expr: region
  - name: Vulnerability Tier
    expr: vulnerability_tier
measures:
  - name: Task Rows
    expr: COUNT(1)
  - name: Dependent Tasks
    expr: COUNT_IF(is_dependent)
  - name: Pct Dependent
    expr: COUNT_IF(is_dependent) / COUNT(1)
""",

    # ---- Clinical: condition prevalence ----
    "metric_clinical": f"""
version: 1.1
source: {G}.fact_plan_condition
comment: "Primary-condition prevalence across clients and plans."
dimensions:
  - name: Condition
    expr: primary_condition
  - name: Region
    expr: region
  - name: Funder
    expr: funder
  - name: Vulnerability Tier
    expr: vulnerability_tier
measures:
  - name: Client Count
    expr: COUNT(DISTINCT client_key)
  - name: Plan Count
    expr: COUNT(DISTINCT plan_key)
  - name: Condition Occurrences
    expr: COUNT(1)
""",
}

# COMMAND ----------

created, failed = [], []
for name, yaml_body in METRIC_VIEWS.items():
    fqn = f"{G}.{name}"
    ddl = f"CREATE OR REPLACE VIEW {fqn}\nWITH METRICS\nLANGUAGE YAML\nAS $$\n{yaml_body}\n$$"
    try:
        spark.sql(ddl)
        created.append(name)
        print(f"  ✓ {fqn}")
    except Exception as e:
        failed.append((name, str(e)))
        print(f"  ✗ {fqn}: {e}")

print(f"\nMetric views created: {len(created)}/{len(METRIC_VIEWS)}")
if failed:
    raise Exception(f"Metric view creation failed for: {[n for n, _ in failed]}")
