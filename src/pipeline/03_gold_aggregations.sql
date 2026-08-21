-- Databricks notebook source
-- DBTITLE 1,Gold Layer - Dimensional Data Model
-- MAGIC %md
-- MAGIC # Gold Layer - Dimensional Data Model
-- MAGIC
-- MAGIC Dimensional model (2 dimensions, 2 facts, 1 demand fact, 1 aggregate) powering analytics and Genie Q&A.
-- MAGIC Source: DocProcessing.DocProcess_Silver.service_plan_extracted
-- MAGIC Target schema: DocProcessing.DocProcess_Gold

-- COMMAND ----------

-- DBTITLE 1,dim_client
CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.dim_client
COMMENT 'Client dimension: one row per NHI number with latest demographics and aggregated conditions'
AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY nhi_number ORDER BY ingestion_timestamp DESC) AS rn
  FROM DocProcessing.DocProcess_Silver.service_plan_extracted
  WHERE nhi_number IS NOT NULL
),
latest AS (
  SELECT * FROM ranked WHERE rn = 1
),
all_conditions AS (
  SELECT nhi_number, COLLECT_SET(condition) AS all_primary_conditions
  FROM DocProcessing.DocProcess_Silver.service_plan_extracted
  LATERAL VIEW EXPLODE(primary_conditions) t AS condition
  WHERE nhi_number IS NOT NULL AND condition IS NOT NULL
  GROUP BY nhi_number
)
SELECT
  l.nhi_number,
  l.client_first_name,
  l.client_last_name,
  l.prefers_to_be_called,
  l.gender,
  TRY_CAST(l.date_of_birth AS DATE) AS date_of_birth,
  l.ethnicity,
  l.epoa_status,
  l.interrai_score,
  l.gp_name,
  l.region,
  l.vulnerability_tier,
  COALESCE(ac.all_primary_conditions, ARRAY()) AS primary_conditions,
  (SELECT MIN(TRY_CAST(r2.referral_date AS DATE)) FROM DocProcessing.DocProcess_Silver.service_plan_extracted r2 WHERE r2.nhi_number = l.nhi_number) AS first_referral_date,
  TRY_CAST(l.service_start_date AS DATE) AS latest_service_start_date,
  (SELECT COUNT(*) FROM DocProcessing.DocProcess_Silver.service_plan_extracted r3 WHERE r3.nhi_number = l.nhi_number) AS plan_count
FROM latest l
LEFT JOIN all_conditions ac ON l.nhi_number = ac.nhi_number;

-- COMMAND ----------

-- DBTITLE 1,dim_care_coordinator
CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.dim_care_coordinator
COMMENT 'Care coordinator dimension with workload and capacity metrics'
AS
WITH coordinator_stats AS (
  SELECT
    care_coordinator,
    region,
    COUNT(DISTINCT nhi_number) AS active_client_count,
    SUM(weekly_care_hours) AS total_weekly_hours_assigned,
    COUNT(DISTINCT file_path) AS total_plans_managed,
    ROW_NUMBER() OVER (PARTITION BY care_coordinator ORDER BY COUNT(*) DESC) AS region_rank
  FROM DocProcessing.DocProcess_Silver.service_plan_extracted
  WHERE care_coordinator IS NOT NULL
  GROUP BY care_coordinator, region
)
SELECT
  care_coordinator,
  region AS primary_region,
  SUM(active_client_count) AS active_client_count,
  SUM(total_weekly_hours_assigned) AS total_weekly_hours_assigned,
  SUM(total_plans_managed) AS total_plans_managed
FROM coordinator_stats
WHERE region_rank = 1
GROUP BY care_coordinator, region;

-- COMMAND ----------

-- DBTITLE 1,fact_service_plan
CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.fact_service_plan
COMMENT 'Service plan fact table: one row per plan with proper date types and foreign keys'
AS SELECT
  file_path AS plan_id,
  nhi_number,
  care_coordinator,
  funder,
  contract_type,
  package_of_care,
  vulnerability_tier,
  TRY_CAST(referral_date AS DATE) AS referral_date,
  TRY_CAST(service_start_date AS DATE) AS service_start_date,
  weekly_care_hours,
  review_frequency,
  TRY_CAST(review_date AS DATE) AS review_date,
  long_term_goal,
  manual_handling_plan_completed,
  pressure_area_plan_completed,
  interrai_score,
  allergies,
  nasc_contact_name,
  gp_name,
  emergency_contact_name,
  emergency_contact_relationship,
  submitter_email,
  region,
  ingestion_timestamp,
  DATEDIFF(TRY_CAST(service_start_date AS DATE), TRY_CAST(referral_date AS DATE)) AS days_referral_to_start
FROM DocProcessing.DocProcess_Silver.service_plan_extracted;

-- COMMAND ----------

-- DBTITLE 1,fact_service_demand_by_region
CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.fact_service_demand_by_region
COMMENT 'Service demand exploded by service type, region, funder, and referral month'
AS SELECT
  service,
  region,
  funder,
  DATE_TRUNC('MONTH', TRY_CAST(referral_date AS DATE)) AS referral_month,
  COUNT(DISTINCT file_path) AS num_plans,
  SUM(weekly_care_hours) AS total_weekly_hours
FROM DocProcessing.DocProcess_Silver.service_plan_extracted
LATERAL VIEW EXPLODE(services_required) t AS service
WHERE service IS NOT NULL AND region IS NOT NULL AND funder IS NOT NULL
GROUP BY service, region, funder, DATE_TRUNC('MONTH', TRY_CAST(referral_date AS DATE));

-- COMMAND ----------

-- DBTITLE 1,fact_risk_profile
CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.fact_risk_profile
COMMENT 'Risk profile: each risk flag per client with vulnerability and region context'
AS SELECT
  nhi_number,
  risk_flag,
  vulnerability_tier,
  region,
  COUNT(DISTINCT file_path) AS plan_count,
  BOOL_OR(NOT manual_handling_plan_completed) AS has_incomplete_handling_plan
FROM DocProcessing.DocProcess_Silver.service_plan_extracted
LATERAL VIEW EXPLODE(risk_flags) t AS risk_flag
WHERE risk_flag IS NOT NULL AND nhi_number IS NOT NULL
GROUP BY nhi_number, risk_flag, vulnerability_tier, region;

-- COMMAND ----------

-- DBTITLE 1,agg_intake_funnel
CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.agg_intake_funnel
COMMENT 'Intake funnel metrics by month, region, and funder for operational reporting'
AS SELECT
  DATE_TRUNC('MONTH', TRY_CAST(referral_date AS DATE)) AS referral_month,
  region,
  funder,
  COUNT(*) AS plans_referred,
  SUM(CASE WHEN service_start_date IS NOT NULL THEN 1 ELSE 0 END) AS plans_started,
  AVG(DATEDIFF(TRY_CAST(service_start_date AS DATE), TRY_CAST(referral_date AS DATE))) AS avg_days_referral_to_start,
  AVG(weekly_care_hours) AS avg_weekly_hours,
  SUM(weekly_care_hours) AS total_weekly_hours
FROM DocProcessing.DocProcess_Silver.service_plan_extracted
WHERE referral_date IS NOT NULL AND region IS NOT NULL AND funder IS NOT NULL
GROUP BY DATE_TRUNC('MONTH', TRY_CAST(referral_date AS DATE)), region, funder;