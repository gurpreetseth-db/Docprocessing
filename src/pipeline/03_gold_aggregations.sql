-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer - Business Aggregations
-- MAGIC
-- MAGIC Materialized views powering Genie Q&A and analytics dashboards.
-- MAGIC All MVs are created in DocProcessing.DocProcess_Gold schema with fully-qualified names.
-- MAGIC Silver tables are read via fully-qualified names (DocProcessing.DocProcess_Silver).

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.service_plans
COMMENT "Flattened service plan records for Genie and analytics; one row per plan"
AS SELECT
  file_path,
  file_name,
  submitter_email,
  ingestion_timestamp,
  client_first_name,
  client_last_name,
  nhi_number,
  gender,
  date_of_birth,
  funder,
  contract_type,
  package_of_care,
  vulnerability_tier,
  referral_date,
  service_start_date,
  review_frequency,
  weekly_care_hours,
  care_coordinator,
  region,
  long_term_goal,
  manual_handling_plan_completed,
  ingestion_timestamp AS processed_at
FROM DocProcessing.DocProcess_Silver.service_plan_extracted;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.care_hours_by_region
COMMENT "Care hours aggregated by region and funder; powers regional insights"
AS SELECT
  region,
  funder,
  COUNT(DISTINCT file_path) AS num_plans,
  SUM(weekly_care_hours) AS total_weekly_hours,
  AVG(weekly_care_hours) AS avg_weekly_hours
FROM DocProcessing.DocProcess_Silver.service_plan_extracted
WHERE region IS NOT NULL AND funder IS NOT NULL
GROUP BY region, funder;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.conditions_summary
COMMENT "Health conditions aggregated; each row is one condition found across plans"
AS SELECT
  condition,
  COUNT(DISTINCT file_path) AS num_clients
FROM DocProcessing.DocProcess_Silver.service_plan_extracted
LATERAL VIEW EXPLODE(primary_conditions) t AS condition
WHERE condition IS NOT NULL
GROUP BY condition;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.services_demand
COMMENT "Services required aggregated; each row is one service type with demand"
AS SELECT
  service,
  COUNT(DISTINCT file_path) AS num_plans,
  SUM(weekly_care_hours) AS total_weekly_hours
FROM DocProcessing.DocProcess_Silver.service_plan_extracted
LATERAL VIEW EXPLODE(services_required) t AS service
WHERE service IS NOT NULL
GROUP BY service;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.risk_register
COMMENT "Risk assessment register; each row is one risk flag with occurrence and manual handling status"
AS SELECT
  risk_flag,
  COUNT(DISTINCT file_path) AS num_clients,
  SUM(CASE WHEN manual_handling_plan_completed = FALSE THEN 1 ELSE 0 END) AS num_without_handling_plan
FROM DocProcessing.DocProcess_Silver.service_plan_extracted
LATERAL VIEW EXPLODE(risk_flags) t AS risk_flag
WHERE risk_flag IS NOT NULL
GROUP BY risk_flag;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.processing_overview
COMMENT "Operational visibility: document processing summary with client and care coordinator details"
AS SELECT
  file_name,
  client_first_name,
  client_last_name,
  funder,
  region,
  care_coordinator,
  ingestion_timestamp AS processed_at
FROM DocProcessing.DocProcess_Silver.service_plan_extracted;
