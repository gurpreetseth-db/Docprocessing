-- Databricks notebook source
-- DBTITLE 1,Gold Layer - De-identified Dimensional Model
-- MAGIC %md
-- MAGIC # Gold Layer - HBSS Service Plan Intelligence (star schema)
-- MAGIC
-- MAGIC De-identified dimensional model built from ${catalog}.${silver_schema}.service_plan_extracted,
-- MAGIC designed for a **Genie Agent** (natural-language Q&A) and **AI/BI dashboards**.
-- MAGIC
-- MAGIC **De-identification (client PII):** clients are keyed by a pseudonymous
-- MAGIC `client_key = sha2(nhi_number)`. No names / NHI / DOB / address / contacts are
-- MAGIC carried into Gold — date of birth becomes `age` + `age_band`. Care-coordinator and
-- MAGIC GP names are retained (staff / provider, needed for workforce & provider analysis).
-- MAGIC
-- MAGIC **Shape:** a central plan-grain fact (`fact_service_plan`) + conformed dimensions,
-- MAGIC plus one atomic **bridge-fact per multi-valued attribute** (conditions, risk flags,
-- MAGIC home-safety hazards, support tasks, care domains, services, equipment, provider
-- MAGIC linkage). Every fact carries the common slice attributes (region, funder,
-- MAGIC referral_month, vulnerability_tier, age_band) so Genie/AI-BI rarely need a join.

-- COMMAND ----------

-- DBTITLE 1,dim_client (latest snapshot per client, de-identified)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.dim_client
COMMENT 'Client dimension (de-identified): one row per pseudonymous client_key with latest demographics, region, vulnerability and aggregated plan history. No direct identifiers.'
AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY nhi_number ORDER BY ingestion_timestamp DESC) AS rn
  FROM ${catalog}.${silver_schema}.service_plan_extracted
  WHERE nhi_number IS NOT NULL
),
latest AS (SELECT * FROM ranked WHERE rn = 1),
hist AS (
  SELECT nhi_number, MIN(referral_date) AS first_referral_date, COUNT(*) AS plan_count
  FROM ${catalog}.${silver_schema}.service_plan_extracted
  WHERE nhi_number IS NOT NULL
  GROUP BY nhi_number
)
SELECT
  sha2(l.nhi_number, 256)                                    AS client_key,
  l.gender,
  CAST(floor(datediff(current_date(), l.date_of_birth) / 365.25) AS INT) AS age,
  CASE
    WHEN l.date_of_birth IS NULL THEN 'Unknown'
    WHEN floor(datediff(current_date(), l.date_of_birth) / 365.25) < 65 THEN 'Under 65'
    WHEN floor(datediff(current_date(), l.date_of_birth) / 365.25) < 75 THEN '65-74'
    WHEN floor(datediff(current_date(), l.date_of_birth) / 365.25) < 85 THEN '75-84'
    ELSE '85+'
  END                                                        AS age_band,
  l.ethnicity,
  l.region,
  l.vulnerability_tier,
  l.epoa_status,
  l.interrai_score,
  l.gp_name,
  h.first_referral_date,
  l.service_start_date                                       AS latest_service_start_date,
  h.plan_count
FROM latest l
JOIN hist h ON l.nhi_number = h.nhi_number;

-- COMMAND ----------

-- DBTITLE 1,dim_care_coordinator (workforce)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.dim_care_coordinator
COMMENT 'Care coordinator dimension with role/office (from Bronze users) and workload metrics.'
AS
WITH stats AS (
  SELECT
    care_coordinator,
    COUNT(DISTINCT nhi_number) AS active_client_count,
    SUM(weekly_care_hours)     AS total_weekly_hours_assigned,
    COUNT(*)                   AS plans_managed
  FROM ${catalog}.${silver_schema}.service_plan_extracted
  WHERE care_coordinator IS NOT NULL
  GROUP BY care_coordinator
)
SELECT
  sha2(s.care_coordinator, 256) AS coordinator_key,
  s.care_coordinator,
  u.region AS primary_region,
  u.role,
  u.office,
  s.active_client_count,
  s.total_weekly_hours_assigned,
  s.plans_managed
FROM stats s
LEFT JOIN ${catalog}.${bronze_schema}.users u ON s.care_coordinator = u.full_name;

-- COMMAND ----------

-- DBTITLE 1,dim_region
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.dim_region
COMMENT 'Region reference dimension with NZ island grouping.'
AS
SELECT DISTINCT
  region,
  CASE
    WHEN region IN ('Auckland', 'Waikato', 'Wellington') THEN 'North Island'
    WHEN region IN ('Christchurch', 'Otago') THEN 'South Island'
    ELSE 'Other'
  END AS island
FROM ${catalog}.${silver_schema}.service_plan_extracted
WHERE region IS NOT NULL;

-- COMMAND ----------

-- DBTITLE 1,dim_funder
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.dim_funder
COMMENT 'Funder reference dimension (DHB/MOH/ACC) with full names.'
AS
SELECT DISTINCT
  funder,
  CASE upper(funder)
    WHEN 'DHB' THEN 'District Health Board'
    WHEN 'MOH' THEN 'Ministry of Health'
    WHEN 'ACC' THEN 'Accident Compensation Corporation'
    ELSE funder
  END AS funder_full_name
FROM ${catalog}.${silver_schema}.service_plan_extracted
WHERE funder IS NOT NULL;

-- COMMAND ----------

-- DBTITLE 1,dim_date (calendar)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.dim_date
COMMENT 'Calendar dimension spanning the referral/service/review date range; join on any DATE column.'
AS
WITH bounds AS (
  SELECT
    date_trunc('MONTH', LEAST(
      COALESCE(MIN(referral_date), current_date()),
      COALESCE(MIN(service_start_date), current_date())
    )) AS min_d,
    GREATEST(
      COALESCE(MAX(review_date), current_date()),
      COALESCE(MAX(service_start_date), current_date()),
      current_date()
    ) AS max_d
  FROM ${catalog}.${silver_schema}.service_plan_extracted
),
cal AS (
  SELECT explode(sequence((SELECT min_d FROM bounds), (SELECT max_d FROM bounds), INTERVAL 1 DAY)) AS d
)
SELECT
  CAST(d AS DATE)                 AS date_key,
  YEAR(d)                         AS year,
  QUARTER(d)                      AS quarter,
  MONTH(d)                        AS month_num,
  date_format(d, 'MMMM')          AS month_name,
  date_trunc('MONTH', d)          AS month_start,
  date_format(d, 'yyyy-MM')       AS year_month,
  DAY(d)                          AS day_of_month,
  date_format(d, 'EEEE')          AS day_of_week,
  (dayofweek(d) IN (1, 7))        AS is_weekend
FROM cal;

-- COMMAND ----------

-- DBTITLE 1,fact_service_plan (central plan-grain fact)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.fact_service_plan
COMMENT 'Central fact: one row per service plan (submission). FKs to client/coordinator/date; slice attributes denormalized; acuity counts precomputed.'
AS SELECT
  sha2(file_path, 256)         AS plan_key,
  sha2(nhi_number, 256)        AS client_key,
  sha2(care_coordinator, 256)  AS coordinator_key,
  -- Dates (join dim_date on any of these)
  referral_date,
  service_start_date,
  review_date,
  date_trunc('MONTH', referral_date) AS referral_month,
  -- Denormalized slice attributes
  region,
  funder,
  contract_type,
  vulnerability_tier,
  CASE
    WHEN date_of_birth IS NULL THEN 'Unknown'
    WHEN floor(datediff(current_date(), date_of_birth) / 365.25) < 65 THEN 'Under 65'
    WHEN floor(datediff(current_date(), date_of_birth) / 365.25) < 75 THEN '65-74'
    WHEN floor(datediff(current_date(), date_of_birth) / 365.25) < 85 THEN '75-84'
    ELSE '85+'
  END                          AS age_band,
  -- Measures
  weekly_care_hours,
  package_of_care_hours,
  interrai_score,
  review_frequency,
  DATEDIFF(service_start_date, referral_date) AS days_referral_to_start,
  manual_handling_plan_completed,
  pressure_area_plan_completed,
  -- Precomputed acuity counts (convenience for simple KPIs)
  size(COALESCE(primary_conditions, ARRAY()))                                                 AS num_conditions,
  size(COALESCE(risk_flags, ARRAY()))                                                          AS num_risk_flags,
  size(COALESCE(filter(home_safety_risks, r -> r.present AND upper(r.risk_rating) = 'H'), ARRAY())) AS num_high_safety_hazards,
  size(COALESCE(filter(
      concat(COALESCE(household_support_tasks, ARRAY()), COALESCE(personal_support_tasks, ARRAY())),
      t -> lower(t.level) = 'dependent'), ARRAY()))                                            AS num_dependent_tasks,
  -- Operational
  submitter_email,
  ingestion_timestamp
FROM ${catalog}.${silver_schema}.service_plan_extracted;

-- COMMAND ----------

-- DBTITLE 1,fact_plan_condition (plan × primary condition)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.fact_plan_condition
COMMENT 'Bridge-fact: one row per plan and primary health condition. Use for condition prevalence.'
AS SELECT
  sha2(file_path, 256)  AS plan_key,
  sha2(nhi_number, 256) AS client_key,
  region, funder,
  date_trunc('MONTH', referral_date) AS referral_month,
  vulnerability_tier,
  condition AS primary_condition
FROM ${catalog}.${silver_schema}.service_plan_extracted
LATERAL VIEW EXPLODE(primary_conditions) t AS condition
WHERE condition IS NOT NULL;

-- COMMAND ----------

-- DBTITLE 1,fact_plan_risk_flag (plan × risk flag)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.fact_plan_risk_flag
COMMENT 'Bridge-fact: one row per plan and risk flag. Use for risk-flag prevalence and co-occurrence with incomplete plans.'
AS SELECT
  sha2(file_path, 256)  AS plan_key,
  sha2(nhi_number, 256) AS client_key,
  region, funder,
  date_trunc('MONTH', referral_date) AS referral_month,
  vulnerability_tier,
  risk_flag,
  manual_handling_plan_completed,
  pressure_area_plan_completed
FROM ${catalog}.${silver_schema}.service_plan_extracted
LATERAL VIEW EXPLODE(risk_flags) t AS risk_flag
WHERE risk_flag IS NOT NULL;

-- COMMAND ----------

-- DBTITLE 1,fact_home_safety_hazard (plan × hazard)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.fact_home_safety_hazard
COMMENT 'Bridge-fact: one row per plan and Home Safety Risk Assessment hazard, with present flag, H/M/L rating and mitigation strategy.'
AS SELECT
  sha2(file_path, 256)  AS plan_key,
  sha2(nhi_number, 256) AS client_key,
  region, funder,
  date_trunc('MONTH', referral_date) AS referral_month,
  vulnerability_tier,
  h.hazard,
  h.present AS is_present,
  upper(h.risk_rating) AS risk_rating,
  h.strategy
FROM ${catalog}.${silver_schema}.service_plan_extracted
LATERAL VIEW EXPLODE(home_safety_risks) t AS h
WHERE h.hazard IS NOT NULL;

-- COMMAND ----------

-- DBTITLE 1,fact_support_task (plan × ADL task)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.fact_support_task
COMMENT 'Bridge-fact: one row per plan and support task (Household + Personal grids) with the ticked dependency level. Use for ADL dependency analysis.'
AS
SELECT
  sha2(file_path, 256)  AS plan_key,
  sha2(nhi_number, 256) AS client_key,
  region, funder,
  date_trunc('MONTH', referral_date) AS referral_month,
  vulnerability_tier,
  'Household' AS support_type,
  t.action,
  t.level AS dependency_level,
  (lower(t.level) = 'dependent') AS is_dependent,
  t.details
FROM ${catalog}.${silver_schema}.service_plan_extracted
LATERAL VIEW EXPLODE(household_support_tasks) x AS t
WHERE t.action IS NOT NULL
UNION ALL
SELECT
  sha2(file_path, 256)  AS plan_key,
  sha2(nhi_number, 256) AS client_key,
  region, funder,
  date_trunc('MONTH', referral_date) AS referral_month,
  vulnerability_tier,
  'Personal' AS support_type,
  t.action,
  t.level AS dependency_level,
  (lower(t.level) = 'dependent') AS is_dependent,
  t.details
FROM ${catalog}.${silver_schema}.service_plan_extracted
LATERAL VIEW EXPLODE(personal_support_tasks) x AS t
WHERE t.action IS NOT NULL;

-- COMMAND ----------

-- DBTITLE 1,fact_care_domain (plan × care domain)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.fact_care_domain
COMMENT 'Bridge-fact: one row per plan and care-plan domain (15 domains) with goal/comments text and coverage flags.'
AS SELECT
  sha2(file_path, 256)  AS plan_key,
  sha2(nhi_number, 256) AS client_key,
  region, funder,
  date_trunc('MONTH', referral_date) AS referral_month,
  vulnerability_tier,
  cd.domain,
  cd.goal,
  cd.comments,
  (cd.goal IS NOT NULL AND trim(cd.goal) <> '')     AS has_goal,
  (cd.comments IS NOT NULL AND trim(cd.comments) <> '') AS has_comments
FROM ${catalog}.${silver_schema}.service_plan_extracted
LATERAL VIEW EXPLODE(care_domains) t AS cd
WHERE cd.domain IS NOT NULL;

-- COMMAND ----------

-- DBTITLE 1,fact_plan_service (plan × service)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.fact_plan_service
COMMENT 'Bridge-fact: one row per plan and required service. Use for service demand by region/funder.'
AS SELECT
  sha2(file_path, 256)  AS plan_key,
  sha2(nhi_number, 256) AS client_key,
  region, funder,
  date_trunc('MONTH', referral_date) AS referral_month,
  vulnerability_tier,
  weekly_care_hours,
  service
FROM ${catalog}.${silver_schema}.service_plan_extracted
LATERAL VIEW EXPLODE(services_required) t AS service
WHERE service IS NOT NULL;

-- COMMAND ----------

-- DBTITLE 1,fact_plan_equipment (plan × equipment)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.fact_plan_equipment
COMMENT 'Bridge-fact: one row per plan and allied-health equipment item.'
AS SELECT
  sha2(file_path, 256)  AS plan_key,
  sha2(nhi_number, 256) AS client_key,
  region, funder,
  date_trunc('MONTH', referral_date) AS referral_month,
  vulnerability_tier,
  equipment
FROM ${catalog}.${silver_schema}.service_plan_extracted
LATERAL VIEW EXPLODE(allied_health_equipment) t AS equipment
WHERE equipment IS NOT NULL;

-- COMMAND ----------

-- DBTITLE 1,fact_provider_linkage (plan × other provider)
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.fact_provider_linkage
COMMENT 'Bridge-fact: one row per plan and external provider with engagement status (Yes/No/NA).'
AS SELECT
  sha2(file_path, 256)  AS plan_key,
  sha2(nhi_number, 256) AS client_key,
  region, funder,
  date_trunc('MONTH', referral_date) AS referral_month,
  vulnerability_tier,
  op.provider,
  op.status,
  (lower(op.status) = 'yes') AS is_engaged
FROM ${catalog}.${silver_schema}.service_plan_extracted
LATERAL VIEW EXPLODE(other_providers) t AS op
WHERE op.provider IS NOT NULL;
