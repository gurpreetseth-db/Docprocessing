-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Layer - Document Parsing & Extraction
-- MAGIC
-- MAGIC Two materialized views in ${catalog}.${silver_schema}:
-- MAGIC
-- MAGIC 1. **parsed_documents** - `ai_parse_document` turns each PDF into text. With
-- MAGIC    `version 2.0` the parser emits clean **HTML tables** (`<table>…</table>`) and
-- MAGIC    renders checkboxes as `☒` (ticked) / `☐` (not ticked).
-- MAGIC 2. **service_plan_extracted** - a single `ai_query` call against a frontier model
-- MAGIC    (`databricks-claude-sonnet-4-5`) reads that HTML and returns **every field** on
-- MAGIC    the HBSS Complex Service Plan as one structured JSON object: cover grid,
-- MAGIC    clinical background, referral goals + goal plan, other-provider linkage, home &
-- MAGIC    community, participants, all 15 care-domain goal/comment sections, emergency
-- MAGIC    management, the Household/Personal support task grids (with the ticked
-- MAGIC    dependency level per row), essential classification codes, and the full Home
-- MAGIC    Safety Risk Assessment (Yes/No + H/M/L + strategy) plus its rating legend.
-- MAGIC
-- MAGIC Why `ai_query` and not `ai_extract`: the form is dense and conditional — the value
-- MAGIC of a task row is *which checkbox column is ticked*, the vulnerability tier is the
-- MAGIC ticked level, etc. A frontier model interprets those `☒`/`☐` positions reliably,
-- MAGIC where flat entity extraction mis-picked (e.g. Level 1 vs Level 2).
-- MAGIC
-- MAGIC Dates are returned as printed then normalized to real `DATE` here (ISO + NZ
-- MAGIC day-first `dd/MM/yyyy` and `dd.MM.yyyy`) so Gold gets valid dates.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${silver_schema}.parsed_documents
COMMENT "Documents parsed by AI document intelligence; text (incl. HTML tables + ☒/☐ checkboxes) extracted from PDF elements"
AS WITH parsed AS (
  SELECT
    file_path,
    file_name,
    ingestion_timestamp,
    ai_parse_document(content, map('version', '2.0')) AS parsed_struct
  FROM raw_documents
)
SELECT
  file_path,
  file_name,
  ingestion_timestamp,
  concat_ws('\n', transform(parsed_struct:document:elements::ARRAY<VARIANT>, e -> e:content::STRING)) AS parsed_text
FROM parsed
-- ai_parse_document returns JSON null for error_status on success. A variant holding
-- JSON null is NOT SQL NULL, so we must cast to string before the IS NULL test.
WHERE parsed_struct:error_status::string IS NULL;

-- COMMAND ----------

-- DBTITLE 1,service_plan_extracted — full-document structured extraction via ai_query
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${silver_schema}.service_plan_extracted
COMMENT "Every field on the HBSS Complex Service Plan, extracted from parsed HTML via ai_query (Claude); dates normalized to DATE; joined to submitter_email"
AS
WITH queried AS (
  SELECT
    pd.file_path,
    pd.file_name,
    pd.ingestion_timestamp,
    ai_query(
      'databricks-claude-sonnet-4-5',
      concat(
        'You are extracting structured data from a New Zealand HBSS Complex Service Plan ',
        'that was parsed to text containing HTML tables. Output ONLY a raw JSON object ',
        '(no markdown fences, no commentary). Read every table and paragraph and capture ALL ',
        'information. Rules:\n',
        '- Checkboxes: a cell showing "☒" or "[X]" (or containing an X) is TICKED/selected; ',
        '"☐", "[ ]" or an empty box cell is NOT selected. Only report values that are ticked.\n',
        '- Vulnerability tier: return the single tier whose box is ticked, one of ',
        '"Level 1", "Level 2", "Level 3", "N/A". Note the tick may appear in the cell ',
        'immediately AFTER the tier label.\n',
        '- Task grids (Household Support, Personal Support): for each row, "level" is the ',
        'column whose box is ticked — map Dep. to "Dependent", Assist to "Assist", Indep. to ',
        '"Independent", N/A to "N/A". Include the Details/Routine/Frequency text.\n',
        '- Care domains: return ALL of them (Personal Support, Bowel/Bladder Support, ',
        'Mobility, Skin Care Support, Communication, Sensory Function, Breathing, ',
        'Nutrition/Hydration, Sleeping, Pain, Medication Management, Psychological Support, ',
        'Values and beliefs Support, Sexuality, Activities of daily living) with their goal ',
        'and comments text (use empty string when a cell is blank).\n',
        '- manual_handling_plan_completed: from the Mobility goal cell (Manual Handling Plan ',
        'Completed? Yes/No). pressure_area_plan_completed: from the Skin Care goal cell.\n',
        '- Home Safety Risk Assessment: one entry per hazard row with present=true if Yes is ',
        'ticked (else false), risk_rating = whichever of H/M/L is ticked (else null), and the ',
        'strategy text. Also capture the likelihood/consequence/risk-rating legend lines.\n',
        '- Essential services required: list the services whose box is ticked ',
        '(Household Support, Personal Support, Childcare).\n',
        '- Dates: return exactly as printed (do not reformat). Numbers as numbers.\n',
        '- Arrays with no items: return []. Unknown scalars: null.\n',
        'Output JSON with EXACTLY these keys:\n',
        '{"client_first_name","client_last_name","prefers_to_be_called","nhi_number",',
        '"gender","date_of_birth","address","phone","email","epoa_status","interrai_score",',
        '"package_of_care","package_of_care_hours","funder","contract_type","region",',
        '"vulnerability_tier","completed_by","completed_date","nasc_contact_name",',
        '"nasc_contact_phone","care_coordinator","review_frequency","gp_name","gp_contact",',
        '"emergency_contact_name","emergency_contact_relationship","emergency_contact_phone",',
        '"emergency_contact_2","referral_date","service_start_date","review_date",',
        '"weekly_care_hours","referral_narrative","primary_conditions":[],"medications",',
        '"allergies","home_situation","other_formal_supports","hazards_risks_barriers",',
        '"risk_flags":[],"allied_health_equipment":[],"cultural_considerations","ethnicity",',
        '"long_term_goal","short_term_goals":[],',
        '"goal_plan":[{"timeframe","steps":[]}],',
        '"other_providers":[{"provider","status"}],"home_description","community_activities",',
        '"transport_access","essential_services_required":[],',
        '"participants":[{"name","relationship"}],"manual_handling_plan_completed",',
        '"pressure_area_plan_completed",',
        '"care_domains":[{"domain","goal","comments"}],',
        '"emergency_management":[{"potential_emergency","management"}],',
        '"household_support_tasks":[{"action","level","details"}],',
        '"personal_support_tasks":[{"action","level","details"}],',
        '"essential_classification":[{"item","value"}],',
        '"home_safety_risks":[{"hazard","present","risk_rating","strategy"}],',
        '"risk_rating_legend":{"likelihood","consequence","risk_rating"}}\n\n',
        'DOCUMENT:\n',
        pd.parsed_text
      ),
      -- NOTE: no responseFormat — the Claude endpoint rejects the OpenAI-style
      -- '{"type":"json_object"}'. The prompt pins the model to raw JSON instead.
      -- failOnError=>false makes ai_query return STRUCT<result, errorMessage>
      -- (per-row errors are routed out below rather than failing the pipeline).
      failOnError => false
    ) AS resp
  FROM ${catalog}.${silver_schema}.parsed_documents pd
),
parsed AS (
  SELECT
    file_path,
    file_name,
    ingestion_timestamp,
    resp.errorMessage AS extract_error,
    from_json(
      -- Defensive: strip any ``` / ```json fences if the model ever adds them.
      regexp_replace(resp.result, '(^\\s*```[a-zA-Z]*\\s*)|(\\s*```\\s*$)', ''),
      'STRUCT<
        client_first_name:STRING, client_last_name:STRING, prefers_to_be_called:STRING,
        nhi_number:STRING, gender:STRING, date_of_birth:STRING, address:STRING, phone:STRING,
        email:STRING, epoa_status:STRING, interrai_score:DOUBLE, package_of_care:STRING,
        package_of_care_hours:DOUBLE, funder:STRING, contract_type:STRING, region:STRING,
        vulnerability_tier:STRING, completed_by:STRING, completed_date:STRING,
        nasc_contact_name:STRING, nasc_contact_phone:STRING, care_coordinator:STRING,
        review_frequency:STRING, gp_name:STRING, gp_contact:STRING,
        emergency_contact_name:STRING, emergency_contact_relationship:STRING,
        emergency_contact_phone:STRING, emergency_contact_2:STRING, referral_date:STRING,
        service_start_date:STRING, review_date:STRING, weekly_care_hours:DOUBLE,
        referral_narrative:STRING, primary_conditions:ARRAY<STRING>, medications:STRING,
        allergies:STRING, home_situation:STRING, other_formal_supports:STRING,
        hazards_risks_barriers:STRING, risk_flags:ARRAY<STRING>,
        allied_health_equipment:ARRAY<STRING>, cultural_considerations:STRING,
        ethnicity:STRING, long_term_goal:STRING, short_term_goals:ARRAY<STRING>,
        goal_plan:ARRAY<STRUCT<timeframe:STRING, steps:ARRAY<STRING>>>,
        other_providers:ARRAY<STRUCT<provider:STRING, status:STRING>>,
        home_description:STRING, community_activities:STRING, transport_access:STRING,
        essential_services_required:ARRAY<STRING>,
        participants:ARRAY<STRUCT<name:STRING, relationship:STRING>>,
        manual_handling_plan_completed:STRING, pressure_area_plan_completed:STRING,
        care_domains:ARRAY<STRUCT<domain:STRING, goal:STRING, comments:STRING>>,
        emergency_management:ARRAY<STRUCT<potential_emergency:STRING, management:STRING>>,
        household_support_tasks:ARRAY<STRUCT<action:STRING, level:STRING, details:STRING>>,
        personal_support_tasks:ARRAY<STRUCT<action:STRING, level:STRING, details:STRING>>,
        essential_classification:ARRAY<STRUCT<item:STRING, value:STRING>>,
        home_safety_risks:ARRAY<STRUCT<hazard:STRING, present:BOOLEAN, risk_rating:STRING, strategy:STRING>>,
        risk_rating_legend:STRUCT<likelihood:STRING, consequence:STRING, risk_rating:STRING>
      >'
    ) AS x
  FROM queried
)
SELECT
  p.file_path,
  p.file_name,
  ds.submitter_email,
  p.ingestion_timestamp,

  -- ---- Identity / cover grid ------------------------------------------------
  x.client_first_name,
  x.client_last_name,
  x.prefers_to_be_called,
  x.nhi_number,
  x.gender,
  -- Normalize DOB to a real DATE (ISO or NZ day-first formats).
  coalesce(
    try_to_date(x.date_of_birth, 'yyyy-MM-dd'), try_to_date(x.date_of_birth, 'dd/MM/yyyy'),
    try_to_date(x.date_of_birth, 'dd.MM.yyyy'), try_to_date(x.date_of_birth, 'd/M/yyyy'),
    try_to_date(x.date_of_birth, 'd.M.yyyy')
  ) AS date_of_birth,
  x.address,
  x.phone,
  x.email,
  x.epoa_status,
  x.interrai_score,
  x.package_of_care,
  x.package_of_care_hours,
  x.funder,
  x.contract_type,
  x.region,
  x.vulnerability_tier,
  x.completed_by,
  x.completed_date,
  x.nasc_contact_name,
  x.nasc_contact_phone,
  x.care_coordinator,
  x.review_frequency,
  x.gp_name,
  x.gp_contact,
  x.emergency_contact_name,
  x.emergency_contact_relationship,
  x.emergency_contact_phone,
  x.emergency_contact_2,
  coalesce(
    try_to_date(x.referral_date, 'yyyy-MM-dd'), try_to_date(x.referral_date, 'dd/MM/yyyy'),
    try_to_date(x.referral_date, 'dd.MM.yyyy'), try_to_date(x.referral_date, 'd/M/yyyy'),
    try_to_date(x.referral_date, 'd.M.yyyy')
  ) AS referral_date,
  coalesce(
    try_to_date(x.service_start_date, 'yyyy-MM-dd'), try_to_date(x.service_start_date, 'dd/MM/yyyy'),
    try_to_date(x.service_start_date, 'dd.MM.yyyy'), try_to_date(x.service_start_date, 'd/M/yyyy'),
    try_to_date(x.service_start_date, 'd.M.yyyy')
  ) AS service_start_date,
  coalesce(
    try_to_date(x.review_date, 'yyyy-MM-dd'), try_to_date(x.review_date, 'dd/MM/yyyy'),
    try_to_date(x.review_date, 'dd.MM.yyyy'), try_to_date(x.review_date, 'd/M/yyyy'),
    try_to_date(x.review_date, 'd.M.yyyy')
  ) AS review_date,
  x.weekly_care_hours,
  x.referral_narrative,

  -- ---- Clinical background --------------------------------------------------
  x.primary_conditions,
  x.medications,
  x.allergies,
  x.home_situation,
  x.other_formal_supports,
  x.hazards_risks_barriers,
  x.risk_flags,
  x.allied_health_equipment,
  x.cultural_considerations,
  x.ethnicity,

  -- ---- Referral goals -------------------------------------------------------
  x.long_term_goal,
  x.short_term_goals,
  x.goal_plan,

  -- ---- Providers / home & community / participants --------------------------
  x.other_providers,
  x.home_description,
  x.community_activities,
  x.transport_access,
  x.essential_services_required,
  -- Kept for backward compatibility with Gold (fact_service_demand_by_region
  -- explodes services_required): the ticked essential services are the services
  -- actually recorded on the plan.
  x.essential_services_required AS services_required,
  x.participants,

  -- ---- Care domains + plan-completion flags ---------------------------------
  -- The Mobility / Skin Care cells say "Completed? Yes|No"; coerce to real BOOLEAN
  -- (Gold's fact_service_plan and fact_risk_profile rely on these being boolean).
  CASE WHEN lower(trim(x.manual_handling_plan_completed)) IN ('yes','y','true','completed') THEN true
       WHEN lower(trim(x.manual_handling_plan_completed)) IN ('no','n','false') THEN false END
    AS manual_handling_plan_completed,
  CASE WHEN lower(trim(x.pressure_area_plan_completed)) IN ('yes','y','true','completed') THEN true
       WHEN lower(trim(x.pressure_area_plan_completed)) IN ('no','n','false') THEN false END
    AS pressure_area_plan_completed,
  x.care_domains,

  -- ---- Operational tables ---------------------------------------------------
  x.emergency_management,
  x.household_support_tasks,
  x.personal_support_tasks,
  x.essential_classification,
  x.home_safety_risks,
  x.risk_rating_legend
FROM parsed p
LEFT JOIN document_submissions ds ON p.file_path = ds.file_path
-- Route per-row failures out (ai_query failOnError=>false surfaces errorMessage).
WHERE p.extract_error IS NULL AND x.nhi_number IS NOT NULL;
