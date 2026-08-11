-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Layer - Document Parsing & Extraction
-- MAGIC
-- MAGIC Parses PDFs via ai_parse_document to extract text, then applies ai_extract
-- MAGIC for structured field extraction. MVs are created in DocProcessing.DocProcess_Silver schema
-- MAGIC with fully-qualified names. Bronze tables are read by bare name from the default schema.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Silver.parsed_documents
COMMENT "Documents parsed by AI document intelligence; text extracted from PDF elements"
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

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Silver.service_plan_extracted
COMMENT "Service plan fields extracted via ai_extract from parsed text; includes join to submitter_email from document_submissions"
AS WITH extraction_result AS (
  SELECT
    pd.file_path,
    pd.file_name,
    pd.ingestion_timestamp,
    pd.parsed_text,
    ai_extract(
      pd.parsed_text,
      '{
        "client_first_name": {"type": "string", "description": "First name of the service plan client"},
        "client_last_name": {"type": "string", "description": "Last name of the service plan client"},
        "nhi_number": {"type": "string", "description": "National Health Index number"},
        "gender": {"type": "string", "description": "Gender (M/F/Other)"},
        "date_of_birth": {"type": "string", "description": "Date of birth (YYYY-MM-DD)"},
        "funder": {"type": "string", "description": "Funding organization (DHB/MOH/ACC)"},
        "contract_type": {"type": "string", "description": "Type of service contract"},
        "package_of_care": {"type": "string", "description": "Care package description"},
        "vulnerability_tier": {"type": "string", "description": "Vulnerability level (Level 1/2/3/N/A)"},
        "referral_date": {"type": "string", "description": "Date of referral (YYYY-MM-DD)"},
        "service_start_date": {"type": "string", "description": "Service start date (YYYY-MM-DD)"},
        "review_frequency": {"type": "string", "description": "Care review frequency"},
        "weekly_care_hours": {"type": "number", "description": "Total weekly care hours"},
        "care_coordinator": {"type": "string", "description": "Assigned care coordinator name"},
        "region": {"type": "string", "description": "Geographic region (Auckland/Wellington/Christchurch/Waikato/Otago)"},
        "primary_conditions": {"type": "array", "items": {"type": "string"}, "description": "Primary health conditions"},
        "services_required": {"type": "array", "items": {"type": "string"}, "description": "Services required (Personal Support/Household Support/Nursing/etc)"},
        "risk_flags": {"type": "array", "items": {"type": "string"}, "description": "Risk assessment flags (Falls/Fragile skin/Bed bound/Seizure risk/etc)"},
        "long_term_goal": {"type": "string", "description": "Long-term care goal"},
        "manual_handling_plan_completed": {"type": "boolean", "description": "Manual handling plan status"}
      }',
      map('version', '2.0')
    ) AS extract_result
  FROM DocProcessing.DocProcess_Silver.parsed_documents pd
)
SELECT
  extraction_result.file_path,
  extraction_result.file_name,
  ds.submitter_email,
  extraction_result.ingestion_timestamp,
  extract_result:response:client_first_name::STRING AS client_first_name,
  extract_result:response:client_last_name::STRING AS client_last_name,
  extract_result:response:nhi_number::STRING AS nhi_number,
  extract_result:response:gender::STRING AS gender,
  extract_result:response:date_of_birth::STRING AS date_of_birth,
  extract_result:response:funder::STRING AS funder,
  extract_result:response:contract_type::STRING AS contract_type,
  extract_result:response:package_of_care::STRING AS package_of_care,
  extract_result:response:vulnerability_tier::STRING AS vulnerability_tier,
  extract_result:response:referral_date::STRING AS referral_date,
  extract_result:response:service_start_date::STRING AS service_start_date,
  extract_result:response:review_frequency::STRING AS review_frequency,
  extract_result:response:weekly_care_hours::DOUBLE AS weekly_care_hours,
  extract_result:response:care_coordinator::STRING AS care_coordinator,
  extract_result:response:region::STRING AS region,
  from_json(extract_result:response:primary_conditions::STRING, 'ARRAY<STRING>') AS primary_conditions,
  from_json(extract_result:response:services_required::STRING, 'ARRAY<STRING>') AS services_required,
  from_json(extract_result:response:risk_flags::STRING, 'ARRAY<STRING>') AS risk_flags,
  extract_result:response:long_term_goal::STRING AS long_term_goal,
  extract_result:response:manual_handling_plan_completed::BOOLEAN AS manual_handling_plan_completed
FROM extraction_result
LEFT JOIN document_submissions ds ON extraction_result.file_path = ds.file_path
-- ai_extract reports failures via error_message (JSON null on success); cast to string
-- so the IS NULL test is a real SQL NULL check rather than a variant-null comparison.
WHERE extract_result:error_message::string IS NULL;
