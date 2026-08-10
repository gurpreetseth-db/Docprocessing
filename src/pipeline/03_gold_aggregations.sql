-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer - Business Aggregations
-- MAGIC Materialized views powering the Genie agent and dashboards

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.sales_summary
COMMENT "Monthly sales performance by agent and region"
AS SELECT
  extracted_data:agent_name::STRING AS agent_name,
  extracted_data:agent_email::STRING AS agent_email,
  extracted_data:report_period::STRING AS report_period,
  extracted_data:region::STRING AS region,
  extracted_data:product_line::STRING AS product_line,
  extracted_data:total_sales_amount::DOUBLE AS total_sales_amount,
  extracted_data:number_of_policies_sold::INT AS policies_sold,
  ingestion_timestamp
FROM DocProcessing.DocProcess_Silver.extracted_insurance_data
WHERE extracted_data:document_type::STRING = 'sales_report';

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.claims_processed
COMMENT "Summary of processed insurance claims by agent and period"
AS SELECT
  extracted_data:agent_name::STRING AS agent_name,
  extracted_data:agent_email::STRING AS agent_email,
  extracted_data:report_period::STRING AS report_period,
  extracted_data:region::STRING AS region,
  extracted_data:product_line::STRING AS product_line,
  extracted_data:claims_processed_count::INT AS claims_count,
  extracted_data:claims_processed_amount::DOUBLE AS claims_amount,
  ingestion_timestamp
FROM DocProcessing.DocProcess_Silver.extracted_insurance_data
WHERE extracted_data:document_type::STRING = 'claim_processed';

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.outstanding_claims
COMMENT "Outstanding claims with reasons for delay"
AS SELECT
  extracted_data:agent_name::STRING AS agent_name,
  extracted_data:agent_email::STRING AS agent_email,
  extracted_data:report_period::STRING AS report_period,
  extracted_data:region::STRING AS region,
  extracted_data:product_line::STRING AS product_line,
  extracted_data:claims_outstanding_count::INT AS outstanding_count,
  extracted_data:claims_outstanding_amount::DOUBLE AS outstanding_amount,
  explode(from_json(extracted_data:outstanding_reasons::STRING, 'ARRAY<STRUCT<reason:STRING, count:INT, amount:DOUBLE>>')) AS reason_detail,
  ingestion_timestamp
FROM DocProcessing.DocProcess_Silver.extracted_insurance_data
WHERE extracted_data:document_type::STRING = 'claim_outstanding';

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Gold.processing_overview
COMMENT "Overall document processing metrics"
AS SELECT
  extracted_data:agent_name::STRING AS agent_name,
  extracted_data:document_type::STRING AS document_type,
  extracted_data:report_period::STRING AS report_period,
  extracted_data:region::STRING AS region,
  file_path,
  ingestion_timestamp AS processed_at
FROM DocProcessing.DocProcess_Silver.extracted_insurance_data;
