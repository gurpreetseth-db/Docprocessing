-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Layer - Document Parsing & Extraction
-- MAGIC Uses ai_parse_document() and ai_extract() to structure insurance documents

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Silver.parsed_documents
COMMENT "Documents parsed by AI document intelligence"
AS SELECT
  file_path,
  file_size,
  file_modification_time,
  ingestion_timestamp,
  ai_parse_document(content, MAP('version', '2.0')) AS parsed_content
FROM LIVE.raw_documents;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW DocProcessing.DocProcess_Silver.extracted_insurance_data
COMMENT "Structured insurance data extracted from parsed documents"
AS SELECT
  file_path,
  ingestion_timestamp,
  ai_extract(
    parsed_content,
    '{
      "document_type": {"type": "string", "description": "Type: sales_report, claim_processed, claim_outstanding"},
      "report_period": {"type": "string", "description": "Month/Year of the report"},
      "agent_name": {"type": "string", "description": "Insurance agent or submitter name"},
      "agent_email": {"type": "string", "description": "Agent email address"},
      "total_sales_amount": {"type": "number", "description": "Total sales/premium amount in dollars"},
      "number_of_policies_sold": {"type": "integer", "description": "Number of new policies sold"},
      "claims_processed_count": {"type": "integer", "description": "Number of claims processed"},
      "claims_processed_amount": {"type": "number", "description": "Total dollar amount of processed claims"},
      "claims_outstanding_count": {"type": "integer", "description": "Number of outstanding/pending claims"},
      "claims_outstanding_amount": {"type": "number", "description": "Total dollar amount of outstanding claims"},
      "outstanding_reasons": {
        "type": "array",
        "description": "Reasons for outstanding claims",
        "items": {
          "type": "object",
          "properties": {
            "reason": {"type": "string"},
            "count": {"type": "integer"},
            "amount": {"type": "number"}
          }
        }
      },
      "region": {"type": "string", "description": "Geographic region or branch"},
      "product_line": {"type": "string", "description": "Insurance product: auto, home, life, health, commercial"}
    }',
    MAP('version', '2.0', 'instructions', 'Extract insurance document data. Documents contain monthly sales reports, claim processing summaries, and outstanding claims with reasons. Extract all numerical values as numbers without currency symbols.')
  ) AS extracted_data
FROM DocProcessing.DocProcess_Silver.parsed_documents
WHERE try_cast(parsed_content:error_status AS STRING) IS NULL;
