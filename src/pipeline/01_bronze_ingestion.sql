-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze Layer - Document Ingestion
-- MAGIC
-- MAGIC Auto Loader ingests PDF files from the InputPDFs volume as binary content.
-- MAGIC These streaming tables are created with bare names and published to DocProcessing.DocProcess_Bronze
-- MAGIC (the pipeline default schema).

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE raw_documents
COMMENT "Raw binary PDF documents ingested via Auto Loader"
AS SELECT
  path AS file_path,
  regexp_extract(path, '.*/([^/]+)$', 1) AS file_name,
  length AS file_size,
  modificationTime AS file_modification_time,
  content,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '/Volumes/DocProcessing/DocProcess_Bronze/InputPDFs',
  format => 'binaryFile',
  recursiveFileLookup => 'true'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE document_submissions
COMMENT "Tracks document submissions. New layout stores files as InputPDFs/{email_slug}/{submission_id}/{original_name} so the real filename is preserved; the submitter email and submission id come from the folder path. Legacy flat files named {email_slug}__{submission_id}__service_plan.pdf are still supported."
AS SELECT
  path AS file_path,
  -- Real (original) filename: the final path segment, unchanged.
  regexp_extract(path, '.*/([^/]+)$', 1) AS file_name,
  -- Submitter email: prefer the folder segment (new layout .../InputPDFs/{email_slug}/{submission_id}/{name});
  -- fall back to the legacy filename prefix before the first '__'.
  replace(replace(
    coalesce(
      nullif(regexp_extract(path, '/InputPDFs/([^/]+)/[^/]+/[^/]+$', 1), ''),
      split(regexp_extract(path, '.*/([^/]+)$', 1), '__')[0]
    ), '_at_', '@'), '_dot_', '.') AS submitter_email,
  -- Submission id: the second folder segment in the new layout, else the middle of the legacy name.
  coalesce(
    nullif(regexp_extract(path, '/InputPDFs/[^/]+/([^/]+)/[^/]+$', 1), ''),
    split(regexp_extract(path, '.*/([^/]+)$', 1), '__')[1]
  ) AS submission_id,
  length AS file_size,
  modificationTime AS submission_time,
  current_timestamp() AS ingestion_timestamp,
  'INGESTED' AS processing_status
FROM STREAM read_files(
  '/Volumes/DocProcessing/DocProcess_Bronze/InputPDFs',
  format => 'binaryFile',
  recursiveFileLookup => 'true'
);
