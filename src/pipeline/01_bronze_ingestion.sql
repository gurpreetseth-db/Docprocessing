-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze Layer - Document Ingestion
-- MAGIC
-- MAGIC Auto Loader ingests PDF files from the input volume as binary content.
-- MAGIC These streaming tables are created with **bare names** and therefore land in the
-- MAGIC pipeline's default catalog/schema (${catalog}.${bronze_schema}, set in resources/pipeline.yml).
-- MAGIC
-- MAGIC All catalog/schema/volume names come from the pipeline `configuration` block
-- MAGIC (${catalog}, ${bronze_schema}, ${volume_name}) which is fed from config.yml — so
-- MAGIC nothing here is hardcoded.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE raw_documents
CLUSTER BY (file_name)
COMMENT "Raw binary PDF documents ingested via Auto Loader. As a streaming table, Auto Loader checkpoints processed files, so each run ingests ONLY new files (never re-reads the whole volume)."
AS SELECT
  path AS file_path,
  regexp_extract(path, '.*/([^/]+)$', 1) AS file_name,
  length AS file_size,
  modificationTime AS file_modification_time,
  content,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '/Volumes/${catalog}/${bronze_schema}/${volume_name}',
  format => 'binaryFile',
  recursiveFileLookup => 'true'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE document_submissions
CLUSTER BY (submitter_email)
COMMENT "Tracks document submissions (incremental — only new files each run). New layout stores files as InputPDFs/{email_slug}/{submission_id}/{original_name} so the real filename is preserved; the submitter email and submission id come from the folder path. Legacy flat files named {email_slug}__{submission_id}__service_plan.pdf are still supported. Clustered by submitter_email (the app's primary filter)."
AS SELECT
  path AS file_path,
  -- Real (original) filename: the final path segment, unchanged.
  regexp_extract(path, '.*/([^/]+)$', 1) AS file_name,
  -- Submitter email: prefer the folder segment (new layout .../${volume_name}/{email_slug}/{submission_id}/{name});
  -- fall back to the legacy filename prefix before the first '__'.
  replace(replace(
    coalesce(
      nullif(regexp_extract(path, '/${volume_name}/([^/]+)/[^/]+/[^/]+$', 1), ''),
      split(regexp_extract(path, '.*/([^/]+)$', 1), '__')[0]
    ), '_at_', '@'), '_dot_', '.') AS submitter_email,
  -- Submission id: the second folder segment in the new layout, else the middle of the legacy name.
  coalesce(
    nullif(regexp_extract(path, '/${volume_name}/[^/]+/([^/]+)/[^/]+$', 1), ''),
    split(regexp_extract(path, '.*/([^/]+)$', 1), '__')[1]
  ) AS submission_id,
  length AS file_size,
  modificationTime AS submission_time,
  current_timestamp() AS ingestion_timestamp,
  'INGESTED' AS processing_status
FROM STREAM read_files(
  '/Volumes/${catalog}/${bronze_schema}/${volume_name}',
  format => 'binaryFile',
  recursiveFileLookup => 'true'
);
