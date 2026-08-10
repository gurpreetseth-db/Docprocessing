-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze Layer - Document Ingestion
-- MAGIC Auto Loader ingests PDF files from the InputPDFs volume

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE raw_documents
COMMENT "Raw binary PDF documents ingested via Auto Loader"
AS SELECT
  path AS file_path,
  length AS file_size,
  modificationTime AS file_modification_time,
  content,
  current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
  '/Volumes/DocProcessing/DocProcess_Bronze/InputPDFs',
  format => 'binaryFile'
);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE document_submissions
COMMENT "Tracks document submissions and their processing status"
AS SELECT
  path AS file_path,
  regexp_extract(path, '.*/([^/]+), 1) AS file_name,
  regexp_extract(path, '.*/([^_]+)_.*', 1) AS submitter_email,
  length AS file_size,
  modificationTime AS submission_time,
  current_timestamp() AS ingestion_timestamp,
  'PROCESSED' AS processing_status
FROM STREAM read_files(
  '/Volumes/DocProcessing/DocProcess_Bronze/InputPDFs',
  format => 'binaryFile'
);
