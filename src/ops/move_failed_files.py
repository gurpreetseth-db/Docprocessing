# Databricks notebook source
# MAGIC %md
# MAGIC # Move Failed PDFs → FailedFiles volume
# MAGIC
# MAGIC The Silver pipeline is **declarative** — it can validate documents and record the
# MAGIC rejects in `service_plan_quarantine`, but it cannot move files. This job task runs
# MAGIC **after** the pipeline and physically relocates every quarantined PDF out of the
# MAGIC Auto Loader landing volume (`volume_name`, e.g. InputPDFs) into a dedicated
# MAGIC **`failed_volume_name`** volume (e.g. FailedFiles), preserving the
# MAGIC `{email_slug}/{submission_id}/{name}` sub-path.
# MAGIC
# MAGIC Why a separate volume and not a subfolder of the landing zone: Auto Loader scans the
# MAGIC landing volume recursively and checkpoints by file path, so a file moved *within* it
# MAGIC would be re-ingested (new path) — re-running the expensive extraction and re-adding
# MAGIC the quarantine row forever. Moving to a sibling volume that is never scanned avoids
# MAGIC that loop.
# MAGIC
# MAGIC The task is **idempotent**: a rejected file that has already been moved (no longer
# MAGIC present in the landing volume) is simply skipped.

# COMMAND ----------

def _widget(name, default):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default

catalog            = _widget("catalog", "DocProcessing")
bronze_schema      = _widget("bronze_schema", "DocProcess_Bronze")
silver_schema      = _widget("silver_schema", "DocProcess_Silver")
volume_name        = _widget("volume_name", "InputPDFs")
failed_volume_name = _widget("failed_volume_name", "FailedFiles")

print(f"Catalog: {catalog}")
print(f"Landing volume: {volume_name} | Failed volume: {failed_volume_name}")

# COMMAND ----------

# Ensure the destination volume exists (setup normally creates it; create defensively so
# this task also works if run standalone).
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{bronze_schema}.{failed_volume_name}")

quarantine_table = f"{catalog}.{silver_schema}.service_plan_quarantine"
landing_root = f"/Volumes/{catalog}/{bronze_schema}/{volume_name}/"
failed_root  = f"/Volumes/{catalog}/{bronze_schema}/{failed_volume_name}/"

# COMMAND ----------

# The quarantine table may not exist until the pipeline has run at least once.
try:
    rows = spark.sql(
        f"SELECT DISTINCT file_path FROM {quarantine_table} WHERE file_path IS NOT NULL"
    ).collect()
except Exception as e:
    print(f"Quarantine table not available yet ({e}); nothing to move.")
    rows = []

def _norm(p):
    """Normalize a stored path to a /Volumes/... form dbutils.fs understands."""
    if p.startswith("dbfs:"):
        p = p[len("dbfs:"):]
    return p

moved, skipped, errors = 0, 0, 0
for row in rows:
    src = _norm(row["file_path"])
    # Only relocate files that are still in the landing volume.
    if f"/{volume_name}/" not in src:
        skipped += 1
        continue
    dst = src.replace(f"/{volume_name}/", f"/{failed_volume_name}/", 1)

    # Skip if the source is already gone (previously moved) — idempotent.
    try:
        dbutils.fs.ls(src)
    except Exception:
        skipped += 1
        continue

    try:
        # Ensure the destination sub-directory exists, then move.
        dst_dir = dst.rsplit("/", 1)[0]
        dbutils.fs.mkdirs(dst_dir)
        dbutils.fs.mv(src, dst)
        moved += 1
        print(f"MOVED  {src}\n    -> {dst}")
    except Exception as e:
        errors += 1
        print(f"ERROR moving {src}: {e}")

print("\n" + "=" * 60)
print(f"Failed-file relocation complete: {moved} moved, {skipped} skipped, {errors} error(s).")
print(f"Rejected PDFs now live under: {failed_root}")
print("=" * 60)
