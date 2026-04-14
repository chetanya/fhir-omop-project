# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2: Bronze Ingestion — FHIR Bundles → Delta Lake
# MAGIC **Phase 1 | Learning Step 2 of 19**
# MAGIC
# MAGIC **What this does:** Reads Synthea-generated FHIR R4 JSON bundles from S3,
# MAGIC extracts individual resources (Patient, Condition, MedicationRequest, etc.),
# MAGIC and lands them into Bronze Delta tables with full audit metadata.
# MAGIC
# MAGIC **Stack:** dbignite + Auto Loader + Delta Lake
# MAGIC
# MAGIC **Key concepts introduced:**
# MAGIC - FHIR Bundle structure (resourceType, entry[], resource)
# MAGIC - Auto Loader for streaming FHIR file ingestion
# MAGIC - lineage_id spine for cross-layer traceability
# MAGIC - SHA-256 audit hash (same pattern as Genmab SEER DAG)

# COMMAND ----------
# MAGIC %pip install fhir.resources dbignite

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import hashlib

# -------------------------------------------------------------------
# CONFIG — update these for your environment
# -------------------------------------------------------------------
CATALOG        = "health_lh"
BRONZE_SCHEMA  = "fhir_bronze"
S3_LANDING     = "s3://your-bucket/fhir-landing/synthea/"
CHECKPOINT_DIR = "s3://your-bucket/checkpoints/fhir_bronze/"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# -------------------------------------------------------------------
# STEP 1: Read raw FHIR bundle JSON files via Auto Loader
# Each Synthea file is one FHIR Bundle containing multiple resources.
# -------------------------------------------------------------------
raw_bundles = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "text")          # FHIR JSON as raw text
         .option("cloudFiles.schemaLocation", CHECKPOINT_DIR + "schema/")
         .option("cloudFiles.inferColumnTypes", "false")
         .load(S3_LANDING)
         .withColumn("_source_file", F.input_file_name())
         .withColumn("_loaded_at", F.current_timestamp())
         # SHA-256 of raw content for audit integrity (same as SEER DAG pattern)
         .withColumn("_content_hash",
             F.sha2(F.col("value"), 256))
         .withColumnRenamed("value", "bundle_json")
)

# -------------------------------------------------------------------
# STEP 2: Explode FHIR Bundle entries → one row per resource
#
# FHIR Bundle structure:
# {
#   "resourceType": "Bundle",
#   "entry": [
#     { "resource": { "resourceType": "Patient", ... } },
#     { "resource": { "resourceType": "Condition", ... } },
#     ...
#   ]
# }
# -------------------------------------------------------------------
def explode_bundle(df):
    return (
        df
        # Parse the entry array
        .withColumn("entries",
            F.from_json(
                F.get_json_object(F.col("bundle_json"), "$.entry"),
                "array<struct<resource:string>>"
            )
        )
        .withColumn("entry", F.explode("entries"))
        .withColumn("resource_json",
            F.col("entry.resource").cast(StringType()))
        # Extract resource type for routing to correct Bronze table
        .withColumn("resource_type",
            F.get_json_object(F.col("resource_json"), "$.resourceType"))
        # Generate lineage_id: deterministic UUID from source file + resource id
        .withColumn("fhir_resource_id",
            F.get_json_object(F.col("resource_json"), "$.id"))
        .withColumn("_lineage_id",
            F.sha2(
                F.concat(F.col("_source_file"), F.col("fhir_resource_id")),
                256
            )
        )
        .drop("entries", "entry", "bundle_json")
    )

# -------------------------------------------------------------------
# STEP 3: Write to Bronze Delta table (append-only, no dedup here)
# Deduplication happens in Silver layer.
# -------------------------------------------------------------------
(
    raw_bundles
    .transform(explode_bundle)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_DIR + "raw_bundles/")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)                        # batch mode for learning
    .toTable(f"{CATALOG}.{BRONZE_SCHEMA}.raw_bundles")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify Bronze Load
# MAGIC Check resource type distribution — you should see all FHIR resource types
# MAGIC from your Synthea population.

# COMMAND ----------
display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_bundles")
         .groupBy("resource_type")
         .agg(F.count("*").alias("record_count"))
         .orderBy(F.desc("record_count"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Expected Output
# MAGIC For a 1,000-patient Synthea population, typical counts:
# MAGIC
# MAGIC | resource_type | record_count |
# MAGIC |---|---|
# MAGIC | Observation | ~150,000 |
# MAGIC | Claim | ~40,000 |
# MAGIC | Encounter | ~30,000 |
# MAGIC | Condition | ~15,000 |
# MAGIC | MedicationRequest | ~12,000 |
# MAGIC | Patient | 1,000 |
# MAGIC
# MAGIC **ABDM note:** In real hospital data, Observation counts will be much lower
# MAGIC than Synthea (Indian public hospitals have fewer structured lab results).
# MAGIC The pipeline handles sparse data gracefully via LEFT JOINs in Silver.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next Step
# MAGIC Run `03_silver_flattening.py` to parse and flatten each resource type
# MAGIC into typed Silver Delta tables.
