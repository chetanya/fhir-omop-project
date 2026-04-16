# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2: Bronze Ingestion — FHIR Bundles → Delta Lake
# MAGIC **Phase 1 | Learning Step 2 of 19**
# MAGIC
# MAGIC **What this does:** Reads Synthea-generated FHIR R4 JSON bundles from a Unity Catalog
# MAGIC Volume, explodes individual resources out of each Bundle, and lands them into a
# MAGIC Bronze Delta table with full audit metadata.
# MAGIC
# MAGIC **Stack:** spark.read.text (batch) + Delta Lake
# MAGIC
# MAGIC **Key concepts:**
# MAGIC - FHIR Bundle structure: `{ "resourceType": "Bundle", "entry": [ { "resource": {...} }, ... ] }`
# MAGIC - Why Bronze is append-only: deduplication and schema enforcement happen in Silver
# MAGIC - `_lineage_id`: deterministic SHA-256 spine for cross-layer traceability
# MAGIC - `_content_hash`: SHA-256 of raw bundle for audit integrity
# MAGIC
# MAGIC **Note on batch vs streaming:** We use `spark.read.text` (batch) instead of Auto Loader
# MAGIC (streaming) because this is a static, one-time load of synthetic data. For a production
# MAGIC pipeline ingesting files continuously from S3, you would switch to `spark.readStream`
# MAGIC with `cloudFiles` format and `trigger(availableNow=True)`.

# COMMAND ----------
# MAGIC %pip install --quiet fhir.resources

# COMMAND ----------
import json
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType, StructField

# -------------------------------------------------------------------
# CONFIG — Databricks (Unity Catalog)
# -------------------------------------------------------------------
CATALOG       = "workspace"
BRONZE_SCHEMA = "fhir_bronze"
VOLUME_PATH   = "/Volumes/workspace/fhir_bronze/synthea_raw/"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Read raw FHIR bundle files (batch)
# MAGIC
# MAGIC `spark.read.text` reads every `.json` file in the Volume as one row per file.
# MAGIC Each row's `value` column is the entire file content as a string.
# MAGIC `_metadata.file_path` is the Unity Catalog-compatible way to get the source path
# MAGIC (`input_file_name()` is blocked in UC environments).

# COMMAND ----------
raw_bundles = (
    spark.read
         .option("wholetext", "true")  # one row per file, not per line
         .text(VOLUME_PATH)
         .withColumn("_source_file", F.col("_metadata.file_path"))
         .withColumn("_loaded_at", F.current_timestamp())
         .withColumn("_content_hash", F.sha2(F.col("value"), 256))
         .withColumnRenamed("value", "bundle_json")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Explode Bundle entries → one row per resource
# MAGIC
# MAGIC **Why a UDF here?**
# MAGIC
# MAGIC A FHIR Bundle `entry[].resource` field is a nested JSON *object* with a
# MAGIC variable schema (Patient looks nothing like Observation). Spark's `from_json`
# MAGIC requires a fixed schema — using `struct<resource:string>` would silently
# MAGIC return nulls because Spark can't cast a JSON object literal to a string.
# MAGIC
# MAGIC The UDF reads the raw JSON string with Python's `json` module, then
# MAGIC re-serializes each resource to a JSON string. Bronze keeps it as a string;
# MAGIC Silver will parse it with resource-specific schemas.

# COMMAND ----------
_RESOURCE_SCHEMA = ArrayType(StructType([
    StructField("resource_json",    StringType(), nullable=True),
    StructField("resource_type",    StringType(), nullable=True),
    StructField("fhir_resource_id", StringType(), nullable=True),
]))

@F.udf(returnType=_RESOURCE_SCHEMA)
def explode_bundle(bundle_json):
    """
    Extract all resources from a FHIR Bundle JSON string.
    Returns a list of (resource_json, resource_type, fhir_resource_id) structs.
    Skips entries without a resource or without a resourceType.
    """
    if not bundle_json:
        return []
    try:
        bundle = json.loads(bundle_json)
    except (ValueError, TypeError):
        return []

    # practitionerInformation*.json files are JSON arrays, not Bundles
    if not isinstance(bundle, dict):
        return []

    results = []
    for entry in bundle.get("entry", []):
        resource = entry.get("resource")
        if not resource:
            continue
        resource_type = resource.get("resourceType")
        if not resource_type:
            continue
        results.append({
            "resource_json":    json.dumps(resource, separators=(",", ":")),
            "resource_type":    resource_type,
            "fhir_resource_id": resource.get("id"),
        })
    return results

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Build the Bronze table schema
# MAGIC
# MAGIC | Column | Description |
# MAGIC |--------|-------------|
# MAGIC | `resource_json` | Full FHIR resource as a JSON string — schema-on-read in Silver |
# MAGIC | `resource_type` | e.g. `Patient`, `Condition`, `Observation` — used for routing |
# MAGIC | `fhir_resource_id` | FHIR `.id` field — used in `_lineage_id` |
# MAGIC | `_lineage_id` | SHA-256(source_file \| fhir_resource_id) — stable cross-layer join key |
# MAGIC | `_source_file` | Volume path of the originating bundle file |
# MAGIC | `_loaded_at` | Timestamp when this run ingested the file |
# MAGIC | `_content_hash` | SHA-256 of raw bundle — detects upstream file changes |

# COMMAND ----------
bronze_resources = (
    raw_bundles
    .withColumn("resource", F.explode(explode_bundle(F.col("bundle_json"))))
    .select(
        F.col("resource.resource_json"),
        F.col("resource.resource_type"),
        F.col("resource.fhir_resource_id"),
        # Deterministic lineage spine: same resource from same file always gets
        # the same _lineage_id, enabling safe re-runs and cross-layer joins
        F.sha2(
            F.concat_ws("|", F.col("_source_file"), F.col("resource.fhir_resource_id")),
            256
        ).alias("_lineage_id"),
        "_source_file",
        "_loaded_at",
        "_content_hash",
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: Write to Bronze Delta
# MAGIC
# MAGIC `overwrite` mode atomically replaces the table on each run — idempotent
# MAGIC without needing a separate DROP TABLE. `overwriteSchema=true` allows
# MAGIC schema evolution if the notebook is updated.

# COMMAND ----------
(
    bronze_resources
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.raw_bundles")
)
print("Bronze write complete.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify: Resource type distribution

# COMMAND ----------
display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_bundles")
         .groupBy("resource_type")
         .agg(F.count("*").alias("record_count"))
         .orderBy(F.desc("record_count"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC **Expected output for a 1,000-patient Maharashtra run:**
# MAGIC
# MAGIC | resource_type | record_count |
# MAGIC |---|---|
# MAGIC | Observation | ~150,000 |
# MAGIC | Claim | ~40,000 |
# MAGIC | ExplanationOfBenefit | ~30,000 |
# MAGIC | Encounter | ~30,000 |
# MAGIC | Condition | ~15,000 |
# MAGIC | MedicationRequest | ~12,000 |
# MAGIC | Immunization | ~8,000 |
# MAGIC | DiagnosticReport | ~6,000 |
# MAGIC | Patient | 1,000 |
# MAGIC
# MAGIC **ABDM note:** Real Indian hospital data will have far fewer Observations
# MAGIC (structured lab results are sparse in public hospitals). The pipeline handles
# MAGIC this gracefully — Silver uses LEFT JOINs so patients without observations
# MAGIC still appear in OMOP.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify: Null check on key fields
# MAGIC
# MAGIC Any nulls in `resource_type` or `fhir_resource_id` indicate malformed
# MAGIC bundle entries. Investigate before proceeding to Silver.

# COMMAND ----------
bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_bundles")

stats = bronze.select(
    F.count("*").alias("total"),
    F.sum(F.col("resource_type").isNull().cast("int")).alias("null_resource_type"),
    F.sum(F.col("fhir_resource_id").isNull().cast("int")).alias("null_fhir_resource_id"),
    F.sum(F.col("_lineage_id").isNull().cast("int")).alias("null_lineage_id"),
).collect()[0]

print(f"Total rows            : {stats['total']:,}")
print(f"Null resource_type    : {stats['null_resource_type']}")
print(f"Null fhir_resource_id : {stats['null_fhir_resource_id']}")
print(f"Null _lineage_id      : {stats['null_lineage_id']}")

assert stats["null_resource_type"] == 0, "Malformed entries — check bundle source"
assert stats["null_lineage_id"]    == 0, "Lineage spine broken — cannot proceed to Silver"
print("All checks passed.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify: Sample Patient resource
# MAGIC
# MAGIC Spot-check that ABDM fields are present in the raw JSON.

# COMMAND ----------
sample = (
    bronze
    .filter(F.col("resource_type") == "Patient")
    .select("resource_json")
    .limit(1)
    .collect()[0]["resource_json"]
)

patient = json.loads(sample)
print(f"Patient id    : {patient.get('id')}")
print(f"Birth date    : {patient.get('birthDate')}")
print(f"Gender        : {patient.get('gender')}")
print(f"Identifiers   : {[i.get('system') for i in patient.get('identifier', [])]}")
# In real ABDM data, you'd see "https://healthid.ndhm.gov.in" here

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next Step
# MAGIC
# MAGIC Run **`03_silver_flattening.py`** to parse `resource_json` for each resource
# MAGIC type into typed Silver Delta tables with proper column names and data types.
