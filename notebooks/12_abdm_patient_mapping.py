# Databricks notebook source
# MAGIC %md
# MAGIC # Step 12: ABDM Patient Mapping — ABHA ID Integration
# MAGIC **Phase 3 | Map ABHA ID to OMOP person_source_value**
# MAGIC
# MAGIC **What this does:** Demonstrates ABDM-specific patient mapping,
# MAGIC focusing on ABHA ID (Ayushman Bharat Health Account) integration
# MAGIC as the primary patient identifier.
# MAGIC
# MAGIC **ABHA ID Details:**
# MAGIC - National health identifier for India
# MAGIC - 16-digit format: XX-XXXX-XXXX-XXXX
# MAGIC - System URL: https://healthid.ndhm.gov.in
# MAGIC - Stored as: patient_source_value in OMOP person table
# MAGIC
# MAGIC **Mapping Logic:**
# MAGIC 1. Extract ABHA ID from Patient.identifier (system = healthid.ndhm.gov.in)
# MAGIC 2. Verify 16-digit format
# MAGIC 3. Store as person_source_value (fallback: FHIR resource ID)
# MAGIC 4. Track lineage for audit trail
# MAGIC
# MAGIC **Stack:** PySpark + Databricks SQL

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG = "workspace"
BRONZE_SCHEMA = "fhir_bronze"
SILVER_SCHEMA = "fhir_silver"
GOLD_SCHEMA = "omop_gold"

# COMMAND ----------
# MAGIC %md
# MAGIC ## ABHA ID extraction and validation

# COMMAND ----------
abha_validation = spark.sql(f"""
    SELECT
        COUNT(*) as total_patients,
        COUNT(CASE WHEN abha_id IS NOT NULL THEN 1 END) as with_abha,
        COUNT(CASE WHEN abha_id IS NULL THEN 1 END) as without_abha,
        ROUND(100.0 * COUNT(CASE WHEN abha_id IS NOT NULL THEN 1 END) / COUNT(*), 1) as abha_coverage_pct
    FROM {CATALOG}.{SILVER_SCHEMA}.stg_fhir_patient
""")

abha_validation.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## ABHA ID format samples (should be XX-XXXX-XXXX-XXXX)

# COMMAND ----------
abha_samples = spark.sql(f"""
    SELECT
        fhir_patient_id,
        abha_id,
        LENGTH(REPLACE(abha_id, '-', '')) as digit_count,
        CASE
            WHEN abha_id IS NULL THEN 'Missing'
            WHEN LENGTH(REPLACE(abha_id, '-', '')) = 16 THEN 'Valid'
            ELSE 'Invalid'
        END as abha_status
    FROM {CATALOG}.{SILVER_SCHEMA}.stg_fhir_patient
    WHERE abha_id IS NOT NULL
    LIMIT 20
""")

abha_samples.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## ABHA ID → person_source_value mapping in OMOP

# COMMAND ----------
# Show how ABHA IDs are mapped to OMOP person table
person_abdm = spark.sql(f"""
    SELECT
        person_id,
        person_source_value,
        gender_source_value,
        year_of_birth,
        _source_file
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_person
    WHERE person_source_value LIKE '%-%-%-%'  -- ABHA ID pattern
    LIMIT 20
""")

person_abdm.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fallback tracking: patients without ABHA ID (use FHIR ID)

# COMMAND ----------
fallback_mapping = spark.sql(f"""
    SELECT
        COUNT(*) as fallback_persons,
        COUNT(DISTINCT _lineage_id) as unique_bundles
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_person
    WHERE person_source_value NOT LIKE '%-%-%-%'  -- Not ABHA ID format
""")

fallback_mapping.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Linkage validation: ABHA ID uniqueness

# COMMAND ----------
abha_uniqueness = spark.sql(f"""
    SELECT
        COUNT(*) as total_persons,
        COUNT(DISTINCT person_source_value) as unique_person_sources,
        COUNT(*) - COUNT(DISTINCT person_source_value) as duplicate_sources
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_person
""")

abha_uniqueness.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## ABDM recommendation: Use ABHA ID for patient matching

# COMMAND ----------
recommendation = """
ABDM Patient Matching Strategy
==============================

Current Implementation:
  ✓ ABHA ID extracted and stored in person_source_value
  ✓ Fallback to FHIR resource ID if ABHA not present
  ✓ Lineage preserved for audit trail

Recommended For Production:
  1. Use ABHA ID as primary patient identifier in linked systems
  2. Validate ABHA ID format (16 digits) before import
  3. Handle ABHA ID collisions (rare, but possible)
  4. Track ABHA ID → person_id mappings in external registry
  5. Audit changes to person records via _transformed_at timestamp

Integration with Consent Management:
  - Link ABHA ID to Consent resource (ABDM Consent API)
  - Track data sharing permissions per ABHA ID
  - Enforce consent checks before exposing PII in queries
"""

print(recommendation)

# COMMAND ----------
print("✓ Step 12 complete: ABDM patient mapping and ABHA ID integration documented")
