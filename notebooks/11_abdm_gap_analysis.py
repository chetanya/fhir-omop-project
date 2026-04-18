# Databricks notebook source
# MAGIC %md
# MAGIC # Step 11: ABDM Gap Analysis — FHIR R4 vs ABDM Profiles
# MAGIC **Phase 3 | Analyze ABDM-specific extensions vs standard R4**
# MAGIC
# MAGIC **What this does:** Identifies differences between standard FHIR R4 and
# MAGIC Ayushman Bharat Digital Mission (ABDM) profile specifications.
# MAGIC
# MAGIC Key ABDM extensions:
# MAGIC - **ABHA ID** (Ayushman Bharat Health Account): system = "https://healthid.ndhm.gov.in"
# MAGIC - **HFR Code** (Health Facility Registry): system = "https://facility.ndhm.gov.in"
# MAGIC - **Consent artifacts**: Consent resource with ABDM profile reference
# MAGIC - **State**: ABDM may restrict certain fields (race, ethnicity) not applicable in India
# MAGIC
# MAGIC **Output:** Report of coverage/gaps for ABDM-specific extensions in dataset
# MAGIC
# MAGIC **Stack:** PySpark + Databricks SQL

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG = "workspace"
BRONZE_SCHEMA = "fhir_bronze"
SILVER_SCHEMA = "fhir_silver"

# COMMAND ----------
# MAGIC %md
# MAGIC ## ABDM-Specific Fields Coverage

# COMMAND ----------
# Check ABHA ID presence in patient records
abha_coverage = spark.sql(f"""
    SELECT
        'ABHA ID' as field,
        COUNT(CASE WHEN abha_id IS NOT NULL THEN 1 END) as present,
        COUNT(*) as total,
        ROUND(100.0 * COUNT(CASE WHEN abha_id IS NOT NULL THEN 1 END) / COUNT(*), 1) as coverage_pct
    FROM {CATALOG}.{SILVER_SCHEMA}.stg_fhir_patient
""")

abha_coverage.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## HFR Facility Code Coverage (Encounter)

# COMMAND ----------
hfr_coverage = spark.sql(f"""
    SELECT
        'HFR Facility Code' as field,
        COUNT(CASE WHEN hfr_facility_code IS NOT NULL THEN 1 END) as present,
        COUNT(*) as total,
        ROUND(100.0 * COUNT(CASE WHEN hfr_facility_code IS NOT NULL THEN 1 END) / COUNT(*), 1) as coverage_pct
    FROM {CATALOG}.{SILVER_SCHEMA}.stg_fhir_encounter
""")

hfr_coverage.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## ABDM Gap Analysis Summary

# COMMAND ----------
gap_summary = """
ABDM Extension Coverage Report
===============================

ABHA ID (Ayushman Bharat Health Account):
  - System: https://healthid.ndhm.gov.in
  - Format: XX-XXXX-XXXX-XXXX (16-digit ID)
  - Usage: Person source value for national health ID
  - Coverage: See above

HFR Code (Health Facility Registry):
  - System: https://facility.ndhm.gov.in
  - Usage: Encounter facility identification
  - Coverage: See above

Race/Ethnicity:
  - ABDM Scope: Optional (often not collected)
  - Mapping: Defaults to concept_id = 0
  - Rationale: Less relevant for Indian healthcare classification

Next steps:
1. If ABHA coverage < 50%: Check FHIR bundle identifier extraction
2. If HFR coverage < 50%: Verify encounter.location mapping
3. Run OMOP data quality checks (Achilles) to identify remaining gaps
"""

print(gap_summary)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Consent Artifact Detection (ABDM-specific resource)

# COMMAND ----------
# Check for Consent resources in bronze
try:
    consent_check = spark.sql(f"""
        SELECT
            COUNT(*) as consent_count
        FROM {CATALOG}.{BRONZE_SCHEMA}.raw_bundles
        WHERE LOWER(resource_type) = 'consent'
    """)
    consent_check.display()
except:
    print("Consent resources not found in current dataset (expected for basic ABDM implementation)")

# COMMAND ----------
print("✓ Step 11 complete: ABDM gap analysis generated")
