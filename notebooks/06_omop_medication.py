# Databricks notebook source
# MAGIC %md
# MAGIC # Step 6: OMOP Medication Mapping — FHIR MedicationRequest → OMOP drug_exposure
# MAGIC **Phase 2 | Materialize OMOP drug_exposure table**
# MAGIC
# MAGIC **What this does:** Explains and validates the FHIR MedicationRequest → OMOP drug_exposure mapping.
# MAGIC The mapping uses dbt (gold/omop_drug_exposure.sql) which:
# MAGIC - Maps RxNorm codes to OMOP drug_concept_id (via vocabulary RxNorm table)
# MAGIC - Extracts medication start/end dates from prescription period
# MAGIC - Extracts dosage information (dose value, unit, doses per day)
# MAGIC - Uses drug_type_concept_id = 38000177 (Prescription written)
# MAGIC - Preserves Indian drug brand names in drug_source_display
# MAGIC
# MAGIC **ABDM note:** Indian FHIR may include local drug brand names
# MAGIC not in RxNorm; these map to concept_id = 0 (no matching concept).
# MAGIC
# MAGIC **Stack:** dbt (models/gold/omop_drug_exposure.sql) + Databricks SQL

# COMMAND ----------
CATALOG = "workspace"
GOLD_SCHEMA = "omop_gold"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Drug exposure table statistics

# COMMAND ----------
drug_stats = spark.sql(f"""
    SELECT
        COUNT(*) as total_drug_exposures,
        COUNT(CASE WHEN drug_concept_id > 0 THEN 1 END) as mapped_drugs,
        ROUND(100.0 * COUNT(CASE WHEN drug_concept_id > 0 THEN 1 END) / COUNT(*), 1) as coverage_pct,
        COUNT(CASE WHEN drug_concept_id = 0 THEN 1 END) as unmapped_drugs
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_drug_exposure
""")

drug_stats.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Top mapped RxNorm codes

# COMMAND ----------
top_drugs = spark.sql(f"""
    SELECT
        drug_source_code,
        drug_source_display,
        drug_concept_id,
        COUNT(*) as count
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_drug_exposure
    WHERE drug_concept_id > 0
    GROUP BY drug_source_code, drug_source_display, drug_concept_id
    ORDER BY count DESC
    LIMIT 10
""")

top_drugs.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Medication status distribution (active, stopped, cancelled)

# COMMAND ----------
status_dist = spark.sql(f"""
    SELECT
        stop_reason,
        COUNT(*) as count
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_drug_exposure
    GROUP BY stop_reason
    ORDER BY count DESC
""")

status_dist.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Dosage information (dose value and units)

# COMMAND ----------
dosage_sample = spark.sql(f"""
    SELECT
        drug_source_display,
        quantity,
        dose_unit_source_value,
        effective_drug_dose,
        COUNT(*) as count
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_drug_exposure
    WHERE quantity IS NOT NULL
    GROUP BY drug_source_display, quantity, dose_unit_source_value, effective_drug_dose
    LIMIT 10
""")

dosage_sample.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Unmapped medications (concept_id = 0) — may include Indian brand names

# COMMAND ----------
unmapped = spark.sql(f"""
    SELECT
        drug_source_code,
        drug_source_display,
        COUNT(*) as count
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_drug_exposure
    WHERE drug_concept_id = 0
    GROUP BY drug_source_code, drug_source_display
    ORDER BY count DESC
    LIMIT 10
""")

unmapped.display()

# COMMAND ----------
print("✓ Step 6 complete: Drug exposure table mapped and loaded")
