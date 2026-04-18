# Databricks notebook source
# MAGIC %md
# MAGIC # Step 8: OMOP Encounter Mapping — FHIR Encounter → OMOP visit_occurrence
# MAGIC **Phase 2 | Materialize OMOP visit_occurrence table**
# MAGIC
# MAGIC **What this does:** Explains and validates the FHIR Encounter → OMOP visit_occurrence mapping.
# MAGIC The mapping uses dbt (gold/omop_visit_occurrence.sql) which:
# MAGIC - Maps FHIR encounter class to OMOP visit_type_concept_id
# MAGIC   - AMB (ambulatory) → 9202 (Outpatient visit)
# MAGIC   - IMP (inpatient) → 9201 (Inpatient visit)
# MAGIC   - EMER (emergency) → 9203 (Emergency room visit)
# MAGIC   - HH (home/house) → 9204 (Home visit)
# MAGIC - Extracts period.start and period.end for visit dates
# MAGIC - Preserves encounter type code for traceability
# MAGIC - ABDM-specific: captures HFR (Health Facility Registry) code for facility tracking
# MAGIC
# MAGIC **Stack:** dbt (models/gold/omop_visit_occurrence.sql) + Databricks SQL

# COMMAND ----------
CATALOG = "workspace"
GOLD_SCHEMA = "omop_gold"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Visit occurrence table statistics

# COMMAND ----------
visit_stats = spark.sql(f"""
    SELECT
        COUNT(*) as total_visits,
        COUNT(DISTINCT person_id) as unique_persons,
        ROUND(AVG(duration_minutes), 1) as avg_duration_minutes,
        COUNT(CASE WHEN visit_type_concept_id > 0 THEN 1 END) as mapped_visit_types
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_visit_occurrence
""")

visit_stats.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Visit type distribution (encounter class mapping)

# COMMAND ----------
visit_types = spark.sql(f"""
    SELECT
        visit_source_value as encounter_class,
        visit_type_concept_id,
        COUNT(*) as count
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_visit_occurrence
    GROUP BY visit_source_value, visit_type_concept_id
    ORDER BY count DESC
""")

visit_types.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Visit dates sample

# COMMAND ----------
date_sample = spark.sql(f"""
    SELECT
        visit_occurrence_id,
        CAST(visit_start_date AS STRING) as start_date,
        CAST(visit_end_date AS STRING) as end_date,
        (CAST(visit_end_date AS INT) - CAST(visit_start_date AS INT)) * 24 * 60 as duration_minutes,
        encounter_type_display
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_visit_occurrence
    LIMIT 20
""")

date_sample.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## ABDM: HFR facility codes (facility tracking)

# COMMAND ----------
hfr_sample = spark.sql(f"""
    SELECT
        hfr_facility_code,
        facility_name,
        COUNT(*) as visit_count
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_visit_occurrence
    WHERE hfr_facility_code IS NOT NULL
    GROUP BY hfr_facility_code, facility_name
    LIMIT 10
""")

hfr_sample.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Visit lineage: sample trace through person → visit → conditions/drugs

# COMMAND ----------
lineage_sample = spark.sql(f"""
    SELECT
        p.person_id,
        v.visit_occurrence_id,
        COUNT(DISTINCT c.condition_occurrence_id) as condition_count,
        COUNT(DISTINCT d.drug_exposure_id) as drug_count
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_person p
    LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.omop_visit_occurrence v ON p.person_id = v.person_id
    LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.omop_condition_occurrence c ON p.person_id = c.person_id
    LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.omop_drug_exposure d ON p.person_id = d.person_id
    GROUP BY p.person_id, v.visit_occurrence_id
    LIMIT 10
""")

lineage_sample.display()

# COMMAND ----------
print("✓ Step 8 complete: Visit occurrence table mapped and loaded")
