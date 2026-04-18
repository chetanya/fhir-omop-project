# Databricks notebook source
# MAGIC %md
# MAGIC # Step 4: OMOP Patient Mapping — FHIR Patient → OMOP person
# MAGIC **Phase 2 | Materialize OMOP person table**
# MAGIC
# MAGIC **What this does:** Explains and validates the FHIR Patient → OMOP person mapping.
# MAGIC The mapping uses dbt (gold/omop_person.sql) which:
# MAGIC - Extracts birth date components (year, month, day)
# MAGIC - Maps FHIR gender string to OMOP gender_concept_id (via vocabulary Gender table)
# MAGIC - Maps FHIR race extension to OMOP race_concept_id (if present; defaults to 0 for ABDM)
# MAGIC - Stores ABHA ID as person_source_value for Indian ABDM profiles
# MAGIC
# MAGIC **ABDM note:** Indian patients often have no race/ethnicity coding;
# MAGIC these fields default to 0 (no matching concept) per CDM standard.
# MAGIC
# MAGIC **Stack:** dbt (models/gold/omop_person.sql) + Databricks SQL

# COMMAND ----------
from pyspark.sql import functions as F

CATALOG = "workspace"
GOLD_SCHEMA = "omop_gold"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Person table statistics

# COMMAND ----------
person_stats = spark.sql(f"""
    SELECT
        COUNT(*) as total_persons,
        COUNT(DISTINCT person_id) as unique_persons,
        COUNT(CASE WHEN gender_concept_id > 0 THEN 1 END) as persons_with_gender,
        COUNT(CASE WHEN race_concept_id > 0 THEN 1 END) as persons_with_race,
        COUNT(CASE WHEN year_of_birth IS NOT NULL THEN 1 END) as persons_with_birth_year
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_person
""")

person_stats.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gender mapping coverage

# COMMAND ----------
gender_coverage = spark.sql(f"""
    SELECT
        gender_source_value,
        gender_concept_id,
        COUNT(*) as count
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_person
    GROUP BY gender_source_value, gender_concept_id
    ORDER BY count DESC
""")

gender_coverage.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Race mapping (ABDM note: often empty)

# COMMAND ----------
race_coverage = spark.sql(f"""
    SELECT
        race_source_value,
        race_concept_id,
        COUNT(*) as count
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_person
    WHERE race_source_value IS NOT NULL OR race_concept_id > 0
    GROUP BY race_source_value, race_concept_id
    ORDER BY count DESC
""")

race_coverage.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## ABHA ID extraction (person_source_value)

# COMMAND ----------
abha_sample = spark.sql(f"""
    SELECT
        person_id,
        person_source_value,
        year_of_birth,
        gender_source_value
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_person
    LIMIT 10
""")

abha_sample.display()

# COMMAND ----------
print("✓ Step 4 complete: Person table mapped and loaded")
