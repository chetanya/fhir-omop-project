# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5: OMOP Condition Mapping — FHIR Condition → OMOP condition_occurrence
# MAGIC **Phase 2 | Materialize OMOP condition_occurrence table**
# MAGIC
# MAGIC **What this does:** Explains and validates the FHIR Condition → OMOP condition_occurrence mapping.
# MAGIC The mapping uses dbt (gold/omop_condition_occurrence.sql) which:
# MAGIC - Maps SNOMED-CT codes to OMOP condition_concept_id (via vocabulary SNOMED table)
# MAGIC - Extracts condition onset date (preferred) or recorded date
# MAGIC - Preserves clinical status (active/inactive/resolved) as source value
# MAGIC - Uses condition_type_concept_id = 32882 (EHR encounter diagnosis)
# MAGIC
# MAGIC **Stack:** dbt (models/gold/omop_condition_occurrence.sql) + Databricks SQL

# COMMAND ----------
CATALOG = "workspace"
GOLD_SCHEMA = "omop_gold"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Condition table statistics

# COMMAND ----------
condition_stats = spark.sql(f"""
    SELECT
        COUNT(*) as total_conditions,
        COUNT(CASE WHEN condition_concept_id > 0 THEN 1 END) as mapped_conditions,
        ROUND(100.0 * COUNT(CASE WHEN condition_concept_id > 0 THEN 1 END) / COUNT(*), 1) as coverage_pct,
        COUNT(CASE WHEN condition_concept_id = 0 THEN 1 END) as unmapped_conditions
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_condition_occurrence
""")

condition_stats.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Top mapped SNOMED codes

# COMMAND ----------
top_conditions = spark.sql(f"""
    SELECT
        condition_source_code,
        condition_source_display,
        condition_concept_id,
        COUNT(*) as count
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_condition_occurrence
    WHERE condition_concept_id > 0
    GROUP BY condition_source_code, condition_source_display, condition_concept_id
    ORDER BY count DESC
    LIMIT 10
""")

top_conditions.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Clinical status distribution

# COMMAND ----------
status_dist = spark.sql(f"""
    SELECT
        condition_status_source_value,
        COUNT(*) as count
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_condition_occurrence
    GROUP BY condition_status_source_value
    ORDER BY count DESC
""")

status_dist.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Unmapped conditions (concept_id = 0)

# COMMAND ----------
unmapped = spark.sql(f"""
    SELECT
        condition_source_code,
        condition_source_display,
        COUNT(*) as count
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_condition_occurrence
    WHERE condition_concept_id = 0
    GROUP BY condition_source_code, condition_source_display
    ORDER BY count DESC
    LIMIT 10
""")

unmapped.display()

# COMMAND ----------
print("✓ Step 5 complete: Condition table mapped and loaded")
