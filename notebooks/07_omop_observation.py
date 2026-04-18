# Databricks notebook source
# MAGIC %md
# MAGIC # Step 7: OMOP Observation Mapping — FHIR Observation → OMOP measurement/observation
# MAGIC **Phase 2 | Materialize OMOP measurement and observation tables**
# MAGIC
# MAGIC **What this does:** Explains and validates the FHIR Observation → OMOP domain routing.
# MAGIC The mapping uses dbt to route observations to the correct OMOP table:
# MAGIC - **measurement table** (omop_measurement.sql): numeric observations (valueQuantity)
# MAGIC   - Maps LOINC/SNOMED codes to measurement_concept_id
# MAGIC   - Extracts numeric value and units (UCUM)
# MAGIC   - Includes reference ranges (range_low, range_high)
# MAGIC - **observation table** (omop_observation.sql): coded/text observations
# MAGIC   - Routes valueCodeableConcept observations
# MAGIC   - Routes valueString observations
# MAGIC   - Maps observation concept_id
# MAGIC
# MAGIC **Stack:** dbt (models/gold/omop_measurement.sql, omop_observation.sql) + Databricks SQL

# COMMAND ----------
CATALOG = "workspace"
GOLD_SCHEMA = "omop_gold"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Measurement table (numeric observations)

# COMMAND ----------
measurement_stats = spark.sql(f"""
    SELECT
        COUNT(*) as total_measurements,
        COUNT(CASE WHEN measurement_concept_id > 0 THEN 1 END) as mapped_measurements,
        ROUND(100.0 * COUNT(CASE WHEN measurement_concept_id > 0 THEN 1 END) / COUNT(*), 1) as coverage_pct,
        COUNT(DISTINCT observation_source_code) as unique_measurement_codes
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_measurement
""")

measurement_stats.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Top mapped LOINC/SNOMED measurements

# COMMAND ----------
top_measurements = spark.sql(f"""
    SELECT
        observation_source_code,
        observation_source_display,
        COUNT(*) as count,
        ROUND(AVG(value_as_number), 2) as avg_value,
        unit_source_value
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_measurement
    WHERE measurement_concept_id > 0
    GROUP BY observation_source_code, observation_source_display, unit_source_value
    ORDER BY count DESC
    LIMIT 10
""")

top_measurements.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Observation table (coded/text observations)

# COMMAND ----------
observation_stats = spark.sql(f"""
    SELECT
        COUNT(*) as total_observations,
        COUNT(CASE WHEN observation_concept_id > 0 THEN 1 END) as mapped_observations,
        ROUND(100.0 * COUNT(CASE WHEN observation_concept_id > 0 THEN 1 END) / COUNT(*), 1) as coverage_pct,
        COUNT(CASE WHEN value_as_string IS NOT NULL THEN 1 END) as text_observations,
        COUNT(CASE WHEN value_as_concept_id > 0 THEN 1 END) as coded_observations
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_observation
""")

observation_stats.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Top coded observations (valueCodeableConcept)

# COMMAND ----------
top_coded = spark.sql(f"""
    SELECT
        observation_source_code,
        observation_source_display,
        value_as_string,
        COUNT(*) as count
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_observation
    WHERE value_as_concept_id > 0
    GROUP BY observation_source_code, observation_source_display, value_as_string
    ORDER BY count DESC
    LIMIT 10
""")

top_coded.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Text observations (valueString) samples

# COMMAND ----------
text_obs = spark.sql(f"""
    SELECT
        observation_source_code,
        observation_source_display,
        value_as_string,
        COUNT(*) as count
    FROM {CATALOG}.{GOLD_SCHEMA}.omop_observation
    WHERE value_as_string IS NOT NULL
    GROUP BY observation_source_code, observation_source_display, value_as_string
    ORDER BY count DESC
    LIMIT 10
""")

text_obs.display()

# COMMAND ----------
print("✓ Step 7 complete: Measurement and observation tables mapped and loaded")
