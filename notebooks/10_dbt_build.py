# Databricks notebook source
# MAGIC %md
# MAGIC # Step 10: Run dbt Build — Materialize Gold Models
# MAGIC **Phase 2 | Materialize OMOP gold tables**
# MAGIC
# MAGIC **What this does:** Runs dbt to build all silver and gold models.
# MAGIC With vocabularies now loaded, gold models will produce real OMOP concept_ids.
# MAGIC
# MAGIC **Stack:** dbt (via subprocess) + Delta Lake + Databricks SQL
# MAGIC
# MAGIC **Output:**
# MAGIC - Silver tables: stg_fhir_patient, stg_fhir_condition, stg_fhir_medication_request, stg_fhir_observation, stg_fhir_encounter
# MAGIC - Gold tables: omop_person, omop_condition_occurrence, omop_drug_exposure, omop_visit_occurrence, omop_measurement, omop_observation

# COMMAND ----------
import subprocess
import os

print(f"Current working directory: {os.getcwd()}")
print("")

# Note: dbt will use profiles from ~/.dbt/profiles.yml and run from current directory
# No need to change directory

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run dbt build with Databricks profile
# MAGIC
# MAGIC Note: This uses Databricks SQL Warehouse for query execution.
# MAGIC Run time: 5-15 minutes depending on data volume.

# COMMAND ----------
# Install dbt-databricks in the Databricks environment (if not already installed)
print("Installing dbt-databricks...")
subprocess.run(["pip", "install", "-q", "dbt-databricks"], check=False)
print("✓ dbt-databricks installed\n")

# COMMAND ----------
# Run dbt build
print("=" * 60)
print("Running dbt build...")
print("=" * 60)
print("")

result = subprocess.run(
    [
        "dbt", "build",
        "--profiles-dir", "/Users/chetanya/.dbt",
        "--vars", "{catalog: workspace, gold_schema: omop_gold}",
        "--target", "prod"
    ],
    capture_output=False,
    text=True
)

print("")
print("=" * 60)
if result.returncode == 0:
    print("✓ dbt build completed successfully!")
else:
    print(f"⚠ dbt build failed with exit code {result.returncode}")
print("=" * 60)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify gold tables were created
# MAGIC
# MAGIC Query the gold tables to confirm they have data with real concept_ids.

# COMMAND ----------
# Check person table
person_count = spark.sql("SELECT COUNT(*) as count FROM workspace.omop_gold.omop_person").collect()[0][0]
print(f"✓ omop_person: {person_count:,} rows")

# Check condition_occurrence
condition_count = spark.sql("SELECT COUNT(*) as count FROM workspace.omop_gold.omop_condition_occurrence").collect()[0][0]
print(f"✓ omop_condition_occurrence: {condition_count:,} rows")

# Check drug_exposure
drug_count = spark.sql("SELECT COUNT(*) as count FROM workspace.omop_gold.omop_drug_exposure").collect()[0][0]
print(f"✓ omop_drug_exposure: {drug_count:,} rows")

# Check visit_occurrence
visit_count = spark.sql("SELECT COUNT(*) as count FROM workspace.omop_gold.omop_visit_occurrence").collect()[0][0]
print(f"✓ omop_visit_occurrence: {visit_count:,} rows")

# Check measurement
measurement_count = spark.sql("SELECT COUNT(*) as count FROM workspace.omop_gold.omop_measurement").collect()[0][0]
print(f"✓ omop_measurement: {measurement_count:,} rows")

# Check observation
observation_count = spark.sql("SELECT COUNT(*) as count FROM workspace.omop_gold.omop_observation").collect()[0][0]
print(f"✓ omop_observation: {observation_count:,} rows")

print("")
print(f"Total gold tables: {person_count + condition_count + drug_count + visit_count + measurement_count + observation_count:,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Sample concept mappings
# MAGIC
# MAGIC Verify that concept_ids are now real values (not 0).

# COMMAND ----------
# Show condition concepts that were mapped
print("Sample Condition Concepts:")
spark.sql("""
    SELECT
        condition_source_code,
        condition_source_display,
        condition_concept_id,
        COUNT(*) as count
    FROM workspace.omop_gold.omop_condition_occurrence
    WHERE condition_concept_id > 0
    GROUP BY 1, 2, 3
    LIMIT 5
""").show(truncate=False)

print("\nSample Drug Concepts:")
spark.sql("""
    SELECT
        drug_source_code,
        drug_source_display,
        drug_concept_id,
        COUNT(*) as count
    FROM workspace.omop_gold.omop_drug_exposure
    WHERE drug_concept_id > 0
    GROUP BY 1, 2, 3
    LIMIT 5
""").show(truncate=False)

print("\nSample Measurement Concepts:")
spark.sql("""
    SELECT
        observation_source_code,
        observation_source_display,
        measurement_concept_id,
        COUNT(*) as count
    FROM workspace.omop_gold.omop_measurement
    WHERE measurement_concept_id > 0
    GROUP BY 1, 2, 3
    LIMIT 5
""").show(truncate=False)
