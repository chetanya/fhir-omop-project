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
# MAGIC %md
# MAGIC ## Configuration: Set your Databricks workspace values
# MAGIC
# MAGIC Replace these with your actual values from `~/.dbt/profiles.yml` or Databricks workspace info.

# COMMAND ----------
# EDIT THESE VALUES FOR YOUR WORKSPACE
DATABRICKS_HOST = "dbc-8b9c9e4e-9d6e.cloud.databricks.com"  # Your workspace hostname
DATABRICKS_WAREHOUSE_ID = "a68da0aa4e1bde22"  # Your SQL warehouse ID

import subprocess
import os
import shutil

print("Setting up dbt environment for Databricks execution...\n")

# Determine paths
home_dir = os.path.expanduser("~")
profiles_dir = os.path.join(home_dir, ".dbt")

# Try multiple possible Repos paths (users can clone to different locations)
possible_repos_paths = [
    "/Workspace/Repos/chetanya.pandya@gmail.com/fhir-omop-project/dbt",
    "/Workspace/Repos/fhir-omop-project/dbt",
    "/Workspace/Repos/anthropic/fhir-omop-project/dbt",
    "/dbfs/Workspace/Repos/fhir-omop-project/dbt",
]

dbt_project_dir = None
print(f"Home directory: {home_dir}")
print(f"Profiles directory: {profiles_dir}")
print(f"Current working directory: {os.getcwd()}\n")
print("Looking for dbt project in Repos...")

for path in possible_repos_paths:
    if os.path.isdir(path):
        dbt_project_dir = path
        print(f"✓ Found dbt project at: {dbt_project_dir}\n")
        break

if not dbt_project_dir:
    print(f"✗ dbt project not found in Repos.")
    print("\nTo proceed, clone the repository into Databricks Repos:")
    print("  1. In Databricks workspace, click 'Repos'")
    print("  2. Click 'Clone a repository'")
    print("  3. URL: https://github.com/chetanyapandya/fhir-omop-project")
    print("  4. Name: fhir-omop-project")
    print("  5. Click Clone")
    print("\nThen re-run this notebook.")
    raise FileNotFoundError("dbt project not found in Databricks Repos")

if not os.path.isdir(profiles_dir):
    print(f"Creating .dbt directory at {profiles_dir}")
    os.makedirs(profiles_dir, exist_ok=True)

# Ensure profiles.yml exists with Databricks connection config
profiles_yml = os.path.join(profiles_dir, "profiles.yml")

# Validate configuration values
if not DATABRICKS_HOST or DATABRICKS_HOST == "YOUR_HOST.cloud.databricks.com":
    raise ValueError("❌ DATABRICKS_HOST not configured. Edit the cell above with your workspace hostname.")
if not DATABRICKS_WAREHOUSE_ID or DATABRICKS_WAREHOUSE_ID == "YOUR_WAREHOUSE_ID":
    raise ValueError("❌ DATABRICKS_WAREHOUSE_ID not configured. Edit the cell above with your warehouse ID.")

print(f"Using Databricks workspace: {DATABRICKS_HOST}")
print(f"Using warehouse: {DATABRICKS_WAREHOUSE_ID}\n")

if not os.path.exists(profiles_yml):
    print(f"Creating profiles.yml at {profiles_yml}")
    # http_path must include warehouse ID: /sql/1.0/warehouses/<warehouse_id>
    http_path = f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}"

    # Note: Databricks will use workspace default auth, no token needed in Notebooks
    profiles_content = f"""fhir_omop_databricks:
  target: prod
  outputs:
    prod:
      type: databricks
      host: {DATABRICKS_HOST}
      http_path: {http_path}
      schema: omop_gold
      catalog: workspace
      threads: 2
      timeout_seconds: 3600
"""
    with open(profiles_yml, 'w') as f:
        f.write(profiles_content)
    print(f"✓ Created profiles.yml\n")
else:
    print(f"✓ profiles.yml exists at {profiles_yml}\n")

# Install dbt-databricks in the Databricks environment
print("Installing dbt-databricks...")
result = subprocess.run(["pip", "install", "-q", "dbt-databricks"], capture_output=True, text=True)
if result.returncode == 0:
    print("✓ dbt-databricks installed\n")
else:
    print(f"⚠ Installation returned {result.returncode}, but continuing...\n")

# Verify dbt is available
dbt_path = shutil.which("dbt")
if not dbt_path:
    raise FileNotFoundError("dbt command not found after installation")
print(f"Using dbt at: {dbt_path}\n")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validate dbt configuration (without connecting)
# MAGIC
# MAGIC Check dbt version and configuration paths. Full connection validation happens during dbt build.

# COMMAND ----------
# Validate dbt config without attempting connection
# (Connection validation would hang with incomplete http_path, so we skip it here)
print("Validating dbt configuration...")
result = subprocess.run(
    ["dbt", "--version"],
    cwd=dbt_project_dir,
    capture_output=True,
    text=True
)
if result.returncode == 0:
    print("✓ dbt version check passed:")
    print(result.stdout)
else:
    print("⚠ dbt version check failed:")
    print(result.stderr)

# Show parsed profiles (does not attempt connection)
print("dbt profile location:", profiles_yml)
print("dbt project directory:", dbt_project_dir)
print("\nConfiguration will be validated when dbt build runs in the next step.")
print("If connection issues occur, check:")
print("  1. DATABRICKS_HOST and DATABRICKS_WAREHOUSE_ID environment variables")
print("  2. Your Databricks workspace has SQL warehouse running")
print("  3. You have appropriate workspace permissions\n")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run dbt build with Databricks profile
# MAGIC
# MAGIC Compiling and materializing all silver and gold models.
# MAGIC Run time: 30-60 minutes depending on data volume.
# MAGIC
# MAGIC **Note:** Full connection validation happens here. If you encounter timeouts,
# MAGIC verify that your warehouse ID is correct and the warehouse is running.

# COMMAND ----------
# Run dbt build
print("=" * 60)
print("Running dbt build...")
print("=" * 60)
print("")

result = subprocess.run(
    [
        "dbt", "build",
        "--project-dir", dbt_project_dir,
        "--profiles-dir", profiles_dir,
        "--target", "prod",
        "--threads", "2"
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
