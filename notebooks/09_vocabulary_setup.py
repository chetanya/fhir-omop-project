# Databricks notebook source
# MAGIC %md
# MAGIC # Step 9: Vocabulary Setup — Load OMOP Concept Tables
# MAGIC **Phase 2 | Learning Step 9 of 19**
# MAGIC
# MAGIC **What this does:** Reads OMOP vocabularies downloaded from Athena
# MAGIC (SNOMED, RxNorm, Gender, Race, LOINC, etc.) and loads them as Delta tables
# MAGIC in the Gold schema. The concept and concept_relationship tables enable
# MAGIC dbt gold models to map FHIR codes to OMOP concept_ids.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Download vocabularies from https://athena.ohdsi.org
# MAGIC 2. Select: SNOMED, ICD10CM, RxNorm, RxNorm Extension, LOINC, Visit, Gender, Race
# MAGIC 3. Extract ZIP to data/vocabularies/ directory
# MAGIC 4. You should see CSV files: CONCEPT.csv, CONCEPT_RELATIONSHIP.csv, etc.
# MAGIC
# MAGIC **Stack:** PySpark DataFrame API + Delta Lake
# MAGIC
# MAGIC **Key concepts:**
# MAGIC - CONCEPT table: vocabulary_id + concept_code → concept_id (the mapping)
# MAGIC - CONCEPT_RELATIONSHIP: relationships between concepts (e.g. maps-to)
# MAGIC - Domain filtering: only S (standard) concepts used for mapping
# MAGIC - Invalidated concepts: filtered out (valid_end_date = 2099-12-31)

# COMMAND ----------
from pyspark.sql import types as T
import os

CATALOG = "workspace"
GOLD_SCHEMA = "omop_gold"
VOCABULARIES_PATH = "/Volumes/workspace/omop_gold/vocabularies/vocabularies"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helper: Read vocabulary CSV with explicit schema

# COMMAND ----------
# Define OMOP concept table schema (explicit, not inferred)
CONCEPT_SCHEMA = T.StructType([
    T.StructField("concept_id", T.LongType(), True),
    T.StructField("concept_name", T.StringType(), True),
    T.StructField("domain_id", T.StringType(), True),
    T.StructField("vocabulary_id", T.StringType(), True),
    T.StructField("concept_class_id", T.StringType(), True),
    T.StructField("standard_concept", T.StringType(), True),
    T.StructField("concept_code", T.StringType(), True),
    T.StructField("valid_start_date", T.DateType(), True),
    T.StructField("valid_end_date", T.DateType(), True),
    T.StructField("invalid_reason", T.StringType(), True)
])

CONCEPT_RELATIONSHIP_SCHEMA = T.StructType([
    T.StructField("concept_id_1", T.LongType(), True),
    T.StructField("concept_id_2", T.LongType(), True),
    T.StructField("relationship_id", T.StringType(), True),
    T.StructField("valid_start_date", T.DateType(), True),
    T.StructField("valid_end_date", T.DateType(), True)
])

def load_vocabulary_csv(filename, schema=None):
    """
    Load a vocabulary CSV from Volumes with explicit schema.
    If file doesn't exist, returns None.

    Args:
        filename: e.g., 'CONCEPT.csv'
        schema: StructType for the CSV (prevents inference issues)

    Returns:
        DataFrame with vocabulary data, or None if file missing
    """
    filepath = f"{VOCABULARIES_PATH}/{filename}"

    try:
        # Read CSV with tab separator (OMOP standard) and explicit schema
        reader = spark.read \
            .option("header", "true") \
            .option("sep", "\t")

        if schema:
            reader = reader.schema(schema)
        else:
            reader = reader.option("inferSchema", "true")

        df = reader.csv(filepath)

        print(f"✓ Loaded {filename}: {df.count():,} rows")
        return df

    except Exception as e:
        print(f"⚠ {filename} not found at {filepath}: {str(e)}")
        return None

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Load CONCEPT table
# MAGIC
# MAGIC **Schema:** concept_id | concept_name | domain_id | vocabulary_id | concept_class_id | standard_concept | concept_code | valid_start_date | valid_end_date
# MAGIC
# MAGIC **Key filters:**
# MAGIC - `standard_concept = 'S'` — only use standard (non-source) concepts for mapping
# MAGIC - `valid_end_date = '2099-12-31'` — exclude invalidated concepts
# MAGIC
# MAGIC **Usage in dbt:** `WHERE vocabulary_id = 'SNOMED' AND standard_concept = 'S'`

# COMMAND ----------
concept_df = load_vocabulary_csv("CONCEPT.csv", schema=CONCEPT_SCHEMA)

if concept_df is not None:
    # Drop existing table to avoid schema conflicts
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{GOLD_SCHEMA}.concept")

    # Filter to active, standard concepts only
    concept_df = concept_df.filter(
        (concept_df.standard_concept == 'S') |
        (concept_df.vocabulary_id.isin(['Gender', 'Race', 'Visit']))  # non-SNOMED vocabs may lack standard_concept
    )

    # Write to Delta table (overwrite for idempotency)
    concept_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.concept")

    print(f"concept table: {spark.table(f'{CATALOG}.{GOLD_SCHEMA}.concept').count():,} active concepts")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Load CONCEPT_RELATIONSHIP table
# MAGIC
# MAGIC **Schema:** concept_id_1 | concept_id_2 | relationship_id | valid_start_date | valid_end_date
# MAGIC
# MAGIC **Key relationship types:**
# MAGIC - `Maps to` → source code maps to standard concept
# MAGIC - `Has component` → complex concepts broken into parts
# MAGIC - `Relates to` → generic relationship
# MAGIC
# MAGIC **Usage:** For validation and finding mappings between code systems.
# MAGIC Not critical for Phase 2; included for completeness.

# COMMAND ----------
concept_rel_df = load_vocabulary_csv("CONCEPT_RELATIONSHIP.csv", schema=CONCEPT_RELATIONSHIP_SCHEMA)

if concept_rel_df is not None:
    # Drop existing table to avoid schema conflicts
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{GOLD_SCHEMA}.concept_relationship")

    # Filter to active relationships
    concept_rel_df = concept_rel_df.filter(concept_rel_df.valid_end_date == '2099-12-31')

    concept_rel_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.concept_relationship")

    print(f"concept_relationship table: {spark.table(f'{CATALOG}.{GOLD_SCHEMA}.concept_relationship').count():,} relationships")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Validate vocabulary coverage
# MAGIC
# MAGIC Check which vocabularies are loaded and report counts.
# MAGIC This helps diagnose missing vocabulary downloads.

# COMMAND ----------
if concept_df is not None:
    vocab_stats = spark.sql(f"""
        SELECT
            vocabulary_id,
            COUNT(*) AS concept_count,
            COUNT(CASE WHEN standard_concept = 'S' THEN 1 END) AS standard_concepts,
            COUNT(DISTINCT domain_id) AS domains
        FROM {CATALOG}.{GOLD_SCHEMA}.concept
        GROUP BY vocabulary_id
        ORDER BY concept_count DESC
    """)

    print("\n=== Vocabulary Coverage ===")
    vocab_stats.show(truncate=False)

    # Check for required vocabularies
    required_vocabs = ['Gender', 'Race', 'SNOMED', 'RxNorm', 'LOINC']
    available_vocabs = [row.vocabulary_id for row in vocab_stats.collect()]

    missing = [v for v in required_vocabs if v not in available_vocabs]
    if missing:
        print(f"\n⚠ Missing vocabularies: {', '.join(missing)}")
        print("   Download from https://athena.ohdsi.org and place in data/vocabularies/")
    else:
        print("\n✓ All required vocabularies loaded")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Vocabulary mapping samples
# MAGIC
# MAGIC Show example mappings to verify correctness.

# COMMAND ----------
if concept_df is not None:
    print("=== Sample SNOMED Concepts ===")
    spark.sql(f"""
        SELECT concept_id, concept_code, concept_name, domain_id
        FROM {CATALOG}.{GOLD_SCHEMA}.concept
        WHERE vocabulary_id = 'SNOMED' AND standard_concept = 'S'
        LIMIT 5
    """).show(truncate=False)

    print("\n=== Sample RxNorm Concepts ===")
    spark.sql(f"""
        SELECT concept_id, concept_code, concept_name, domain_id
        FROM {CATALOG}.{GOLD_SCHEMA}.concept
        WHERE vocabulary_id IN ('RxNorm', 'RxNorm Extension') AND standard_concept = 'S'
        LIMIT 5
    """).show(truncate=False)

    print("\n=== Gender Concepts ===")
    spark.sql(f"""
        SELECT concept_id, concept_code, concept_name
        FROM {CATALOG}.{GOLD_SCHEMA}.concept
        WHERE vocabulary_id = 'Gender'
    """).show(truncate=False)

    print("\n=== Race Concepts ===")
    spark.sql(f"""
        SELECT concept_id, concept_code, concept_name
        FROM {CATALOG}.{GOLD_SCHEMA}.concept
        WHERE vocabulary_id = 'Race'
    """).show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC **Next steps:**
# MAGIC 1. If vocabularies not yet downloaded:
# MAGIC    - Visit https://athena.ohdsi.org
# MAGIC    - Select SNOMED, ICD10CM, RxNorm, RxNorm Extension, LOINC, Visit, Gender, Race
# MAGIC    - Download and extract to `data/vocabularies/`
# MAGIC    - Re-run this notebook
# MAGIC
# MAGIC 2. Once loaded, dbt gold models will use concept lookups:
# MAGIC    ```sql
# MAGIC    LEFT JOIN concept c
# MAGIC        ON source.code = c.concept_code
# MAGIC        WHERE c.vocabulary_id = 'SNOMED' AND c.standard_concept = 'S'
# MAGIC    ```
# MAGIC
# MAGIC 3. The concept table enables FHIR→OMOP code mapping across all resource types.
