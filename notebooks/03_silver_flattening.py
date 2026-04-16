# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3: Silver Flattening — Bronze JSON → Typed Silver Tables
# MAGIC **Phase 1 | Learning Step 3 of 19**
# MAGIC
# MAGIC **What this does:** Reads `resource_json` strings from the Bronze table,
# MAGIC extracts typed columns for each FHIR resource type, and upserts into
# MAGIC per-resource-type Silver Delta tables.
# MAGIC
# MAGIC **Stack:** PySpark SQL functions + Delta MERGE
# MAGIC
# MAGIC **Key concepts:**
# MAGIC - Silver = Bronze with schema enforced; still FHIR-shaped (not OMOP yet)
# MAGIC - MERGE on `_lineage_id` makes re-runs idempotent
# MAGIC - `get()` vs `[0]` for safe array access (returns NULL vs raises)
# MAGIC - FHIR Observation `value[x]` polymorphism — multiple value types
# MAGIC - ABDM ABHA ID extraction from the identifier array

# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql import functions as F

CATALOG       = "workspace"
BRONZE_SCHEMA = "fhir_bronze"
SILVER_SCHEMA = "fhir_silver"
BRONZE_TABLE  = f"{CATALOG}.{BRONZE_SCHEMA}.raw_bundles"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# FHIR timestamps use ISO 8601 with timezone offsets (e.g. 2025-01-06T07:24:26-05:00,
# or +05:30 for IST). Photon's strict ANSI mode rejects these with the default format.
# Disabling ANSI mode makes to_timestamp() return NULL on unparseable strings
# instead of throwing — acceptable for Silver where NULLs are handled in Gold.
spark.conf.set("spark.sql.ansi.enabled", "false")

# Shared ABDM system URL — matches conftest.py constant
ABDM_ABHA_SYSTEM = "https://healthid.ndhm.gov.in"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helper: incremental filter
# MAGIC
# MAGIC Silver tables store the latest `_loaded_at` seen. Each run only processes
# MAGIC Bronze rows that arrived after that watermark. On the first run (empty table)
# MAGIC the watermark is epoch 0, so everything is processed.

# COMMAND ----------
def bronze_since_last_load(resource_type, silver_table):
    """Return Bronze rows for resource_type not yet in Silver."""
    try:
        watermark = spark.table(silver_table).agg(F.max("_loaded_at")).collect()[0][0]
    except Exception:
        watermark = None  # table doesn't exist yet

    df = spark.table(BRONZE_TABLE).filter(F.col("resource_type") == resource_type)
    if watermark:
        df = df.filter(F.col("_loaded_at") > watermark)
    return df


def upsert_to_silver(df, silver_table, merge_key="_lineage_id"):
    """MERGE incoming rows into Silver table, inserting new and updating changed."""
    if not spark.catalog.tableExists(silver_table):
        df.write.format("delta").saveAsTable(silver_table)
        return

    (
        DeltaTable.forName(spark, silver_table).alias("target")
        .merge(df.alias("source"), f"target.{merge_key} = source.{merge_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Patient → `stg_fhir_patient`
# MAGIC
# MAGIC **ABDM-specific:** ABHA ID lives in `identifier[]` filtered by system URL.
# MAGIC Use `get(filter(...), 0)` — safe array access that returns NULL when absent,
# MAGIC unlike `[0]` which throws `ArrayIndexOutOfBoundsException`.
# MAGIC
# MAGIC **Deceased logic:** FHIR uses either `deceasedBoolean` (true/false) or
# MAGIC `deceasedDateTime` (date of death). Both must be checked.

# COMMAND ----------
SILVER_PATIENT = f"{CATALOG}.{SILVER_SCHEMA}.stg_fhir_patient"

patients = (
    bronze_since_last_load("Patient", SILVER_PATIENT)
    .select(
        F.get_json_object("resource_json", "$.id").alias("fhir_patient_id"),
        F.to_date(F.get_json_object("resource_json", "$.birthDate")).alias("birth_date"),
        F.get_json_object("resource_json", "$.gender").alias("gender_code"),
        F.get_json_object("resource_json", "$.address[0].state").alias("state"),
        F.get_json_object("resource_json", "$.address[0].postalCode").alias("postal_code"),
        F.get_json_object("resource_json", "$.name[0].family").alias("family_name"),
        # ABDM ABHA ID — filter identifier array by system URL
        # get() returns NULL on empty array; [0] would throw
        F.expr(f"""
            get(
                filter(
                    from_json(
                        get_json_object(resource_json, '$.identifier'),
                        'array<struct<system:string,value:string>>'
                    ),
                    x -> x.system = '{ABDM_ABHA_SYSTEM}'
                ),
                0
            ).value
        """).alias("abha_id"),
        # MRN: first non-ABDM identifier
        F.expr(f"""
            get(
                filter(
                    from_json(
                        get_json_object(resource_json, '$.identifier'),
                        'array<struct<system:string,value:string>>'
                    ),
                    x -> x.system != '{ABDM_ABHA_SYSTEM}'
                ),
                0
            ).value
        """).alias("mrn"),
        # Deceased: true if deceasedBoolean=true OR deceasedDateTime is present
        F.expr("""
            CASE
                WHEN get_json_object(resource_json, '$.deceasedBoolean') = 'true' THEN TRUE
                WHEN get_json_object(resource_json, '$.deceasedDateTime') IS NOT NULL THEN TRUE
                ELSE FALSE
            END
        """).alias("is_deceased"),
        F.to_timestamp(F.get_json_object("resource_json", "$.deceasedDateTime")).alias("deceased_datetime"),
        "_lineage_id", "_source_file", "_loaded_at",
    )
)

upsert_to_silver(patients, SILVER_PATIENT)
print(f"stg_fhir_patient: {spark.table(SILVER_PATIENT).count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Condition → `stg_fhir_condition`
# MAGIC
# MAGIC **Onset date logic:** FHIR allows `onsetDateTime`, `onsetPeriod`, or
# MAGIC `onsetAge`. Synthea uses `onsetDateTime`. `COALESCE` prefers onset over
# MAGIC recorded date — this becomes `condition_start_date` in OMOP.
# MAGIC
# MAGIC **Reference stripping:** `subject.reference` = `"Patient/UUID"`.
# MAGIC `regexp_replace` strips the `Patient/` prefix to get the bare FHIR ID
# MAGIC for joining to `stg_fhir_patient`.

# COMMAND ----------
SILVER_CONDITION = f"{CATALOG}.{SILVER_SCHEMA}.stg_fhir_condition"

conditions = (
    bronze_since_last_load("Condition", SILVER_CONDITION)
    .select(
        F.get_json_object("resource_json", "$.id").alias("fhir_condition_id"),
        F.regexp_replace(
            F.get_json_object("resource_json", "$.subject.reference"),
            "^Patient/", ""
        ).alias("fhir_patient_id"),
        F.get_json_object("resource_json", "$.code.coding[0].system").alias("condition_source_vocabulary"),
        F.get_json_object("resource_json", "$.code.coding[0].code").alias("condition_source_code"),
        F.get_json_object("resource_json", "$.code.coding[0].display").alias("condition_source_display"),
        # Prefer onsetDateTime, fall back to recordedDate
        F.coalesce(
            F.to_date(F.get_json_object("resource_json", "$.onsetDateTime")),
            F.to_date(F.get_json_object("resource_json", "$.onsetPeriod.start")),
            F.to_date(F.get_json_object("resource_json", "$.recordedDate")),
        ).alias("condition_start_date"),
        F.to_date(F.get_json_object("resource_json", "$.recordedDate")).alias("condition_recorded_date"),
        F.get_json_object("resource_json", "$.clinicalStatus.coding[0].code").alias("clinical_status"),
        F.get_json_object("resource_json", "$.verificationStatus.coding[0].code").alias("verification_status"),
        "_lineage_id", "_source_file", "_loaded_at",
    )
)

upsert_to_silver(conditions, SILVER_CONDITION)
print(f"stg_fhir_condition: {spark.table(SILVER_CONDITION).count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 3. MedicationRequest → `stg_fhir_medication_request`
# MAGIC
# MAGIC **Indian drug names:** Synthea uses RxNorm codes. Real ABDM data may carry
# MAGIC Indian brand names under a local system URL. The Silver layer preserves
# MAGIC `drug_source_code` + `drug_source_vocabulary` so Gold can route to the
# MAGIC correct OMOP concept lookup (RxNorm, or Indian drug dictionary).

# COMMAND ----------
SILVER_MED = f"{CATALOG}.{SILVER_SCHEMA}.stg_fhir_medication_request"

medications = (
    bronze_since_last_load("MedicationRequest", SILVER_MED)
    .select(
        F.get_json_object("resource_json", "$.id").alias("fhir_medication_request_id"),
        F.regexp_replace(
            F.get_json_object("resource_json", "$.subject.reference"),
            "^Patient/", ""
        ).alias("fhir_patient_id"),
        F.get_json_object("resource_json", "$.medicationCodeableConcept.coding[0].system").alias("drug_source_vocabulary"),
        F.get_json_object("resource_json", "$.medicationCodeableConcept.coding[0].code").alias("drug_source_code"),
        F.get_json_object("resource_json", "$.medicationCodeableConcept.coding[0].display").alias("drug_source_display"),
        F.to_date(F.get_json_object("resource_json", "$.authoredOn")).alias("drug_exposure_start_date"),
        F.get_json_object("resource_json", "$.status").alias("status"),
        F.get_json_object("resource_json", "$.intent").alias("intent"),
        # Dosage — NULL when absent (OMOP allows NULL quantity fields)
        F.get_json_object(
            "resource_json", "$.dosageInstruction[0].doseAndRate[0].doseQuantity.value"
        ).cast("double").alias("dose_value"),
        F.get_json_object(
            "resource_json", "$.dosageInstruction[0].doseAndRate[0].doseQuantity.unit"
        ).alias("dose_unit"),
        # doses_per_day = frequency / period (periodUnit must be 'd' for this to be meaningful)
        F.expr("""
            CASE
                WHEN get_json_object(resource_json, '$.dosageInstruction[0].timing.repeat.periodUnit') = 'd'
                THEN CAST(get_json_object(resource_json, '$.dosageInstruction[0].timing.repeat.frequency') AS DOUBLE)
                   / CAST(get_json_object(resource_json, '$.dosageInstruction[0].timing.repeat.period') AS DOUBLE)
                ELSE NULL
            END
        """).alias("doses_per_day"),
        "_lineage_id", "_source_file", "_loaded_at",
    )
)

upsert_to_silver(medications, SILVER_MED)
print(f"stg_fhir_medication_request: {spark.table(SILVER_MED).count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Observation → `stg_fhir_observation`
# MAGIC
# MAGIC **`value[x]` polymorphism** is the hardest FHIR mapping challenge.
# MAGIC An Observation's value can be any of:
# MAGIC - `valueQuantity` → numeric lab result (→ OMOP `measurement`)
# MAGIC - `valueCodeableConcept` → coded result, e.g. positive/negative (→ OMOP `observation`)
# MAGIC - `valueString` → free text (→ OMOP `observation`)
# MAGIC
# MAGIC Silver extracts all three. Gold routes to `measurement` or `observation`
# MAGIC based on the OMOP domain of the LOINC/SNOMED concept.
# MAGIC
# MAGIC **Category:** Observations are categorized as `vital-signs`, `laboratory`,
# MAGIC `social-history`, etc. This also informs the OMOP domain routing.

# COMMAND ----------
SILVER_OBS = f"{CATALOG}.{SILVER_SCHEMA}.stg_fhir_observation"

observations = (
    bronze_since_last_load("Observation", SILVER_OBS)
    .select(
        F.get_json_object("resource_json", "$.id").alias("fhir_observation_id"),
        F.regexp_replace(
            F.get_json_object("resource_json", "$.subject.reference"),
            "^Patient/", ""
        ).alias("fhir_patient_id"),
        F.regexp_replace(
            F.get_json_object("resource_json", "$.encounter.reference"),
            "^Encounter/", ""
        ).alias("fhir_encounter_id"),
        F.get_json_object("resource_json", "$.status").alias("status"),
        # Category (vital-signs, laboratory, social-history, etc.)
        F.get_json_object("resource_json", "$.category[0].coding[0].code").alias("category"),
        # LOINC or SNOMED code
        F.get_json_object("resource_json", "$.code.coding[0].system").alias("observation_source_vocabulary"),
        F.get_json_object("resource_json", "$.code.coding[0].code").alias("observation_source_code"),
        F.get_json_object("resource_json", "$.code.coding[0].display").alias("observation_source_display"),
        # Effective date/time
        F.coalesce(
            F.to_timestamp(F.get_json_object("resource_json", "$.effectiveDateTime")),
            F.to_timestamp(F.get_json_object("resource_json", "$.effectivePeriod.start")),
        ).alias("effective_datetime"),
        # value[x] — all three types; Gold picks the non-null one
        F.get_json_object("resource_json", "$.valueQuantity.value").cast("double").alias("value_quantity"),
        F.get_json_object("resource_json", "$.valueQuantity.unit").alias("value_unit"),
        F.get_json_object("resource_json", "$.valueQuantity.code").alias("value_unit_code"),
        F.get_json_object("resource_json", "$.valueCodeableConcept.coding[0].code").alias("value_concept_code"),
        F.get_json_object("resource_json", "$.valueCodeableConcept.coding[0].display").alias("value_concept_display"),
        F.get_json_object("resource_json", "$.valueString").alias("value_string"),
        # Reference range (for lab normals)
        F.get_json_object("resource_json", "$.referenceRange[0].low.value").cast("double").alias("range_low"),
        F.get_json_object("resource_json", "$.referenceRange[0].high.value").cast("double").alias("range_high"),
        "_lineage_id", "_source_file", "_loaded_at",
    )
)

upsert_to_silver(observations, SILVER_OBS)
print(f"stg_fhir_observation: {spark.table(SILVER_OBS).count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Encounter → `stg_fhir_encounter`
# MAGIC
# MAGIC **Visit type mapping:** FHIR `class.code` (AMB, IMP, EMER, etc.) maps to
# MAGIC OMOP `visit_type_concept_id`. The mapping lives in Gold; Silver just preserves
# MAGIC the raw code.
# MAGIC
# MAGIC **ABDM HFR code:** The Health Facility Registry code lives in
# MAGIC `serviceProvider.identifier` where `system = https://facility.ndhm.gov.in`.
# MAGIC This becomes OMOP `care_site.care_site_source_value`.

# COMMAND ----------
SILVER_ENC = f"{CATALOG}.{SILVER_SCHEMA}.stg_fhir_encounter"
ABDM_HFR_SYSTEM = "https://facility.ndhm.gov.in"

encounters = (
    bronze_since_last_load("Encounter", SILVER_ENC)
    .select(
        F.get_json_object("resource_json", "$.id").alias("fhir_encounter_id"),
        F.regexp_replace(
            F.get_json_object("resource_json", "$.subject.reference"),
            "^Patient/", ""
        ).alias("fhir_patient_id"),
        F.get_json_object("resource_json", "$.status").alias("status"),
        # Visit class: AMB=outpatient, IMP=inpatient, EMER=emergency, etc.
        F.get_json_object("resource_json", "$.class.code").alias("encounter_class"),
        F.get_json_object("resource_json", "$.type[0].coding[0].code").alias("encounter_type_code"),
        F.get_json_object("resource_json", "$.type[0].coding[0].display").alias("encounter_type_display"),
        F.to_timestamp(F.get_json_object("resource_json", "$.period.start")).alias("period_start"),
        F.to_timestamp(F.get_json_object("resource_json", "$.period.end")).alias("period_end"),
        # Duration in minutes (for OMOP visit_occurrence)
        F.expr("""
            CAST(
                (unix_timestamp(get_json_object(resource_json, '$.period.end'))
                 - unix_timestamp(get_json_object(resource_json, '$.period.start')))
                / 60 AS INT
            )
        """).alias("duration_minutes"),
        # ABDM Health Facility Registry code → OMOP care_site_source_value
        F.expr(f"""
            CASE
                WHEN get_json_object(resource_json, '$.serviceProvider.identifier.system')
                     = '{ABDM_HFR_SYSTEM}'
                THEN get_json_object(resource_json, '$.serviceProvider.identifier.value')
            END
        """).alias("hfr_facility_code"),
        F.get_json_object("resource_json", "$.serviceProvider.display").alias("facility_name"),
        "_lineage_id", "_source_file", "_loaded_at",
    )
)

upsert_to_silver(encounters, SILVER_ENC)
print(f"stg_fhir_encounter: {spark.table(SILVER_ENC).count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Verify: Silver layer summary

# COMMAND ----------
silver_tables = {
    "stg_fhir_patient":            SILVER_PATIENT,
    "stg_fhir_condition":          SILVER_CONDITION,
    "stg_fhir_medication_request": SILVER_MED,
    "stg_fhir_observation":        SILVER_OBS,
    "stg_fhir_encounter":          SILVER_ENC,
}

print(f"{'Table':<35} {'Rows':>10}  {'Columns':>8}")
print("-" * 56)
for name, table in silver_tables.items():
    df = spark.table(table)
    print(f"{name:<35} {df.count():>10,}  {len(df.columns):>8}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify: ABHA ID coverage

# COMMAND ----------
patient_df = spark.table(SILVER_PATIENT)
total     = patient_df.count()
with_abha = patient_df.filter(F.col("abha_id").isNotNull()).count()

print(f"Total patients : {total:,}")
print(f"With ABHA ID   : {with_abha:,}  ({with_abha/total*100:.1f}%)")
print()
print("Note: Synthea generates synthetic US-style identifiers, so ABHA IDs")
print("will be 0% here. In real ABDM data this should be close to 100%.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify: Observation value type distribution
# MAGIC
# MAGIC Understanding this breakdown informs how many rows will route to
# MAGIC `measurement` vs `observation` in the Gold layer.

# COMMAND ----------
display(
    spark.table(SILVER_OBS)
    .select(
        F.when(F.col("value_quantity").isNotNull(), "valueQuantity")
         .when(F.col("value_concept_code").isNotNull(), "valueCodeableConcept")
         .when(F.col("value_string").isNotNull(), "valueString")
         .otherwise("no_value").alias("value_type"),
        "category",
    )
    .groupBy("value_type", "category")
    .agg(F.count("*").alias("count"))
    .orderBy(F.desc("count"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next Step
# MAGIC
# MAGIC Run **`04_omop_patient.py`** to map `stg_fhir_patient` → OMOP `person`,
# MAGIC including gender/race concept ID lookups against the OMOP vocabulary tables.
