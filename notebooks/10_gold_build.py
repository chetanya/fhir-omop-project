# Databricks notebook source
# MAGIC %md
# MAGIC # Step 10: Build Silver → Gold (SparkSQL, no dbt)
# MAGIC **Phase 2 | Materialize OMOP Gold tables**
# MAGIC
# MAGIC **Why not dbt?** `dbt-databricks` requires a SQL Warehouse endpoint.
# MAGIC Databricks Community Edition only has an all-purpose cluster — no warehouses.
# MAGIC This notebook runs the exact same SQL logic via `spark.sql()` instead.
# MAGIC
# MAGIC **What this builds:**
# MAGIC - Silver: stg_fhir_patient, stg_fhir_condition, stg_fhir_medication_request,
# MAGIC           stg_fhir_observation, stg_fhir_encounter
# MAGIC - Gold:   omop_person, omop_visit_occurrence, omop_condition_occurrence,
# MAGIC           omop_drug_exposure, omop_measurement, omop_observation
# MAGIC - View:   lineage_audit

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Setup

# COMMAND ----------

CATALOG       = "workspace"
BRONZE_SCHEMA = "fhir_bronze"
SILVER_SCHEMA = "fhir_silver"
GOLD_SCHEMA   = "omop_gold"

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD   = f"{CATALOG}.{GOLD_SCHEMA}"

# FHIR timestamps use ISO 8601 with timezone offsets (e.g. +05:30 IST, -04:00 EDT).
# Photon's strict ANSI mode rejects these in TO_TIMESTAMP — returns an error instead
# of NULL. Disabling ANSI mode makes TO_TIMESTAMP return NULL on unparseable strings,
# which is correct for Bronze/Silver where bad timestamps are handled downstream.
spark.conf.set("spark.sql.ansi.enabled", "false")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

print(f"Bronze : {BRONZE}")
print(f"Silver : {SILVER}")
print(f"Gold   : {GOLD}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Silver — Flatten FHIR resources
# MAGIC
# MAGIC Each CTAS reads from Bronze `raw_bundles`, filters by `resource_type`,
# MAGIC and extracts key fields from `resource_json` using `get_json_object`.

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1a. stg_fhir_patient

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.stg_fhir_patient
USING DELTA AS

WITH raw AS (
    SELECT * FROM {BRONZE}.raw_bundles
    WHERE resource_type = 'Patient'
),

identifiers AS (
    SELECT
        fhir_resource_id,
        get(
            filter(
                from_json(
                    get_json_object(resource_json, '$.identifier'),
                    'array<struct<system:string,value:string>>'
                ),
                x -> x.system = 'https://healthid.ndhm.gov.in'
            ), 0
        ).value AS abha_id,
        get(
            filter(
                from_json(
                    get_json_object(resource_json, '$.identifier'),
                    'array<struct<system:string,value:string>>'
                ),
                x -> x.system != 'https://healthid.ndhm.gov.in'
            ), 0
        ).value AS mrn
    FROM raw
)

SELECT
    get_json_object(r.resource_json, '$.id')                    AS fhir_patient_id,
    TO_DATE(get_json_object(r.resource_json, '$.birthDate'))     AS birth_date,
    get_json_object(r.resource_json, '$.gender')                 AS gender_code,
    -- US Core Race extension (Synthea); NULL for ABDM profiles
    get_json_object(r.resource_json,
        '$.extension[0].extension[0].valueCoding.code')          AS race_code,
    get_json_object(r.resource_json, '$.address[0].state')       AS state,
    get_json_object(r.resource_json, '$.address[0].postalCode')  AS postal_code,
    get_json_object(r.resource_json, '$.name[0].family')         AS family_name,
    i.abha_id,
    i.mrn,
    CASE
        WHEN get_json_object(r.resource_json, '$.deceasedBoolean') = 'true' THEN TRUE
        WHEN get_json_object(r.resource_json, '$.deceasedDateTime') IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_deceased,
    TO_TIMESTAMP(
        get_json_object(r.resource_json, '$.deceasedDateTime')
    ) AS deceased_datetime,
    r._lineage_id,
    r._source_file,
    r._loaded_at
FROM raw r
LEFT JOIN identifiers i ON r.fhir_resource_id = i.fhir_resource_id
""")

count = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.stg_fhir_patient").collect()[0][0]
print(f"✓ stg_fhir_patient: {count:,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1b. stg_fhir_condition

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.stg_fhir_condition
USING DELTA AS

SELECT
    get_json_object(resource_json, '$.id')                      AS fhir_condition_id,
    regexp_replace(
        get_json_object(resource_json, '$.subject.reference'),
        '^Patient/', ''
    )                                                           AS fhir_patient_id,
    get_json_object(resource_json, '$.code.coding[0].system')   AS condition_source_vocabulary,
    get_json_object(resource_json, '$.code.coding[0].code')     AS condition_source_code,
    get_json_object(resource_json, '$.code.coding[0].display')  AS condition_source_display,
    COALESCE(
        TO_DATE(get_json_object(resource_json, '$.onsetDateTime')),
        TO_DATE(get_json_object(resource_json, '$.onsetPeriod.start')),
        TO_DATE(get_json_object(resource_json, '$.recordedDate'))
    )                                                           AS condition_start_date,
    TO_DATE(get_json_object(resource_json, '$.recordedDate'))   AS condition_recorded_date,
    get_json_object(resource_json, '$.clinicalStatus.coding[0].code')     AS clinical_status,
    get_json_object(resource_json, '$.verificationStatus.coding[0].code') AS verification_status,
    _lineage_id,
    _source_file,
    _loaded_at
FROM {BRONZE}.raw_bundles
WHERE resource_type = 'Condition'
""")

count = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.stg_fhir_condition").collect()[0][0]
print(f"✓ stg_fhir_condition: {count:,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1c. stg_fhir_medication_request

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.stg_fhir_medication_request
USING DELTA AS

SELECT
    get_json_object(resource_json, '$.id')                      AS fhir_medication_request_id,
    regexp_replace(
        get_json_object(resource_json, '$.subject.reference'),
        '^Patient/', ''
    )                                                           AS fhir_patient_id,
    get_json_object(resource_json,
        '$.medicationCodeableConcept.coding[0].system')         AS drug_source_vocabulary,
    get_json_object(resource_json,
        '$.medicationCodeableConcept.coding[0].code')           AS drug_source_code,
    get_json_object(resource_json,
        '$.medicationCodeableConcept.coding[0].display')        AS drug_source_display,
    TO_DATE(get_json_object(resource_json, '$.authoredOn'))     AS drug_exposure_start_date,
    get_json_object(resource_json, '$.status')                  AS status,
    get_json_object(resource_json, '$.intent')                  AS intent,
    CAST(get_json_object(resource_json,
        '$.dosageInstruction[0].doseAndRate[0].doseQuantity.value')
        AS DOUBLE)                                              AS dose_value,
    get_json_object(resource_json,
        '$.dosageInstruction[0].doseAndRate[0].doseQuantity.unit') AS dose_unit,
    CASE
        WHEN get_json_object(resource_json,
             '$.dosageInstruction[0].timing.repeat.periodUnit') = 'd'
        THEN CAST(get_json_object(resource_json,
                  '$.dosageInstruction[0].timing.repeat.frequency') AS DOUBLE)
           / CAST(get_json_object(resource_json,
                  '$.dosageInstruction[0].timing.repeat.period') AS DOUBLE)
        ELSE NULL
    END                                                         AS doses_per_day,
    _lineage_id,
    _source_file,
    _loaded_at
FROM {BRONZE}.raw_bundles
WHERE resource_type = 'MedicationRequest'
""")

count = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.stg_fhir_medication_request").collect()[0][0]
print(f"✓ stg_fhir_medication_request: {count:,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1d. stg_fhir_observation

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.stg_fhir_observation
USING DELTA AS

SELECT
    get_json_object(resource_json, '$.id')                      AS fhir_observation_id,
    regexp_replace(
        get_json_object(resource_json, '$.subject.reference'),
        '^Patient/', ''
    )                                                           AS fhir_patient_id,
    regexp_replace(
        get_json_object(resource_json, '$.encounter.reference'),
        '^Encounter/', ''
    )                                                           AS fhir_encounter_id,
    get_json_object(resource_json, '$.status')                  AS status,
    get_json_object(resource_json, '$.category[0].coding[0].code') AS category,
    get_json_object(resource_json, '$.code.coding[0].system')   AS observation_source_vocabulary,
    get_json_object(resource_json, '$.code.coding[0].code')     AS observation_source_code,
    get_json_object(resource_json, '$.code.coding[0].display')  AS observation_source_display,
    COALESCE(
        TO_TIMESTAMP(get_json_object(resource_json, '$.effectiveDateTime')),
        TO_TIMESTAMP(get_json_object(resource_json, '$.effectivePeriod.start'))
    )                                                           AS effective_datetime,
    CAST(get_json_object(resource_json, '$.valueQuantity.value')
         AS DOUBLE)                                             AS value_quantity,
    get_json_object(resource_json, '$.valueQuantity.unit')      AS value_unit,
    get_json_object(resource_json, '$.valueQuantity.code')      AS value_unit_code,
    get_json_object(resource_json,
        '$.valueCodeableConcept.coding[0].code')                AS value_concept_code,
    get_json_object(resource_json,
        '$.valueCodeableConcept.coding[0].display')             AS value_concept_display,
    get_json_object(resource_json, '$.valueString')             AS value_string,
    CAST(get_json_object(resource_json, '$.referenceRange[0].low.value')
         AS DOUBLE)                                             AS range_low,
    CAST(get_json_object(resource_json, '$.referenceRange[0].high.value')
         AS DOUBLE)                                             AS range_high,
    _lineage_id,
    _source_file,
    _loaded_at
FROM {BRONZE}.raw_bundles
WHERE resource_type = 'Observation'
""")

count = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.stg_fhir_observation").collect()[0][0]
print(f"✓ stg_fhir_observation: {count:,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1e. stg_fhir_encounter

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.stg_fhir_encounter
USING DELTA AS

SELECT
    get_json_object(resource_json, '$.id')                      AS fhir_encounter_id,
    regexp_replace(
        get_json_object(resource_json, '$.subject.reference'),
        '^Patient/', ''
    )                                                           AS fhir_patient_id,
    get_json_object(resource_json, '$.status')                  AS status,
    get_json_object(resource_json, '$.class.code')              AS encounter_class,
    get_json_object(resource_json, '$.type[0].coding[0].code')  AS encounter_type_code,
    get_json_object(resource_json, '$.type[0].coding[0].display') AS encounter_type_display,
    TO_TIMESTAMP(get_json_object(resource_json, '$.period.start')) AS period_start,
    TO_TIMESTAMP(get_json_object(resource_json, '$.period.end'))   AS period_end,
    CAST(
        (unix_timestamp(get_json_object(resource_json, '$.period.end'))
         - unix_timestamp(get_json_object(resource_json, '$.period.start')))
        / 60 AS INT
    )                                                           AS duration_minutes,
    CASE
        WHEN get_json_object(resource_json,
             '$.serviceProvider.identifier.system') = 'https://facility.ndhm.gov.in'
        THEN get_json_object(resource_json, '$.serviceProvider.identifier.value')
    END                                                         AS hfr_facility_code,
    get_json_object(resource_json, '$.serviceProvider.display') AS facility_name,
    _lineage_id,
    _source_file,
    _loaded_at
FROM {BRONZE}.raw_bundles
WHERE resource_type = 'Encounter'
""")

count = spark.sql(f"SELECT COUNT(*) FROM {SILVER}.stg_fhir_encounter").collect()[0][0]
print(f"✓ stg_fhir_encounter: {count:,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Gold — OMOP CDM tables
# MAGIC
# MAGIC Surrogate keys use `ABS(xxhash64(...))` — deterministic positive BIGINT,
# MAGIC replacing `dbt_utils.generate_surrogate_key` which needs the dbt_utils package.
# MAGIC
# MAGIC Concept lookups join to the `concept` table loaded by notebook 09.
# MAGIC If vocabularies are not yet loaded, concept_ids default to 0 (unmapped).
# MAGIC
# MAGIC Build order: person → visit_occurrence → condition/drug/measurement/observation

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2a. omop_person

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.omop_person
USING DELTA AS

WITH source AS (
    SELECT * FROM {SILVER}.stg_fhir_patient
),

gender_concepts AS (
    SELECT concept_code, concept_id AS gender_concept_id
    FROM {GOLD}.concept
    WHERE vocabulary_id = 'Gender' AND standard_concept = 'S'
),

race_concepts AS (
    SELECT concept_code, concept_id AS race_concept_id
    FROM {GOLD}.concept
    WHERE vocabulary_id = 'Race' AND standard_concept = 'S'
)

SELECT
    ABS(xxhash64(fhir_patient_id))                              AS person_id,
    COALESCE(gc.gender_concept_id, 0)                           AS gender_concept_id,
    YEAR(birth_date)                                            AS year_of_birth,
    MONTH(birth_date)                                           AS month_of_birth,
    DAY(birth_date)                                             AS day_of_birth,
    COALESCE(rc.race_concept_id, 0)                             AS race_concept_id,
    0                                                           AS ethnicity_concept_id,
    gender_code                                                 AS gender_source_value,
    race_code                                                   AS race_source_value,
    0                                                           AS race_source_concept_id,
    COALESCE(abha_id, fhir_patient_id)                          AS person_source_value,
    NULL                                                        AS provider_id,
    NULL                                                        AS care_site_id,
    NULL                                                        AS location_id,
    _lineage_id,
    _source_file,
    _loaded_at,
    CURRENT_TIMESTAMP()                                         AS _transformed_at
FROM source s
LEFT JOIN gender_concepts gc ON LOWER(s.gender_code) = LOWER(gc.concept_code)
LEFT JOIN race_concepts rc   ON s.race_code = rc.concept_code
""")

count = spark.sql(f"SELECT COUNT(*) FROM {GOLD}.omop_person").collect()[0][0]
print(f"✓ omop_person: {count:,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2b. omop_visit_occurrence

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.omop_visit_occurrence
USING DELTA AS

WITH source AS (
    SELECT * FROM {SILVER}.stg_fhir_encounter
),

person AS (
    SELECT person_id, person_source_value FROM {GOLD}.omop_person
),

visit_type_mapping AS (
    SELECT 'AMB'  AS encounter_class, 9202 AS visit_type_concept_id
    UNION ALL SELECT 'IMP',  9201
    UNION ALL SELECT 'EMER', 9203
    UNION ALL SELECT 'HH',   9204
)

SELECT
    ABS(xxhash64(fhir_encounter_id))                            AS visit_occurrence_id,
    p.person_id,
    COALESCE(vtm.visit_type_concept_id, 0)                      AS visit_type_concept_id,
    CAST(period_start AS DATE)                                  AS visit_start_date,
    CAST(period_end   AS DATE)                                  AS visit_end_date,
    period_start                                                AS visit_start_datetime,
    period_end                                                  AS visit_end_datetime,
    581399                                                      AS visit_concept_id,
    NULL                                                        AS provider_id,
    NULL                                                        AS care_site_id,
    NULL                                                        AS admitting_concept_id,
    NULL                                                        AS admitting_source_concept_id,
    NULL                                                        AS discharge_to_concept_id,
    duration_minutes                                            AS preceding_visit_occurrence_id,
    encounter_class                                             AS visit_source_value,
    encounter_type_code                                         AS visit_source_concept_id,
    encounter_type_display,
    hfr_facility_code,
    facility_name,
    YEAR(CAST(period_start AS DATE))                            AS year_start,
    _lineage_id,
    _source_file,
    _loaded_at,
    CURRENT_TIMESTAMP()                                         AS _transformed_at
FROM source s
LEFT JOIN person p         ON s.fhir_patient_id = p.person_source_value
LEFT JOIN visit_type_mapping vtm ON s.encounter_class = vtm.encounter_class
""")

count = spark.sql(f"SELECT COUNT(*) FROM {GOLD}.omop_visit_occurrence").collect()[0][0]
print(f"✓ omop_visit_occurrence: {count:,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2c. omop_condition_occurrence

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.omop_condition_occurrence
USING DELTA AS

WITH source AS (
    SELECT * FROM {SILVER}.stg_fhir_condition
),

person AS (
    SELECT person_id, person_source_value FROM {GOLD}.omop_person
),

concept_lookup AS (
    SELECT concept_code, concept_id AS condition_concept_id, domain_id
    FROM {GOLD}.concept
    WHERE vocabulary_id = 'SNOMED' AND standard_concept = 'S'
)

SELECT
    ABS(xxhash64(fhir_condition_id))                            AS condition_occurrence_id,
    p.person_id,
    COALESCE(c.condition_concept_id, 0)                         AS condition_concept_id,
    condition_start_date,
    NULL                                                        AS condition_end_date,
    clinical_status                                             AS condition_status_source_value,
    32882                                                       AS condition_type_concept_id,
    condition_source_code,
    condition_source_vocabulary,
    condition_source_display,
    0                                                           AS condition_source_concept_id,
    CASE
        WHEN clinical_status = 'resolved' THEN 'Resolved'
        ELSE NULL
    END                                                         AS stop_reason,
    YEAR(condition_start_date)                                  AS year_start,
    _lineage_id,
    _source_file,
    _loaded_at,
    CURRENT_TIMESTAMP()                                         AS _transformed_at
FROM source s
LEFT JOIN person p ON s.fhir_patient_id = p.person_source_value
LEFT JOIN concept_lookup c
    ON s.condition_source_code = c.concept_code
    AND c.domain_id = 'Condition'
""")

count = spark.sql(f"SELECT COUNT(*) FROM {GOLD}.omop_condition_occurrence").collect()[0][0]
print(f"✓ omop_condition_occurrence: {count:,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2d. omop_drug_exposure

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.omop_drug_exposure
USING DELTA AS

WITH source AS (
    SELECT * FROM {SILVER}.stg_fhir_medication_request
),

person AS (
    SELECT person_id, person_source_value FROM {GOLD}.omop_person
),

concept_lookup AS (
    SELECT concept_code, concept_id AS drug_concept_id, domain_id
    FROM {GOLD}.concept
    WHERE vocabulary_id IN ('RxNorm', 'RxNorm Extension') AND standard_concept = 'S'
)

SELECT
    ABS(xxhash64(fhir_medication_request_id))                   AS drug_exposure_id,
    p.person_id,
    COALESCE(c.drug_concept_id, 0)                              AS drug_concept_id,
    drug_exposure_start_date,
    CASE
        WHEN status IN ('stopped', 'cancelled') THEN drug_exposure_start_date
        ELSE NULL
    END                                                         AS drug_exposure_end_date,
    38000177                                                    AS drug_type_concept_id,
    dose_value                                                  AS quantity,
    dose_unit,
    NULL                                                        AS days_supply,
    NULL                                                        AS route_concept_id,
    doses_per_day                                               AS effective_drug_dose,
    dose_unit                                                   AS dose_unit_source_value,
    drug_source_code,
    drug_source_vocabulary,
    drug_source_display,
    0                                                           AS drug_source_concept_id,
    CASE
        WHEN status = 'stopped'   THEN 'Stopped'
        WHEN status = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END                                                         AS stop_reason,
    NULL                                                        AS refills,
    NULL                                                        AS lot_number,
    NULL                                                        AS visit_occurrence_id,
    NULL                                                        AS visit_detail_id,
    NULL                                                        AS provider_id,
    YEAR(drug_exposure_start_date)                              AS year_start,
    _lineage_id,
    _source_file,
    _loaded_at,
    CURRENT_TIMESTAMP()                                         AS _transformed_at
FROM source s
LEFT JOIN person p ON s.fhir_patient_id = p.person_source_value
LEFT JOIN concept_lookup c
    ON s.drug_source_code = c.concept_code
    AND c.domain_id = 'Drug'
""")

count = spark.sql(f"SELECT COUNT(*) FROM {GOLD}.omop_drug_exposure").collect()[0][0]
print(f"✓ omop_drug_exposure: {count:,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2e. omop_measurement
# MAGIC Routes FHIR Observations with `valueQuantity` (numeric results) to the measurement table.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.omop_measurement
USING DELTA AS

WITH source AS (
    SELECT * FROM {SILVER}.stg_fhir_observation
    WHERE value_quantity IS NOT NULL
),

person AS (
    SELECT person_id, person_source_value FROM {GOLD}.omop_person
),

visit AS (
    SELECT ABS(xxhash64(fhir_encounter_id)) AS visit_occurrence_id, fhir_encounter_id
    FROM {SILVER}.stg_fhir_encounter
),

concept_lookup AS (
    SELECT concept_code, concept_id AS measurement_concept_id, domain_id
    FROM {GOLD}.concept
    WHERE vocabulary_id IN ('LOINC', 'SNOMED') AND standard_concept = 'S'
)

SELECT
    ABS(xxhash64(fhir_observation_id))                          AS measurement_id,
    p.person_id,
    COALESCE(v.visit_occurrence_id, 0)                          AS visit_occurrence_id,
    NULL                                                        AS visit_detail_id,
    COALESCE(c.measurement_concept_id, 0)                       AS measurement_concept_id,
    CAST(effective_datetime AS DATE)                            AS measurement_date,
    effective_datetime                                          AS measurement_datetime,
    38000280                                                    AS measurement_type_concept_id,
    value_quantity                                              AS value_as_number,
    NULL                                                        AS value_as_concept_id,
    value_unit                                                  AS unit_concept_id,
    value_unit_code                                             AS unit_source_value,
    range_low,
    range_high,
    NULL                                                        AS provider_id,
    NULL                                                        AS value_source_value,
    observation_source_code,
    observation_source_vocabulary,
    observation_source_display,
    0                                                           AS observation_source_concept_id,
    category                                                    AS observation_category_concept_id,
    YEAR(CAST(effective_datetime AS DATE))                      AS year_start,
    _lineage_id,
    _source_file,
    _loaded_at,
    CURRENT_TIMESTAMP()                                         AS _transformed_at
FROM source s
LEFT JOIN person p ON s.fhir_patient_id = p.person_source_value
LEFT JOIN visit v  ON s.fhir_encounter_id = v.fhir_encounter_id
LEFT JOIN concept_lookup c
    ON s.observation_source_code = c.concept_code
    AND c.domain_id = 'Measurement'
""")

count = spark.sql(f"SELECT COUNT(*) FROM {GOLD}.omop_measurement").collect()[0][0]
print(f"✓ omop_measurement: {count:,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2f. omop_observation
# MAGIC Routes FHIR Observations with coded/text values (non-numeric) to the observation table.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.omop_observation
USING DELTA AS

WITH source AS (
    SELECT * FROM {SILVER}.stg_fhir_observation
    WHERE value_string IS NOT NULL
       OR value_concept_code IS NOT NULL
       OR value_quantity IS NULL
),

person AS (
    SELECT person_id, person_source_value FROM {GOLD}.omop_person
),

visit AS (
    SELECT ABS(xxhash64(fhir_encounter_id)) AS visit_occurrence_id, fhir_encounter_id
    FROM {SILVER}.stg_fhir_encounter
),

concept_lookup AS (
    SELECT concept_code, concept_id AS observation_concept_id, domain_id
    FROM {GOLD}.concept
    WHERE vocabulary_id IN ('LOINC', 'SNOMED') AND standard_concept = 'S'
),

value_concept_lookup AS (
    SELECT concept_code, concept_id AS value_concept_id
    FROM {GOLD}.concept
    WHERE standard_concept = 'S'
)

SELECT
    ABS(xxhash64(fhir_observation_id))                          AS observation_id,
    p.person_id,
    COALESCE(v.visit_occurrence_id, 0)                          AS visit_occurrence_id,
    NULL                                                        AS visit_detail_id,
    COALESCE(c.observation_concept_id, 0)                       AS observation_concept_id,
    CAST(effective_datetime AS DATE)                            AS observation_date,
    effective_datetime                                          AS observation_datetime,
    38000280                                                    AS observation_type_concept_id,
    CASE
        WHEN value_concept_code IS NOT NULL THEN NULL
        ELSE value_quantity
    END                                                         AS value_as_number,
    COALESCE(vcl.value_concept_id, 0)                           AS value_as_concept_id,
    value_string                                                AS value_as_string,
    NULL                                                        AS qualifier_concept_id,
    NULL                                                        AS unit_concept_id,
    NULL                                                        AS unit_source_value,
    NULL                                                        AS provider_id,
    NULL                                                        AS value_source_value,
    observation_source_code,
    observation_source_vocabulary,
    observation_source_display,
    0                                                           AS observation_source_concept_id,
    category                                                    AS obs_event_field_concept_id,
    YEAR(CAST(effective_datetime AS DATE))                      AS year_start,
    _lineage_id,
    _source_file,
    _loaded_at,
    CURRENT_TIMESTAMP()                                         AS _transformed_at
FROM source s
LEFT JOIN person p ON s.fhir_patient_id = p.person_source_value
LEFT JOIN visit v  ON s.fhir_encounter_id = v.fhir_encounter_id
LEFT JOIN concept_lookup c
    ON s.observation_source_code = c.concept_code
LEFT JOIN value_concept_lookup vcl
    ON s.value_concept_code = vcl.concept_code
""")

count = spark.sql(f"SELECT COUNT(*) FROM {GOLD}.omop_observation").collect()[0][0]
print(f"✓ omop_observation: {count:,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Lineage audit view
# MAGIC
# MAGIC Traces a FHIR bundle through all OMOP tables via `_lineage_id`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.lineage_audit AS

SELECT 'person'               AS omop_table, _lineage_id,
       CAST(person_id AS STRING)              AS record_id,
       person_source_value                    AS fhir_resource_id,
       _source_file, _loaded_at, _transformed_at
FROM {GOLD}.omop_person

UNION ALL
SELECT 'condition_occurrence', _lineage_id,
       CAST(condition_occurrence_id AS STRING), NULL,
       _source_file, _loaded_at, _transformed_at
FROM {GOLD}.omop_condition_occurrence

UNION ALL
SELECT 'drug_exposure',        _lineage_id,
       CAST(drug_exposure_id AS STRING),        NULL,
       _source_file, _loaded_at, _transformed_at
FROM {GOLD}.omop_drug_exposure

UNION ALL
SELECT 'visit_occurrence',     _lineage_id,
       CAST(visit_occurrence_id AS STRING),     NULL,
       _source_file, _loaded_at, _transformed_at
FROM {GOLD}.omop_visit_occurrence

UNION ALL
SELECT 'measurement',          _lineage_id,
       CAST(measurement_id AS STRING),          NULL,
       _source_file, _loaded_at, _transformed_at
FROM {GOLD}.omop_measurement

UNION ALL
SELECT 'observation',          _lineage_id,
       CAST(observation_id AS STRING),          NULL,
       _source_file, _loaded_at, _transformed_at
FROM {GOLD}.omop_observation
""")

print("✓ lineage_audit view created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. OPTIMIZE — run after all tables are built
# MAGIC
# MAGIC ZORDER on high-cardinality join columns.
# MAGIC Skip if your cluster is running low on memory — tables are queryable without it.

# COMMAND ----------

gold_tables = {
    "omop_person":               "person_id",
    "omop_visit_occurrence":     "person_id, visit_start_date",
    "omop_condition_occurrence": "person_id, condition_start_date",
    "omop_drug_exposure":        "person_id, drug_exposure_start_date",
    "omop_measurement":          "person_id, measurement_date",
    "omop_observation":          "person_id, observation_date",
}

for table, zorder_cols in gold_tables.items():
    print(f"Optimizing {table}...")
    spark.sql(f"OPTIMIZE {GOLD}.{table} ZORDER BY ({zorder_cols})")
    print(f"  ✓ done")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Summary

# COMMAND ----------

tables = [
    ("omop_person",               GOLD),
    ("omop_visit_occurrence",     GOLD),
    ("omop_condition_occurrence", GOLD),
    ("omop_drug_exposure",        GOLD),
    ("omop_measurement",          GOLD),
    ("omop_observation",          GOLD),
]

total = 0
print("Gold table row counts:")
print("-" * 40)
for table, schema in tables:
    n = spark.sql(f"SELECT COUNT(*) FROM {schema}.{table}").collect()[0][0]
    print(f"  {table:<30} {n:>8,}")
    total += n
print("-" * 40)
print(f"  {'TOTAL':<30} {total:>8,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Spot-check concept mappings
# MAGIC
# MAGIC Verify that vocabulary lookups produced non-zero concept_ids.
# MAGIC If all concept_ids are 0, vocabulary tables are empty — run notebook 09 first.

# COMMAND ----------

print("Concept mapping rates (% of rows with concept_id > 0):\n")

checks = [
    ("omop_condition_occurrence", "condition_concept_id"),
    ("omop_drug_exposure",        "drug_concept_id"),
    ("omop_measurement",          "measurement_concept_id"),
    ("omop_observation",          "observation_concept_id"),
]

for table, col in checks:
    df = spark.sql(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN {col} > 0 THEN 1 ELSE 0 END) AS mapped
        FROM {GOLD}.{table}
    """).collect()[0]
    pct = (df.mapped / df.total * 100) if df.total > 0 else 0
    print(f"  {table}: {df.mapped:,} / {df.total:,} mapped ({pct:.1f}%)")
