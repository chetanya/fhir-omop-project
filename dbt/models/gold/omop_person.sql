-- models/gold/omop_person.sql
-- FHIR Patient → OMOP person
-- Mapping reference: https://github.com/CodeX-HL7-FHIR-Accelerator/fhir2omop-cookbook
-- ABDM note: ABHA ID (system: https://healthid.ndhm.gov.in) stored as person_source_value

{{ config(
    materialized='incremental',
    unique_key='person_id',
    file_format='delta',
    incremental_strategy='merge',
    zorderby=['person_id'],
    meta={'source_resource': 'FHIR/Patient'}
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_fhir_patient') }}
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),

-- Vocabulary lookup: FHIR gender string → OMOP gender_concept_id
-- OMOP standard: Male=8507, Female=8532, Unknown=0
gender_concepts AS (
    SELECT
        concept_code,
        concept_id AS gender_concept_id
    FROM {{ var('catalog') }}.{{ var('gold_schema') }}.concept
    WHERE vocabulary_id = 'Gender'
      AND standard_concept = 'S'
),

-- Vocabulary lookup: FHIR race extension → OMOP race_concept_id
-- Note: Indian patients often have no race coding in ABDM profiles.
-- Default to 0 (no matching concept) when absent.
race_concepts AS (
    SELECT
        concept_code,
        concept_id AS race_concept_id
    FROM {{ var('catalog') }}.{{ var('gold_schema') }}.concept
    WHERE vocabulary_id = 'Race'
      AND standard_concept = 'S'
)

SELECT
    -- OMOP surrogate key: hash of FHIR patient resource ID for idempotency
    {{ dbt_utils.generate_surrogate_key(['fhir_patient_id']) }} AS person_id,

    -- Gender: map FHIR gender string to OMOP concept_id
    -- FHIR values: 'male' | 'female' | 'other' | 'unknown'
    COALESCE(gc.gender_concept_id, 0)                           AS gender_concept_id,

    -- Birth year/month/day: extracted from FHIR birthDate (YYYY-MM-DD)
    YEAR(birth_date)                                            AS year_of_birth,
    MONTH(birth_date)                                           AS month_of_birth,
    DAY(birth_date)                                             AS day_of_birth,

    -- Race: FHIR US Core race extension; often absent in Indian ABDM records
    -- When absent, defaults to 0 (no matching concept)
    COALESCE(rc.race_concept_id, 0)                             AS race_concept_id,
    0                                                           AS ethnicity_concept_id, -- not coded in ABDM

    -- Source values: preserve original FHIR codes for traceability
    gender_code                                                 AS gender_source_value,
    race_code                                                   AS race_source_value,
    0                                                           AS race_source_concept_id,

    -- ABDM-specific: store ABHA ID as person_source_value
    -- ABHA ID format: XX-XXXX-XXXX-XXXX (16-digit national health ID)
    -- Falls back to FHIR resource ID if ABHA ID absent
    COALESCE(abha_id, fhir_patient_id)                          AS person_source_value,

    -- Location: map to care_site_id if facility known; else NULL
    NULL                                                        AS provider_id,
    NULL                                                        AS care_site_id,
    NULL                                                        AS location_id,

    -- Audit columns (lineage back to Bronze)
    _lineage_id,
    _source_file,
    _loaded_at,
    CURRENT_TIMESTAMP() AS _transformed_at

FROM source s
LEFT JOIN gender_concepts gc
    ON LOWER(s.gender_code) = LOWER(gc.concept_code)
LEFT JOIN race_concepts rc
    ON s.race_code = rc.concept_code
