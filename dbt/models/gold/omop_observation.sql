-- models/gold/omop_observation.sql
-- FHIR Observation (valueCodeableConcept, valueString) → OMOP observation
-- Routes observations with coded or text values to observation table

{{ config(
    materialized='incremental',
    unique_key='observation_id',
    file_format='delta',
    incremental_strategy='merge',
    partition_by={'field': 'year_start', 'data_type': 'int'},
    zorderby=['person_id', 'observation_datetime']
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_fhir_observation') }}
    -- Route to observation if: valueString present, OR valueCodeableConcept present, OR
    -- valueQuantity is null (failed to parse as numeric)
    WHERE (value_string IS NOT NULL
        OR value_concept_code IS NOT NULL
        OR value_quantity IS NULL)
    {% if is_incremental() %}
    AND _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),

-- Join to person to get person_id
person AS (
    SELECT person_id, person_source_value
    FROM {{ ref('omop_person') }}
),

-- Join to visit to get visit_occurrence_id
visit AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['fhir_encounter_id']) }} AS visit_occurrence_id,
        fhir_encounter_id
    FROM {{ ref('stg_fhir_encounter') }}
),

-- Vocabulary lookup: LOINC/SNOMED code → OMOP observation_concept_id
-- When vocabularies not yet loaded, defaults to 0
concept_lookup AS (
    SELECT
        concept_code,
        concept_id AS observation_concept_id,
        domain_id
    FROM {{ var('catalog') }}.{{ var('gold_schema') }}.concept
    WHERE vocabulary_id IN ('LOINC', 'SNOMED')
      AND standard_concept = 'S'
    UNION ALL
    SELECT concept_code, 0, 'Observation'
    LIMIT 1  -- Ensures 0 maps exist even with empty concept table
),

-- Vocabulary lookup: observation value (if coded)
value_concept_lookup AS (
    SELECT
        concept_code,
        concept_id AS value_concept_id
    FROM {{ var('catalog') }}.{{ var('gold_schema') }}.concept
    WHERE standard_concept = 'S'
    UNION ALL
    SELECT concept_code, 0
    LIMIT 1  -- Ensures 0 maps exist even with empty concept table
)

SELECT
    -- OMOP surrogate key: hash of FHIR observation ID for idempotency
    {{ dbt_utils.generate_surrogate_key(['fhir_observation_id']) }} AS observation_id,

    -- Foreign keys
    p.person_id,
    COALESCE(v.visit_occurrence_id, 0)                           AS visit_occurrence_id,
    NULL AS visit_detail_id,

    -- LOINC/SNOMED code → OMOP observation concept (domain may be 'Observation' or other)
    COALESCE(c.observation_concept_id, 0)                        AS observation_concept_id,

    -- Date/time of observation
    CAST(effective_datetime AS DATE)                             AS observation_date,
    effective_datetime                                           AS observation_datetime,

    -- Type: 38000280 = Lab measurement (reuse; could also be 38000276 for social history)
    38000280 AS observation_type_concept_id,

    -- Value: either numeric, coded, or text
    -- If valueCodeableConcept: look up value_concept_code
    -- If valueString: store in value_as_string
    CASE
        WHEN value_concept_code IS NOT NULL THEN NULL  -- Coded value goes to value_as_concept_id
        ELSE value_quantity
    END AS value_as_number,

    COALESCE(vcl.value_concept_id, 0)                            AS value_as_concept_id,
    value_string                                                 AS value_as_string,

    -- Qualifier: not typically in FHIR Observation
    NULL AS qualifier_concept_id,

    -- Unit: typically null for coded/text observations
    NULL AS unit_concept_id,
    NULL AS unit_source_value,

    -- Provider: not in FHIR Observation
    NULL AS provider_id,
    NULL AS value_source_value,

    -- Observation source: preserve original FHIR codes for traceability
    observation_source_code,
    observation_source_vocabulary,
    observation_source_display,
    0 AS observation_source_concept_id,

    -- Observation category (vital-signs, laboratory, social-history, etc.)
    category                                                     AS obs_event_field_concept_id,

    -- Audit columns
    _lineage_id,
    _source_file,
    _loaded_at,

    -- Partition column
    YEAR(CAST(effective_datetime AS DATE)) AS year_start

FROM source s
LEFT JOIN person p
    ON s.fhir_patient_id = p.person_source_value
LEFT JOIN visit v
    ON s.fhir_encounter_id = v.fhir_encounter_id
LEFT JOIN concept_lookup c
    ON s.observation_source_code = c.concept_code
LEFT JOIN value_concept_lookup vcl
    ON s.value_concept_code = vcl.concept_code
