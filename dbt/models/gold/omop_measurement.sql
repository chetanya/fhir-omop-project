-- models/gold/omop_measurement.sql
-- FHIR Observation (valueQuantity) → OMOP measurement
-- Routes observations with numeric values to measurement table

{{ config(
    materialized='incremental',
    unique_key='measurement_id',
    file_format='delta',
    incremental_strategy='merge',
    partition_by={'field': 'year_start', 'data_type': 'int'},
    zorderby=['person_id', 'measurement_datetime']
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_fhir_observation') }}
    WHERE value_quantity IS NOT NULL  -- Only numeric observations
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

-- Vocabulary lookup: LOINC/SNOMED code → OMOP measurement_concept_id
-- When vocabularies not yet loaded, defaults to 0
concept_lookup AS (
    SELECT
        concept_code,
        concept_id AS measurement_concept_id,
        domain_id
    FROM {{ var('catalog') }}.{{ var('gold_schema') }}.concept
    WHERE vocabulary_id IN ('LOINC', 'SNOMED')
      AND standard_concept = 'S'
    UNION ALL
    SELECT concept_code, 0, 'Measurement'
    LIMIT 1  -- Ensures 0 maps exist even with empty concept table
)

SELECT
    -- OMOP surrogate key: hash of FHIR observation ID for idempotency
    {{ dbt_utils.generate_surrogate_key(['fhir_observation_id']) }} AS measurement_id,

    -- Foreign keys
    p.person_id,
    COALESCE(v.visit_occurrence_id, 0)                           AS visit_occurrence_id,
    NULL AS visit_detail_id,

    -- LOINC/SNOMED code → OMOP measurement concept (domain must be 'Measurement')
    COALESCE(c.measurement_concept_id, 0)                        AS measurement_concept_id,

    -- Date/time of measurement
    CAST(effective_datetime AS DATE)                             AS measurement_date,
    effective_datetime                                           AS measurement_datetime,

    -- Type: 38000280 = Lab measurement
    38000280 AS measurement_type_concept_id,

    -- Numeric result and units
    value_quantity                                               AS value_as_number,
    NULL AS value_as_concept_id,

    -- UCUM unit code (e.g., 'mg/dL', 'mmol/L')
    value_unit                                                   AS unit_concept_id,
    value_unit_code                                              AS unit_source_value,

    -- Reference ranges (normal values)
    range_low,
    range_high,

    -- Provider and operator: not in FHIR Observation
    NULL AS provider_id,
    NULL AS value_source_value,

    -- Observation source: preserve original FHIR codes for traceability
    observation_source_code,
    observation_source_vocabulary,
    observation_source_display,
    0 AS observation_source_concept_id,

    -- Observation category (vital-signs, laboratory, etc.)
    category                                                     AS observation_category_concept_id,

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
    AND c.domain_id = 'Measurement'
