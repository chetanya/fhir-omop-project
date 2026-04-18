-- models/gold/omop_condition_occurrence.sql
-- FHIR Condition → OMOP condition_occurrence
-- Mapping reference: SNOMED-CT codes to OMOP condition_concept_id

{{ config(
    materialized='incremental',
    unique_key='condition_occurrence_id',
    file_format='delta',
    incremental_strategy='merge',
    partition_by={'field': 'year_start', 'data_type': 'int'},
    zorderby=['person_id', 'condition_start_date'],
    meta={'source_resource': 'FHIR/Condition'}
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_fhir_condition') }}
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),

-- Join to person to get person_id
person AS (
    SELECT person_id, person_source_value
    FROM {{ ref('omop_person') }}
),

-- Vocabulary lookup: SNOMED-CT code → OMOP condition_concept_id
-- When vocabularies not yet loaded, defaults to 0 (no mapping)
concept_lookup AS (
    SELECT
        concept_code,
        concept_id AS condition_concept_id,
        domain_id
    FROM {{ var('catalog') }}.{{ var('gold_schema') }}.concept
    WHERE vocabulary_id = 'SNOMED'
      AND standard_concept = 'S'
    UNION ALL
    SELECT concept_code, 0, 'Condition'
    LIMIT 1  -- Ensures 0 maps exist even with empty concept table
)

SELECT
    -- OMOP surrogate key: hash of FHIR condition ID for idempotency
    {{ dbt_utils.generate_surrogate_key(['fhir_condition_id']) }} AS condition_occurrence_id,

    -- Foreign key to person
    p.person_id,

    -- SNOMED code → OMOP condition concept (domain must be 'Condition')
    COALESCE(c.condition_concept_id, 0)                          AS condition_concept_id,

    -- Dates: prefer onset, fall back to recorded date
    condition_start_date,
    NULL                                                         AS condition_end_date,

    -- Condition status: active/inactive/resolved (maps to condition_status_concept_id in OMOP v6)
    clinical_status                                              AS condition_status_source_value,

    -- Type: inferred from FHIR verificationStatus (confirmed/unconfirmed/refuted/entered-in-error)
    -- OMOP uses this to determine visit_occurrence_id (NULL for non-visit conditions)
    32882 AS condition_type_concept_id,  -- Fixed to EHR encounter diagnosis

    -- Source values: preserve original FHIR codes for traceability
    condition_source_code,
    condition_source_vocabulary,
    condition_source_display,
    0 AS condition_source_concept_id,

    -- Stop reason: NULL unless clinicalStatus = 'resolved'
    CASE
        WHEN clinical_status = 'resolved' THEN 'Resolved'
        ELSE NULL
    END AS stop_reason,

    -- Audit columns
    _lineage_id,
    _source_file,
    _loaded_at,
    CURRENT_TIMESTAMP() AS _transformed_at,

    -- Partition column
    YEAR(condition_start_date) AS year_start

FROM source s
LEFT JOIN person p
    ON s.fhir_patient_id = p.person_source_value
LEFT JOIN concept_lookup c
    ON s.condition_source_code = c.concept_code
    AND c.domain_id = 'Condition'
