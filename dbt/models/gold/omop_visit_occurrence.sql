-- models/gold/omop_visit_occurrence.sql
-- FHIR Encounter → OMOP visit_occurrence
-- Maps encounter class (AMB=outpatient, IMP=inpatient, EMER=emergency) to visit_type_concept_id

{{ config(
    materialized='incremental',
    unique_key='visit_occurrence_id',
    file_format='delta',
    incremental_strategy='merge',
    partition_by={'field': 'year_start', 'data_type': 'int'},
    zorderby=['person_id', 'visit_start_date'],
    meta={'source_resource': 'FHIR/Encounter'}
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_fhir_encounter') }}
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),

-- Join to person to get person_id
person AS (
    SELECT person_id, person_source_value
    FROM {{ ref('omop_person') }}
),

-- Map FHIR encounter class to OMOP visit_type_concept_id
-- Standard OMOP concepts:
-- - 9201 = Inpatient visit
-- - 9202 = Outpatient visit
-- - 9203 = Emergency room visit
-- - 9204 = Home visit
visit_type_mapping AS (
    SELECT 'AMB' AS encounter_class, 9202 AS visit_type_concept_id  -- Outpatient
    UNION ALL
    SELECT 'IMP', 9201  -- Inpatient
    UNION ALL
    SELECT 'EMER', 9203  -- Emergency room
    UNION ALL
    SELECT 'HH', 9204  -- Home visit
)

SELECT
    -- OMOP surrogate key: hash of FHIR encounter ID for idempotency
    {{ dbt_utils.generate_surrogate_key(['fhir_encounter_id']) }} AS visit_occurrence_id,

    -- Foreign key to person
    p.person_id,

    -- Visit type: map FHIR encounter class to OMOP concept
    -- Defaults to 0 if class not recognized
    COALESCE(vtm.visit_type_concept_id, 0)                       AS visit_type_concept_id,

    -- Dates: encounter period.start and period.end
    CAST(period_start AS DATE)                                   AS visit_start_date,
    CAST(period_end AS DATE)                                     AS visit_end_date,

    -- Timestamps for precise time tracking
    period_start                                                 AS visit_start_datetime,
    period_end                                                   AS visit_end_datetime,

    -- Concept representing source of visit (EHR record)
    581399 AS visit_concept_id,  -- SNOMED 438653004 = Clinical encounter

    -- Provider and facility: not directly available in FHIR Encounter
    NULL AS provider_id,
    NULL AS care_site_id,

    -- Admission details: not in FHIR Encounter
    NULL AS admitting_concept_id,
    NULL AS admitting_source_concept_id,
    NULL AS discharge_to_concept_id,

    -- Length of stay in minutes (calculated in Silver)
    duration_minutes                                              AS preceding_visit_occurrence_id,

    -- Visit source: preserve original FHIR codes for traceability
    encounter_class                                              AS visit_source_value,
    encounter_type_code                                          AS visit_source_concept_id,
    encounter_type_display,

    -- ABDM-specific: HFR facility code maps to care_site.care_site_source_value
    hfr_facility_code,
    facility_name,

    -- Audit columns
    _lineage_id,
    _source_file,
    _loaded_at,
    CURRENT_TIMESTAMP() AS _transformed_at,

    -- Partition column
    YEAR(CAST(period_start AS DATE)) AS year_start

FROM source s
LEFT JOIN person p
    ON s.fhir_patient_id = p.person_source_value
LEFT JOIN visit_type_mapping vtm
    ON s.encounter_class = vtm.encounter_class
