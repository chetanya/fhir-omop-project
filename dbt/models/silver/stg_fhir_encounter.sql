-- models/silver/stg_fhir_encounter.sql
-- Flatten FHIR Encounter → Silver
-- Input:  workspace.fhir_bronze.raw_bundles WHERE resource_type = 'Encounter'
-- Output: One row per encounter with visit class, period, and HFR facility code

{{ config(
    materialized='incremental',
    unique_key='fhir_encounter_id',
    file_format='delta',
    incremental_strategy='merge',
    zorderby=['fhir_patient_id', 'period_start']
) }}

WITH raw AS (
    SELECT * FROM {{ source('fhir_bronze', 'raw_bundles') }}
    WHERE resource_type = 'Encounter'
    {% if is_incremental() %}
    AND _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    get_json_object(resource_json, '$.id')                      AS fhir_encounter_id,
    regexp_replace(
        get_json_object(resource_json, '$.subject.reference'),
        '^Patient/', ''
    )                                                           AS fhir_patient_id,
    get_json_object(resource_json, '$.status')                  AS status,
    -- Visit class: AMB=outpatient, IMP=inpatient, EMER=emergency
    -- Maps to OMOP visit_type_concept_id in Gold
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
    -- ABDM HFR code: present when serviceProvider uses https://facility.ndhm.gov.in
    -- Maps to OMOP care_site.care_site_source_value
    CASE
        WHEN get_json_object(resource_json,
             '$.serviceProvider.identifier.system') = 'https://facility.ndhm.gov.in'
        THEN get_json_object(resource_json, '$.serviceProvider.identifier.value')
    END                                                         AS hfr_facility_code,
    get_json_object(resource_json, '$.serviceProvider.display') AS facility_name,
    _lineage_id,
    _source_file,
    _loaded_at

FROM raw
