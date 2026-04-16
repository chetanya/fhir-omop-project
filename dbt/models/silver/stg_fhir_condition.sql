-- models/silver/stg_fhir_condition.sql
-- Flatten FHIR Condition → Silver
-- Input:  workspace.fhir_bronze.raw_bundles WHERE resource_type = 'Condition'
-- Output: One row per condition with SNOMED code, dates, status

{{ config(
    materialized='incremental',
    unique_key='fhir_condition_id',
    file_format='delta',
    incremental_strategy='merge',
    zorderby=['fhir_patient_id', 'condition_start_date']
) }}

WITH raw AS (
    SELECT * FROM {{ source('fhir_bronze', 'raw_bundles') }}
    WHERE resource_type = 'Condition'
    {% if is_incremental() %}
    AND _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    get_json_object(resource_json, '$.id')                      AS fhir_condition_id,
    regexp_replace(
        get_json_object(resource_json, '$.subject.reference'),
        '^Patient/', ''
    )                                                           AS fhir_patient_id,
    get_json_object(resource_json, '$.code.coding[0].system')   AS condition_source_vocabulary,
    get_json_object(resource_json, '$.code.coding[0].code')     AS condition_source_code,
    get_json_object(resource_json, '$.code.coding[0].display')  AS condition_source_display,
    -- Prefer onsetDateTime, fall back through onsetPeriod.start to recordedDate
    COALESCE(
        TO_DATE(get_json_object(resource_json, '$.onsetDateTime')),
        TO_DATE(get_json_object(resource_json, '$.onsetPeriod.start')),
        TO_DATE(get_json_object(resource_json, '$.recordedDate'))
    )                                                           AS condition_start_date,
    TO_DATE(get_json_object(resource_json, '$.recordedDate'))   AS condition_recorded_date,
    get_json_object(resource_json, '$.clinicalStatus.coding[0].code')      AS clinical_status,
    get_json_object(resource_json, '$.verificationStatus.coding[0].code')  AS verification_status,
    _lineage_id,
    _source_file,
    _loaded_at

FROM raw
