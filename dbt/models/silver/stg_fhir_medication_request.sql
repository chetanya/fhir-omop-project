-- models/silver/stg_fhir_medication_request.sql
-- Flatten FHIR MedicationRequest → Silver
-- Input:  health_lh.fhir_bronze.raw_bundles WHERE resource_type = 'MedicationRequest'
-- Output: One row per prescription with RxNorm code and dosage

{{ config(
    materialized='incremental',
    unique_key='fhir_medication_request_id',
    file_format='delta',
    incremental_strategy='merge',
    zorderby=['fhir_patient_id', 'drug_exposure_start_date']
) }}

WITH raw AS (
    SELECT * FROM {{ source('fhir_bronze', 'raw_bundles') }}
    WHERE resource_type = 'MedicationRequest'
    {% if is_incremental() %}
    AND _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
)

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
    -- doses_per_day only meaningful when periodUnit = 'd'
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

FROM raw
