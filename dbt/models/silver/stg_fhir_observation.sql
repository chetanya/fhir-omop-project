-- models/silver/stg_fhir_observation.sql
-- Flatten FHIR Observation → Silver
-- Input:  health_lh.fhir_bronze.raw_bundles WHERE resource_type = 'Observation'
-- Output: One row per observation; all value[x] types preserved for Gold routing

{{ config(
    materialized='incremental',
    unique_key='fhir_observation_id',
    file_format='delta',
    incremental_strategy='merge',
    zorderby=['fhir_patient_id', 'effective_datetime']
) }}

WITH raw AS (
    SELECT * FROM {{ source('fhir_bronze', 'raw_bundles') }}
    WHERE resource_type = 'Observation'
    {% if is_incremental() %}
    AND _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
)

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
    -- value[x]: all three types preserved; Gold routes based on OMOP domain
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

FROM raw
