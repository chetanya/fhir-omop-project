-- models/silver/stg_fhir_patient.sql
-- Flatten raw FHIR Patient JSON (Bronze) → structured Silver table
-- Input:  workspace.fhir_bronze.raw_bundles WHERE resource_type = 'Patient'
-- Output: One row per patient, key fields extracted

{{ config(
    materialized='incremental',
    unique_key='fhir_patient_id',
    file_format='delta',
    incremental_strategy='merge',
    zorderby=['fhir_patient_id']
) }}

WITH raw AS (
    SELECT * FROM {{ source('fhir_bronze', 'raw_bundles') }}
    WHERE resource_type = 'Patient'
    {% if is_incremental() %}
    AND _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),

-- Extract ABHA ID and MRN from the identifier array.
-- Use FILTER + get() for safe access: get() returns NULL on empty array;
-- array[0] throws ArrayIndexOutOfBoundsException.
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
            ),
            0
        ).value                                                 AS abha_id,
        get(
            filter(
                from_json(
                    get_json_object(resource_json, '$.identifier'),
                    'array<struct<system:string,value:string>>'
                ),
                x -> x.system != 'https://healthid.ndhm.gov.in'
            ),
            0
        ).value                                                 AS mrn
    FROM raw
)

SELECT
    get_json_object(r.resource_json, '$.id')                    AS fhir_patient_id,
    TO_DATE(get_json_object(r.resource_json, '$.birthDate'))    AS birth_date,
    get_json_object(r.resource_json, '$.gender')                AS gender_code,
    get_json_object(r.resource_json, '$.address[0].state')      AS state,
    get_json_object(r.resource_json, '$.address[0].postalCode') AS postal_code,
    -- Family name for QA only — do not carry forward to Gold
    get_json_object(r.resource_json, '$.name[0].family')        AS family_name,
    i.abha_id,
    i.mrn,
    CASE
        WHEN get_json_object(r.resource_json, '$.deceasedBoolean') = 'true' THEN TRUE
        WHEN get_json_object(r.resource_json, '$.deceasedDateTime') IS NOT NULL THEN TRUE
        ELSE FALSE
    END                                                         AS is_deceased,
    TO_TIMESTAMP(
        get_json_object(r.resource_json, '$.deceasedDateTime')
    )                                                           AS deceased_datetime,
    r._lineage_id,
    r._source_file,
    r._loaded_at

FROM raw r
LEFT JOIN identifiers i ON r.fhir_resource_id = i.fhir_resource_id
