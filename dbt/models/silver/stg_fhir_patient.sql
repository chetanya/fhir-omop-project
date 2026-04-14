-- models/silver/stg_fhir_patient.sql
-- Flatten raw FHIR Patient JSON (Bronze) → structured Silver table
-- Input: Bronze Delta table with FHIR bundle JSON as string column
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

-- Explode identifier array to extract ABHA ID and MRN separately.
-- FHIR Patient.identifier is an array of {system, value} objects.
-- ABDM ABHA ID system: https://healthid.ndhm.gov.in
identifiers AS (
    SELECT
        fhir_id,
        MAX(CASE WHEN id.system = 'https://healthid.ndhm.gov.in'
                 THEN id.value END)                             AS abha_id,
        MAX(CASE WHEN id.system NOT LIKE '%ndhm%'
                 THEN id.value END)                             AS mrn
    FROM raw,
    LATERAL VIEW EXPLODE(from_json(resource_json, 'array<struct<system:string,value:string>>')(
        get_json_object(resource_json, '$.identifier')
    )) AS id
    GROUP BY fhir_id
),

-- Extract US Core race extension if present.
-- In ABDM profiles this is typically absent; default returns NULL.
extensions AS (
    SELECT
        fhir_id,
        get_json_object(ext.value, '$.valueString')             AS race_code
    FROM raw,
    LATERAL VIEW OUTER EXPLODE(
        from_json(get_json_object(resource_json, '$.extension'),
                  'array<struct<url:string,value:string>>')
    ) AS ext
    WHERE ext.url LIKE '%us-core-race%'
)

SELECT
    -- FHIR resource ID (UUID)
    get_json_object(r.resource_json, '$.id')                    AS fhir_patient_id,

    -- Demographics
    TO_DATE(get_json_object(r.resource_json, '$.birthDate'))    AS birth_date,
    get_json_object(r.resource_json, '$.gender')                AS gender_code,
    get_json_object(r.resource_json,
        '$.address[0].state')                                   AS state,
    get_json_object(r.resource_json,
        '$.address[0].postalCode')                              AS postal_code,

    -- Name (for QA only — do not carry forward to Gold)
    get_json_object(r.resource_json,
        '$.name[0].family')                                     AS family_name,

    -- ABDM identifiers
    i.abha_id,
    i.mrn,

    -- Race (optional; typically absent in Indian ABDM records)
    e.race_code,

    -- Deceased flag
    CASE WHEN get_json_object(r.resource_json,
              '$.deceasedBoolean') = 'true'
         THEN TRUE
         WHEN get_json_object(r.resource_json,
              '$.deceasedDateTime') IS NOT NULL
         THEN TRUE
         ELSE FALSE END                                         AS is_deceased,

    TO_TIMESTAMP(get_json_object(r.resource_json,
        '$.deceasedDateTime'))                                  AS deceased_datetime,

    -- Audit lineage
    r._lineage_id,
    r._source_file,
    r._loaded_at

FROM raw r
LEFT JOIN identifiers i USING (fhir_id)
LEFT JOIN extensions e USING (fhir_id)
