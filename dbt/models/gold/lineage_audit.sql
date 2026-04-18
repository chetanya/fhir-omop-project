-- models/gold/lineage_audit.sql
-- Lineage tracking view: traces a single FHIR bundle through all OMOP tables
-- Joins all Gold tables on _lineage_id to show data provenance from Bronze to Gold

{{ config(
    materialized='view',
    meta={'source_resource': 'FHIR/*'}
) }}

WITH person_lineage AS (
    SELECT
        'person' AS omop_table,
        _lineage_id,
        person_id AS record_id,
        person_source_value AS fhir_resource_id,
        _source_file,
        _loaded_at,
        _transformed_at
    FROM {{ ref('omop_person') }}
),

condition_lineage AS (
    SELECT
        'condition_occurrence' AS omop_table,
        _lineage_id,
        condition_occurrence_id AS record_id,
        NULL AS fhir_resource_id,
        _source_file,
        _loaded_at,
        _transformed_at
    FROM {{ ref('omop_condition_occurrence') }}
),

drug_lineage AS (
    SELECT
        'drug_exposure' AS omop_table,
        _lineage_id,
        drug_exposure_id AS record_id,
        NULL AS fhir_resource_id,
        _source_file,
        _loaded_at,
        _transformed_at
    FROM {{ ref('omop_drug_exposure') }}
),

visit_lineage AS (
    SELECT
        'visit_occurrence' AS omop_table,
        _lineage_id,
        visit_occurrence_id AS record_id,
        NULL AS fhir_resource_id,
        _source_file,
        _loaded_at,
        _transformed_at
    FROM {{ ref('omop_visit_occurrence') }}
),

measurement_lineage AS (
    SELECT
        'measurement' AS omop_table,
        _lineage_id,
        measurement_id AS record_id,
        NULL AS fhir_resource_id,
        _source_file,
        _loaded_at,
        _transformed_at
    FROM {{ ref('omop_measurement') }}
),

observation_lineage AS (
    SELECT
        'observation' AS omop_table,
        _lineage_id,
        observation_id AS record_id,
        NULL AS fhir_resource_id,
        _source_file,
        _loaded_at,
        _transformed_at
    FROM {{ ref('omop_observation') }}
)

SELECT * FROM person_lineage
UNION ALL
SELECT * FROM condition_lineage
UNION ALL
SELECT * FROM drug_lineage
UNION ALL
SELECT * FROM visit_lineage
UNION ALL
SELECT * FROM measurement_lineage
UNION ALL
SELECT * FROM observation_lineage

-- To trace a bundle: SELECT * FROM lineage_audit WHERE _lineage_id = 'xxx'
-- This shows all OMOP records derived from a single FHIR bundle
