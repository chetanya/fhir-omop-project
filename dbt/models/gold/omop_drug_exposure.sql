-- models/gold/omop_drug_exposure.sql
-- FHIR MedicationRequest → OMOP drug_exposure
-- Mapping: RxNorm (US) or Indian drug codes to OMOP drug_concept_id

{{ config(
    materialized='incremental',
    unique_key='drug_exposure_id',
    file_format='delta',
    incremental_strategy='merge',
    partition_by={'field': 'year_start', 'data_type': 'int'},
    zorderby=['person_id', 'drug_exposure_start_date'],
    meta={'source_resource': 'FHIR/MedicationRequest'}
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_fhir_medication_request') }}
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),

-- Join to person to get person_id
person AS (
    SELECT person_id, person_source_value
    FROM {{ ref('omop_person') }}
),

-- Vocabulary lookup: RxNorm (or Indian drug code) → OMOP drug_concept_id
-- Indian brand names use custom vocabulary; Synthea uses RxNorm
-- When vocabularies not yet loaded, defaults to 0
concept_lookup AS (
    SELECT
        concept_code,
        concept_id AS drug_concept_id,
        domain_id
    FROM {{ var('catalog') }}.{{ var('gold_schema') }}.concept
    WHERE vocabulary_id IN ('RxNorm', 'RxNorm Extension')
      AND standard_concept = 'S'
    UNION ALL
    SELECT concept_code, 0, 'Drug'
    LIMIT 1  -- Ensures 0 maps exist even with empty concept table
)

SELECT
    -- OMOP surrogate key: hash of FHIR medication request ID for idempotency
    {{ dbt_utils.generate_surrogate_key(['fhir_medication_request_id']) }} AS drug_exposure_id,

    -- Foreign key to person
    p.person_id,

    -- RxNorm (or Indian drug) code → OMOP drug concept (domain must be 'Drug')
    COALESCE(c.drug_concept_id, 0)                               AS drug_concept_id,

    -- Dates: drug_exposure_start_date from authoredOn (prescription date)
    -- drug_exposure_end_date inferred from status (stopped/cancelled means NULL end)
    drug_exposure_start_date,
    CASE
        WHEN status = 'stopped' OR status = 'cancelled' THEN drug_exposure_start_date
        ELSE NULL
    END AS drug_exposure_end_date,

    -- Type: 38000177 = Prescription written
    38000177 AS drug_type_concept_id,

    -- Dosage information
    dose_value                                                    AS quantity,
    dose_unit,
    NULL                                                         AS days_supply,

    -- Route: typically oral in FHIR (would be in dosageInstruction.route, not extracted here)
    NULL AS route_concept_id,

    -- Effective dose: doses per day * number of days (crude estimate)
    doses_per_day                                                AS effective_drug_dose,
    dose_unit                                                    AS dose_unit_source_value,

    -- Medication source: preserve original FHIR codes for traceability
    drug_source_code,
    drug_source_vocabulary,
    drug_source_display,
    0 AS drug_source_concept_id,

    -- Stop reason: why medication was stopped (inferred from status)
    CASE
        WHEN status = 'stopped' THEN 'Stopped'
        WHEN status = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS stop_reason,

    -- Refills and lot number: not typically in FHIR MedicationRequest
    NULL AS refills,
    NULL AS lot_number,

    -- Visit and provider: not mapped from MedicationRequest (null for now)
    NULL AS visit_occurrence_id,
    NULL AS visit_detail_id,
    NULL AS provider_id,

    -- Audit columns
    _lineage_id,
    _source_file,
    _loaded_at,
    CURRENT_TIMESTAMP() AS _transformed_at,

    -- Partition column
    YEAR(drug_exposure_start_date) AS year_start

FROM source s
LEFT JOIN person p
    ON s.fhir_patient_id = p.person_source_value
LEFT JOIN concept_lookup c
    ON s.drug_source_code = c.concept_code
    AND c.domain_id = 'Drug'
