-- dbt/tests/assert_condition_valid_concepts.sql
-- Ensure condition_concept_id has been mapped (not defaulting to 0)
-- Allows some unmapped conditions (concept_id = 0) but flags if >50% are unmapped

WITH concept_coverage AS (
    SELECT
        COUNT(*) AS total,
        COUNTIF(condition_concept_id > 0) AS mapped,
        COUNTIF(condition_concept_id = 0) AS unmapped,
        ROUND(100.0 * COUNTIF(condition_concept_id > 0) / COUNT(*), 1) AS coverage_pct
    FROM {{ ref('omop_condition_occurrence') }}
)

SELECT *
FROM concept_coverage
WHERE coverage_pct < 50  -- Fail if less than 50% of conditions are mapped

-- dbt test will fail if coverage < 50%
