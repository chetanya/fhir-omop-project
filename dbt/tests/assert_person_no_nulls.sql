-- dbt/tests/assert_person_no_nulls.sql
-- Ensure required person fields are not NULL

SELECT *
FROM {{ ref('omop_person') }}
WHERE person_id IS NULL
   OR gender_concept_id IS NULL
   OR year_of_birth IS NULL
   OR person_source_value IS NULL

-- dbt test will fail if any rows are returned
