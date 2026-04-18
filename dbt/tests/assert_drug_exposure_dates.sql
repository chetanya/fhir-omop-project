-- dbt/tests/assert_drug_exposure_dates.sql
-- Ensure drug_exposure_start_date <= drug_exposure_end_date (when end_date exists)

SELECT *
FROM {{ ref('omop_drug_exposure') }}
WHERE drug_exposure_end_date IS NOT NULL
  AND drug_exposure_start_date > drug_exposure_end_date

-- dbt test will fail if any rows are returned
