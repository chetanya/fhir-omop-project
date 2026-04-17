-- sql/OMOP_CDM_v54_Delta.sql
-- OMOP Common Data Model v5.4 DDL for Delta Lake
-- Adapted for FHIR→OMOP pipeline with Databricks/Delta Lake constraints
-- Reference: https://ohdsi.github.io/CommonDataModel/

-- =============================================================================
-- VOCABULARY TABLES
-- =============================================================================

-- CONCEPT: All concepts (codes) in OMOP, from all vocabularies
-- Downloaded from Athena; includes SNOMED, RxNorm, ICD10CM, LOINC, Gender, Race, etc.
CREATE TABLE IF NOT EXISTS workspace.omop_gold.concept (
    concept_id              BIGINT NOT NULL,
    concept_name            STRING NOT NULL,
    domain_id               STRING,
    vocabulary_id           STRING NOT NULL,
    concept_class_id        STRING,
    standard_concept        STRING,  -- 'S' = standard, NULL = source code
    concept_code            STRING NOT NULL,
    valid_start_date        DATE NOT NULL,
    valid_end_date          DATE NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'comment' = 'OMOP vocabulary: all concepts from all vocabularies (SNOMED, RxNorm, ICD10CM, LOINC, Gender, Race, Visit, etc.)'
);

-- CONCEPT_RELATIONSHIP: Links between concepts (e.g., source→standard mappings)
CREATE TABLE IF NOT EXISTS workspace.omop_gold.concept_relationship (
    concept_id_1            BIGINT NOT NULL,
    concept_id_2            BIGINT NOT NULL,
    relationship_id         STRING NOT NULL,
    valid_start_date        DATE NOT NULL,
    valid_end_date          DATE NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'comment' = 'Relationships between concepts: Maps-to, Has-component, Relates-to, etc.'
);

-- =============================================================================
-- CORE CLINICAL TABLES (Phase 2)
-- =============================================================================

-- PERSON: Demographics and identifiers (one row per patient)
-- Source: FHIR Patient resource
CREATE TABLE IF NOT EXISTS workspace.omop_gold.person (
    person_id               BIGINT NOT NULL,
    gender_concept_id       BIGINT NOT NULL,
    year_of_birth           INT,
    month_of_birth          INT,
    day_of_birth            INT,
    birth_datetime          TIMESTAMP,
    race_concept_id         BIGINT,
    ethnicity_concept_id    BIGINT,
    location_id             BIGINT,
    provider_id             BIGINT,
    care_site_id            BIGINT,
    person_source_value     STRING,  -- FHIR patient ID or ABHA ID
    gender_source_value     STRING,  -- FHIR gender code
    gender_source_concept_id BIGINT,
    race_source_value       STRING,
    race_source_concept_id  BIGINT,
    ethnicity_source_value  STRING,
    ethnicity_source_concept_id BIGINT
)
USING DELTA
PARTITIONED BY (year_of_birth)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'comment' = 'Patient demographics from FHIR Patient resources',
    'delta.target.fileSize' = '134217728'
);

CREATE INDEX IF NOT EXISTS idx_person_id ON workspace.omop_gold.person(person_id);

-- VISIT_OCCURRENCE: Patient visits/encounters (inpatient, outpatient, emergency)
-- Source: FHIR Encounter resource
CREATE TABLE IF NOT EXISTS workspace.omop_gold.visit_occurrence (
    visit_occurrence_id     BIGINT NOT NULL,
    person_id               BIGINT NOT NULL,
    visit_concept_id        BIGINT NOT NULL,
    visit_start_date        DATE NOT NULL,
    visit_start_datetime    TIMESTAMP,
    visit_end_date          DATE,
    visit_end_datetime      TIMESTAMP,
    visit_type_concept_id   BIGINT NOT NULL,  -- 9201=inpatient, 9202=outpatient, 9203=ER
    provider_id             BIGINT,
    care_site_id            BIGINT,
    visit_source_value      STRING,  -- FHIR encounter.class (AMB, IMP, EMER, HH)
    visit_source_concept_id BIGINT,
    admitting_concept_id    BIGINT,
    admitting_source_concept_id BIGINT,
    admitting_source_value  STRING,
    discharge_to_concept_id BIGINT,
    discharge_to_source_value STRING,
    preceding_visit_occurrence_id BIGINT  -- In our case, encounter duration_minutes
)
USING DELTA
PARTITIONED BY (YEAR(visit_start_date))
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'comment' = 'Patient encounters/visits from FHIR Encounter resources',
    'delta.target.fileSize' = '134217728'
);

CREATE INDEX IF NOT EXISTS idx_visit_person ON workspace.omop_gold.visit_occurrence(person_id, visit_start_date);

-- CONDITION_OCCURRENCE: Diagnoses (one row per condition per patient)
-- Source: FHIR Condition resource
CREATE TABLE IF NOT EXISTS workspace.omop_gold.condition_occurrence (
    condition_occurrence_id BIGINT NOT NULL,
    person_id               BIGINT NOT NULL,
    condition_concept_id    BIGINT NOT NULL,  -- SNOMED code
    condition_start_date    DATE NOT NULL,
    condition_start_datetime TIMESTAMP,
    condition_end_date      DATE,
    condition_end_datetime  TIMESTAMP,
    condition_type_concept_id BIGINT NOT NULL,  -- 32882 = EHR encounter diagnosis
    condition_status_source_value STRING,  -- FHIR clinicalStatus (active, inactive, resolved)
    stop_reason             STRING,  -- Why condition was resolved
    provider_id             BIGINT,
    visit_occurrence_id     BIGINT,
    visit_detail_id         BIGINT,
    condition_source_value  STRING,  -- Original FHIR code
    condition_source_concept_id BIGINT
)
USING DELTA
PARTITIONED BY (YEAR(condition_start_date))
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'comment' = 'Patient diagnoses from FHIR Condition resources (SNOMED-mapped)',
    'delta.target.fileSize' = '134217728'
);

CREATE INDEX IF NOT EXISTS idx_condition_person ON workspace.omop_gold.condition_occurrence(person_id, condition_start_date);

-- DRUG_EXPOSURE: Medications (one row per prescription/order)
-- Source: FHIR MedicationRequest resource
CREATE TABLE IF NOT EXISTS workspace.omop_gold.drug_exposure (
    drug_exposure_id        BIGINT NOT NULL,
    person_id               BIGINT NOT NULL,
    drug_concept_id         BIGINT NOT NULL,  -- RxNorm code
    drug_exposure_start_date DATE NOT NULL,
    drug_exposure_start_datetime TIMESTAMP,
    drug_exposure_end_date  DATE,
    drug_exposure_end_datetime TIMESTAMP,
    verbatim_end_date       DATE,
    drug_type_concept_id    BIGINT NOT NULL,  -- 38000177 = Prescription written
    stop_reason             STRING,  -- Why stopped (stopped, cancelled, etc.)
    refills                 INT,
    quantity                DOUBLE,  -- Dose quantity
    days_supply             INT,
    dose_unit_source_value  STRING,  -- FHIR unit (mg, mg/dL, etc.)
    lot_number              STRING,
    provider_id             BIGINT,
    visit_occurrence_id     BIGINT,
    visit_detail_id         BIGINT,
    drug_source_value       STRING,  -- Original FHIR code
    drug_source_concept_id  BIGINT,
    route_concept_id        BIGINT,  -- Administration route (oral, IV, etc.)
    effective_drug_dose     DOUBLE,
    dose_unit_concept_id    BIGINT
)
USING DELTA
PARTITIONED BY (YEAR(drug_exposure_start_date))
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'comment' = 'Medication exposures from FHIR MedicationRequest resources (RxNorm-mapped)',
    'delta.target.fileSize' = '134217728'
);

CREATE INDEX IF NOT EXISTS idx_drug_person ON workspace.omop_gold.drug_exposure(person_id, drug_exposure_start_date);

-- MEASUREMENT: Lab results and vital signs (numeric observations)
-- Source: FHIR Observation resource (valueQuantity)
CREATE TABLE IF NOT EXISTS workspace.omop_gold.measurement (
    measurement_id          BIGINT NOT NULL,
    person_id               BIGINT NOT NULL,
    measurement_concept_id  BIGINT NOT NULL,  -- LOINC/SNOMED code
    measurement_date        DATE NOT NULL,
    measurement_datetime    TIMESTAMP,
    measurement_time        STRING,
    measurement_type_concept_id BIGINT NOT NULL,  -- 38000280 = Lab measurement
    operator_concept_id     BIGINT,
    value_as_number         DOUBLE,  -- Numeric result
    value_as_concept_id     BIGINT,  -- If result was coded
    unit_concept_id         BIGINT,  -- UCUM unit (mg/dL, mmol/L, etc.)
    range_low               DOUBLE,  -- Normal range low
    range_high              DOUBLE,  -- Normal range high
    provider_id             BIGINT,
    visit_occurrence_id     BIGINT,
    visit_detail_id         BIGINT,
    measurement_source_value STRING,  -- Original FHIR code
    measurement_source_concept_id BIGINT,
    unit_source_value       STRING,  -- FHIR unit code
    unit_source_concept_id  BIGINT,
    value_source_value      STRING,
    observation_concept_id  BIGINT
)
USING DELTA
PARTITIONED BY (YEAR(measurement_date))
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'comment' = 'Lab results and vital signs from FHIR Observation (numeric values)',
    'delta.target.fileSize' = '134217728'
);

CREATE INDEX IF NOT EXISTS idx_measurement_person ON workspace.omop_gold.measurement(person_id, measurement_date);

-- OBSERVATION: Other clinical observations (coded and text values)
-- Source: FHIR Observation resource (valueCodeableConcept, valueString)
CREATE TABLE IF NOT EXISTS workspace.omop_gold.observation (
    observation_id          BIGINT NOT NULL,
    person_id               BIGINT NOT NULL,
    observation_concept_id  BIGINT NOT NULL,  -- LOINC/SNOMED code
    observation_date        DATE NOT NULL,
    observation_datetime    TIMESTAMP,
    observation_type_concept_id BIGINT NOT NULL,  -- 38000280 = Lab measurement
    value_as_number         DOUBLE,
    value_as_string         STRING,  -- Text result
    value_as_concept_id     BIGINT,  -- Coded result
    qualifier_concept_id    BIGINT,
    unit_concept_id         BIGINT,
    provider_id             BIGINT,
    visit_occurrence_id     BIGINT,
    visit_detail_id         BIGINT,
    observation_source_value STRING,  -- Original FHIR code
    observation_source_concept_id BIGINT,
    unit_source_value       STRING,
    unit_source_concept_id  BIGINT,
    obs_event_field_concept_id BIGINT,  -- FHIR category (vital-signs, laboratory, etc.)
    value_source_value      STRING
)
USING DELTA
PARTITIONED BY (YEAR(observation_date))
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'comment' = 'Other clinical observations from FHIR (coded/text values)',
    'delta.target.fileSize' = '134217728'
);

CREATE INDEX IF NOT EXISTS idx_observation_person ON workspace.omop_gold.observation(person_id, observation_date);

-- =============================================================================
-- SUPPORTING TABLES
-- =============================================================================

-- PROVIDER: Healthcare providers (physicians, nurses, etc.)
-- Source: FHIR Practitioner resource (not yet mapped in Phase 2)
CREATE TABLE IF NOT EXISTS workspace.omop_gold.provider (
    provider_id             BIGINT NOT NULL,
    provider_name           STRING,
    npi                     STRING,
    dea                     STRING,
    specialty_concept_id    BIGINT,
    care_site_id            BIGINT,
    year_of_birth           INT,
    gender_concept_id       BIGINT,
    provider_source_value   STRING,
    specialty_source_value  STRING,
    specialty_source_concept_id BIGINT,
    gender_source_value     STRING,
    gender_source_concept_id BIGINT
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'comment' = 'Healthcare providers (physicians, nurses, etc.) — not yet mapped from FHIR'
);

-- CARE_SITE: Healthcare facilities (hospitals, clinics, etc.)
-- Source: FHIR Organization resource (referenced via ABDM HFR code)
CREATE TABLE IF NOT EXISTS workspace.omop_gold.care_site (
    care_site_id            BIGINT NOT NULL,
    care_site_name          STRING,
    place_of_service_concept_id BIGINT,
    location_id             BIGINT,
    care_site_source_value  STRING,  -- ABDM HFR facility code
    place_of_service_source_value STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'comment' = 'Healthcare facilities/organizations (hospitals, clinics, etc.)'
);

-- LOCATION: Physical addresses and geographic info
-- Source: FHIR Patient address, Practitioner address, Organization address
CREATE TABLE IF NOT EXISTS workspace.omop_gold.location (
    location_id             BIGINT NOT NULL,
    address_1               STRING,
    address_2               STRING,
    city                    STRING,
    state                   STRING,
    zip                     STRING,
    county                  STRING,
    country_concept_id      BIGINT,
    latitude                DOUBLE,
    longitude               DOUBLE,
    country_source_value    STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'comment' = 'Geographic locations from patient, provider, and facility addresses'
);

-- =============================================================================
-- NOTES
-- =============================================================================
--
-- This DDL defines the core OMOP CDM v5.4 tables used in the FHIR→OMOP pipeline.
--
-- Phase 2 Focus: person, visit_occurrence, condition_occurrence, drug_exposure,
--                measurement, observation
--
-- Partitioning Strategy:
-- - All date-partitioned tables use the primary date column (e.g., visit_start_date)
-- - Partitions help with range queries and query optimization
-- - ZORDER on (person_id, date_column) in dbt config for better clustering
--
-- Indexes:
-- - Created on foreign keys (person_id) for join performance
-- - Include date columns for range query optimization
--
-- Key Mappings (Phase 2):
-- - FHIR Patient → person (gender, race, birth date)
-- - FHIR Encounter → visit_occurrence (class, dates, facility code)
-- - FHIR Condition → condition_occurrence (SNOMED code, dates, status)
-- - FHIR MedicationRequest → drug_exposure (RxNorm code, dosage)
-- - FHIR Observation (valueQuantity) → measurement (numeric lab/vital)
-- - FHIR Observation (valueCodeableConcept/valueString) → observation (coded/text)
--
-- Concept Lookups:
-- - All concept_id columns are looked up from workspace.omop_gold.concept
-- - concept_code from source (SNOMED, RxNorm, LOINC, etc.) matches to concept_id
-- - Defaults to 0 when concept lookup fails (vocabulary not yet loaded)
--
-- ABDM-Specific Fields:
-- - person.person_source_value: ABHA ID (format: XX-XXXX-XXXX-XXXX)
-- - visit_occurrence.visit_source_value: HFR facility code
-- - care_site.care_site_source_value: ABDM Health Facility Registry ID
--
