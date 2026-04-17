# OMOP Schema Guide

This document maps OMOP CDM tables to FHIR resources and the dbt gold models that transform them.

## Quick Reference: FHIR → OMOP Mapping

| FHIR Resource | OMOP Table | dbt Model | Key Columns |
|---------------|-----------|-----------|------------|
| Patient | person | omop_person.sql | person_id, gender_concept_id, year_of_birth, person_source_value (ABHA ID) |
| Encounter | visit_occurrence | omop_visit_occurrence.sql | visit_occurrence_id, person_id, visit_type_concept_id (9201/9202/9203) |
| Condition | condition_occurrence | omop_condition_occurrence.sql | condition_occurrence_id, person_id, condition_concept_id (SNOMED) |
| MedicationRequest | drug_exposure | omop_drug_exposure.sql | drug_exposure_id, person_id, drug_concept_id (RxNorm) |
| Observation (valueQuantity) | measurement | omop_measurement.sql | measurement_id, person_id, measurement_concept_id (LOINC/SNOMED) |
| Observation (valueCodeableConcept/valueString) | observation | omop_observation.sql | observation_id, person_id, observation_concept_id (LOINC/SNOMED) |

---

## Table Details

### PERSON
**Source:** FHIR Patient

| OMOP Column | FHIR Field | Type | Notes |
|------------|-----------|------|-------|
| person_id | (generated) | BIGINT | Hash of patient resource ID (dbt_utils.generate_surrogate_key) |
| gender_concept_id | gender | BIGINT | Lookup: gender_code → Gender vocabulary concept_id |
| year_of_birth | birthDate | INT | Extracted from YYYY-MM-DD |
| race_concept_id | (extension) | BIGINT | FHIR US Core race extension; often NULL in ABDM profiles |
| ethnicity_concept_id | (not mapped) | BIGINT | Not in FHIR; defaults to 0 |
| person_source_value | identifier | STRING | ABHA ID if present; falls back to patient resource ID |

**dbt Model:** `omop_person.sql`
- References: `stg_fhir_patient` (Silver)
- Concept lookups: Gender vocabulary
- Filters: incremental on `_loaded_at`

---

### VISIT_OCCURRENCE
**Source:** FHIR Encounter

| OMOP Column | FHIR Field | Type | Notes |
|------------|-----------|------|-------|
| visit_occurrence_id | (generated) | BIGINT | Hash of encounter resource ID |
| person_id | subject.reference | BIGINT | Foreign key to person |
| visit_type_concept_id | class.code | BIGINT | Hard-coded mapping: AMB→9202, IMP→9201, EMER→9203, HH→9204 |
| visit_start_date | period.start | DATE | Cast from TIMESTAMP |
| visit_start_datetime | period.start | TIMESTAMP | Preserve time component |
| visit_end_date | period.end | DATE | Cast from TIMESTAMP; NULL for in-progress |
| visit_end_datetime | period.end | TIMESTAMP | Preserve time component |
| visit_source_value | class.code | STRING | FHIR encounter class (AMB, IMP, EMER, etc.) |
| preceding_visit_occurrence_id | (calculated) | BIGINT | In our pipeline, stores duration_minutes |

**dbt Model:** `omop_visit_occurrence.sql`
- References: `stg_fhir_encounter` (Silver), `omop_person` (Gold)
- No concept lookups (visit types hard-coded)
- ABDM note: `hfr_facility_code` from serviceProvider.identifier (system: https://facility.ndhm.gov.in)

---

### CONDITION_OCCURRENCE
**Source:** FHIR Condition

| OMOP Column | FHIR Field | Type | Notes |
|------------|-----------|------|-------|
| condition_occurrence_id | (generated) | BIGINT | Hash of condition resource ID |
| person_id | subject.reference | BIGINT | Foreign key to person |
| condition_concept_id | code.coding[0].code | BIGINT | Lookup: SNOMED code → OMOP concept_id |
| condition_start_date | onsetDateTime / recordedDate | DATE | Prefer onsetDateTime; fall back to recordedDate |
| condition_type_concept_id | (fixed) | BIGINT | 32882 = EHR encounter diagnosis |
| condition_status_source_value | clinicalStatus.coding[0].code | STRING | FHIR status (active, inactive, resolved) |
| stop_reason | (inferred) | STRING | "Resolved" if clinicalStatus = "resolved"; else NULL |
| condition_source_value | code.coding[0].code | STRING | SNOMED code as string |
| condition_source_vocabulary | code.coding[0].system | STRING | Code system (e.g., http://snomed.info/sct) |

**dbt Model:** `omop_condition_occurrence.sql`
- References: `stg_fhir_condition` (Silver), `omop_person` (Gold)
- Concept lookups: SNOMED vocabulary
- Filters: incremental on `_loaded_at`

---

### DRUG_EXPOSURE
**Source:** FHIR MedicationRequest

| OMOP Column | FHIR Field | Type | Notes |
|------------|-----------|------|-------|
| drug_exposure_id | (generated) | BIGINT | Hash of medication request resource ID |
| person_id | subject.reference | BIGINT | Foreign key to person |
| drug_concept_id | medicationCodeableConcept.coding[0].code | BIGINT | Lookup: RxNorm code → OMOP concept_id |
| drug_exposure_start_date | authoredOn | DATE | Prescription date |
| drug_type_concept_id | (fixed) | BIGINT | 38000177 = Prescription written |
| quantity | dosageInstruction[0].doseAndRate[0].doseQuantity.value | DOUBLE | Dose amount |
| dose_unit_source_value | dosageInstruction[0].doseAndRate[0].doseQuantity.unit | STRING | Dose unit (mg, mg/dL, etc.) |
| doses_per_day | (calculated) | DOUBLE | frequency / period (when period unit = 'd') |
| stop_reason | (inferred) | STRING | "Stopped" if status=stopped; "Cancelled" if status=cancelled; else NULL |
| drug_source_value | medicationCodeableConcept.coding[0].code | STRING | Original code as string |
| drug_source_vocabulary | medicationCodeableConcept.coding[0].system | STRING | Code system (http://www.nlm.nih.gov/research/umls/rxnorm for Synthea) |

**dbt Model:** `omop_drug_exposure.sql`
- References: `stg_fhir_medication_request` (Silver), `omop_person` (Gold)
- Concept lookups: RxNorm, RxNorm Extension vocabularies
- ABDM note: Real ABDM data may use Indian drug names with custom vocabulary

---

### MEASUREMENT
**Source:** FHIR Observation (numeric values)

| OMOP Column | FHIR Field | Type | Notes |
|------------|-----------|------|-------|
| measurement_id | (generated) | BIGINT | Hash of observation resource ID |
| person_id | subject.reference | BIGINT | Foreign key to person |
| measurement_concept_id | code.coding[0].code | BIGINT | Lookup: LOINC/SNOMED code → OMOP concept_id |
| measurement_date | effectiveDateTime | DATE | Observation effective date |
| measurement_datetime | effectiveDateTime | TIMESTAMP | Preserve time component |
| measurement_type_concept_id | (fixed) | BIGINT | 38000280 = Lab measurement |
| value_as_number | valueQuantity.value | DOUBLE | Numeric result |
| unit_concept_id | valueQuantity.unit | BIGINT | UCUM unit code (mg/dL, mmol/L, etc.) |
| range_low | referenceRange[0].low.value | DOUBLE | Normal range low |
| range_high | referenceRange[0].high.value | DOUBLE | Normal range high |
| visit_occurrence_id | encounter.reference | BIGINT | Foreign key to visit (if present) |
| observation_source_value | code.coding[0].code | STRING | Original LOINC/SNOMED code |

**dbt Model:** `omop_measurement.sql`
- References: `stg_fhir_observation` (Silver, WHERE value_quantity IS NOT NULL), `omop_person`, `omop_visit_occurrence`
- Concept lookups: LOINC, SNOMED vocabularies
- Filters: incremental on `_loaded_at`, value_quantity NOT NULL

---

### OBSERVATION
**Source:** FHIR Observation (coded and text values)

| OMOP Column | FHIR Field | Type | Notes |
|------------|-----------|------|-------|
| observation_id | (generated) | BIGINT | Hash of observation resource ID |
| person_id | subject.reference | BIGINT | Foreign key to person |
| observation_concept_id | code.coding[0].code | BIGINT | Lookup: LOINC/SNOMED code → OMOP concept_id |
| observation_date | effectiveDateTime | DATE | Observation effective date |
| observation_datetime | effectiveDateTime | TIMESTAMP | Preserve time component |
| observation_type_concept_id | (fixed) | BIGINT | 38000280 = Lab measurement |
| value_as_number | (null for coded) | DOUBLE | NULL for coded/text observations |
| value_as_string | valueString | STRING | Free-text result |
| value_as_concept_id | valueCodeableConcept.coding[0].code | BIGINT | Coded result (e.g., positive/negative) |
| obs_event_field_concept_id | category[0].coding[0].code | BIGINT | Observation category (vital-signs, laboratory, social-history, etc.) |
| visit_occurrence_id | encounter.reference | BIGINT | Foreign key to visit (if present) |
| observation_source_value | code.coding[0].code | STRING | Original LOINC/SNOMED code |

**dbt Model:** `omop_observation.sql`
- References: `stg_fhir_observation` (Silver, WHERE value_quantity IS NULL OR value_string IS NOT NULL OR value_concept_code IS NOT NULL), `omop_person`, `omop_visit_occurrence`
- Concept lookups: LOINC, SNOMED vocabularies (both for observation_concept_id and value_as_concept_id)
- Filters: incremental on `_loaded_at`, routes out numeric observations

---

## Concept Mapping Strategy

All OMOP tables use concept lookups to map FHIR codes to OMOP standard concepts:

```sql
-- Pattern used in dbt gold models
LEFT JOIN concept c
    ON source.fhir_code = c.concept_code
    WHERE c.vocabulary_id = 'SNOMED'
      AND c.standard_concept = 'S'
```

**Vocabulary Requirements:**
| FHIR Concept | OMOP Vocabulary | Used In | Notes |
|-------------|-----------------|---------|-------|
| Patient gender | Gender | person | male → 8507, female → 8532 |
| Patient race | Race | person | Often NULL in ABDM profiles |
| Condition code | SNOMED | condition_occurrence | e.g., 73211009 = Diabetes |
| Medication | RxNorm / RxNorm Extension | drug_exposure | Synthea uses RxNorm; ABDM may use Indian brand names |
| Observation code | LOINC, SNOMED | measurement, observation | LOINC for lab tests; SNOMED for clinical findings |
| Visit type | (hard-coded) | visit_occurrence | AMB=9202, IMP=9201, EMER=9203, HH=9204 |

---

## Concept ID Defaults

When vocabularies are **not yet loaded**, all concept lookups **default to 0**:

```sql
COALESCE(c.condition_concept_id, 0) AS condition_concept_id
```

This allows dbt models to run and produce data, but the concept_id fields will be 0. Once vocabularies are loaded via `09_vocabulary_setup.py`, re-running the dbt models will produce real concept_ids.

---

## Column Naming Conventions

- `*_concept_id`: Foreign key to concept table (OMOP standard concept)
- `*_source_value`: Original code/string from source system (FHIR)
- `*_source_concept_id`: Source vocabulary concept_id (if different from standard)
- `*_date`: Date only (YYYY-MM-DD)
- `*_datetime`: Date and time (TIMESTAMP)

---

## Partitioning and Performance

All clinical tables are partitioned by year of the primary date column:

| Table | Partition Column | Example |
|-------|-----------------|---------|
| visit_occurrence | YEAR(visit_start_date) | 2025, 2026, etc. |
| condition_occurrence | YEAR(condition_start_date) | 2025, 2026, etc. |
| drug_exposure | YEAR(drug_exposure_start_date) | 2025, 2026, etc. |
| measurement | YEAR(measurement_date) | 2025, 2026, etc. |
| observation | YEAR(observation_date) | 2025, 2026, etc. |

Partitioning enables:
- **Faster range queries:** "All conditions in 2025"
- **Parallel writes:** MERGE on partitioned tables is more efficient
- **Easier data governance:** Can archive or delete old years

ZORDER clustering on `(person_id, date_column)` further optimizes joins and range scans.
