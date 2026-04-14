# FHIR → OMOP Learning Project
# CLAUDE.md — Claude Code Project Instructions

## Project Purpose
This project teaches FHIR R4 → OMOP CDM transformation on a Databricks lakehouse,
with a specific focus on Indian ABDM FHIR profiles. It is a hands-on learning
environment, not a production codebase — prioritize clear, well-commented code
over production optimizations.

---

## Directory Structure

```
fhir-omop-project/
├── CLAUDE.md                        ← You are here (Claude Code reads this)
├── CLAUDE_PROJECT_SYSTEM_PROMPT.md  ← Paste into Claude.ai Project instructions
├── README.md                        ← Human-readable project guide
│
├── data/
│   ├── synthea/                     ← Synthea-generated FHIR bundles (gitignored)
│   │   └── fhir/                    ← Raw FHIR R4 JSON bundles
│   ├── vocabularies/                ← OMOP vocabulary CSVs from Athena (gitignored)
│   └── samples/                     ← Small curated samples for unit tests
│       ├── patient_bundle.json
│       ├── condition_bundle.json
│       └── medication_bundle.json
│
├── notebooks/                       ← Databricks notebooks (Python)
│   ├── 01_synthea_setup.py          ← Phase 1: Generate synthetic FHIR data
│   ├── 02_bronze_ingestion.py       ← Phase 1: Land FHIR JSON → Bronze Delta
│   ├── 03_silver_flattening.py      ← Phase 1: Flatten FHIR resources → Silver
│   ├── 04_omop_patient.py           ← Phase 2: Patient → person
│   ├── 05_omop_condition.py         ← Phase 2: Condition → condition_occurrence
│   ├── 06_omop_medication.py        ← Phase 2: MedicationRequest → drug_exposure
│   ├── 07_omop_observation.py       ← Phase 2: Observation → measurement
│   ├── 08_omop_encounter.py         ← Phase 2: Encounter → visit_occurrence
│   ├── 09_vocabulary_setup.py       ← Phase 2: OMOP concept tables
│   ├── 10_achilles_dqd.py           ← Phase 2: Data quality checks
│   ├── 11_abdm_gap_analysis.py      ← Phase 3: ABDM vs R4 diff
│   └── 12_abdm_patient_mapping.py   ← Phase 3: ABHA ID integration
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example         ← Databricks connection template
│   ├── models/
│   │   ├── bronze/
│   │   │   └── sources.yml          ← Bronze Delta table sources
│   │   ├── silver/
│   │   │   ├── stg_fhir_patient.sql
│   │   │   ├── stg_fhir_condition.sql
│   │   │   ├── stg_fhir_medication_request.sql
│   │   │   ├── stg_fhir_observation.sql
│   │   │   └── stg_fhir_encounter.sql
│   │   └── gold/
│   │       ├── omop_person.sql
│   │       ├── omop_condition_occurrence.sql
│   │       ├── omop_drug_exposure.sql
│   │       ├── omop_measurement.sql
│   │       ├── omop_observation.sql
│   │       └── omop_visit_occurrence.sql
│   └── tests/
│       ├── assert_person_no_nulls.sql
│       ├── assert_condition_valid_concepts.sql
│       └── assert_drug_exposure_dates.sql
│
├── sql/
│   └── omop_cdm_ddl/
│       ├── OMOP_CDM_v54_Delta.sql   ← DDL adapted for Delta Lake
│       └── vocabulary_tables.sql
│
├── tests/
│   ├── conftest.py                  ← Pytest fixtures with Synthea sample data
│   ├── test_patient_mapping.py
│   ├── test_condition_mapping.py
│   ├── test_medication_mapping.py
│   └── test_abdm_extensions.py      ← ABDM-specific field tests
│
├── docs/
│   ├── mapping_decisions.md         ← Running log of FHIR→OMOP mapping choices
│   ├── abdm_gap_analysis.md         ← ABDM vs R4 diff (auto-generated)
│   └── learning_log.md              ← Your notes per step
│
└── scripts/
    ├── generate_synthea.sh          ← Wrapper to run Synthea with config
    ├── download_vocabularies.sh     ← Athena vocabulary download helper
    └── validate_fhir.sh             ← HAPI FHIR validator wrapper
```

---

## Stack

| Layer | Tool |
|-------|------|
| Compute | Databricks on AWS |
| Storage | Delta Lake on S3 |
| Catalog | Unity Catalog |
| Orchestration | Airflow (MWAA) |
| Transformation | dbt (Silver→Gold) + PySpark (Bronze) |
| FHIR parsing | dbignite |
| Synthetic data | Synthea |
| Vocabulary | OHDSI Athena |
| Data quality | OHDSI Achilles + DQD |
| Testing | pytest + pyspark local mode |

---

## Naming Conventions

### Unity Catalog
```
{catalog}.{schema}.{table}

Bronze:  health_lh.fhir_bronze.raw_bundles
Silver:  health_lh.fhir_silver.stg_fhir_patient
Gold:    health_lh.omop_gold.person
         health_lh.omop_gold.condition_occurrence
         health_lh.omop_gold.drug_exposure
```

### Delta Table Standards
- All tables include: `_loaded_at TIMESTAMP`, `_source_file STRING`, `_lineage_id STRING`
- ZORDER on: `person_id`, `visit_occurrence_id`, date columns for range queries
- Partitioning: Gold tables partitioned by year of primary date column

### dbt Model Config Template
```sql
{{ config(
    materialized='incremental',
    unique_key='[omop_table]_id',
    file_format='delta',
    incremental_strategy='merge',
    partition_by={'field': '[date_col]_year', 'data_type': 'int'},
    zorderby=['person_id', '[date_col]']
) }}
```

---

## Key FHIR → OMOP Mapping Reference

| FHIR Resource | OMOP Table | Key Mapping Challenge |
|---------------|------------|----------------------|
| Patient | person | gender/race concept_id lookup; ABHA ID as person_source_value |
| Condition | condition_occurrence | SNOMED → standard concept; onset date logic |
| MedicationRequest | drug_exposure | RxNorm normalization; Indian brand names |
| Observation | measurement OR observation | domain routing via concept vocabulary |
| Encounter | visit_occurrence | visit_type_concept_id from encounter class |
| Procedure | procedure_occurrence | SNOMED/CPT concept mapping |
| DiagnosticReport | note_nlp (unstructured) | Requires NLP pipeline |

---

## ABDM-Specific Extensions to Watch

```json
// ABHA ID — appears as identifier with this system URL
{
  "system": "https://healthid.ndhm.gov.in",
  "value": "91-1234-5678-9012"
}

// Health Facility Registry code
{
  "system": "https://facility.ndhm.gov.in",
  "value": "HFR_FACILITY_ID"
}

// Consent artifact reference (ABDM-specific resource)
// ResourceType: Consent with ABDM-specific profile URL
"meta": {
  "profile": ["https://nrces.in/ndhm/fhir/r4/StructureDefinition/Consent"]
}
```

---

## Open Source Dependencies

```bash
# FHIR tooling
pip install fhir.resources        # Python FHIR R4 resource models
pip install fhirclient             # FHIR REST client

# OMOP tooling
pip install pyomop                 # Python OMOP CDM wrapper

# Databricks / Delta
pip install databricks-sdk
pip install delta-spark

# Testing
pip install pytest pyspark pytest-spark

# Install dbignite (Databricks-native, install in cluster)
# https://github.com/databricks-industry-solutions (search dbignite)
```

---

## Getting Started Checklist

```bash
# 1. Clone Synthea and generate test population
git clone https://github.com/synthetichealth/synthea
cd synthea
./run_synthea -p 1000 "Maharashtra" --exporter.fhir.export=true

# 2. Clone Databricks OMOP accelerator for reference
git clone https://github.com/databricks-industry-solutions/omop-cdm

# 3. Clone FHIR-to-OMOP Cookbook
git clone https://github.com/CodeX-HL7-FHIR-Accelerator/fhir2omop-cookbook

# 4. Download OMOP vocabularies from Athena
# https://athena.ohdsi.org → select vocabularies:
# SNOMED, ICD10CM, RxNorm, RxNorm Extension, LOINC, Visit, Gender, Race

# 5. Set up HAPI FHIR server locally for validation
docker pull hapiproject/hapi:latest
docker run -p 8080:8080 hapiproject/hapi:latest
```

---

## How to Use This Project with Claude Code

Claude Code reads CLAUDE.md automatically. When working in this project:

**To get a mapping generated:**
```
"Generate the dbt model for FHIR Condition → OMOP condition_occurrence"
```

**To check your progress:**
```
"status"
```

**To get an ABDM gap analysis:**
```
"What changes do I need for ABDM profiles in the Patient mapping?"
```

**To debug a pipeline error:**
Paste the error and Claude Code will diagnose, fix, and explain.

**To generate a new test:**
```
"Write a pytest for the MedicationRequest → drug_exposure mapping"
```

---

## Learning Log Convention

After completing each step, append to docs/learning_log.md:

```markdown
## Step N — [Title]
**Date:** YYYY-MM-DD
**Key insight:** [one sentence]
**Gotcha:** [one thing that tripped you up]
**ABDM note:** [any India-specific consideration]
```
