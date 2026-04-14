# Architecture & Project Plan

## Architecture

```mermaid
flowchart TD
    %% ── Data Sources ──────────────────────────────────────────────
    subgraph SRC["Data Sources"]
        SYNTHEA["Synthea\n(1,000 patients · Maharashtra)"]
        ABDM["Real ABDM FHIR\n(ABHA IDs · HFR codes · Consent)"]
        ATHENA["OHDSI Athena\n(OMOP Vocabularies)"]
    end

    %% ── Landing ───────────────────────────────────────────────────
    subgraph LAND["S3 Landing Zone"]
        S3F["s3://…/fhir-landing/\nFHIR R4 JSON Bundles"]
        S3V["s3://…/vocabularies/\nOMOP vocab CSVs"]
    end

    %% ── Bronze ────────────────────────────────────────────────────
    subgraph BRZ["Bronze · Delta Lake"]
        BR["fhir_bronze.raw_bundles\n─────────────────────\nresource_json  STRING\nresource_type  STRING\nfhir_resource_id\n_lineage_id  SHA-256\n_source_file · _loaded_at\n_content_hash"]
    end

    %% ── Silver ────────────────────────────────────────────────────
    subgraph SLV["Silver · Delta Lake  (dbt incremental MERGE)"]
        SP["stg_fhir_patient\nabha_id · mrn\nbirth_date · gender\nis_deceased"]
        SC["stg_fhir_condition\ncondition_source_code\ncondition_start_date\nclinical_status"]
        SM["stg_fhir_medication_request\ndrug_source_code\ndrug_exposure_start_date\ndose_value · doses_per_day"]
        SO["stg_fhir_observation\nvalue_quantity\nvalue_concept_code\nvalue_string · category"]
        SE["stg_fhir_encounter\nencounter_class\nperiod_start · period_end\nhfr_facility_code"]
    end

    %% ── Vocabulary ─────────────────────────────────────────────────
    subgraph VOC["OMOP Vocabulary · Delta Lake"]
        VC["concept\nconcept_relationship\nconcept_ancestor\nvocabulary"]
    end

    %% ── Gold / OMOP ───────────────────────────────────────────────
    subgraph GLD["Gold · OMOP CDM v5.4  (dbt incremental MERGE)"]
        OP["person"]
        OVO["visit_occurrence"]
        OCO["condition_occurrence"]
        ODE["drug_exposure"]
        OM["measurement"]
        OOB["observation"]
    end

    %% ── Quality & Research ─────────────────────────────────────────
    subgraph OUT["Quality & Research"]
        DQD["OHDSI Achilles + DQD\nData Quality Dashboard"]
        ATLAS["OHDSI ATLAS\nCohort definitions"]
        RWE["RWE Studies\nfederated queries"]
    end

    %% ── Flow ───────────────────────────────────────────────────────
    SYNTHEA & ABDM --> S3F
    ATHENA --> S3V

    S3F -- "Auto Loader\n(cloudFiles)" --> BR
    S3V --> VC

    BR -- "Step 3\ndbt silver" --> SP & SC & SM & SO & SE

    SP -- "Step 4" --> OP
    SE -- "Step 8" --> OVO
    SC -- "Step 5" --> OCO
    SM -- "Step 6" --> ODE
    SO -- "Step 7" --> OM & OOB

    VC -. "concept_id\nlookup" .-> GLD

    GLD --> DQD
    GLD --> ATLAS --> RWE
```

---

## FHIR → OMOP Mapping

```mermaid
flowchart LR
    subgraph FHIR["FHIR R4 Resources (Silver)"]
        P["Patient\nabha_id · birth_date\ngender · address"]
        C["Condition\nSNOMED code\nonset / recorded date"]
        MR["MedicationRequest\nRxNorm code\nauthoredOn · dosage"]
        OB["Observation\nLOINC code\nvalue[x] · category"]
        EN["Encounter\nclass (AMB/IMP/EMER)\nperiod · HFR facility"]
    end

    subgraph VOCAB["OMOP Vocabulary"]
        V1["SNOMED → standard concept_id"]
        V2["RxNorm → standard concept_id"]
        V3["LOINC → domain routing\nmeasurement vs observation"]
        V4["Gender / Race concept_id"]
        V5["Visit type concept_id"]
    end

    subgraph OMOP["OMOP CDM v5.4 (Gold)"]
        PR["person\nperson_source_value = ABHA ID\ngender_concept_id\nbirth_datetime"]
        VO["visit_occurrence\nvisit_type_concept_id\nvisit_start / end_date\ncare_site_id → HFR"]
        CO["condition_occurrence\ncondition_concept_id\ncondition_start_date"]
        DE["drug_exposure\ndrug_concept_id\ndrug_exposure_start_date\nquantity · days_supply"]
        ME["measurement\nmeasurement_concept_id\nvalue_as_number / concept_id\nunit_concept_id"]
        OBS["observation\nobservation_concept_id\nvalue_as_string / concept_id"]
    end

    P --> V4 --> PR
    EN --> V5 --> VO
    C --> V1 --> CO
    MR --> V2 --> DE
    OB --> V3
    V3 -- "domain=Measurement" --> ME
    V3 -- "domain=Observation" --> OBS
```

---

## Project Plan

### Phase 1 — Foundations `Steps 1–3`

| Step | Notebook | What it builds | Status |
|------|----------|----------------|--------|
| 1 | `01_synthea_setup.py` | Generate 1,000 synthetic patients (Maharashtra) | ✅ Done |
| 2 | `02_bronze_ingestion.py` | Auto Loader → `fhir_bronze.raw_bundles` (Delta, append-only) | ✅ Done |
| 3 | `03_silver_flattening.py` | Bronze → 5 Silver tables (dbt MERGE on `_lineage_id`) | ✅ Done |

---

### Phase 2 — FHIR → OMOP Mapping `Steps 4–10`

| Step | Notebook | FHIR → OMOP | Key challenge |
|------|----------|-------------|---------------|
| 4 | `04_omop_patient.py` | Patient → `person` | gender/race `concept_id` lookup; ABHA ID as `person_source_value` |
| 5 | `05_omop_condition.py` | Condition → `condition_occurrence` | SNOMED → standard concept; source vs standard concept split |
| 6 | `06_omop_medication.py` | MedicationRequest → `drug_exposure` | RxNorm normalization; `days_supply` derivation |
| 7 | `07_omop_observation.py` | Observation → `measurement` / `observation` | LOINC domain routing; unit concept_id lookup |
| 8 | `08_omop_encounter.py` | Encounter → `visit_occurrence` | visit type concept_id; care_site from HFR code |
| 9 | `09_vocabulary_setup.py` | Load OMOP vocab tables | Athena download; Delta Lake vocab tables |
| 10 | `10_achilles_dqd.py` | Run Achilles + DQD | Automated data quality checks against OMOP CDM |

---

### Phase 3 — India / ABDM Layer `Steps 11–15`

| Step | Notebook | Focus |
|------|----------|-------|
| 11 | `11_abdm_gap_analysis.py` | Diff ABDM FHIR IG v6.5 vs Synthea output; identify missing extensions |
| 12 | `12_abdm_patient_mapping.py` | ABHA ID deduplication; ABDM Consent resource → OMOP observation |
| 13 | *(planned)* | Indian drug dictionary mapping (CDSCO codes → RxNorm/ATC) |
| 14 | *(planned)* | HFR facility registry → OMOP `care_site` table |
| 15 | *(planned)* | ICD-10-CM India variant handling; SNOMED India extension |

---

### Phase 4 — RWD Research Layer `Steps 16–19`

| Step | Focus |
|------|-------|
| 16 | Cohort definitions in ATLAS (diabetes, hypertension, oncology) |
| 17 | Federated OMOP query across multiple hospital sites |
| 18 | RWE study setup: treatment pathways, incidence rates |
| 19 | Export to OHDSI network study format |

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Bronze dedup | None (append-only) | Auto Loader guarantees file-level exactly-once; dedup at Silver |
| Silver merge key | `_lineage_id` = SHA-256(file + resource ID) | Deterministic across re-runs; stable for cross-layer joins |
| FHIR bundle parsing | Python UDF (json.loads) | `from_json` with `resource:string` silently returns nulls — object ≠ string |
| Observation routing | Preserve all `value[x]` in Silver | Gold routes to `measurement` vs `observation` via OMOP domain lookup |
| ABHA ID extraction | `get(filter(...), 0)` not `[0]` | `[0]` throws on empty array; `get()` returns NULL safely |
| ABDM identifiers | `person_source_value` = ABHA ID | Enables patient matching across hospital systems |

---

## Testing Strategy

```
Unit tests (local PySpark, no cluster)   ← pytest tests/ -v
├── test_patient_mapping.py              Patient field extraction + ABHA fallback
├── test_condition_mapping.py            SNOMED extraction + onset date fallback
├── test_medication_mapping.py           RxNorm extraction + dosage parsing
└── test_abdm_extensions.py             ABHA format · HFR codes · Consent profile

Integration tests (Databricks cluster)  ← dbt test
├── assert_person_no_nulls.sql
├── assert_condition_valid_concepts.sql
└── assert_drug_exposure_dates.sql

Data quality (post-load)                ← OHDSI Achilles + DQD
└── Automated CDM conformance checks
```
