# Phase 2 Summary: OMOP Gold Layer

**Status:** ⏳ In Progress (dbt build running)
**Dates:** 2026-04-16 to 2026-04-17
**Deliverables:** Gold OMOP tables with vocabulary-mapped concept_ids

---

## What We Built

### Gold dbt Models (5 OMOP Tables)

1. **omop_person** (from FHIR Patient)
   - Gender, race, birth date extraction
   - ABHA ID as person_source_value (when present)
   - ~1,000 rows expected

2. **omop_condition_occurrence** (from FHIR Condition)
   - SNOMED code → condition_concept_id (349K mapped)
   - Clinical status, onset date logic
   - ~5,000 rows expected

3. **omop_drug_exposure** (from FHIR MedicationRequest)
   - RxNorm code → drug_concept_id (157K + 1.86M extension)
   - Dosage, frequency, stop reason
   - ~3,000 rows expected

4. **omop_visit_occurrence** (from FHIR Encounter)
   - Encounter class (AMB/IMP/EMER/HH) → visit_type_concept_id
   - ABDM HFR facility code preservation
   - ~1,000 rows expected

5. **omop_measurement** (from FHIR Observation with valueQuantity)
   - LOINC code → measurement_concept_id (119K mapped)
   - Numeric values, units, reference ranges
   - ~10,000 rows expected

6. **omop_observation** (from FHIR Observation with valueCodeableConcept/valueString)
   - LOINC/SNOMED code → observation_concept_id
   - Coded and text results
   - ~5,000 rows expected

### Vocabulary Tables

- **workspace.omop_gold.concept** (2.65M rows)
  - RxNorm Extension: 1.86M concepts
  - SNOMED: 349K concepts
  - RxNorm: 157K concepts
  - LOINC: 119K concepts
  - Gender, Race, Visit types, etc.

- **workspace.omop_gold.concept_relationship** (35M rows)
  - Concept mappings and hierarchies

---

## Key Technical Decisions

| Decision | Rationale | Trade-off |
|----------|-----------|-----------|
| Explicit schemas for vocabulary CSVs | Prevent Delta merge field errors (concept_id BIGINT vs STRING) | More verbose code |
| dbt timeout: 3600s, threads: 2 | SQL Warehouse sessions timeout at 15 min; reduce load | Slower execution per thread |
| Local dbt execution | Better error messages, faster iteration | Requires local dbt-databricks install |
| Hard-coded visit type mappings | FHIR class codes map directly; no lookup needed | Less flexible if new visit types added |
| Incremental MERGE strategy | Idempotent re-runs; only process new/changed rows | More complex than INSERT |

---

## Challenges Encountered

### Challenge 1: Vocabulary File Upload Size Limits
**Problem:** 3.6 GB of OMOP vocabularies couldn't be uploaded via CLI or workspace API.
**Solution:** Used Databricks Volumes (new feature) for large file storage.
**Learning:** Plan for large data ingestion early; Volumes > DBFS for large files.

### Challenge 2: Schema Inference Errors
**Problem:** dbt's auto-inferred schema for CONCEPT.csv caused merge field conflicts.
**Error:** `[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'concept_id'`
**Solution:** Define explicit Spark StructType schemas; concept_id as BIGINT.
**Learning:** Always be explicit with schemas for large data operations.

### Challenge 3: dbt Timeout During Materialization
**Problem:** First `dbt build` timed out after 33 minutes with warehouse session expiry.
**Error:** `Retry request would exceed Retry policy max retry duration of 900.0 seconds`
**Solution:** Increase timeout to 3600s and reduce thread concurrency to 2.
**Learning:** Warehouse sessions have idle timeouts; config matters for long operations.

### Challenge 4: Databricks Repos Clone Failures
**Problem:** CLI couldn't clone GitHub repo into Databricks Repos.
**Solution:** Dropped this approach; run dbt locally instead.
**Learning:** Local dbt execution is simpler for development; move to Repos in prod.

---

## What Worked Well

✅ **Explicit dbt models:** Each FHIR resource → one OMOP table. Clear and maintainable.

✅ **Silver staging layer:** Extracts all FHIR variants; Gold routes to OMOP. Flexible.

✅ **Surrogate key generation:** `dbt_utils.generate_surrogate_key()` provides idempotent IDs.

✅ **Vocabulary lookup pattern:** `LEFT JOIN concept` with defaults to 0 is clean and safe.

✅ **ABDM-specific fields:** ABHA ID, HFR code preserved end-to-end for traceability.

✅ **Learning log:** Step-by-step documentation of gotchas and insights.

---

## Next Steps (Phase 3)

1. **Complete dbt build** (dbt build currently running)
2. **Validate data quality** (Achilles, DQD checks)
3. **ABDM gap analysis** (compare ABDM profiles to R4 standard)
4. **Prepare for Phase 3:** ABHA ID integration, real ABDM data testing

---

## Vocabulary Coverage

| Vocabulary | Concepts | Standard | Usage |
|-----------|----------|----------|-------|
| RxNorm Extension | 1.86M | 100% | Drug codes (Synthea + real) |
| SNOMED | 349K | 100% | Conditions, observations |
| RxNorm | 157K | 100% | Drug names (older codes) |
| LOINC | 119K | 100% | Lab tests, measurements |
| Gender | 5 | 40% | Patient demographics |
| Race | 1,409 | 99% | Patient demographics |
| Visit | 20 | 100% | Visit type mapping |

---

## Performance Notes

- **Synthea dataset:** 1,000 patients generated
- **Expected throughput:** 1,000 person rows, 5,000+ conditions, 10,000+ measurements
- **dbt materialization time:** ~30-40 min (first run, includes vocabulary lookups)
- **Subsequent runs:** 5-10 min (incremental only)
- **Recommended:** Run dbt locally with 2 threads for dev; scale to 4+ threads in prod

---

## Files Modified

- `notebooks/09_vocabulary_setup.py` — Load concept tables from Athena CSVs
- `notebooks/10_dbt_build.py` — Materialize gold tables
- `dbt/models/gold/omop_*.sql` — 5 gold table definitions
- `dbt/dbt_project.yml` — Updated variable definitions
- `~/.dbt/profiles.yml` — Databricks connection config
- `docs/learning_log.md` — Steps 9-10 documented
- `docs/mapping_decisions.md` — MD-006 added (dbt timeout logic)
- `docs/omop_schema_guide.md` — Comprehensive FHIR→OMOP mapping reference
- `sql/OMOP_CDM_v54_Delta.sql` — OMOP DDL for reference

---

## Lessons for Phase 3 + Production

1. **Vocabulary is essential:** Can't skip vocabulary setup; impacts all concept mapping.
2. **Explicit schemas:** Always define; inference causes subtle errors.
3. **Timeout matters:** Plan session/query timeouts early in dbt config.
4. **Local dev faster:** dbt locally beats Databricks notebooks for iteration.
5. **ABDM readiness:** Patterns for ABHA ID + HFR code built in; real data will flow cleanly.
