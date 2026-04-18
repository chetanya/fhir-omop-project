# Phase 2 Summary: OMOP Gold Layer

**Status:** ✅ Complete
**Dates:** 2026-04-16 to 2026-04-17
**Deliverables:** Gold OMOP tables with vocabulary-mapped concept_ids, 939,949 total rows

---

## What We Built

### Gold Tables (6 OMOP Tables via `notebooks/10_gold_build.py`)

Gold is materialised by direct `spark.sql()` CTAS statements instead of dbt — see MD-006
in `mapping_decisions.md` for why dbt was replaced.

1. **omop_person** (from FHIR Patient)
   - Gender, race, birth date extraction
   - ABHA ID as person_source_value (when present, else FHIR resource ID)
   - ~1,000 rows

2. **omop_condition_occurrence** (from FHIR Condition)
   - SNOMED code → condition_concept_id (349K mapped)
   - Clinical status, onset date logic, stop_reason for resolved conditions
   - ~5,000 rows

3. **omop_drug_exposure** (from FHIR MedicationRequest)
   - RxNorm code → drug_concept_id (157K + 1.86M extension)
   - Dosage, frequency, stop_reason for stopped/cancelled scripts
   - ~3,000 rows

4. **omop_visit_occurrence** (from FHIR Encounter)
   - Encounter class (AMB/IMP/EMER/HH) → visit_type_concept_id via inline CTE
   - ABDM HFR facility code preserved
   - ~1,000 rows

5. **omop_measurement** (from FHIR Observation where `value_quantity IS NOT NULL`)
   - LOINC code → measurement_concept_id (119K mapped)
   - Numeric values, units, reference ranges
   - ~900,000+ rows (dominant table)

6. **omop_observation** (from FHIR Observation where value is coded/string/absent)
   - LOINC/SNOMED code → observation_concept_id
   - Coded and text results; catch-all for observations with no value
   - ~28,000 rows

**Total Gold rows: 939,949**

### Vocabulary Tables

- **workspace.omop_gold.concept** (2.65M rows)
  - RxNorm Extension: 1.86M concepts
  - SNOMED: 349K concepts
  - RxNorm: 157K concepts
  - LOINC: 119K concepts
  - Gender, Race, Visit types, etc.

- **workspace.omop_gold.concept_relationship** (35M rows)
  - Concept mappings and hierarchies

### Supporting Artefacts

- **lineage_audit VIEW** — traces any `_lineage_id` through all 6 Gold tables back to
  the originating Bronze `_source_file`
- **OPTIMIZE + ZORDER** — run on all Gold tables after materialisation; ZORDER by
  `person_id` and the primary date column for fast cohort queries

---

## Key Technical Decisions

| Decision | Rationale | Trade-off |
|----------|-----------|-----------|
| SparkSQL CTAS instead of dbt | dbt-databricks requires a SQL Warehouse endpoint; direct spark.sql() needs no external connection | Lose dbt lineage graph; mitigated by lineage_audit VIEW |
| `ABS(xxhash64(...))` surrogate keys | Native Spark, deterministic, positive BIGINT — no dbt_utils required | Not portable to non-Spark engines |
| `spark.sql.ansi.enabled = false` | Photon rejects ISO 8601 timestamps with tz offsets in strict mode; FHIR dateTime mandates tz offsets | Disables some overflow guards; acceptable for trusted FHIR input |
| Hard-coded visit type mappings (inline CTE) | AMB/IMP/EMER/HH → concept_ids are stable OMOP constants; no vocabulary lookup needed | Add new entry if new encounter classes appear |
| `CREATE OR REPLACE TABLE` (not incremental MERGE) | CTAS is simpler and sufficient for a learning dataset | Full rebuild on every run; use MERGE for production with millions of rows |

---

## Bugs Found During First Live Run

### Bug 1: Photon ANSI timestamp rejection
**Error:** `DateTimeException: [CANNOT_PARSE_TIMESTAMP] Text '1966-10-28T22:23:36-04:00'`
**Root cause:** Photon ANSI mode rejects ISO 8601 strings with timezone offsets.
**Fix:** `spark.conf.set("spark.sql.ansi.enabled", "false")` at notebook start.
**Regression test:** `tests/test_gold_regression.py::TestAnsiTimestampRegression`

### Bug 2: Ambiguous `encounter_class` reference
**Error:** `[AMBIGUOUS_REFERENCE] Reference 'encounter_class' is ambiguous`
**Root cause:** Both the Silver source table (`s`) and the inline `visit_type_mapping` CTE
expose a column named `encounter_class`. An unqualified `SELECT encounter_class` is ambiguous.
**Fix:** Qualify all source-table SELECT columns with the `s.` alias.
**Regression test:** `tests/test_gold_regression.py::TestEncounterClassAmbiguityRegression`

---

## Challenges Encountered

### Challenge 1: Vocabulary File Upload Size Limits
**Problem:** 3.6 GB of OMOP vocabularies couldn't be uploaded via workspace API.
**Solution:** Used Databricks Volumes for large file storage.
**Learning:** Plan for large data ingestion early; Volumes > DBFS for large files.

### Challenge 2: Schema Inference Errors
**Problem:** Auto-inferred schema for CONCEPT.csv caused merge field conflicts.
**Error:** `[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'concept_id'`
**Solution:** Define explicit Spark StructType schemas; concept_id as BIGINT.
**Learning:** Always be explicit with schemas for large data operations.

### Challenge 3: dbt Not Viable Without SQL Warehouse Endpoint
**Problem:** `dbt-databricks` requires a configured SQL Warehouse HTTP path.
**Solution:** Replaced dbt with native `spark.sql()` CTAS statements.
**Learning:** Check execution environment constraints before committing to a tool.

---

## What Worked Well

✅ **Silver staging layer:** Extracts all FHIR `value[x]` variants; Gold routes to OMOP. Keeps Silver agnostic to OMOP semantics.

✅ **`ABS(xxhash64)` surrogate keys:** Deterministic, positive, fits BIGINT — idempotent across re-runs.

✅ **Vocabulary lookup pattern:** `LEFT JOIN concept … COALESCE(concept_id, 0)` is clean, safe, and handles missing vocabularies gracefully.

✅ **ABDM-specific fields:** ABHA ID, HFR code preserved end-to-end for traceability.

✅ **Regression tests:** Both production bugs are now covered by tests that run locally without a cluster.

✅ **lineage_audit VIEW:** Single query to trace any Gold row back to its originating JSON file.

---

## Next Steps (Phase 3)

1. **Data quality checks** — Achilles + DQD against the materialised Gold tables
2. **ABDM gap analysis** — compare ABDM FHIR IG profiles to Synthea output; identify missing extensions
3. **ABHA ID integration** — test with real ABDM-formatted FHIR bundles
4. **Incremental MERGE strategy** — replace CTAS with MERGE for production-scale datasets

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

- **Synthea dataset:** 1,000 patients, 1,187 FHIR bundles
- **Actual throughput:** 939,949 Gold rows on first run
- **Concept mapping rate:** 37–91% depending on vocabulary (condition lowest; visit highest)
- **materialisation approach:** `CREATE OR REPLACE TABLE` (full rebuild); first run ~20 min
- **OPTIMIZE + ZORDER:** run once after all tables built; skip if memory-constrained

---

## Files Modified / Created

- `notebooks/09_vocabulary_setup.py` — Load concept tables from Athena CSVs
- `notebooks/10_gold_build.py` — Materialise Silver → Gold via spark.sql() CTAS (replaces dbt)
- `tests/test_gold_transformations.py` — 43 unit tests for Gold layer logic
- `tests/test_gold_regression.py` — 16 regression tests for the two production bugs
- `tests/conftest.py` — Added PYSPARK_PYTHON env var for venv compatibility
- `docs/learning_log.md` — Steps 9–10 documented
- `docs/mapping_decisions.md` — MD-006 through MD-009 added
- `docs/phase_2_summary.md` — This file
