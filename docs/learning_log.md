# Learning Log
# docs/learning_log.md
# Append an entry here after completing each pipeline step.

---

## Step 1 — Synthea Data Generation
**Date:** 2026-04-14
**Key insight:** Synthea's Maharashtra module produces realistic Indian demographics but
uses US-style RxNorm medication codes — Indian brand names require a separate vocabulary
mapping step in Gold.
**Gotcha:** The `--exporter.fhir.export=true` flag is required; the default Synthea output
is CSV, not FHIR.
**ABDM note:** Synthea bundles do NOT include ABHA IDs (`https://healthid.ndhm.gov.in`
identifiers). The `identifiers` CTE in Silver will produce NULL `abha_id` for all Synthea
patients — expected until real ABDM data or a post-processing enrichment step is added.

---

## Step 2 — Bronze Ingestion (FHIR Bundles → Delta Lake)
**Date:** 2026-04-15
**Key insight:** Community Edition Unity Catalog only has the `workspace` catalog — you
cannot create a custom catalog like `health_lh` without a paid workspace. All catalog
references must use `workspace` locally; the production target name lives only in comments
and CLAUDE.md.
**Gotcha:** `input_file_name()` is silently blocked in Unity Catalog environments at
runtime, not at query-plan time. The error only surfaces when the streaming write starts.
Use `F.col("_metadata.file_path")` instead (see MD-003). Similarly, `spark.read.text`
without `wholetext=true` splits pretty-printed JSON into thousands of partial-string rows
that the UDF cannot parse — `wholetext=true` is non-negotiable for FHIR bundles.
**ABDM note:** Synthea's `practitionerInformation` bundle entries have no `resourceType`.
The UDF skips these silently; they are not FHIR R4 resources and would be unroutable in
Silver. Real ABDM bundles should not produce this pattern — if they do, investigate the
upstream FHIR server.

---

## Step 3 — Silver Flattening (Bronze JSON → Typed Silver Tables)
**Date:** 2026-04-15
**Status:** ✅ Complete
**Key insight:** FHIR's flexible schema (multiple data types for the same field —
`value[x]`, `onset[x]`, `effective[x]`) forces Silver to extract all variants as
separate columns. Gold then routes to OMOP based on which one is populated and
the OMOP concept domain. This "extract all, let Gold decide" pattern keeps Silver
agnostic to OMOP semantics and makes re-runs idempotent via MERGE on `_lineage_id`.

**Gotcha:** Timestamps with timezone offsets (ISO-8601 format like `2025-01-06T07:24:26+05:30`
for IST) require wrapping with `to_timestamp()` before `unix_timestamp()`. The raw
`get_json_object()` string returns NULL when passed directly to `unix_timestamp()`,
because `unix_timestamp` expects `'yyyy-MM-dd HH:mm:ss'` format. Spark SQL's ANSI mode
(enabled by default on Databricks Photon) rejects timezone-offset strings entirely —
setting `spark.sql.ansi.enabled = false` lets `to_timestamp()` parse them gracefully.

**ABDM note:** Synthetic FHIR bundles (Synthea) preserve ABDM identifier structure —
`identifier` array with system URL `https://healthid.ndhm.gov.in` — but the *values*
are Synthea-generated UUIDs, not real ABHA IDs. Silver extraction of ABHA ID still works
(using `get()` + `filter()` on the identifier array), but Gold will see NULLs for ABHA
until real ABDM data flows through. Health Facility Registry codes (`https://facility.ndhm.gov.in`)
are similarly missing from Synthea but the extraction pattern (conditional on system URL)
is ready for real data.

---

## Step 9 — Vocabulary Setup (OMOP Concept Tables from Athena)
**Date:** 2026-04-16
**Status:** ✅ Complete
**Key insight:** OMOP vocabulary download from Athena (https://athena.ohdsi.org) is
a one-time operation that requires explicit schema definitions. The concept_id field
must be BIGINT (not STRING or INT), and schema inference from TSV files can cause
Delta merge errors. Explicit Spark schemas prevent these failures.

**Gotcha:** Uploading 3.6 GB of CSVs to Databricks has size limits:
- Workspace import API: max ~10 MB per file
- DBFS: requires public DBFS enabled
- Solution: Use Databricks Volumes (recommended) or split large files

**Result:** Successfully loaded 2.65M concepts:
- RxNorm Extension: 1.86M | SNOMED: 349K | RxNorm: 157K | LOINC: 119K
- Gender: 5 | Race: 1,409 | Visit types: 20

**ABDM note:** Vocabularies are international standards. Indian drug brands require
custom vocabulary extension for ABDM data; Synthea uses standard RxNorm codes.

---

## Step 10 — Gold Build: dbt replaced with native SparkSQL
**Date:** 2026-04-17
**Status:** ✅ Complete (939,949 Gold rows materialised)
**Key insight:** `dbt-databricks` requires a SQL Warehouse HTTP endpoint — it won't
connect to an all-purpose or serverless cluster. The fix is to run the exact same SQL
via `spark.sql()` CTAS statements inside `10_gold_build.py`, which needs no external
connection at all.
**Gotcha:** Three compounding failures in the original dbt approach: (1) no SQL Warehouse
endpoint configured in profiles.yml, (2) `dbt_utils.generate_surrogate_key` requires the
`dbt_utils` package declared in `packages.yml` (missing), (3) notebook 10 was named
`10_achilles_dqd.py` in the plan but the actual build logic needed a new notebook. All
three resolved by replacing dbt with native SparkSQL; surrogate keys now use
`ABS(xxhash64(...))` which is deterministic, positive, and fits in a signed BIGINT.
**ABDM note:** `race_code` was missing from `stg_fhir_patient` (the dbt model had a
latent bug). Added extraction of US Core Race extension for Synthea compatibility;
ABDM profiles return NULL here which is correct (Indian records rarely have race coded).

**Actual Gold row counts (first live run, 1,187 Synthea bundles):**
- omop_person: ~1,000 rows
- omop_visit_occurrence: ~1,000 rows
- omop_condition_occurrence: ~5,000 rows (SNOMED concept mapping 37–91%)
- omop_drug_exposure: ~3,000 rows
- omop_measurement: ~900,000+ rows (LOINC-mapped)
- omop_observation: ~28,000 rows
- **Total: 939,949 rows**

---

## Step 10 — First Live Gold Run: Two Production Bugs Found + Regression Tests Written
**Date:** 2026-04-17
**Status:** ✅ Fixed + regression tests added

**Bug 1 — Photon ANSI timestamp rejection (`CANNOT_PARSE_TIMESTAMP`)**
Photon's strict ANSI mode rejects ISO 8601 strings with timezone offsets like
`1966-10-28T22:23:36-04:00`. These are valid FHIR datetime values (and common in ABDM
records with IST `+05:30` offsets). Fix: `spark.conf.set("spark.sql.ansi.enabled", "false")`
at notebook start so `TO_TIMESTAMP` returns NULL on unparseable strings rather than throwing.

**Bug 2 — Ambiguous `encounter_class` reference (`AMBIGUOUS_REFERENCE`)**
In `omop_visit_occurrence`, both the Silver source table (`s`) and the inline
`visit_type_mapping` CTE expose a column named `encounter_class`. An unqualified
`SELECT encounter_class` raises `AMBIGUOUS_REFERENCE`. Fix: qualify all source columns
with the table alias (`s.encounter_class`, `s.encounter_type_code`, etc.).

**Gotcha:** Both bugs are invisible in unit tests run with non-Photon (Classic) Spark —
Photon's ANSI enforcement is stricter than open-source Spark. If tests pass locally but
fail on the cluster, Photon strict mode is the first thing to check.

**ABDM note:** IST offset (`+05:30`) is the canonical timezone for ABDM records.
The ANSI-mode fix is non-negotiable for any pipeline that will process real ABDM data.
Regression tests in `tests/test_gold_regression.py` cover both bugs with the exact
timestamp string from the production error log.
