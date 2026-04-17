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

## Step 10 — Gold Materialization (dbt Build)
**Date:** 2026-04-17
**Status:** ⏳ In Progress (dbt build running)
**Key insight:** dbt on Databricks requires timeout configuration. Default 300-second
timeout is too short for materializing incremental tables with large vocabularies.
Increase to 3600 seconds (1 hour) and reduce threads to 2 to prevent "Retry policy
max retry duration exceeded" errors when SQL Warehouse sessions expire.

**Gotcha:** dbt can run locally (profiles.yml) or in Databricks. Local is simpler;
Databricks requires repo in Repos (not just notebook). Local recommended for Phase 2.

**Expected outputs:**
- omop_person: 1,000+ rows
- omop_condition_occurrence: 5,000+ rows (SNOMED-mapped)
- omop_drug_exposure: 3,000+ rows (RxNorm-mapped)
- omop_visit_occurrence: 1,000+ rows
- omop_measurement: 10,000+ rows (LOINC-mapped)
- omop_observation: 5,000+ rows (LOINC/SNOMED-mapped)

**ABDM note:** Gold models now map FHIR codes → OMOP concepts for all resources.
Real ABDM data will use these mappings for code system translation.
