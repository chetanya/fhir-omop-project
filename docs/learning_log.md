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
**Date:** _(fill in when completed)_
**Key insight:**
**Gotcha:**
**ABDM note:**
