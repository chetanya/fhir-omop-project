# FHIR → OMOP Mapping Decisions
# docs/mapping_decisions.md
# Running log of technical decisions made during the pipeline build.
# Format: decision title, context, what we chose, why, and trade-offs.

---

## MD-001 · Catalog name: `workspace` (not `health_lh`)

**Context:** Databricks Community Edition does not support creating custom Unity Catalog
catalogs. Only the built-in `workspace` catalog is available.

**Decision:** Use `catalog: "workspace"` in `dbt_project.yml` and `CATALOG = "workspace"`
in notebooks for Community Edition. The target catalog (`health_lh`) remains the intended
production value and is documented in CLAUDE.md naming conventions.

**Trade-off:** Code reads differently from the OMOP naming convention docs. Mitigated by
keeping the catalog name in a single `vars.catalog` dbt variable and a top-level `CATALOG`
constant in each notebook — change one line to switch environments.

---

## MD-002 · Batch ingestion (`spark.read.text`) vs. Auto Loader streaming

**Context:** The original design used Auto Loader (`cloudFiles`) with
`trigger(availableNow=True)`. In Databricks Community Edition, serverless compute does not
support Structured Streaming checkpoints on Unity Catalog Volumes in the same way.

**Decision:** Switched to `spark.read.text` (batch) with `wholetext=true` for the Bronze
ingestion notebook. The Bronze write uses `mode("overwrite")` (or `DROP TABLE IF EXISTS`
before the write) to ensure idempotency.

**Why `wholetext=true`:** Without this option, Spark reads each *line* of a JSON file as a
separate row. Synthea produces pretty-printed JSON files where a single bundle spans
hundreds of lines. Without `wholetext`, the UDF receives partial JSON strings and returns
empty results for every row.

**Production path:** In a real Databricks workspace, revert to Auto Loader streaming with
`trigger(availableNow=True)` and a proper checkpoint directory *outside* the watched
Volume path. Auto Loader's checkpoint tracks which files have been processed, enabling true
incremental ingestion without re-reading the full dataset each run.

---

## MD-003 · `_metadata.file_path` vs. `input_file_name()`

**Context:** `input_file_name()` is a Spark built-in function that returns the path of the
file currently being processed. It is blocked in Unity Catalog environments — calling it
raises a runtime error about restricted metadata access.

**Decision:** Use `F.col("_metadata.file_path")` instead. The `_metadata` struct is
Unity Catalog's sanctioned way to expose file-level metadata in Delta/Parquet reads.

**Available `_metadata` fields:**
- `_metadata.file_path` — full path of the source file
- `_metadata.file_name` — filename only
- `_metadata.file_size` — bytes
- `_metadata.file_modification_time` — last-modified timestamp

---

## MD-004 · UDF for bundle explode vs. `from_json` + `explode`

**Context:** A FHIR Bundle's `entry[].resource` is a polymorphic JSON object — a Patient
resource and an Observation resource have completely different schemas. Spark's `from_json`
requires a fixed schema; there is no `struct<*>` wildcard.

**Decision:** Use a Python UDF (`explode_bundle`) that:
1. Parses the full bundle JSON with `json.loads`.
2. Iterates `entry[]`, skips entries without a `resource` or without a `resourceType`.
3. Re-serializes each resource to a compact JSON string (`separators=(",",":")` removes
   whitespace).
4. Returns a typed Spark array of structs for the Catalyst planner.

**Trade-off:** Python UDFs are slower than native Spark SQL functions (serialization
overhead per row). For Bronze ingestion this is acceptable — bundles are read once and
the UDF result is written to Delta, so Silver and Gold never pay this cost.

**Why skip `resourceType`-less entries:** Synthea bundles include `practitionerInformation`
pseudo-entries that lack a `resourceType`. These are unroutable in Silver and would produce
null `resource_type` rows that fail the downstream NOT NULL assertion.

---

## MD-005 · `_lineage_id` as SHA-256(source_file | fhir_resource_id)

**Context:** OMOP requires stable, cross-layer join keys. FHIR `.id` values are unique
within a resource type but not globally (two different files could theoretically reuse an
id). We also need idempotency — the same resource from the same file must always produce
the same Bronze row.

**Decision:** `_lineage_id = SHA-256(source_file || "|" || fhir_resource_id)`.

**Why:** A FHIR resource id is unique within a single bundle/file. Combining with the
source file path gives global uniqueness across the dataset. SHA-256 produces a fixed-width
string usable as a merge key in Silver MERGE ON `_lineage_id`.

**OMOP Gold:** `person_source_value`, `condition_occurrence_id`, etc. are derived from
`_lineage_id` in Gold models, giving end-to-end traceability from a Gold row back to the
originating JSON file.

---

## MD-006 · dbt timeout configuration for Databricks SQL Warehouse

**Context:** dbt on Databricks uses SQL Warehouse sessions which timeout after 15 minutes
of inactivity by default. The initial Phase 2 dbt build (materializing 6 large gold tables
with vocabulary lookups) took 30+ minutes and failed with:
```
Retry request would exceed Retry policy max retry duration of 900.0 seconds
```

**Decision:** Increase `timeout_seconds: 3600` (1 hour) and reduce `threads: 2` in dbt profiles.yml.

**Why 3600 seconds:**
- Allows long-running transformations (large incremental MERGEs) to complete without timing out
- 1 hour is overkill for most runs, but safe for the first materialization with full concept lookups
- Subsequent runs will be faster (incremental only)

**Why reduce threads from 4 to 2:**
- Lower concurrency = less load on warehouse = more stable connections
- For a 1,000-person dataset, parallelizing across 4 threads adds overhead without benefit
- Reduces risk of connection resets under load

**Trade-off:** Slower per-thread execution, but more stable for Community Edition warehouses.
In production (larger warehouse), increase threads back to 4 and use auto-scaling.

**Alternative:** Run dbt locally instead of in Databricks. Requires:
- `dbt-databricks` installed (via pip, uv, or homebrew)
- profiles.yml configured with Databricks host/token
- No Databricks Repos required
- Faster iteration and better error messages

**Chosen approach:** Run locally for Phase 2 development; move to Databricks jobs/CI in Phase 3.

---

## MD-006 · ABHA ID extraction: `FILTER` + `get()` vs. `identifier[0]`

**Context:** FHIR `Patient.identifier` is an array of `{system, value}` objects. The ABDM
ABHA ID has `system = "https://healthid.ndhm.gov.in"`. A patient may have multiple
identifiers (MRN, passport, etc.) in any order.

**Decision:** Use `FILTER(identifier_array, x -> x.system = 'https://healthid.ndhm.gov.in')`
to isolate the ABHA entry, then `get(filtered_array, 0).value` to safely extract the value.

**Why not `identifier[0]`:** Array subscript raises `ArrayIndexOutOfBoundsException` on an
empty array. `get()` returns NULL, which is the safe default — a patient without an ABHA
ID is expected (e.g. non-ABDM Synthea data).

**Why not `get_json_object`:** Filtering on a nested array field requires parsing the full
array into a typed struct first. `from_json(..., 'array<struct<system:string,value:string>>')
+ FILTER + get()` is more explicit and handles the ABDM-specific lookup cleanly.
