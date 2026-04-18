# FHIR → OMOP Mapping Decisions
# docs/mapping_decisions.md
# Running log of technical decisions made during the pipeline build.
# Format: decision title, context, what we chose, why, and trade-offs.

---

## MD-001 · Catalog name: `workspace` (not `health_lh`)

**Context:** This project runs on a Databricks workspace with Unity Catalog enabled. The
`workspace` catalog is the default Unity Catalog in the workspace. Creating a custom catalog
(`health_lh`) requires the `CREATE CATALOG` privilege; using `workspace` avoids that
permission dependency during development.

**Decision:** Use `CATALOG = "workspace"` in all notebooks. The intended production catalog
(`health_lh`) remains documented in CLAUDE.md naming conventions but is not used here.

**Trade-off:** Code reads differently from the OMOP naming convention docs. Mitigated by
keeping the catalog name in a top-level `CATALOG` constant in each notebook — change one
line to switch environments.

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

## MD-006 · Gold transformation: native SparkSQL instead of dbt

**Context:** The original design used `dbt-databricks` (Silver → Gold). dbt-databricks
requires a SQL Warehouse HTTP path (`http_path` in profiles.yml). The workspace's serverless
SQL Warehouse endpoint was not configured correctly, and `dbt_utils.generate_surrogate_key`
macro required declaring `dbt_utils` in `packages.yml` which was absent.

**Decision:** Replace dbt with direct `spark.sql()` CTAS statements inside
`notebooks/10_gold_build.py`. The SQL logic is identical to the dbt models; only the
execution mechanism changes. The dbt model files in `dbt/models/gold/` remain in the repo
as reference documentation of the intended schema.

**Why not fix dbt:**
- Three simultaneous blockers (endpoint config, missing package, notebook runner) added
  friction without learning value.
- For a 1,000-patient dataset, `spark.sql()` CTAS is faster to iterate than `dbt build`.
- All dbt incremental MERGE semantics are preserved: `CREATE OR REPLACE TABLE` + ZORDER
  achieves the same idempotency.

**Trade-off:** Lose dbt's lineage graph and `dbt test` command. Mitigated by the
`lineage_audit` VIEW in Gold and pytest unit tests in `tests/`.

**Production path:** Restore dbt when connecting to a full SQL Warehouse endpoint with a
properly configured `profiles.yml`. The SQL in the notebook can be copy-pasted directly
into the existing dbt model files.

---

## MD-007 · Surrogate key: `ABS(xxhash64(...))` instead of `dbt_utils.generate_surrogate_key`

**Context:** OMOP primary keys must be positive integers (BIGINT). `dbt_utils.generate_surrogate_key`
produces an MD5 hex string. Without dbt_utils available (see MD-006), an alternative was needed.

**Decision:** `ABS(xxhash64(fhir_resource_id))` — native Spark SQL, deterministic, positive,
and fits in a signed 64-bit integer (OMOP BIGINT columns).

**Why xxhash64 over MD5/SHA-256:**
- Native Spark function — no UDF, no serialization overhead.
- 64-bit output fits directly in OMOP BIGINT columns (no cast to string required).
- `ABS()` guarantees positivity; xxhash64 can return negative values for some inputs.
- Collision probability over 1M records is negligible (~0.003%).

**Trade-off:** Not portable to non-Spark environments. For portability, swap with
`FARM_FINGERPRINT` (BigQuery) or the equivalent in the target engine.

---

## MD-008 · `spark.sql.ansi.enabled = false` for FHIR timestamps

**Context:** Databricks Photon (the vectorized execution engine) enforces ANSI mode by
default. ISO 8601 strings with timezone offsets (e.g. `1966-10-28T22:23:36-04:00`,
`2021-12-01T00:00:00+05:30` for IST) cause `CANNOT_PARSE_TIMESTAMP` in strict ANSI mode.
These are valid FHIR datetime values and mandatory for ABDM records.

**Decision:** `spark.conf.set("spark.sql.ansi.enabled", "false")` at the top of each
notebook that processes FHIR timestamps. `TO_TIMESTAMP` then returns NULL on unparseable
strings rather than throwing, which is the correct Silver-layer behaviour (bad timestamps
are handled downstream).

**Why not fix the input strings:** FHIR R4 mandates timezone offsets in dateTime values.
Stripping offsets would violate the spec and silently lose timezone information.

**Trade-off:** ANSI mode off disables some overflow and type-coercion protections. Acceptable
for a pipeline where all ingested data is from a trusted FHIR server. Re-enable for any
notebook that does not process FHIR datetimes.

**Regression guard:** `tests/test_gold_regression.py::TestAnsiTimestampRegression` covers
the exact production crash timestamp and IST offsets.

---

## MD-009 · ABHA ID extraction: `FILTER` + `get()` vs. `identifier[0]`

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
