# tests/test_bronze_ingestion.py
# Unit tests for Bronze ingestion logic
# Tests the explode_bundle UDF logic as plain Python (no Spark overhead)
# and the PySpark lineage_id computation.

import hashlib
import json
import pytest
from pyspark.sql import functions as F
from helpers import make_fhir_df


# ---------------------------------------------------------------------------
# Mirror the UDF logic as a plain Python function so we can test it without
# spinning up a Spark job for every case.
# ---------------------------------------------------------------------------

def parse_bundle(bundle_json: str) -> list:
    """Pure-Python mirror of the explode_bundle UDF in 02_bronze_ingestion.py."""
    if not bundle_json:
        return []
    try:
        bundle = json.loads(bundle_json)
    except (ValueError, TypeError):
        return []
    if not isinstance(bundle, dict):
        return []
    results = []
    for entry in bundle.get("entry", []):
        resource = entry.get("resource")
        if not resource:
            continue
        resource_type = resource.get("resourceType")
        if not resource_type:
            continue
        results.append({
            "resource_json":    json.dumps(resource, separators=(",", ":")),
            "resource_type":    resource_type,
            "fhir_resource_id": resource.get("id"),
        })
    return results


def _make_bundle(*resources):
    """Wrap resource dicts into a minimal FHIR Bundle string."""
    return json.dumps({
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [{"resource": r} for r in resources],
    })


PATIENT = {"resourceType": "Patient", "id": "p-001", "gender": "female"}
CONDITION = {"resourceType": "Condition", "id": "c-001", "subject": {"reference": "Patient/p-001"}}


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestParseBundleHappyPath:

    def test_single_resource_extracted(self):
        results = parse_bundle(_make_bundle(PATIENT))
        assert len(results) == 1
        assert results[0]["resource_type"] == "Patient"
        assert results[0]["fhir_resource_id"] == "p-001"

    def test_multiple_resources_extracted(self):
        results = parse_bundle(_make_bundle(PATIENT, CONDITION))
        assert len(results) == 2
        types = {r["resource_type"] for r in results}
        assert types == {"Patient", "Condition"}

    def test_resource_json_is_valid_json(self):
        results = parse_bundle(_make_bundle(PATIENT))
        parsed = json.loads(results[0]["resource_json"])
        assert parsed["id"] == "p-001"

    def test_resource_json_compact_serialized(self):
        """Compact serialization (no spaces) keeps Bronze rows small."""
        results = parse_bundle(_make_bundle(PATIENT))
        assert " " not in results[0]["resource_json"]


# ---------------------------------------------------------------------------
# Malformed / edge-case input
# ---------------------------------------------------------------------------

class TestParseBundleEdgeCases:

    def test_empty_string_returns_empty(self):
        assert parse_bundle("") == []

    def test_none_returns_empty(self):
        assert parse_bundle(None) == []

    def test_invalid_json_returns_empty(self):
        assert parse_bundle("{not valid json") == []

    def test_json_array_not_bundle_returns_empty(self):
        """practitionerInformation*.json files are JSON arrays — must be skipped."""
        array_json = json.dumps([{"resourceType": "Practitioner", "id": "pr-1"}])
        assert parse_bundle(array_json) == []

    def test_bundle_with_empty_entry_array(self):
        bundle = json.dumps({"resourceType": "Bundle", "type": "transaction", "entry": []})
        assert parse_bundle(bundle) == []

    def test_entry_without_resource_key_skipped(self):
        bundle = json.dumps({
            "resourceType": "Bundle",
            "entry": [{"fullUrl": "urn:uuid:123"}],  # no "resource" key
        })
        assert parse_bundle(bundle) == []

    def test_resource_without_resource_type_skipped(self):
        """Resources missing resourceType are unroutable — must be filtered."""
        bundle = json.dumps({
            "resourceType": "Bundle",
            "entry": [{"resource": {"id": "r-001"}}],  # no resourceType
        })
        assert parse_bundle(bundle) == []

    def test_null_resource_type_skipped(self):
        bundle = json.dumps({
            "resourceType": "Bundle",
            "entry": [{"resource": {"resourceType": None, "id": "r-002"}}],
        })
        assert parse_bundle(bundle) == []

    def test_resource_without_id_still_included(self):
        """FHIR id is optional; resource should still be extracted with None id."""
        resource_no_id = {"resourceType": "Patient", "gender": "male"}
        results = parse_bundle(_make_bundle(resource_no_id))
        assert len(results) == 1
        assert results[0]["fhir_resource_id"] is None

    def test_mixed_valid_and_invalid_entries(self):
        """Only valid entries extracted; invalid ones silently skipped."""
        bundle = json.dumps({
            "resourceType": "Bundle",
            "entry": [
                {"resource": PATIENT},
                {"fullUrl": "urn:uuid:x"},            # no resource key
                {"resource": {"id": "r-003"}},         # no resourceType
                {"resource": CONDITION},
            ],
        })
        results = parse_bundle(bundle)
        assert len(results) == 2
        assert {r["resource_type"] for r in results} == {"Patient", "Condition"}


# ---------------------------------------------------------------------------
# Lineage ID computation (PySpark)
# The _lineage_id is SHA-256(source_file | fhir_resource_id).
# Same inputs must always produce the same hash across runs.
# ---------------------------------------------------------------------------

class TestLineageId:

    def test_lineage_id_deterministic(self, spark):
        """Same source file + resource id → same lineage_id every time."""
        source = "dbfs:/Volumes/workspace/fhir_bronze/synthea_raw/patient.json"
        fhir_id = "p-001"
        expected = hashlib.sha256(f"{source}|{fhir_id}".encode()).hexdigest()

        df = spark.createDataFrame([{"_source_file": source, "fhir_resource_id": fhir_id}])
        result = df.select(
            F.sha2(F.concat_ws("|", F.col("_source_file"), F.col("fhir_resource_id")), 256)
            .alias("lineage_id")
        ).collect()[0]["lineage_id"]

        assert result == expected

    def test_lineage_id_differs_across_resources(self, spark):
        """Two resources from the same file get different lineage_ids."""
        rows = [
            {"_source_file": "file.json", "fhir_resource_id": "p-001"},
            {"_source_file": "file.json", "fhir_resource_id": "c-001"},
        ]
        ids = (
            spark.createDataFrame(rows)
            .select(F.sha2(F.concat_ws("|", "_source_file", "fhir_resource_id"), 256).alias("lid"))
            .collect()
        )
        assert ids[0]["lid"] != ids[1]["lid"]

    def test_lineage_id_no_nulls_when_resource_id_present(self, spark):
        df = make_fhir_df(spark, {"resourceType": "Patient", "id": "p-001"})
        result = df.select(
            F.sha2(F.concat_ws("|", F.col("_source_file"), F.col("fhir_resource_id")), 256)
            .alias("lid")
        ).collect()[0]
        assert result["lid"] is not None


# ---------------------------------------------------------------------------
# Content hash
# ---------------------------------------------------------------------------

class TestContentHash:

    def test_content_hash_is_sha256(self, spark):
        raw = '{"resourceType":"Bundle","entry":[]}'
        expected = hashlib.sha256(raw.encode()).hexdigest()
        df = spark.createDataFrame([{"value": raw}])
        result = df.select(F.sha2(F.col("value"), 256).alias("h")).collect()[0]["h"]
        assert result == expected

    def test_different_files_have_different_hashes(self, spark):
        rows = [{"value": '{"resourceType":"Bundle","entry":[]}'}, {"value": '{"resourceType":"Bundle","entry":[1]}'}]
        hashes = spark.createDataFrame(rows).select(F.sha2("value", 256).alias("h")).collect()
        assert hashes[0]["h"] != hashes[1]["h"]
