"""Shared constants and utilities for FHIR→OMOP test suite."""
import json

ABDM_ABHA_SYSTEM = "https://healthid.ndhm.gov.in"
ABDM_HFR_SYSTEM = "https://facility.ndhm.gov.in"
PATIENT_REF_STRIP = "^Patient/"


def make_fhir_df(spark, resource_dict, lineage_id="test-lineage", source_file="test.json"):
    """Create a single-row Bronze-schema DataFrame from a FHIR resource dict."""
    return spark.createDataFrame([{
        "fhir_id": resource_dict["id"],
        "resource_json": json.dumps(resource_dict),
        "resource_type": resource_dict["resourceType"],
        "_lineage_id": lineage_id,
        "_source_file": source_file,
        "_loaded_at": "2024-01-01T00:00:00",
    }])
