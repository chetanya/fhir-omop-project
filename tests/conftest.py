# tests/conftest.py
# Pytest fixtures using minimal Synthea-style FHIR sample data
# No Databricks cluster needed — runs in local PySpark mode

import json
import pytest
from pyspark.sql import SparkSession
from helpers import ABDM_ABHA_SYSTEM, make_fhir_df

# -------------------------------------------------------------------
# Sample FHIR resources (minimal valid R4 + ABDM extensions)
# These mirror real Synthea output with ABDM fields added
# -------------------------------------------------------------------

SAMPLE_PATIENT = {
    "resourceType": "Patient",
    "id": "patient-001-test",
    "identifier": [
        {
            # ABDM ABHA ID — the key India-specific field
            "system": ABDM_ABHA_SYSTEM,
            "value": "91-1234-5678-9012"
        },
        {
            "system": "https://hospital.example.in/mrn",
            "value": "MRN-456"
        }
    ],
    "name": [{"family": "Sharma", "given": ["Priya"]}],
    "gender": "female",
    "birthDate": "1985-06-15",
    "address": [{"state": "Maharashtra", "postalCode": "400001"}]
}

SAMPLE_CONDITION = {
    "resourceType": "Condition",
    "id": "condition-001-test",
    "subject": {"reference": "Patient/patient-001-test"},
    "code": {
        "coding": [{
            "system": "http://snomed.info/sct",
            "code": "44054006",          # Type 2 diabetes mellitus
            "display": "Type 2 diabetes mellitus"
        }]
    },
    "onsetDateTime": "2020-03-10",
    "recordedDate": "2020-03-15",
    "clinicalStatus": {
        "coding": [{"code": "active"}]
    }
}

SAMPLE_MEDICATION_REQUEST = {
    "resourceType": "MedicationRequest",
    "id": "medrx-001-test",
    "subject": {"reference": "Patient/patient-001-test"},
    "medicationCodeableConcept": {
        "coding": [{
            # Indian brand name — tests RxNorm normalization challenge
            "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
            "code": "860975",
            "display": "Metformin 500 MG Oral Tablet"
        }]
    },
    "authoredOn": "2020-03-15",
    "status": "active",
    "intent": "order",
    "dosageInstruction": [{
        "timing": {"repeat": {"frequency": 2, "period": 1, "periodUnit": "d"}},
        "doseAndRate": [{"doseQuantity": {"value": 500, "unit": "mg"}}]
    }]
}


@pytest.fixture(scope="session")
def spark():
    """Local PySpark session for unit tests — no cluster needed."""
    return (
        SparkSession.builder
            .master("local[2]")
            .appName("fhir_omop_tests")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
    )


@pytest.fixture(scope="class")
def patient_df(spark):
    return make_fhir_df(spark, SAMPLE_PATIENT, lineage_id="test-lineage-001",
                        source_file="test_bundle.json")


@pytest.fixture(scope="class")
def condition_df(spark):
    return make_fhir_df(spark, SAMPLE_CONDITION, lineage_id="test-lineage-002",
                        source_file="test_bundle.json")


@pytest.fixture(scope="class")
def medication_request_df(spark):
    return make_fhir_df(spark, SAMPLE_MEDICATION_REQUEST, lineage_id="test-lineage-003",
                        source_file="test_bundle.json")
