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

SAMPLE_OBSERVATION = {
    "resourceType": "Observation",
    "id": "obs-001-test",
    "subject": {"reference": "Patient/patient-001-test"},
    "encounter": {"reference": "Encounter/enc-001-test"},
    "status": "final",
    "category": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/observation-category", "code": "vital-signs"}]}],
    "code": {"coding": [{"system": "http://loinc.org", "code": "8867-4", "display": "Heart rate"}]},
    # ISO 8601 with IST offset (+05:30) — the format we fixed for Photon
    "effectiveDateTime": "2020-03-15T09:30:00+05:30",
    "valueQuantity": {"value": 72.0, "unit": "beats/minute", "code": "/min"},
    "referenceRange": [{"low": {"value": 60.0}, "high": {"value": 100.0}}],
}

SAMPLE_ENCOUNTER = {
    "resourceType": "Encounter",
    "id": "enc-001-test",
    "subject": {"reference": "Patient/patient-001-test"},
    "status": "finished",
    "class": {"code": "AMB", "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode"},
    "type": [{"coding": [{"code": "185349003", "display": "Encounter for check up"}]}],
    "period": {
        "start": "2020-03-15T09:00:00+05:30",
        "end":   "2020-03-15T10:00:00+05:30",
    },
    "serviceProvider": {
        "display": "Apollo Hospital Mumbai",
        "identifier": {"system": "https://facility.ndhm.gov.in", "value": "HFR_MH_MUMBAI_001"},
    },
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
            .config("spark.sql.ansi.enabled", "false")   # FHIR ISO-8601 timestamps with tz offsets (e.g. +05:30) fail in strict mode
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


@pytest.fixture(scope="class")
def observation_df(spark):
    return make_fhir_df(spark, SAMPLE_OBSERVATION, lineage_id="test-lineage-004",
                        source_file="test_bundle.json")


@pytest.fixture(scope="class")
def encounter_df(spark):
    return make_fhir_df(spark, SAMPLE_ENCOUNTER, lineage_id="test-lineage-005",
                        source_file="test_bundle.json")
