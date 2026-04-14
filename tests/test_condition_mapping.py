# tests/test_condition_mapping.py
# Unit tests for FHIR Condition → OMOP condition_occurrence mapping
# Focus: SNOMED code extraction, onset date logic, subject reference parsing

import json
import pytest
from pyspark.sql import functions as F
from helpers import PATIENT_REF_STRIP, make_fhir_df


def extract_condition_fields(df):
    """Mirror the Silver stg_fhir_condition transformation logic."""
    return df.select(
        F.get_json_object("resource_json", "$.id").alias("fhir_condition_id"),
        F.regexp_replace(
            F.get_json_object("resource_json", "$.subject.reference"),
            PATIENT_REF_STRIP, ""
        ).alias("fhir_patient_id"),
        F.get_json_object("resource_json", "$.code.coding[0].code").alias("condition_source_code"),
        F.get_json_object("resource_json", "$.code.coding[0].system").alias("condition_source_vocabulary"),
        F.get_json_object("resource_json", "$.code.coding[0].display").alias("condition_source_display"),
        # Prefer onsetDateTime, fall back to recordedDate
        F.coalesce(
            F.to_date(F.get_json_object("resource_json", "$.onsetDateTime")),
            F.to_date(F.get_json_object("resource_json", "$.recordedDate")),
        ).alias("condition_start_date"),
        F.to_date(
            F.get_json_object("resource_json", "$.recordedDate")
        ).alias("condition_recorded_date"),
        F.get_json_object(
            "resource_json", "$.clinicalStatus.coding[0].code"
        ).alias("clinical_status"),
    )


@pytest.fixture(scope="class")
def condition_row(condition_df):
    """Collect once per class — avoids a Spark job per test method."""
    return extract_condition_fields(condition_df).collect()[0]


class TestConditionFieldExtraction:

    def test_condition_id_extracted(self, condition_row):
        assert condition_row["fhir_condition_id"] == "condition-001-test"

    def test_patient_reference_stripped(self, condition_row):
        """
        OMOP requires bare patient ID, not "Patient/xxx".
        The 'Patient/' prefix must be stripped from subject.reference.
        """
        assert condition_row["fhir_patient_id"] == "patient-001-test"
        assert not condition_row["fhir_patient_id"].startswith("Patient/")

    def test_snomed_code_extracted(self, condition_row):
        assert condition_row["condition_source_code"] == "44054006"

    def test_snomed_vocabulary_extracted(self, condition_row):
        assert condition_row["condition_source_vocabulary"] == "http://snomed.info/sct"

    def test_onset_date_parsed(self, condition_row):
        assert str(condition_row["condition_start_date"]) == "2020-03-10"

    def test_recorded_date_parsed(self, condition_row):
        assert str(condition_row["condition_recorded_date"]) == "2020-03-15"

    def test_clinical_status_extracted(self, condition_row):
        assert condition_row["clinical_status"] == "active"


class TestConditionOnsetDateFallback:
    """
    onsetDateTime is optional in FHIR. When absent, recordedDate is the
    only date available — condition_start_date must not be NULL in that case.
    """

    def test_fallback_to_recorded_date_when_no_onset(self, spark):
        condition_no_onset = {
            "resourceType": "Condition",
            "id": "condition-002-test",
            "subject": {"reference": "Patient/patient-001-test"},
            "code": {
                "coding": [{
                    "system": "http://snomed.info/sct",
                    "code": "73211009",
                    "display": "Diabetes mellitus"
                }]
            },
            "recordedDate": "2021-05-20",
            "clinicalStatus": {"coding": [{"code": "active"}]}
        }
        df = make_fhir_df(spark, condition_no_onset)
        result = extract_condition_fields(df).collect()[0]
        assert str(result["condition_start_date"]) == "2021-05-20"
