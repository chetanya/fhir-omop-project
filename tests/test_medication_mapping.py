# tests/test_medication_mapping.py
# Unit tests for FHIR MedicationRequest → OMOP drug_exposure mapping
# Focus: RxNorm code extraction, dosage parsing, date fields, quantity

import json
import pytest
from pyspark.sql import functions as F
from helpers import PATIENT_REF_STRIP, make_fhir_df


def extract_medication_fields(df):
    """Mirror the Silver stg_fhir_medication_request transformation logic."""
    return df.select(
        F.get_json_object("resource_json", "$.id").alias("fhir_medication_request_id"),
        F.regexp_replace(
            F.get_json_object("resource_json", "$.subject.reference"),
            PATIENT_REF_STRIP, ""
        ).alias("fhir_patient_id"),
        F.get_json_object(
            "resource_json", "$.medicationCodeableConcept.coding[0].code"
        ).alias("drug_source_code"),
        F.get_json_object(
            "resource_json", "$.medicationCodeableConcept.coding[0].system"
        ).alias("drug_source_vocabulary"),
        F.get_json_object(
            "resource_json", "$.medicationCodeableConcept.coding[0].display"
        ).alias("drug_source_display"),
        F.to_date(
            F.get_json_object("resource_json", "$.authoredOn")
        ).alias("drug_exposure_start_date"),
        F.get_json_object("resource_json", "$.status").alias("status"),
        F.get_json_object(
            "resource_json",
            "$.dosageInstruction[0].doseAndRate[0].doseQuantity.value"
        ).cast("double").alias("dose_value"),
        F.get_json_object(
            "resource_json",
            "$.dosageInstruction[0].doseAndRate[0].doseQuantity.unit"
        ).alias("dose_unit"),
        # Frequency: doses per day = frequency / period (in days)
        F.get_json_object(
            "resource_json", "$.dosageInstruction[0].timing.repeat.frequency"
        ).cast("double").alias("frequency"),
    )


@pytest.fixture(scope="class")
def medication_row(medication_request_df):
    """Collect once per class — avoids a Spark job per test method."""
    return extract_medication_fields(medication_request_df).collect()[0]


class TestMedicationFieldExtraction:

    def test_medication_request_id_extracted(self, medication_row):
        assert medication_row["fhir_medication_request_id"] == "medrx-001-test"

    def test_patient_reference_stripped(self, medication_row):
        assert medication_row["fhir_patient_id"] == "patient-001-test"
        assert not medication_row["fhir_patient_id"].startswith("Patient/")

    def test_rxnorm_code_extracted(self, medication_row):
        assert medication_row["drug_source_code"] == "860975"

    def test_rxnorm_vocabulary_extracted(self, medication_row):
        assert "rxnorm" in medication_row["drug_source_vocabulary"].lower()

    def test_authored_on_date_parsed(self, medication_row):
        assert str(medication_row["drug_exposure_start_date"]) == "2020-03-15"

    def test_status_extracted(self, medication_row):
        assert medication_row["status"] == "active"

    def test_dose_value_extracted(self, medication_row):
        assert medication_row["dose_value"] == 500.0

    def test_dose_unit_extracted(self, medication_row):
        assert medication_row["dose_unit"] == "mg"

    def test_frequency_extracted(self, medication_row):
        """Frequency = 2 means twice daily for Metformin."""
        assert medication_row["frequency"] == 2.0


class TestMedicationWithoutDosage:
    """
    Dosage is optional in FHIR. Mapping must not fail when absent —
    OMOP drug_exposure allows NULL quantity/dose fields.
    """

    def test_null_dose_when_dosage_absent(self, spark):
        med_no_dosage = {
            "resourceType": "MedicationRequest",
            "id": "medrx-002-test",
            "subject": {"reference": "Patient/patient-001-test"},
            "medicationCodeableConcept": {
                "coding": [{
                    "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                    "code": "197361",
                    "display": "Amlodipine 5 MG Oral Tablet"
                }]
            },
            "authoredOn": "2021-06-01",
            "status": "active",
            "intent": "order",
        }
        df = make_fhir_df(spark, med_no_dosage)
        result = extract_medication_fields(df).collect()[0]
        assert result["dose_value"] is None
        assert result["dose_unit"] is None
        assert str(result["drug_exposure_start_date"]) == "2021-06-01"
        assert result["drug_source_code"] == "197361"
