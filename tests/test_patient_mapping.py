# tests/test_patient_mapping.py
# Unit tests for FHIR Patient → OMOP person mapping
# Focus: ABHA ID extraction, gender concept mapping, birth date parsing

import json
import pytest
from pyspark.sql import functions as F
from helpers import ABDM_ABHA_SYSTEM, make_fhir_df


def extract_patient_fields(df):
    """Mirror the Silver stg_fhir_patient transformation logic."""
    return df.select(
        F.get_json_object("resource_json", "$.id").alias("fhir_patient_id"),
        F.to_date(
            F.get_json_object("resource_json", "$.birthDate")
        ).alias("birth_date"),
        F.get_json_object("resource_json", "$.gender").alias("gender_code"),
        # Use get() instead of [0] so an empty filter result returns NULL
        # rather than ArrayIndexOutOfBoundsException
        F.expr(f"""
            get(
                filter(
                    from_json(
                        get_json_object(resource_json, '$.identifier'),
                        'array<struct<system:string,value:string>>'
                    ),
                    x -> x.system = '{ABDM_ABHA_SYSTEM}'
                ),
                0
            ).value
        """).alias("abha_id"),
        F.get_json_object("resource_json", "$.address[0].state").alias("state"),
    )


@pytest.fixture(scope="class")
def patient_row(patient_df):
    """Collect once per class — avoids a Spark job per test method."""
    return extract_patient_fields(patient_df).collect()[0]


class TestPatientFieldExtraction:

    def test_fhir_patient_id_extracted(self, patient_row):
        assert patient_row["fhir_patient_id"] == "patient-001-test"

    def test_birth_date_parsed(self, patient_row):
        assert str(patient_row["birth_date"]) == "1985-06-15"

    def test_gender_code_extracted(self, patient_row):
        assert patient_row["gender_code"] == "female"

    def test_abha_id_extracted(self, patient_row):
        """
        ABDM-specific: ABHA ID must be extracted from the identifier array
        by filtering on system = ABDM_ABHA_SYSTEM.
        This is the most critical India-specific field in the Patient mapping.
        """
        assert patient_row["abha_id"] == "91-1234-5678-9012"

    def test_state_extracted(self, patient_row):
        assert patient_row["state"] == "Maharashtra"


class TestPatientWithoutAbhaId:
    """
    Test graceful handling when ABHA ID is absent.
    Common in older hospital records not yet ABDM-enrolled.
    """

    PATIENT_NO_ABHA = {
        "resourceType": "Patient",
        "id": "patient-002-test",
        "identifier": [
            {"system": "https://hospital.example.in/mrn", "value": "MRN-789"}
        ],
        "gender": "male",
        "birthDate": "1970-01-01",
    }

    def test_null_abha_id_when_absent(self, spark):
        df = make_fhir_df(spark, self.PATIENT_NO_ABHA)
        result = extract_patient_fields(df).collect()[0]
        assert result["abha_id"] is None

    def test_person_source_value_falls_back_to_fhir_id(self, spark):
        """
        OMOP person_source_value = ABHA ID if present, else FHIR resource ID.
        Ensures no person row has a NULL person_source_value.
        """
        df = make_fhir_df(spark, self.PATIENT_NO_ABHA)
        result = (
            extract_patient_fields(df)
            .withColumn(
                "person_source_value",
                F.coalesce(F.col("abha_id"), F.col("fhir_patient_id"))
            )
            .collect()[0]
        )
        assert result["person_source_value"] == "patient-002-test"
