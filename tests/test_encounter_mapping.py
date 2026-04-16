# tests/test_encounter_mapping.py
# Unit tests for FHIR Encounter → Silver stg_fhir_encounter mapping
# Focus: visit class extraction, ISO 8601 period parsing, duration calculation,
#        HFR facility code (ABDM), and graceful handling of missing fields.

import pytest
from pyspark.sql import functions as F
from helpers import ABDM_HFR_SYSTEM, make_fhir_df


def extract_encounter_fields(df):
    """Mirror the Silver stg_fhir_encounter transformation logic."""
    return df.select(
        F.get_json_object("resource_json", "$.id").alias("fhir_encounter_id"),
        F.regexp_replace(
            F.get_json_object("resource_json", "$.subject.reference"), "^Patient/", ""
        ).alias("fhir_patient_id"),
        F.get_json_object("resource_json", "$.status").alias("status"),
        F.get_json_object("resource_json", "$.class.code").alias("encounter_class"),
        F.get_json_object("resource_json", "$.type[0].coding[0].code").alias("encounter_type_code"),
        F.get_json_object("resource_json", "$.type[0].coding[0].display").alias("encounter_type_display"),
        F.to_timestamp(F.get_json_object("resource_json", "$.period.start")).alias("period_start"),
        F.to_timestamp(F.get_json_object("resource_json", "$.period.end")).alias("period_end"),
        F.expr("""
            CAST(
                (unix_timestamp(to_timestamp(get_json_object(resource_json, '$.period.end')))
                 - unix_timestamp(to_timestamp(get_json_object(resource_json, '$.period.start'))))
                / 60 AS INT
            )
        """).alias("duration_minutes"),
        F.expr(f"""
            CASE
                WHEN get_json_object(resource_json, '$.serviceProvider.identifier.system')
                     = '{ABDM_HFR_SYSTEM}'
                THEN get_json_object(resource_json, '$.serviceProvider.identifier.value')
            END
        """).alias("hfr_facility_code"),
        F.get_json_object("resource_json", "$.serviceProvider.display").alias("facility_name"),
        "_lineage_id", "_source_file", "_loaded_at",
    )


@pytest.fixture(scope="class")
def encounter_row(encounter_df):
    return extract_encounter_fields(encounter_df).collect()[0]


# ---------------------------------------------------------------------------
# Core field extraction
# ---------------------------------------------------------------------------

class TestEncounterFieldExtraction:

    def test_encounter_id_extracted(self, encounter_row):
        assert encounter_row["fhir_encounter_id"] == "enc-001-test"

    def test_patient_reference_stripped(self, encounter_row):
        assert encounter_row["fhir_patient_id"] == "patient-001-test"
        assert not encounter_row["fhir_patient_id"].startswith("Patient/")

    def test_status_extracted(self, encounter_row):
        assert encounter_row["status"] == "finished"

    def test_visit_class_extracted(self, encounter_row):
        """AMB = ambulatory/outpatient — maps to OMOP visit_concept_id 9202."""
        assert encounter_row["encounter_class"] == "AMB"

    def test_encounter_type_code_extracted(self, encounter_row):
        assert encounter_row["encounter_type_code"] == "185349003"

    def test_encounter_type_display_extracted(self, encounter_row):
        assert encounter_row["encounter_type_display"] == "Encounter for check up"

    def test_facility_name_extracted(self, encounter_row):
        assert encounter_row["facility_name"] == "Apollo Hospital Mumbai"


# ---------------------------------------------------------------------------
# Period / timestamp parsing (IST timezone)
# ---------------------------------------------------------------------------

class TestEncounterPeriodParsing:

    def test_period_start_parsed(self, encounter_row):
        """IST offset (+05:30) must be handled without throwing."""
        assert encounter_row["period_start"] is not None

    def test_period_end_parsed(self, encounter_row):
        assert encounter_row["period_end"] is not None

    def test_duration_60_minutes(self, encounter_row):
        """start=09:00+05:30, end=10:00+05:30 → 60 minutes."""
        assert encounter_row["duration_minutes"] == 60

    def test_null_period_end_is_handled(self, spark):
        """Ongoing (in-progress) encounters have no period.end — duration must be NULL."""
        enc_ongoing = {
            "resourceType": "Encounter", "id": "enc-ongoing-test",
            "subject": {"reference": "Patient/p-001"},
            "status": "in-progress",
            "class": {"code": "IMP"},
            "period": {"start": "2020-03-15T09:00:00+05:30"},
        }
        row = extract_encounter_fields(make_fhir_df(spark, enc_ongoing)).collect()[0]
        assert row["period_end"] is None
        assert row["duration_minutes"] is None


# ---------------------------------------------------------------------------
# Visit class codes — OMOP routing depends on these
# ---------------------------------------------------------------------------

class TestEncounterVisitClass:

    @pytest.mark.parametrize("class_code", ["AMB", "IMP", "EMER", "HH"])
    def test_visit_class_codes_extracted(self, spark, class_code):
        """All standard FHIR visit classes must round-trip through Silver."""
        enc = {
            "resourceType": "Encounter", "id": f"enc-{class_code}-test",
            "subject": {"reference": "Patient/p-001"},
            "status": "finished",
            "class": {"code": class_code},
        }
        row = extract_encounter_fields(make_fhir_df(spark, enc)).collect()[0]
        assert row["encounter_class"] == class_code

    def test_null_class_when_absent(self, spark):
        enc = {
            "resourceType": "Encounter", "id": "enc-noclass-test",
            "subject": {"reference": "Patient/p-001"},
            "status": "finished",
        }
        row = extract_encounter_fields(make_fhir_df(spark, enc)).collect()[0]
        assert row["encounter_class"] is None


# ---------------------------------------------------------------------------
# ABDM Health Facility Registry (HFR)
# ---------------------------------------------------------------------------

class TestEncounterHfrCode:

    def test_hfr_code_extracted(self, encounter_row):
        assert encounter_row["hfr_facility_code"] == "HFR_MH_MUMBAI_001"

    def test_null_hfr_when_system_mismatch(self, spark):
        """A serviceProvider with a non-HFR system URL must not populate hfr_facility_code."""
        enc = {
            "resourceType": "Encounter", "id": "enc-nohfr-test",
            "subject": {"reference": "Patient/p-001"},
            "status": "finished",
            "class": {"code": "AMB"},
            "serviceProvider": {
                "identifier": {
                    "system": "https://some-other-registry.example.com",
                    "value": "OTHER_001",
                }
            },
        }
        row = extract_encounter_fields(make_fhir_df(spark, enc)).collect()[0]
        assert row["hfr_facility_code"] is None

    def test_null_hfr_when_service_provider_absent(self, spark):
        enc = {
            "resourceType": "Encounter", "id": "enc-noprovider-test",
            "subject": {"reference": "Patient/p-001"},
            "status": "finished",
            "class": {"code": "AMB"},
        }
        row = extract_encounter_fields(make_fhir_df(spark, enc)).collect()[0]
        assert row["hfr_facility_code"] is None


# ---------------------------------------------------------------------------
# Patient deceased fields (tested here since it involves timestamps)
# ---------------------------------------------------------------------------

class TestPatientDeceased:

    def test_deceased_boolean_true(self, spark):
        patient = {
            "resourceType": "Patient", "id": "p-deceased-bool",
            "gender": "male", "birthDate": "1940-01-15",
            "deceasedBoolean": True,
        }
        df = make_fhir_df(spark, patient)
        result = df.select(
            F.expr("""
                CASE
                    WHEN get_json_object(resource_json, '$.deceasedBoolean') = 'true' THEN TRUE
                    WHEN get_json_object(resource_json, '$.deceasedDateTime') IS NOT NULL THEN TRUE
                    ELSE FALSE
                END
            """).alias("is_deceased"),
            F.to_timestamp(
                F.get_json_object("resource_json", "$.deceasedDateTime")
            ).alias("deceased_datetime"),
        ).collect()[0]
        assert result["is_deceased"] is True
        assert result["deceased_datetime"] is None

    def test_deceased_datetime_sets_flag_and_timestamp(self, spark):
        patient = {
            "resourceType": "Patient", "id": "p-deceased-dt",
            "gender": "female", "birthDate": "1945-05-20",
            "deceasedDateTime": "2021-12-01T00:00:00+05:30",
        }
        df = make_fhir_df(spark, patient)
        result = df.select(
            F.expr("""
                CASE
                    WHEN get_json_object(resource_json, '$.deceasedBoolean') = 'true' THEN TRUE
                    WHEN get_json_object(resource_json, '$.deceasedDateTime') IS NOT NULL THEN TRUE
                    ELSE FALSE
                END
            """).alias("is_deceased"),
            F.to_timestamp(
                F.get_json_object("resource_json", "$.deceasedDateTime")
            ).alias("deceased_datetime"),
        ).collect()[0]
        assert result["is_deceased"] is True
        assert result["deceased_datetime"] is not None

    def test_living_patient_not_deceased(self, spark):
        patient = {
            "resourceType": "Patient", "id": "p-living",
            "gender": "female", "birthDate": "1985-06-15",
        }
        df = make_fhir_df(spark, patient)
        result = df.select(
            F.expr("""
                CASE
                    WHEN get_json_object(resource_json, '$.deceasedBoolean') = 'true' THEN TRUE
                    WHEN get_json_object(resource_json, '$.deceasedDateTime') IS NOT NULL THEN TRUE
                    ELSE FALSE
                END
            """).alias("is_deceased"),
        ).collect()[0]
        assert result["is_deceased"] is False
