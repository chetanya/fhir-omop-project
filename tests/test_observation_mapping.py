# tests/test_observation_mapping.py
# Unit tests for FHIR Observation → Silver stg_fhir_observation mapping
# Focus: value[x] polymorphism, ISO 8601 timestamp parsing, LOINC extraction,
#        category routing, reference range, and missing-value handling.

import pytest
from pyspark.sql import functions as F
from helpers import make_fhir_df


def extract_observation_fields(df):
    """Mirror the Silver stg_fhir_observation transformation logic."""
    return df.select(
        F.get_json_object("resource_json", "$.id").alias("fhir_observation_id"),
        F.regexp_replace(
            F.get_json_object("resource_json", "$.subject.reference"), "^Patient/", ""
        ).alias("fhir_patient_id"),
        F.regexp_replace(
            F.get_json_object("resource_json", "$.encounter.reference"), "^Encounter/", ""
        ).alias("fhir_encounter_id"),
        F.get_json_object("resource_json", "$.status").alias("status"),
        F.get_json_object("resource_json", "$.category[0].coding[0].code").alias("category"),
        F.get_json_object("resource_json", "$.code.coding[0].system").alias("observation_source_vocabulary"),
        F.get_json_object("resource_json", "$.code.coding[0].code").alias("observation_source_code"),
        F.get_json_object("resource_json", "$.code.coding[0].display").alias("observation_source_display"),
        F.coalesce(
            F.to_timestamp(F.get_json_object("resource_json", "$.effectiveDateTime")),
            F.to_timestamp(F.get_json_object("resource_json", "$.effectivePeriod.start")),
        ).alias("effective_datetime"),
        # value[x] — all three types preserved; Gold routes to measurement vs observation
        F.get_json_object("resource_json", "$.valueQuantity.value").cast("double").alias("value_quantity"),
        F.get_json_object("resource_json", "$.valueQuantity.unit").alias("value_unit"),
        F.get_json_object("resource_json", "$.valueQuantity.code").alias("value_unit_code"),
        F.get_json_object("resource_json", "$.valueCodeableConcept.coding[0].code").alias("value_concept_code"),
        F.get_json_object("resource_json", "$.valueCodeableConcept.coding[0].display").alias("value_concept_display"),
        F.get_json_object("resource_json", "$.valueString").alias("value_string"),
        F.get_json_object("resource_json", "$.referenceRange[0].low.value").cast("double").alias("range_low"),
        F.get_json_object("resource_json", "$.referenceRange[0].high.value").cast("double").alias("range_high"),
        "_lineage_id", "_source_file", "_loaded_at",
    )


@pytest.fixture(scope="class")
def observation_row(observation_df):
    return extract_observation_fields(observation_df).collect()[0]


# ---------------------------------------------------------------------------
# Core field extraction
# ---------------------------------------------------------------------------

class TestObservationFieldExtraction:

    def test_observation_id_extracted(self, observation_row):
        assert observation_row["fhir_observation_id"] == "obs-001-test"

    def test_patient_reference_stripped(self, observation_row):
        assert observation_row["fhir_patient_id"] == "patient-001-test"
        assert not observation_row["fhir_patient_id"].startswith("Patient/")

    def test_encounter_reference_stripped(self, observation_row):
        assert observation_row["fhir_encounter_id"] == "enc-001-test"
        assert not observation_row["fhir_encounter_id"].startswith("Encounter/")

    def test_status_extracted(self, observation_row):
        assert observation_row["status"] == "final"

    def test_category_extracted(self, observation_row):
        assert observation_row["category"] == "vital-signs"

    def test_loinc_code_extracted(self, observation_row):
        assert observation_row["observation_source_code"] == "8867-4"

    def test_loinc_vocabulary_extracted(self, observation_row):
        assert observation_row["observation_source_vocabulary"] == "http://loinc.org"

    def test_display_extracted(self, observation_row):
        assert observation_row["observation_source_display"] == "Heart rate"


# ---------------------------------------------------------------------------
# ISO 8601 timestamp with timezone (the Photon ANSI-mode issue we fixed)
# ---------------------------------------------------------------------------

class TestObservationTimestamp:

    def test_effective_datetime_parsed_with_ist_offset(self, observation_row):
        """
        Observation uses IST timezone (+05:30). Spark's default to_timestamp
        fails on this in ANSI mode — requires spark.sql.ansi.enabled=false.
        """
        assert observation_row["effective_datetime"] is not None

    def test_effective_datetime_parsed_with_us_offset(self, spark):
        """US Eastern offset (-05:00) must also parse correctly."""
        obs = {
            "resourceType": "Observation", "id": "obs-tz-test",
            "subject": {"reference": "Patient/p-001"}, "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "8867-4"}]},
            "effectiveDateTime": "2020-03-15T09:30:00-05:00",
            "valueQuantity": {"value": 72.0, "unit": "beats/minute"},
        }
        df = make_fhir_df(spark, obs)
        row = extract_observation_fields(df).collect()[0]
        assert row["effective_datetime"] is not None

    def test_effective_period_start_used_as_fallback(self, spark):
        """When effectiveDateTime absent, effectivePeriod.start is used."""
        obs = {
            "resourceType": "Observation", "id": "obs-period-test",
            "subject": {"reference": "Patient/p-001"}, "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "8867-4"}]},
            "effectivePeriod": {"start": "2020-03-15T09:00:00+05:30"},
            "valueQuantity": {"value": 72.0, "unit": "beats/minute"},
        }
        df = make_fhir_df(spark, obs)
        row = extract_observation_fields(df).collect()[0]
        assert row["effective_datetime"] is not None


# ---------------------------------------------------------------------------
# value[x] polymorphism — the hardest FHIR mapping challenge
# Gold routes to measurement vs observation based on which field is non-null.
# ---------------------------------------------------------------------------

class TestObservationValuePolymorphism:

    def test_value_quantity_extracted(self, observation_row):
        assert observation_row["value_quantity"] == 72.0
        assert observation_row["value_unit"] == "beats/minute"
        assert observation_row["value_unit_code"] == "/min"

    def test_value_quantity_nulls_other_value_types(self, observation_row):
        assert observation_row["value_concept_code"] is None
        assert observation_row["value_string"] is None

    def test_value_codeable_concept_extracted(self, spark):
        obs_coded = {
            "resourceType": "Observation", "id": "obs-coded-test",
            "subject": {"reference": "Patient/p-001"}, "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "82810-3"}]},
            "effectiveDateTime": "2020-03-15T09:00:00+05:30",
            "valueCodeableConcept": {
                "coding": [{"code": "10828004", "display": "Positive"}]
            },
        }
        row = extract_observation_fields(make_fhir_df(spark, obs_coded)).collect()[0]
        assert row["value_concept_code"] == "10828004"
        assert row["value_concept_display"] == "Positive"
        assert row["value_quantity"] is None
        assert row["value_string"] is None

    def test_value_string_extracted(self, spark):
        obs_string = {
            "resourceType": "Observation", "id": "obs-string-test",
            "subject": {"reference": "Patient/p-001"}, "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "8661-1"}]},
            "effectiveDateTime": "2020-03-15T09:00:00+05:30",
            "valueString": "normal",
        }
        row = extract_observation_fields(make_fhir_df(spark, obs_string)).collect()[0]
        assert row["value_string"] == "normal"
        assert row["value_quantity"] is None
        assert row["value_concept_code"] is None

    def test_no_value_all_nulls(self, spark):
        """Registered observation with no result yet — all value fields NULL."""
        obs_no_value = {
            "resourceType": "Observation", "id": "obs-novalue-test",
            "subject": {"reference": "Patient/p-001"}, "status": "registered",
            "code": {"coding": [{"system": "http://loinc.org", "code": "8867-4"}]},
        }
        row = extract_observation_fields(make_fhir_df(spark, obs_no_value)).collect()[0]
        assert row["value_quantity"] is None
        assert row["value_concept_code"] is None
        assert row["value_string"] is None


# ---------------------------------------------------------------------------
# Reference range
# ---------------------------------------------------------------------------

class TestObservationReferenceRange:

    def test_range_low_extracted(self, observation_row):
        assert observation_row["range_low"] == 60.0

    def test_range_high_extracted(self, observation_row):
        assert observation_row["range_high"] == 100.0

    def test_null_range_when_absent(self, spark):
        obs_no_range = {
            "resourceType": "Observation", "id": "obs-norange-test",
            "subject": {"reference": "Patient/p-001"}, "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "8867-4"}]},
            "effectiveDateTime": "2020-03-15T09:00:00+05:30",
            "valueQuantity": {"value": 72.0, "unit": "beats/minute"},
        }
        row = extract_observation_fields(make_fhir_df(spark, obs_no_range)).collect()[0]
        assert row["range_low"] is None
        assert row["range_high"] is None
