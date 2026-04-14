# tests/test_abdm_extensions.py
# Unit tests for ABDM-specific FHIR extensions
# Focus: ABHA ID format validation, Health Facility Registry (HFR) codes,
#        Consent resource profile detection, multi-identifier handling

import json
import re
import pytest
from pyspark.sql import functions as F
from helpers import ABDM_ABHA_SYSTEM, ABDM_HFR_SYSTEM, make_fhir_df


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extract_identifiers(df):
    """Parse all identifiers from a Patient resource into a flat DataFrame."""
    return (
        df.select(
            F.get_json_object("resource_json", "$.id").alias("fhir_patient_id"),
            F.expr("""
                from_json(
                    get_json_object(resource_json, '$.identifier'),
                    'array<struct<system:string,value:string>>'
                )
            """).alias("identifiers"),
        )
        .select("fhir_patient_id", F.explode("identifiers").alias("identifier"))
        .select(
            "fhir_patient_id",
            F.col("identifier.system").alias("system"),
            F.col("identifier.value").alias("value"),
        )
    )


def extract_abha_id(df):
    return df.select(
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
        """).alias("abha_id")
    )


# ---------------------------------------------------------------------------
# ABHA ID format tests
# ---------------------------------------------------------------------------

class TestAbhaIdFormat:
    """
    ABHA IDs follow the format: XX-XXXX-XXXX-XXXX (14 digits + 3 hyphens).
    This is the canonical Health ID format from ABDM.
    Ref: https://healthid.ndhm.gov.in
    """

    ABHA_PATTERN = re.compile(r"^\d{2}-\d{4}-\d{4}-\d{4}$")

    def test_abha_id_matches_format(self, patient_df):
        result = extract_abha_id(patient_df).collect()[0]
        abha = result["abha_id"]
        assert abha is not None
        assert self.ABHA_PATTERN.match(abha), (
            f"ABHA ID '{abha}' does not match expected format XX-XXXX-XXXX-XXXX"
        )

    def test_abha_id_has_correct_segment_lengths(self, patient_df):
        result = extract_abha_id(patient_df).collect()[0]
        parts = result["abha_id"].split("-")
        assert len(parts) == 4
        assert len(parts[0]) == 2
        assert all(len(p) == 4 for p in parts[1:])

    def test_abha_id_digits_only(self, patient_df):
        result = extract_abha_id(patient_df).collect()[0]
        digits_only = result["abha_id"].replace("-", "")
        assert digits_only.isdigit(), "ABHA ID must contain only digits (and hyphens)"


# ---------------------------------------------------------------------------
# Multi-identifier handling
# ---------------------------------------------------------------------------

class TestMultipleIdentifiers:
    """
    Real ABDM patients have multiple identifiers: ABHA ID, MRN, and possibly
    Health Facility Registry IDs. The mapping must preserve all of them and
    correctly route each to the right OMOP table / source value field.
    """

    EXPECTED_SYSTEMS = {ABDM_ABHA_SYSTEM, "https://hospital.example.in/mrn"}

    def test_all_identifiers_parseable(self, patient_df):
        """Both ABHA and MRN identifiers should be extractable."""
        result = extract_identifiers(patient_df).collect()
        systems = {row["system"] for row in result}
        assert systems == self.EXPECTED_SYSTEMS

    def test_mrn_identifier_value(self, patient_df):
        rows = extract_identifiers(patient_df).collect()
        mrn_row = next(r for r in rows if r["system"] == "https://hospital.example.in/mrn")
        assert mrn_row["value"] == "MRN-456"

    def test_correct_identifier_count(self, patient_df):
        """One row per identifier system — guards against duplicate rows."""
        result = extract_identifiers(patient_df).collect()
        assert len(result) == len(self.EXPECTED_SYSTEMS)


# ---------------------------------------------------------------------------
# Health Facility Registry (HFR)
# ---------------------------------------------------------------------------

class TestHfrCodeExtraction:
    """
    ABDM Encounter resources carry an HFR facility code in serviceProvider.
    This maps to OMOP care_site and visit_occurrence.care_site_id.
    """

    def test_hfr_code_extracted_from_encounter(self, spark):
        encounter_with_hfr = {
            "resourceType": "Encounter",
            "id": "enc-001-test",
            "subject": {"reference": "Patient/patient-001-test"},
            "class": {"code": "AMB", "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode"},
            "status": "finished",
            "period": {"start": "2020-03-15T09:00:00", "end": "2020-03-15T10:00:00"},
            "serviceProvider": {
                "identifier": {
                    "system": ABDM_HFR_SYSTEM,
                    "value": "HFR_MH_MUMBAI_001"
                }
            }
        }
        df = make_fhir_df(spark, encounter_with_hfr)
        hfr_code = df.select(
            F.get_json_object(
                "resource_json", "$.serviceProvider.identifier.value"
            ).alias("hfr_code")
        ).collect()[0]["hfr_code"]

        assert hfr_code == "HFR_MH_MUMBAI_001"
        assert hfr_code.startswith("HFR_")

    def test_null_hfr_when_absent(self, spark):
        """Encounter without serviceProvider HFR → NULL care_site_source_value."""
        encounter_no_hfr = {
            "resourceType": "Encounter",
            "id": "enc-002-test",
            "subject": {"reference": "Patient/patient-001-test"},
            "class": {"code": "AMB"},
            "status": "finished",
        }
        df = make_fhir_df(spark, encounter_no_hfr)
        hfr_code = df.select(
            F.get_json_object(
                "resource_json", "$.serviceProvider.identifier.value"
            ).alias("hfr_code")
        ).collect()[0]["hfr_code"]

        assert hfr_code is None


# ---------------------------------------------------------------------------
# ABDM Consent profile detection
# ---------------------------------------------------------------------------

class TestAbdmConsentProfile:
    """
    ABDM Consent resources carry a specific meta.profile URL.
    The pipeline must detect these and route them to OMOP's
    observation table (or a custom consent table) — not discard them.
    """

    ABDM_CONSENT_PROFILE = "https://nrces.in/ndhm/fhir/r4/StructureDefinition/Consent"

    def _is_abdm_consent(self, df):
        """Extract consent profile flag using F.array_contains + F.lit (no SQL injection)."""
        return df.select(
            F.array_contains(
                F.from_json(
                    F.get_json_object("resource_json", "$.meta.profile"),
                    "array<string>"
                ),
                F.lit(self.ABDM_CONSENT_PROFILE)
            ).alias("is_abdm_consent")
        ).collect()[0]["is_abdm_consent"]

    def test_consent_profile_detected(self, spark):
        consent_resource = {
            "resourceType": "Consent",
            "id": "consent-001-test",
            "meta": {"profile": [self.ABDM_CONSENT_PROFILE]},
            "status": "active",
            "scope": {
                "coding": [{"system": "http://terminology.hl7.org/CodeSystem/consentscope",
                            "code": "patient-privacy"}]
            },
            "category": [{"coding": [{"code": "59284-0"}]}],
            "patient": {"reference": "Patient/patient-001-test"},
            "dateTime": "2020-03-15",
        }
        assert self._is_abdm_consent(make_fhir_df(spark, consent_resource)) is True

    def test_non_abdm_resource_not_flagged_as_consent(self, spark):
        """A standard Condition must not be misidentified as ABDM Consent."""
        standard_condition = {
            "resourceType": "Condition",
            "id": "condition-test",
            "meta": {"profile": ["http://hl7.org/fhir/StructureDefinition/Condition"]},
            "subject": {"reference": "Patient/patient-001-test"},
            "code": {"coding": [{"system": "http://snomed.info/sct", "code": "44054006"}]},
        }
        assert self._is_abdm_consent(make_fhir_df(spark, standard_condition)) is False
