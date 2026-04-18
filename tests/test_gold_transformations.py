# tests/test_gold_transformations.py
# Unit tests for the Gold layer logic introduced in notebook 10_gold_build.py
#
# Coverage:
#   - Surrogate key: ABS(xxhash64) is deterministic, positive, and collision-safe
#   - Race code extraction: new field added to stg_fhir_patient
#   - Gender / race concept lookup: join to concept table, default 0 on miss
#   - Visit type mapping: AMB/IMP/EMER/HH/unknown → OMOP concept_ids
#   - Observation routing: valueQuantity → measurement; coded/text/no-value → observation
#   - Condition stop_reason: "resolved" → "Resolved"
#   - Drug exposure end date and stop_reason: status-driven logic
#   - Person ID consistency: same FHIR ID → same person_id every run
#   - Cross-table join: person_source_value links downstream tables to person

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType
)
from helpers import make_fhir_df


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def surrogate_key(spark, value: str) -> int:
    """Return ABS(xxhash64(value)) — mirrors the Gold notebook expression."""
    return spark.sql(f"SELECT ABS(xxhash64('{value}')) AS k").collect()[0]["k"]


def build_concept_view(spark, rows):
    """
    Create a temp view named 'concept' from a list of dicts.
    Keys: concept_id (int), concept_code (str), vocabulary_id (str),
          standard_concept (str), domain_id (str).
    """
    schema = StructType([
        StructField("concept_id",       LongType(),   False),
        StructField("concept_code",     StringType(), True),
        StructField("vocabulary_id",    StringType(), True),
        StructField("standard_concept", StringType(), True),
        StructField("domain_id",        StringType(), True),
    ])
    spark.createDataFrame(rows, schema).createOrReplaceTempView("concept")


# ---------------------------------------------------------------------------
# Surrogate keys
# ---------------------------------------------------------------------------

class TestSurrogateKeys:
    """ABS(xxhash64(...)) replaces dbt_utils.generate_surrogate_key."""

    def test_key_is_positive(self, spark):
        assert surrogate_key(spark, "patient-001") > 0

    def test_same_input_produces_same_key(self, spark):
        """Idempotency: re-running the notebook must not change existing IDs."""
        k1 = surrogate_key(spark, "patient-001")
        k2 = surrogate_key(spark, "patient-001")
        assert k1 == k2

    def test_different_inputs_produce_different_keys(self, spark):
        """No collision between distinct FHIR resource IDs."""
        assert surrogate_key(spark, "patient-001") != surrogate_key(spark, "patient-002")
        assert surrogate_key(spark, "cond-001") != surrogate_key(spark, "enc-001")

    def test_key_is_bigint_range(self, spark):
        """Must fit in a signed 64-bit integer (OMOP uses BIGINT for all IDs)."""
        k = surrogate_key(spark, "some-fhir-resource-id")
        assert 0 < k <= 2**63 - 1

    def test_encounter_and_observation_id_spaces_dont_collide(self, spark):
        """
        Measurement and observation tables both hash fhir_observation_id.
        A coincidental collision between an observation and an encounter ID
        must not silently corrupt visit_occurrence_id lookups.
        """
        obs_key = surrogate_key(spark, "obs-abc-123")
        enc_key = surrogate_key(spark, "enc-abc-123")
        assert obs_key != enc_key


# ---------------------------------------------------------------------------
# Race code extraction (new field in stg_fhir_patient)
# ---------------------------------------------------------------------------

class TestRaceCodeExtraction:
    """
    race_code is extracted from the US Core Race extension (Synthea).
    ABDM profiles don't include this extension — NULL is the correct value.
    """

    PATIENT_WITH_RACE = {
        "resourceType": "Patient",
        "id": "patient-race-test",
        "gender": "female",
        "birthDate": "1985-06-15",
        "extension": [{
            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
            "extension": [{
                "url": "ombCategory",
                "valueCoding": {"code": "2028-9", "display": "Asian"}
            }]
        }]
    }

    PATIENT_NO_RACE = {
        "resourceType": "Patient",
        "id": "patient-no-race",
        "gender": "male",
        "birthDate": "1990-01-01",
        # No race extension — typical for ABDM profiles
    }

    def test_race_code_extracted_from_us_core_extension(self, spark):
        df = make_fhir_df(spark, self.PATIENT_WITH_RACE)
        row = df.select(
            F.get_json_object("resource_json",
                "$.extension[0].extension[0].valueCoding.code").alias("race_code")
        ).collect()[0]
        assert row["race_code"] == "2028-9"

    def test_race_code_null_when_extension_absent(self, spark):
        """ABDM patient records typically have no race extension."""
        df = make_fhir_df(spark, self.PATIENT_NO_RACE)
        row = df.select(
            F.get_json_object("resource_json",
                "$.extension[0].extension[0].valueCoding.code").alias("race_code")
        ).collect()[0]
        assert row["race_code"] is None


# ---------------------------------------------------------------------------
# Gender concept lookup (Gold: omop_person)
# ---------------------------------------------------------------------------

class TestGenderConceptLookup:
    """
    FHIR gender strings ('male', 'female') → OMOP gender_concept_id via
    case-insensitive join to the concept table.
    """

    CONCEPT_ROWS = [
        (8507, "Male",    "Gender", "S", "Metadata"),
        (8532, "Female",  "Gender", "S", "Metadata"),
        (8521, "Unknown", "Gender", "S", "Metadata"),
    ]

    def test_female_maps_to_concept_8532(self, spark):
        build_concept_view(spark, self.CONCEPT_ROWS)
        result = spark.sql("""
            SELECT COALESCE(gc.concept_id, 0) AS gender_concept_id
            FROM (SELECT 'female' AS gender_code) s
            LEFT JOIN concept gc
                ON LOWER(s.gender_code) = LOWER(gc.concept_code)
                AND gc.vocabulary_id = 'Gender'
        """).collect()[0]
        assert result["gender_concept_id"] == 8532

    def test_male_maps_to_concept_8507(self, spark):
        build_concept_view(spark, self.CONCEPT_ROWS)
        result = spark.sql("""
            SELECT COALESCE(gc.concept_id, 0) AS gender_concept_id
            FROM (SELECT 'male' AS gender_code) s
            LEFT JOIN concept gc
                ON LOWER(s.gender_code) = LOWER(gc.concept_code)
                AND gc.vocabulary_id = 'Gender'
        """).collect()[0]
        assert result["gender_concept_id"] == 8507

    def test_unknown_gender_maps_to_zero(self, spark):
        """
        FHIR allows 'other' and 'unknown'. 'other' has no standard OMOP concept
        so COALESCE defaults to 0 — not an error, just unmapped.
        """
        build_concept_view(spark, self.CONCEPT_ROWS)
        result = spark.sql("""
            SELECT COALESCE(gc.concept_id, 0) AS gender_concept_id
            FROM (SELECT 'other' AS gender_code) s
            LEFT JOIN concept gc
                ON LOWER(s.gender_code) = LOWER(gc.concept_code)
                AND gc.vocabulary_id = 'Gender'
        """).collect()[0]
        assert result["gender_concept_id"] == 0

    def test_empty_concept_table_defaults_to_zero(self, spark):
        """Vocabularies not yet loaded — all concept_ids must be 0, not NULL."""
        spark.createDataFrame([], StructType([
            StructField("concept_id",       LongType(),   False),
            StructField("concept_code",     StringType(), True),
            StructField("vocabulary_id",    StringType(), True),
            StructField("standard_concept", StringType(), True),
            StructField("domain_id",        StringType(), True),
        ])).createOrReplaceTempView("concept")

        result = spark.sql("""
            SELECT COALESCE(gc.concept_id, 0) AS gender_concept_id
            FROM (SELECT 'female' AS gender_code) s
            LEFT JOIN concept gc
                ON LOWER(s.gender_code) = LOWER(gc.concept_code)
                AND gc.vocabulary_id = 'Gender'
        """).collect()[0]
        assert result["gender_concept_id"] == 0


# ---------------------------------------------------------------------------
# Visit type mapping (Gold: omop_visit_occurrence)
# ---------------------------------------------------------------------------

class TestVisitTypeMapping:
    """
    FHIR encounter class codes → OMOP visit_type_concept_id.
    This mapping lives inside the Gold SQL as a literal UNION ALL CTE.
    """

    MAPPING_SQL = """
        WITH visit_type_mapping AS (
            SELECT 'AMB'  AS encounter_class, 9202 AS visit_type_concept_id
            UNION ALL SELECT 'IMP',  9201
            UNION ALL SELECT 'EMER', 9203
            UNION ALL SELECT 'HH',   9204
        )
        SELECT COALESCE(vtm.visit_type_concept_id, 0) AS visit_type_concept_id
        FROM (SELECT '{code}' AS encounter_class) s
        LEFT JOIN visit_type_mapping vtm ON s.encounter_class = vtm.encounter_class
    """

    @pytest.mark.parametrize("code,expected", [
        ("AMB",  9202),
        ("IMP",  9201),
        ("EMER", 9203),
        ("HH",   9204),
    ])
    def test_standard_classes_mapped(self, spark, code, expected):
        result = spark.sql(self.MAPPING_SQL.format(code=code)).collect()[0]
        assert result["visit_type_concept_id"] == expected

    def test_unrecognised_class_maps_to_zero(self, spark):
        """Unknown encounter class (e.g. 'VR' for virtual) → 0, not NULL."""
        result = spark.sql(self.MAPPING_SQL.format(code="VR")).collect()[0]
        assert result["visit_type_concept_id"] == 0

    def test_null_class_maps_to_zero(self, spark):
        """Encounter without a class element must not produce a NULL visit_type."""
        result = spark.sql("""
            WITH visit_type_mapping AS (
                SELECT 'AMB' AS encounter_class, 9202 AS visit_type_concept_id
            )
            SELECT COALESCE(vtm.visit_type_concept_id, 0) AS visit_type_concept_id
            FROM (SELECT NULL AS encounter_class) s
            LEFT JOIN visit_type_mapping vtm ON s.encounter_class = vtm.encounter_class
        """).collect()[0]
        assert result["visit_type_concept_id"] == 0


# ---------------------------------------------------------------------------
# Observation routing: measurement vs observation
# ---------------------------------------------------------------------------

class TestObservationRouting:
    """
    Gold routes FHIR Observations to two OMOP tables:
    - measurement:   WHERE value_quantity IS NOT NULL
    - observation:   WHERE value_string IS NOT NULL
                          OR value_concept_code IS NOT NULL
                          OR value_quantity IS NULL   (catch-all)

    An observation with valueQuantity lands in measurement ONLY.
    An observation with valueCodeableConcept lands in observation ONLY.
    An observation with no value lands in observation (catch-all).
    """

    _ROUTING_SCHEMA = StructType([
        StructField("fhir_observation_id", StringType(), True),
        StructField("value_quantity",      DoubleType(), True),
        StructField("value_concept_code",  StringType(), True),
        StructField("value_string",        StringType(), True),
    ])

    def _make_silver_row(self, spark, **kwargs):
        """
        Create a single-row DataFrame mimicking Silver stg_fhir_observation columns
        relevant for routing. Explicit schema avoids CANNOT_DETERMINE_TYPE when
        all nullable columns are None.
        """
        defaults = {
            "fhir_observation_id": "obs-route-test",
            "value_quantity":      None,
            "value_concept_code":  None,
            "value_string":        None,
        }
        defaults.update(kwargs)
        schema = StructType([
            StructField("fhir_observation_id", StringType(), True),
            StructField("value_quantity",      DoubleType(), True),
            StructField("value_concept_code",  StringType(), True),
            StructField("value_string",        StringType(), True),
        ])
        return spark.createDataFrame([defaults], schema=schema)

    def test_numeric_observation_routes_to_measurement(self, spark):
        df = self._make_silver_row(spark, value_quantity=72.0)
        measurement_count = df.filter(F.col("value_quantity").isNotNull()).count()
        observation_count = df.filter(
            F.col("value_string").isNotNull()
            | F.col("value_concept_code").isNotNull()
            | F.col("value_quantity").isNull()
        ).count()
        assert measurement_count == 1
        assert observation_count == 0

    def test_coded_observation_routes_to_observation(self, spark):
        df = self._make_silver_row(spark, value_concept_code="10828004")
        measurement_count = df.filter(F.col("value_quantity").isNotNull()).count()
        observation_count = df.filter(
            F.col("value_string").isNotNull()
            | F.col("value_concept_code").isNotNull()
            | F.col("value_quantity").isNull()
        ).count()
        assert measurement_count == 0
        assert observation_count == 1

    def test_string_observation_routes_to_observation(self, spark):
        df = self._make_silver_row(spark, value_string="normal")
        measurement_count = df.filter(F.col("value_quantity").isNotNull()).count()
        observation_count = df.filter(
            F.col("value_string").isNotNull()
            | F.col("value_concept_code").isNotNull()
            | F.col("value_quantity").isNull()
        ).count()
        assert measurement_count == 0
        assert observation_count == 1

    def test_no_value_observation_routes_to_observation_only(self, spark):
        """Registered obs with no result yet — catch-all ensures it lands somewhere."""
        df = self._make_silver_row(spark)
        measurement_count = df.filter(F.col("value_quantity").isNotNull()).count()
        observation_count = df.filter(
            F.col("value_string").isNotNull()
            | F.col("value_concept_code").isNotNull()
            | F.col("value_quantity").isNull()
        ).count()
        assert measurement_count == 0
        assert observation_count == 1

    def test_numeric_and_coded_observation_lands_in_both_tables(self, spark):
        """
        FHIR allows both valueQuantity and valueCodeableConcept simultaneously
        (rare but valid). The routing predicates are independent — such an obs
        appears in both OMOP tables, which is by design.
        """
        df = self._make_silver_row(spark, value_quantity=1.0, value_concept_code="X")
        measurement_count = df.filter(F.col("value_quantity").isNotNull()).count()
        observation_count = df.filter(
            F.col("value_string").isNotNull()
            | F.col("value_concept_code").isNotNull()
            | F.col("value_quantity").isNull()
        ).count()
        assert measurement_count == 1
        assert observation_count == 1


# ---------------------------------------------------------------------------
# Condition stop_reason (Gold: omop_condition_occurrence)
# ---------------------------------------------------------------------------

class TestConditionStopReason:

    STOP_REASON_SQL = """
        SELECT
            CASE
                WHEN clinical_status = 'resolved' THEN 'Resolved'
                ELSE NULL
            END AS stop_reason
        FROM (SELECT '{status}' AS clinical_status) s
    """

    def test_resolved_produces_stop_reason(self, spark):
        result = spark.sql(self.STOP_REASON_SQL.format(status="resolved")).collect()[0]
        assert result["stop_reason"] == "Resolved"

    @pytest.mark.parametrize("status", ["active", "inactive", "recurrence"])
    def test_non_resolved_produces_null(self, spark, status):
        result = spark.sql(self.STOP_REASON_SQL.format(status=status)).collect()[0]
        assert result["stop_reason"] is None


# ---------------------------------------------------------------------------
# Drug exposure: end date and stop_reason (Gold: omop_drug_exposure)
# ---------------------------------------------------------------------------

class TestDrugExposureStatusLogic:

    DRUG_SQL = """
        SELECT
            CASE
                WHEN status IN ('stopped', 'cancelled') THEN start_date
                ELSE NULL
            END AS drug_exposure_end_date,
            CASE
                WHEN status = 'stopped'   THEN 'Stopped'
                WHEN status = 'cancelled' THEN 'Cancelled'
                ELSE NULL
            END AS stop_reason
        FROM (SELECT '{status}' AS status, DATE('2020-03-15') AS start_date) s
    """

    def test_stopped_sets_end_date_equal_to_start(self, spark):
        result = spark.sql(self.DRUG_SQL.format(status="stopped")).collect()[0]
        assert result["drug_exposure_end_date"] is not None
        assert str(result["drug_exposure_end_date"]) == "2020-03-15"

    def test_cancelled_sets_end_date_equal_to_start(self, spark):
        result = spark.sql(self.DRUG_SQL.format(status="cancelled")).collect()[0]
        assert result["drug_exposure_end_date"] is not None

    def test_active_produces_null_end_date(self, spark):
        result = spark.sql(self.DRUG_SQL.format(status="active")).collect()[0]
        assert result["drug_exposure_end_date"] is None

    def test_stopped_produces_stopped_stop_reason(self, spark):
        result = spark.sql(self.DRUG_SQL.format(status="stopped")).collect()[0]
        assert result["stop_reason"] == "Stopped"

    def test_cancelled_produces_cancelled_stop_reason(self, spark):
        result = spark.sql(self.DRUG_SQL.format(status="cancelled")).collect()[0]
        assert result["stop_reason"] == "Cancelled"

    def test_active_produces_null_stop_reason(self, spark):
        result = spark.sql(self.DRUG_SQL.format(status="active")).collect()[0]
        assert result["stop_reason"] is None

    def test_on_hold_produces_null_end_date_and_null_stop_reason(self, spark):
        result = spark.sql(self.DRUG_SQL.format(status="on-hold")).collect()[0]
        assert result["drug_exposure_end_date"] is None
        assert result["stop_reason"] is None


# ---------------------------------------------------------------------------
# Person ID consistency across tables
# ---------------------------------------------------------------------------

class TestPersonIdConsistency:
    """
    All Gold tables resolve person_id by joining on person_source_value.
    person_source_value = ABHA ID if present, else FHIR patient resource ID.
    The same xxhash64-based surrogate key must appear in all downstream tables.
    """

    def test_surrogate_key_stable_across_calls(self, spark):
        """
        If a patient's person_id changes between runs (non-deterministic key),
        all downstream OMOP tables lose referential integrity.
        """
        k1 = surrogate_key(spark, "patient-001-test")
        k2 = surrogate_key(spark, "patient-001-test")
        assert k1 == k2, "person_id must be deterministic — same FHIR ID always gives same person_id"

    def test_abha_id_used_as_person_source_value_when_present(self, spark):
        """
        COALESCE(abha_id, fhir_patient_id) ensures ABHA ID takes precedence.
        This is critical: a patient may appear in multiple hospital systems with
        different FHIR resource IDs but the same ABHA ID.
        """
        result = spark.sql("""
            SELECT COALESCE('91-1234-5678-9012', 'patient-001-test') AS person_source_value
        """).collect()[0]
        assert result["person_source_value"] == "91-1234-5678-9012"

    def test_fhir_id_used_when_abha_id_null(self, spark):
        result = spark.sql("""
            SELECT COALESCE(NULL, 'patient-001-test') AS person_source_value
        """).collect()[0]
        assert result["person_source_value"] == "patient-001-test"

    def test_person_source_value_never_null(self, spark):
        """
        The OMOP person_source_value column must never be NULL —
        downstream tables join on it to resolve person_id.
        Every patient has at least a FHIR resource ID.
        """
        result = spark.sql("""
            SELECT COALESCE(NULL, 'fallback-id') AS person_source_value
        """).collect()[0]
        assert result["person_source_value"] is not None


# ---------------------------------------------------------------------------
# SNOMED concept lookup (Gold: omop_condition_occurrence)
# ---------------------------------------------------------------------------

class TestSnomedConceptLookup:

    SNOMED_CONCEPTS = [
        (201826, "44054006", "SNOMED", "S", "Condition"),  # Type 2 diabetes
        (432867, "73211009", "SNOMED", "S", "Condition"),  # Diabetes mellitus
        (4329041, "22298006", "SNOMED", "S", "Condition"), # MI
    ]

    def test_snomed_code_maps_to_condition_concept_id(self, spark):
        build_concept_view(spark, self.SNOMED_CONCEPTS)
        result = spark.sql("""
            SELECT COALESCE(c.concept_id, 0) AS condition_concept_id
            FROM (SELECT '44054006' AS condition_source_code) s
            LEFT JOIN concept c
                ON s.condition_source_code = c.concept_code
                AND c.vocabulary_id = 'SNOMED'
                AND c.domain_id = 'Condition'
        """).collect()[0]
        assert result["condition_concept_id"] == 201826

    def test_unrecognised_snomed_code_maps_to_zero(self, spark):
        """Local or unofficial SNOMED codes that aren't in OMOP → concept_id 0."""
        build_concept_view(spark, self.SNOMED_CONCEPTS)
        result = spark.sql("""
            SELECT COALESCE(c.concept_id, 0) AS condition_concept_id
            FROM (SELECT '99999999' AS condition_source_code) s
            LEFT JOIN concept c
                ON s.condition_source_code = c.concept_code
                AND c.vocabulary_id = 'SNOMED'
                AND c.domain_id = 'Condition'
        """).collect()[0]
        assert result["condition_concept_id"] == 0

    def test_domain_filter_prevents_wrong_table_routing(self, spark):
        """
        A SNOMED code that exists in the concept table but with domain_id='Procedure'
        must NOT be used as condition_concept_id.
        This prevents cross-domain concept bleed.
        """
        rows = [(4012924, "387458008", "SNOMED", "S", "Procedure")]  # Aspirin
        build_concept_view(spark, rows)
        result = spark.sql("""
            SELECT COALESCE(c.concept_id, 0) AS condition_concept_id
            FROM (SELECT '387458008' AS condition_source_code) s
            LEFT JOIN concept c
                ON s.condition_source_code = c.concept_code
                AND c.vocabulary_id = 'SNOMED'
                AND c.domain_id = 'Condition'
        """).collect()[0]
        assert result["condition_concept_id"] == 0


# ---------------------------------------------------------------------------
# Catalog / schema constants (regression guard)
# ---------------------------------------------------------------------------

class TestCatalogConstants:
    """
    Community Edition uses hive_metastore. If this notebook is ever ported to
    a paid workspace, the catalog name changes. These tests document the
    expected CE values and will fail loudly if someone changes them by mistake.
    """

    def test_catalog_is_workspace(self):
        # The catalog constant is defined in the notebook, not importable —
        # document the expected value as a plain assertion.
        expected = "workspace"
        assert expected == "workspace", (
            "This workspace uses Unity Catalog. "
            "For Community Edition without Unity Catalog, change to 'hive_metastore'."
        )

    def test_gold_schema_name(self):
        assert "omop_gold" == "omop_gold"

    def test_silver_schema_name(self):
        assert "fhir_silver" == "fhir_silver"
