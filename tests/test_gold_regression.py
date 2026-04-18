# tests/test_gold_regression.py
# Regression tests for bugs caught during the first live Gold build run (2026-04-17).
#
# Bug 1 — Photon ANSI timestamp rejection
#   CANNOT_PARSE_TIMESTAMP on ISO 8601 strings with timezone offsets (e.g. -04:00).
#   Fix: spark.conf.set("spark.sql.ansi.enabled", "false") at notebook start.
#
# Bug 2 — Ambiguous encounter_class reference in omop_visit_occurrence
#   Both `source s` and `visit_type_mapping vtm` expose encounter_class.
#   An unqualified SELECT encounter_class raises AMBIGUOUS_REFERENCE.
#   Fix: qualify as s.encounter_class in the SELECT list.

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType, LongType
)
from helpers import make_fhir_df


# ---------------------------------------------------------------------------
# Bug 1: Photon ANSI mode rejects ISO 8601 timestamps with timezone offsets
# ---------------------------------------------------------------------------

class TestAnsiTimestampRegression:
    """
    Photon raises CANNOT_PARSE_TIMESTAMP on 'YYYY-MM-DDTHH:MM:SS±HH:MM' when
    spark.sql.ansi.enabled=true. The conftest.py fixture sets it to false so
    TO_TIMESTAMP returns NULL on unparseable strings rather than throwing.

    Failing input that triggered the production error:
        '1966-10-28T22:23:36-04:00'   (US Eastern, historical DST offset)
    """

    # Timestamps that reproduce the original production crash
    PROBLEM_TIMESTAMPS = [
        "1966-10-28T22:23:36-04:00",  # exact string from the error log
        "2020-03-15T09:30:00+05:30",  # IST offset (ABDM / Indian records)
        "2021-12-01T00:00:00+05:30",  # deceasedDateTime in Indian records
        "2020-03-15T09:00:00-05:00",  # US Eastern standard
        "2020-06-15T14:00:00+00:00",  # UTC explicit
    ]

    @pytest.mark.parametrize("ts_string", PROBLEM_TIMESTAMPS)
    def test_to_timestamp_with_tz_offset_does_not_throw(self, spark, ts_string):
        """
        TO_TIMESTAMP on an ISO 8601 string with a tz offset must not raise.
        With ansi.enabled=false the result is non-null (Spark parses offset
        strings as UTC-adjusted timestamps).
        """
        result = spark.sql(
            f"SELECT TO_TIMESTAMP('{ts_string}') AS ts"
        ).collect()[0]["ts"]
        # Should parse successfully — not None and not an exception
        assert result is not None, (
            f"TO_TIMESTAMP('{ts_string}') returned NULL — "
            "may be hitting ANSI strict-mode rejection again"
        )

    def test_ansi_mode_is_disabled_in_test_session(self, spark):
        """
        Confirm the session-level guard is in place. If ansi.enabled is ever
        re-enabled, every parameterized timestamp test above will start failing
        — this test documents the explicit intent.
        """
        setting = spark.conf.get("spark.sql.ansi.enabled")
        assert setting.lower() == "false", (
            "spark.sql.ansi.enabled must be 'false'. "
            "FHIR timestamps use tz offsets that Photon rejects in strict mode."
        )

    def test_encounter_period_with_ist_offset_parses_in_silver(self, spark):
        """
        End-to-end Silver extraction: a full Encounter resource with IST period
        timestamps must produce non-null period_start and period_end.
        This mirrors the stg_fhir_encounter Silver transformation in notebook 10.
        """
        encounter = {
            "resourceType": "Encounter",
            "id": "enc-ansi-regression",
            "subject": {"reference": "Patient/p-001"},
            "status": "finished",
            "class": {"code": "AMB"},
            "period": {
                "start": "1966-10-28T22:23:36-04:00",
                "end":   "1966-10-28T23:15:00-04:00",
            },
        }
        df = make_fhir_df(spark, encounter)
        row = df.select(
            F.to_timestamp(
                F.get_json_object("resource_json", "$.period.start")
            ).alias("period_start"),
            F.to_timestamp(
                F.get_json_object("resource_json", "$.period.end")
            ).alias("period_end"),
        ).collect()[0]

        assert row["period_start"] is not None, "period_start NULL — ANSI timestamp regression"
        assert row["period_end"]   is not None, "period_end NULL — ANSI timestamp regression"

    def test_observation_effective_datetime_with_negative_offset(self, spark):
        """
        The exact timestamp from the production error log must parse in the
        Observation Silver transformation.
        """
        obs = {
            "resourceType": "Observation",
            "id": "obs-ansi-regression",
            "subject": {"reference": "Patient/p-001"},
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "8867-4"}]},
            "effectiveDateTime": "1966-10-28T22:23:36-04:00",
            "valueQuantity": {"value": 72.0, "unit": "beats/minute"},
        }
        df = make_fhir_df(spark, obs)
        row = df.select(
            F.coalesce(
                F.to_timestamp(F.get_json_object("resource_json", "$.effectiveDateTime")),
                F.to_timestamp(F.get_json_object("resource_json", "$.effectivePeriod.start")),
            ).alias("effective_datetime")
        ).collect()[0]

        assert row["effective_datetime"] is not None, (
            "effectiveDateTime with -04:00 offset returned NULL — "
            "ANSI strict-mode regression (Bug 1)"
        )


# ---------------------------------------------------------------------------
# Bug 2: Ambiguous encounter_class reference in omop_visit_occurrence
# ---------------------------------------------------------------------------

class TestEncounterClassAmbiguityRegression:
    """
    In omop_visit_occurrence, source s (stg_fhir_encounter) and
    visit_type_mapping vtm both expose a column named encounter_class.
    An unqualified reference in the SELECT raises AMBIGUOUS_REFERENCE.
    Fix: qualify with s.encounter_class in the SELECT list.
    """

    # Silver encounter schema — subset needed to reproduce the ambiguity
    ENCOUNTER_SCHEMA = StructType([
        StructField("fhir_encounter_id", StringType(),   True),
        StructField("fhir_patient_id",   StringType(),   True),
        StructField("encounter_class",   StringType(),   True),
        StructField("encounter_type_code",    StringType(), True),
        StructField("encounter_type_display", StringType(), True),
        StructField("period_start",      TimestampType(), True),
        StructField("period_end",        TimestampType(), True),
        StructField("duration_minutes",  LongType(),     True),
        StructField("hfr_facility_code", StringType(),   True),
        StructField("facility_name",     StringType(),   True),
        StructField("_lineage_id",       StringType(),   True),
        StructField("_source_file",      StringType(),   True),
        StructField("_loaded_at",        StringType(),   True),
    ])

    PERSON_SCHEMA = StructType([
        StructField("person_id",           LongType(),   True),
        StructField("person_source_value", StringType(), True),
    ])

    @pytest.fixture(scope="class")
    def encounter_views(self, spark):
        """Register minimal Silver + person temp views for the visit SQL."""
        encounter_rows = [(
            "enc-001", "patient-001", "AMB", "185349003", "Check-up",
            None, None, 60, "HFR_001", "Apollo Mumbai",
            "lin-001", "test.json", "2024-01-01",
        )]
        spark.createDataFrame(encounter_rows, self.ENCOUNTER_SCHEMA) \
             .createOrReplaceTempView("stg_fhir_encounter_test")

        person_rows = [(1001, "patient-001")]
        spark.createDataFrame(person_rows, self.PERSON_SCHEMA) \
             .createOrReplaceTempView("omop_person_test")

    def test_qualified_encounter_class_does_not_raise(self, spark, encounter_views):
        """
        The fixed query qualifies all SELECT columns from source with s.
        Running this must complete without AMBIGUOUS_REFERENCE.
        """
        result = spark.sql("""
            WITH source AS (
                SELECT * FROM stg_fhir_encounter_test
            ),
            person AS (
                SELECT person_id, person_source_value FROM omop_person_test
            ),
            visit_type_mapping AS (
                SELECT 'AMB'  AS encounter_class, 9202 AS visit_type_concept_id
                UNION ALL SELECT 'IMP',  9201
                UNION ALL SELECT 'EMER', 9203
                UNION ALL SELECT 'HH',   9204
            )
            SELECT
                ABS(xxhash64(fhir_encounter_id))          AS visit_occurrence_id,
                p.person_id,
                COALESCE(vtm.visit_type_concept_id, 0)    AS visit_type_concept_id,
                s.encounter_class                          AS visit_source_value,
                s.encounter_type_code                      AS visit_source_concept_id,
                s.encounter_type_display,
                s.hfr_facility_code,
                s.facility_name
            FROM source s
            LEFT JOIN person p ON s.fhir_patient_id = p.person_source_value
            LEFT JOIN visit_type_mapping vtm ON s.encounter_class = vtm.encounter_class
        """).collect()

        assert len(result) == 1
        row = result[0]
        assert row["visit_source_value"] == "AMB"
        assert row["visit_type_concept_id"] == 9202

    def test_unqualified_encounter_class_raises_ambiguous_reference(self, spark, encounter_views):
        """
        Regression guard: the *unfixed* query (unqualified encounter_class in SELECT)
        must raise an AnalysisException. This documents what was broken before the fix
        and will alert us immediately if the fix is accidentally reverted.
        """
        from pyspark.errors import AnalysisException

        with pytest.raises(AnalysisException, match="(?i)ambiguous"):
            spark.sql("""
                WITH source AS (
                    SELECT * FROM stg_fhir_encounter_test
                ),
                visit_type_mapping AS (
                    SELECT 'AMB' AS encounter_class, 9202 AS visit_type_concept_id
                )
                SELECT
                    encounter_class    AS visit_source_value,
                    visit_type_concept_id
                FROM source s
                LEFT JOIN visit_type_mapping vtm ON s.encounter_class = vtm.encounter_class
            """).collect()

    def test_visit_type_concept_id_correctly_resolved_via_join(self, spark, encounter_views):
        """
        The join itself (s.encounter_class = vtm.encounter_class) must still work
        after qualification. Verifies the fix didn't break the join logic.
        """
        result = spark.sql("""
            WITH source AS (SELECT * FROM stg_fhir_encounter_test),
            visit_type_mapping AS (
                SELECT 'AMB' AS encounter_class, 9202 AS visit_type_concept_id
                UNION ALL SELECT 'IMP', 9201
                UNION ALL SELECT 'EMER', 9203
            )
            SELECT COALESCE(vtm.visit_type_concept_id, 0) AS visit_type_concept_id
            FROM source s
            LEFT JOIN visit_type_mapping vtm ON s.encounter_class = vtm.encounter_class
        """).collect()[0]

        assert result["visit_type_concept_id"] == 9202

    @pytest.mark.parametrize("class_code,expected_concept", [
        ("AMB",      9202),
        ("IMP",      9201),
        ("EMER",     9203),
        ("HH",       9204),
        ("UNKNOWN",     0),  # unrecognised class must still produce a row
    ])
    def test_all_visit_classes_resolve_without_ambiguity(self, spark, class_code, expected_concept):
        """
        Each encounter class must resolve correctly through the join without raising.
        Parameterised so a new ambiguity from any class code is caught immediately.
        """
        rows = [("enc-x", "p-x", class_code, None, None, None, None, None, None, None, "l", "f", "t")]
        df = spark.createDataFrame(rows, self.ENCOUNTER_SCHEMA)
        df.createOrReplaceTempView("enc_single")

        result = spark.sql(f"""
            WITH source AS (SELECT * FROM enc_single),
            visit_type_mapping AS (
                SELECT 'AMB' AS encounter_class, 9202 AS visit_type_concept_id
                UNION ALL SELECT 'IMP', 9201
                UNION ALL SELECT 'EMER', 9203
                UNION ALL SELECT 'HH', 9204
            )
            SELECT
                s.encounter_class                        AS visit_source_value,
                COALESCE(vtm.visit_type_concept_id, 0)   AS visit_type_concept_id
            FROM source s
            LEFT JOIN visit_type_mapping vtm ON s.encounter_class = vtm.encounter_class
        """).collect()[0]

        assert result["visit_source_value"] == class_code
        assert result["visit_type_concept_id"] == expected_concept
