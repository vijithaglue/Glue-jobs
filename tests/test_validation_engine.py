"""Unit and property tests for ValidationEngine."""

import re

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from glue_job.validation_engine import (
    VALIDATION_ERRORS_COL,
    split_valid_invalid,
    validate,
)
from glue_job.validation_rule import ValidationRule


# ---------------------------------------------------------------------------
# Unit tests (Task 2.6)
# ---------------------------------------------------------------------------


class TestValidateEmptyDataFrame:
    """validate() with an empty DataFrame."""

    def test_empty_df_returns_empty_with_errors_col(self, spark):
        schema = StructType([StructField("col_a", StringType(), True)])
        df = spark.createDataFrame([], schema)
        rules = [ValidationRule(rule_type="not_null", column="col_a")]
        result = validate(df, rules, ["col_a"])
        assert result.count() == 0
        assert VALIDATION_ERRORS_COL in result.columns


class TestValidateEmptyRules:
    """validate() with an empty rules list."""

    def test_empty_rules_all_valid(self, spark):
        df = spark.createDataFrame([("a",), ("b",)], ["col_a"])
        result = validate(df, [], ["col_a"])
        assert result.count() == 2
        # Every row should have an empty errors array
        for row in result.collect():
            assert row[VALIDATION_ERRORS_COL] == []


class TestValidateAllValid:
    """All records pass validation."""

    def test_all_valid_not_null(self, spark):
        df = spark.createDataFrame([("x",), ("y",)], ["col_a"])
        rules = [ValidationRule(rule_type="not_null", column="col_a")]
        result = validate(df, rules, ["col_a"])
        for row in result.collect():
            assert row[VALIDATION_ERRORS_COL] == []

    def test_all_valid_regex(self, spark):
        df = spark.createDataFrame([("123",), ("456",)], ["col_a"])
        rules = [ValidationRule(rule_type="regex", column="col_a", pattern=r"^\d+$")]
        result = validate(df, rules, ["col_a"])
        for row in result.collect():
            assert row[VALIDATION_ERRORS_COL] == []


class TestValidateAllInvalid:
    """All records fail validation."""

    def test_all_null(self, spark):
        schema = StructType([StructField("col_a", StringType(), True)])
        df = spark.createDataFrame([(None,), (None,)], schema)
        rules = [ValidationRule(rule_type="not_null", column="col_a")]
        result = validate(df, rules, ["col_a"])
        for row in result.collect():
            assert len(row[VALIDATION_ERRORS_COL]) == 1

    def test_all_empty_string(self, spark):
        df = spark.createDataFrame([("",), ("  ",)], ["col_a"])
        rules = [ValidationRule(rule_type="not_null", column="col_a")]
        result = validate(df, rules, ["col_a"])
        for row in result.collect():
            assert len(row[VALIDATION_ERRORS_COL]) == 1


class TestValidateMissingColumns:
    """Rules referencing missing columns are skipped."""

    def test_missing_column_skipped(self, spark):
        df = spark.createDataFrame([("x",)], ["col_a"])
        rules = [ValidationRule(rule_type="not_null", column="col_missing")]
        result = validate(df, rules, ["col_a"])
        assert result.count() == 1
        for row in result.collect():
            assert row[VALIDATION_ERRORS_COL] == []


class TestSplitValidInvalid:
    """split_valid_invalid() tests."""

    def test_mixed_split(self, spark):
        df = spark.createDataFrame([("x",), (None,)], ["col_a"])
        rules = [ValidationRule(rule_type="not_null", column="col_a")]
        validated = validate(df, rules, ["col_a"])
        valid_df, invalid_df = split_valid_invalid(validated)
        assert valid_df.count() == 1
        assert invalid_df.count() == 1
        assert VALIDATION_ERRORS_COL not in valid_df.columns
        assert VALIDATION_ERRORS_COL in invalid_df.columns

    def test_all_valid_split(self, spark):
        df = spark.createDataFrame([("a",), ("b",)], ["col_a"])
        rules = [ValidationRule(rule_type="not_null", column="col_a")]
        validated = validate(df, rules, ["col_a"])
        valid_df, invalid_df = split_valid_invalid(validated)
        assert valid_df.count() == 2
        assert invalid_df.count() == 0

    def test_all_invalid_split(self, spark):
        schema = StructType([StructField("col_a", StringType(), True)])
        df = spark.createDataFrame([(None,), (None,)], schema)
        rules = [ValidationRule(rule_type="not_null", column="col_a")]
        validated = validate(df, rules, ["col_a"])
        valid_df, invalid_df = split_valid_invalid(validated)
        assert valid_df.count() == 0
        assert invalid_df.count() == 2


# ---------------------------------------------------------------------------
# Property-based tests (Tasks 2.7 – 2.11)
# ---------------------------------------------------------------------------

# ---- Shared strategies ------------------------------------------------------

# Column names: simple identifiers
_col_name_st = st.text(
    alphabet=st.characters(whitelist_categories=("Ll",), whitelist_characters="_"),
    min_size=1,
    max_size=10,
).filter(lambda s: s[0].isalpha())

# Non-empty string values (guaranteed non-null, non-empty)
_non_empty_str_st = st.text(min_size=1, max_size=20).filter(lambda s: s.strip() != "")


# ---- Property 3: Record count preservation (Task 2.7) ----------------------

@st.composite
def _random_df_and_rules(draw):
    """Generate a random DataFrame spec (rows as list-of-dicts) and rule set."""
    n_cols = draw(st.integers(min_value=1, max_value=5))
    col_names = [f"c{i}" for i in range(n_cols)]
    n_rows = draw(st.integers(min_value=1, max_value=50))
    rows = []
    for _ in range(n_rows):
        row = {}
        for c in col_names:
            row[c] = draw(st.one_of(st.none(), st.text(max_size=10)))
        rows.append(row)
    # Generate rules that reference existing columns
    rules = []
    for c in col_names:
        if draw(st.booleans()):
            rules.append(ValidationRule(rule_type="not_null", column=c))
    return col_names, rows, rules


# Feature: s3-redshift-staging-dlq, Property 3: Validation preserves record count
# **Validates: Requirements 2.1**
@given(data=_random_df_and_rules())
@settings(max_examples=20, deadline=None)
def test_validate_preserves_record_count(data, spark):
    """validate() output has the same row count as input."""
    col_names, rows, rules = data
    schema = StructType([StructField(c, StringType(), True) for c in col_names])
    df = spark.createDataFrame([Row(**r) for r in rows], schema)
    result = validate(df, rules, col_names)
    assert result.count() == len(rows)


# ---- Property 4: not_null correctness (Task 2.8) ---------------------------

@st.composite
def _not_null_test_data(draw):
    """Generate a DataFrame with a mix of null, empty, and non-null values."""
    n_rows = draw(st.integers(min_value=1, max_value=50))
    values = []
    for _ in range(n_rows):
        values.append(
            draw(st.one_of(
                st.none(),
                st.just(""),
                st.just("   "),
                _non_empty_str_st,
            ))
        )
    return values


# Feature: s3-redshift-staging-dlq, Property 4: not_null rule correctly identifies null and empty values
# **Validates: Requirements 2.2, 5.2**
@given(values=_not_null_test_data())
@settings(max_examples=20, deadline=None)
def test_not_null_correctness(values, spark):
    """Errors appear iff column value is null or empty string."""
    col_name = "target"
    schema = StructType([StructField(col_name, StringType(), True)])
    df = spark.createDataFrame([(v,) for v in values], schema)
    rules = [ValidationRule(rule_type="not_null", column=col_name)]
    result = validate(df, rules, [col_name])
    rows = result.collect()
    for row, original_value in zip(rows, values):
        errors = row[VALIDATION_ERRORS_COL]
        should_fail = original_value is None or (
            isinstance(original_value, str) and original_value.strip() == ""
        )
        if should_fail:
            assert len(errors) == 1, f"Expected error for value {original_value!r}"
        else:
            assert len(errors) == 0, f"Unexpected error for value {original_value!r}"


# ---- Property 5: regex correctness (Task 2.9) ------------------------------

# Simple regex patterns that are safe for both Python re and Spark rlike
_simple_patterns = st.sampled_from([
    r"^\d+$",
    r"^[a-z]+$",
    r"^[A-Z]+$",
    r"^[a-zA-Z0-9]+$",
    r"^.+$",
])


@st.composite
def _regex_test_data(draw):
    """Generate values and a regex pattern for testing.

    Values are restricted to printable ASCII to avoid divergence between
    Python ``re`` and Java/Spark regex engines on exotic Unicode characters.
    """
    pattern = draw(_simple_patterns)
    n_rows = draw(st.integers(min_value=1, max_value=50))
    # Printable ASCII only – avoids Python/Java regex mismatch on control chars
    _printable_ascii = st.text(
        alphabet=st.characters(min_codepoint=32, max_codepoint=126),
        max_size=10,
    )
    values = []
    for _ in range(n_rows):
        values.append(
            draw(st.one_of(
                st.none(),
                _printable_ascii,
            ))
        )
    return pattern, values


# Feature: s3-redshift-staging-dlq, Property 5: regex rule correctly identifies non-matching values
# **Validates: Requirements 2.3, 5.3**
@given(data=_regex_test_data())
@settings(max_examples=20, deadline=None)
def test_regex_correctness(data, spark):
    """Errors appear iff column value does not match the pattern."""
    pattern, values = data
    col_name = "target"
    schema = StructType([StructField(col_name, StringType(), True)])
    df = spark.createDataFrame([(v,) for v in values], schema)
    rules = [ValidationRule(rule_type="regex", column=col_name, pattern=pattern)]
    result = validate(df, rules, [col_name])
    rows = result.collect()
    for row, original_value in zip(rows, values):
        errors = row[VALIDATION_ERRORS_COL]
        if original_value is None:
            # Null values should fail regex
            assert len(errors) == 1
        else:
            python_match = bool(re.match(pattern, original_value))
            if python_match:
                assert len(errors) == 0, (
                    f"Unexpected error for value {original_value!r} with pattern {pattern}"
                )
            else:
                assert len(errors) == 1, (
                    f"Expected error for value {original_value!r} with pattern {pattern}"
                )


# ---- Property 6: Split correctness (Task 2.10) -----------------------------

@st.composite
def _split_test_data(draw):
    """Generate DataFrame rows and rules for split testing."""
    n_rows = draw(st.integers(min_value=1, max_value=50))
    values = []
    for _ in range(n_rows):
        values.append(
            draw(st.one_of(
                st.none(),
                st.just(""),
                _non_empty_str_st,
            ))
        )
    # Mix of not_null and regex rules
    rules = [ValidationRule(rule_type="not_null", column="target")]
    if draw(st.booleans()):
        rules.append(
            ValidationRule(rule_type="regex", column="target", pattern=r"^[a-zA-Z]+$")
        )
    return values, rules


# Feature: s3-redshift-staging-dlq, Property 6: Validation split is a correct partition with complete error reports
# **Validates: Requirements 2.4, 2.5, 2.6**
@given(data=_split_test_data())
@settings(max_examples=20, deadline=None)
def test_split_correctness(data, spark):
    """valid + invalid row counts equal input; error arrays are empty/non-empty
    respectively; error count matches failed rule count."""
    values, rules = data
    col_name = "target"
    schema = StructType([StructField(col_name, StringType(), True)])
    df = spark.createDataFrame([(v,) for v in values], schema)
    validated = validate(df, rules, [col_name])
    valid_df, invalid_df = split_valid_invalid(validated)

    # Row counts sum to input
    assert valid_df.count() + invalid_df.count() == len(values)

    # Valid rows have no errors column
    assert VALIDATION_ERRORS_COL not in valid_df.columns

    # Invalid rows all have non-empty errors
    for row in invalid_df.collect():
        errors = row[VALIDATION_ERRORS_COL]
        assert len(errors) > 0

    # Verify error count matches number of failed rules per record
    validated_rows = validated.collect()
    for row, original_value in zip(validated_rows, values):
        errors = row[VALIDATION_ERRORS_COL]
        expected_failures = 0
        for rule in rules:
            if rule.rule_type == "not_null":
                if original_value is None or (
                    isinstance(original_value, str) and original_value.strip() == ""
                ):
                    expected_failures += 1
            elif rule.rule_type == "regex":
                if original_value is None or not re.match(rule.pattern, original_value):
                    expected_failures += 1
        assert len(errors) == expected_failures


# ---- Property 7: Missing column skip (Task 2.11) ---------------------------

@st.composite
def _missing_col_test_data(draw):
    """Generate rules referencing columns NOT in the DataFrame schema."""
    n_rows = draw(st.integers(min_value=1, max_value=30))
    existing_cols = ["existing_a", "existing_b"]
    values = []
    for _ in range(n_rows):
        values.append((
            draw(st.one_of(st.none(), st.text(max_size=10))),
            draw(st.one_of(st.none(), st.text(max_size=10))),
        ))
    # Rules that reference absent columns
    absent_col = draw(_col_name_st.filter(lambda c: c not in existing_cols))
    rules = [
        ValidationRule(rule_type="not_null", column=absent_col),
    ]
    if draw(st.booleans()):
        rules.append(
            ValidationRule(rule_type="regex", column=absent_col, pattern=r"^\d+$")
        )
    return existing_cols, values, rules


# Feature: s3-redshift-staging-dlq, Property 7: Rules referencing missing columns are skipped
# **Validates: Requirements 5.4**
@given(data=_missing_col_test_data())
@settings(max_examples=20, deadline=None)
def test_missing_column_skip(data, spark):
    """Rules referencing absent columns produce no errors and don't raise."""
    existing_cols, values, rules = data
    schema = StructType([StructField(c, StringType(), True) for c in existing_cols])
    df = spark.createDataFrame([Row(**dict(zip(existing_cols, v))) for v in values], schema)
    result = validate(df, rules, existing_cols)
    # No exceptions raised, and no errors produced
    assert result.count() == len(values)
    for row in result.collect():
        assert row[VALIDATION_ERRORS_COL] == []
