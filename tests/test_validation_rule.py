"""Unit and property tests for ValidationRule dataclass and parser."""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from glue_job.validation_rule import ValidationRule, parse_rules


# ---------------------------------------------------------------------------
# Unit tests (Task 1.4)
# ---------------------------------------------------------------------------


class TestFromDictValid:
    """Test from_dict with valid dictionaries."""

    def test_not_null_rule(self):
        rule = ValidationRule.from_dict({"rule_type": "not_null", "column": "vendor_id"})
        assert rule.rule_type == "not_null"
        assert rule.column == "vendor_id"
        assert rule.pattern is None
        assert rule.rule_name == "not_null_vendor_id"

    def test_regex_rule(self):
        rule = ValidationRule.from_dict(
            {"rule_type": "regex", "column": "vendor_id", "pattern": r"^\d+$"}
        )
        assert rule.rule_type == "regex"
        assert rule.column == "vendor_id"
        assert rule.pattern == r"^\d+$"
        assert rule.rule_name == "regex_vendor_id"

    def test_custom_rule_name(self):
        rule = ValidationRule.from_dict(
            {"rule_type": "not_null", "column": "col", "rule_name": "my_rule"}
        )
        assert rule.rule_name == "my_rule"

    def test_auto_generated_rule_name(self):
        rule = ValidationRule.from_dict({"rule_type": "not_null", "column": "col"})
        assert rule.rule_name == "not_null_col"


class TestFromDictMissingFields:
    """Test from_dict with missing required fields."""

    def test_missing_rule_type(self):
        with pytest.raises(ValueError, match="rule_type"):
            ValidationRule.from_dict({"column": "vendor_id"})

    def test_missing_column(self):
        with pytest.raises(ValueError, match="column"):
            ValidationRule.from_dict({"rule_type": "not_null"})

    def test_empty_dict(self):
        with pytest.raises(ValueError):
            ValidationRule.from_dict({})


class TestFromDictUnknownRuleType:
    """Test from_dict with unknown rule types."""

    def test_unknown_type(self):
        with pytest.raises(ValueError, match="Unknown rule type"):
            ValidationRule.from_dict({"rule_type": "unknown", "column": "col"})

    def test_typo_type(self):
        with pytest.raises(ValueError, match="Unknown rule type"):
            ValidationRule.from_dict({"rule_type": "notnull", "column": "col"})


class TestParseRules:
    """Test parse_rules function."""

    def test_multiple_rules(self):
        rules = parse_rules([
            {"rule_type": "not_null", "column": "vendor_id"},
            {"rule_type": "regex", "column": "vendor_id", "pattern": r"^\d+$"},
        ])
        assert len(rules) == 2
        assert rules[0].rule_type == "not_null"
        assert rules[1].rule_type == "regex"

    def test_empty_list(self):
        assert parse_rules([]) == []


# ---------------------------------------------------------------------------
# Property-based test (Task 1.5)
# ---------------------------------------------------------------------------

# Strategy for generating valid rule dictionaries
rule_type_strategy = st.sampled_from(["not_null", "regex"])
column_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="_"),
    min_size=1,
    max_size=30,
)
pattern_strategy = st.text(min_size=1, max_size=30)


@st.composite
def valid_rule_dict(draw):
    """Generate a valid rule dictionary."""
    rule_type = draw(rule_type_strategy)
    column = draw(column_strategy)
    d = {"rule_type": rule_type, "column": column}
    if rule_type == "regex":
        d["pattern"] = draw(pattern_strategy)
    elif draw(st.booleans()):
        # Optionally add a pattern even for not_null
        d["pattern"] = draw(st.one_of(st.none(), pattern_strategy))
    return d


# Feature: s3-redshift-staging-dlq, Property 8: ValidationRule parsing round-trip
# **Validates: Requirements 5.1**
@given(d=valid_rule_dict())
@settings(max_examples=20)
def test_validation_rule_parsing_round_trip(d):
    """For any valid rule dict, from_dict produces a ValidationRule whose
    rule_type, column, and pattern fields match the input dictionary values."""
    rule = ValidationRule.from_dict(d)
    assert rule.rule_type == d["rule_type"]
    assert rule.column == d["column"]
    assert rule.pattern == d.get("pattern")
