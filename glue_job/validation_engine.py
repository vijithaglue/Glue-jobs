"""ValidationEngine — pure-function validation on PySpark DataFrames."""

import logging
from typing import List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from glue_job.validation_rule import ValidationRule

logger = logging.getLogger(__name__)

# Schema for a single validation error entry
_ERROR_ENTRY_SCHEMA = StructType([
    StructField("rule_name", StringType(), False),
    StructField("column", StringType(), False),
    StructField("value", StringType(), True),
])

# Schema for the _validation_errors array column
VALIDATION_ERRORS_SCHEMA = ArrayType(_ERROR_ENTRY_SCHEMA)

VALIDATION_ERRORS_COL = "_validation_errors"


def validate(
    df: DataFrame,
    rules: List[ValidationRule],
    schema_columns: List[str],
) -> DataFrame:
    """Apply all rules to *df*, adding a ``_validation_errors`` array column.

    Rules referencing columns not present in *schema_columns* are skipped with
    a warning.  Returns the original DataFrame with the additional column.
    """
    if not rules:
        logger.warning("No validation rules configured — treating all records as valid.")
        empty_array = F.array().cast(VALIDATION_ERRORS_SCHEMA)
        return df.withColumn(VALIDATION_ERRORS_COL, empty_array)

    error_columns = []
    for rule in rules:
        if rule.column not in schema_columns:
            logger.warning(
                "Column '%s' referenced by rule '%s' not found in schema — skipping.",
                rule.column,
                rule.rule_name,
            )
            continue

        if rule.rule_type == "not_null":
            error_columns.append(_eval_not_null(rule))
        elif rule.rule_type == "regex":
            error_columns.append(_eval_regex(rule))
        else:
            logger.warning("Unsupported rule type '%s' — skipping.", rule.rule_type)

    if not error_columns:
        empty_array = F.array().cast(VALIDATION_ERRORS_SCHEMA)
        return df.withColumn(VALIDATION_ERRORS_COL, empty_array)

    # Combine per-rule columns into a single array, filtering out nulls
    combined = F.array(*error_columns)
    filtered = F.filter(combined, lambda x: x.isNotNull())
    return df.withColumn(VALIDATION_ERRORS_COL, filtered)


# ---- Rule evaluators -------------------------------------------------------


def _eval_not_null(rule: ValidationRule):
    """Return a Column expression that produces an error struct when the column
    value is null or empty string, otherwise null."""
    col = F.col(rule.column)
    is_invalid = col.isNull() | (F.trim(col.cast("string")) == F.lit(""))
    error_struct = F.struct(
        F.lit(rule.rule_name).alias("rule_name"),
        F.lit(rule.column).alias("column"),
        col.cast("string").alias("value"),
    )
    return F.when(is_invalid, error_struct).otherwise(F.lit(None).cast(_ERROR_ENTRY_SCHEMA))


def _eval_regex(rule: ValidationRule):
    """Return a Column expression that produces an error struct when the column
    value does not match the regex pattern, otherwise null.

    Null values are treated as non-matching.
    """
    col = F.col(rule.column)
    str_col = col.cast("string")
    # A value fails if it is null OR does not match the pattern
    is_invalid = col.isNull() | ~str_col.rlike(rule.pattern)
    error_struct = F.struct(
        F.lit(rule.rule_name).alias("rule_name"),
        F.lit(rule.column).alias("column"),
        str_col.alias("value"),
    )
    return F.when(is_invalid, error_struct).otherwise(F.lit(None).cast(_ERROR_ENTRY_SCHEMA))


# ---- Split helper -----------------------------------------------------------


def split_valid_invalid(validated_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Split a validated DataFrame into ``(valid_df, invalid_df)``.

    * ``valid_df`` — rows where ``_validation_errors`` is empty; the column is
      dropped.
    * ``invalid_df`` — rows where ``_validation_errors`` is non-empty; the
      column is retained.
    """
    valid_df = (
        validated_df
        .filter(F.size(F.col(VALIDATION_ERRORS_COL)) == 0)
        .drop(VALIDATION_ERRORS_COL)
    )
    invalid_df = validated_df.filter(F.size(F.col(VALIDATION_ERRORS_COL)) > 0)
    return valid_df, invalid_df
