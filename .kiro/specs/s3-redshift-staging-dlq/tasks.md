# Tasks

## Task 1: Create ValidationRule dataclass and parser

- [x] 1.1 Create `glue_job/validation_rule.py` with `ValidationRule` dataclass containing fields: `rule_type`, `column`, `pattern` (optional), `rule_name` (auto-generated if omitted)
- [x] 1.2 Implement `ValidationRule.from_dict(d: dict) -> ValidationRule` class method that parses a rule dictionary into a `ValidationRule` instance
- [x] 1.3 Implement `parse_rules(rules_list: List[dict]) -> List[ValidationRule]` function that converts a list of rule dicts into `ValidationRule` instances
- [x] 1.4 Write unit tests in `tests/test_validation_rule.py` for `from_dict` with valid dicts, missing fields, and unknown rule types
- [x] 1.5 Write property test (Hypothesis) for ValidationRule parsing round-trip: for any valid rule dict, `from_dict` produces a `ValidationRule` whose fields match the input (Property 8)

## Task 2: Implement ValidationEngine

- [x] 2.1 Create `glue_job/validation_engine.py` with `validate(df, rules, schema_columns) -> DataFrame` function that applies all rules in a single pass, adding a `_validation_errors` array column
- [x] 2.2 Implement `not_null` rule evaluation: check for null or empty string values in the specified column
- [x] 2.3 Implement `regex` rule evaluation: check column values against the provided regex pattern
- [x] 2.4 Implement missing-column handling: log warning and skip rules referencing columns not in the DataFrame schema
- [x] 2.5 Implement `split_valid_invalid(validated_df) -> Tuple[DataFrame, DataFrame]` that splits into valid (empty errors, column dropped) and invalid (non-empty errors, column retained) DataFrames
- [x] 2.6 Write unit tests in `tests/test_validation_engine.py` for edge cases: empty DataFrame, empty rules list, all-valid data, all-invalid data, missing columns
- [x] 2.7 Write property test for record count preservation: validate() output has same row count as input (Property 3)
- [x] 2.8 Write property test for not_null correctness: errors appear iff column value is null or empty string (Property 4)
- [x] 2.9 Write property test for regex correctness: errors appear iff column value does not match pattern (Property 5)
- [x] 2.10 Write property test for split correctness: valid + invalid row counts equal input, error arrays are empty/non-empty respectively, error count matches failed rule count (Property 6)
- [x] 2.11 Write property test for missing column skip: rules referencing absent columns produce no errors and don't raise exceptions (Property 7)

## Task 3: Implement DLQ Writer

- [x] 3.1 Create `glue_job/dlq_writer.py` with `build_dlq_path(dlq_base_path, job_run_date) -> str` function that constructs the partitioned S3 path
  - Path pattern: `<dlq_base_path>/year=YYYY/month=MM/day=DD/`
  - _Requirements: 4.3_

- [x] 3.2 Implement `write_dlq(invalid_df, dlq_base_path, job_run_date) -> int` function that writes invalid records as JSON to the partitioned path and returns the record count
  - Write in JSON format with original fields + `_validation_errors`
  - _Requirements: 4.1, 4.2_

- [ ]* 3.3 Write unit tests in `tests/test_dlq_writer.py` for path construction with various dates and edge dates
  - _Requirements: 4.3_

- [ ]* 3.4 Write property test for DLQ path construction: for any date, path matches `<base>/year=YYYY/month=MM/day=DD/` pattern (Property 10)
  - **Property 10: DLQ path construction follows date partition pattern**
  - **Validates: Requirements 4.3**

- [ ]* 3.5 Write property test for DLQ JSON round-trip: writing and reading back preserves all fields including `_validation_errors` (Property 9)
  - **Property 9: DLQ JSON output preserves all fields**
  - **Validates: Requirements 4.2**

## Task 4: Implement Staging Merger SQL builders

- [x] 4.1 Create `glue_job/staging_merger.py` with `build_merge_preactions(staging_table, target_table) -> str` that generates DROP + CREATE SQL
  - SQL: `DROP TABLE IF EXISTS <staging>; CREATE TABLE <staging> (LIKE <target>);`
  - _Requirements: 1.2_

- [x] 4.2 Implement `build_merge_postactions(staging_table, target_table) -> str` that generates transactional DELETE + INSERT + DROP SQL
  - SQL: `BEGIN TRANSACTION; DELETE FROM <target>; INSERT INTO <target> SELECT * FROM <staging>; DROP TABLE IF EXISTS <staging>; END TRANSACTION;`
  - _Requirements: 3.3_

- [ ]* 4.3 Write unit tests in `tests/test_staging_merger.py` for preactions and postactions with standard table names
  - _Requirements: 1.2, 3.3_

- [ ]* 4.4 Write property test for preactions SQL: contains DROP and CREATE with correct table names (Property 1)
  - **Property 1: Preactions SQL contains DROP and CREATE with correct table names**
  - **Validates: Requirements 1.2**

- [ ]* 4.5 Write property test for postactions SQL: contains BEGIN TRANSACTION, DELETE, INSERT, DROP, END TRANSACTION in order (Property 2)
  - **Property 2: Postactions SQL contains a valid transaction block**
  - **Validates: Requirements 3.3**

## Task 5: Checkpoint - Validate components before integration

- [x] 5. Ensure all tests pass, ask the user if questions arise.

## Task 6: Refactor s3toredshift.py job orchestration

- [x] 6.1 Refactor `glue_job/s3toredshift.py` to extract a `run_job()` function that accepts glue_context, spark, args, and validation_rules parameters
  - Move existing top-level logic into `run_job()`
  - Keep module-level constants (SOURCE_PATH, REDSHIFT_CONNECTION, etc.)
  - _Requirements: 1.1, 1.2_

- [x] 6.2 Define default `VALIDATION_RULES` configuration list and `DLQ_PATH` constant in the job script
  - Rules: not_null on vendor_id, not_null on pickup_datetime, regex on vendor_id
  - DLQ path: `s3://<bucket>/dlq/s3toredshift/`
  - _Requirements: 5.1, 5.2, 5.3_

- [x] 6.3 Integrate ValidationEngine: after staging load, call `validate()` and `split_valid_invalid()` on the staged DataFrame
  - Log total, valid, and invalid record counts
  - _Requirements: 2.1, 2.4, 2.5, 6.1_

- [x] 6.4 Integrate DLQ Writer: write invalid records to S3 DLQ path if any exist, skip and log if none
  - Wrap DLQ write in try/except: log error on failure, continue to merge
  - Log DLQ write count on success
  - _Requirements: 4.1, 4.4, 4.5, 6.2_

- [x] 6.5 Integrate Conditional Merge: use `build_merge_preactions` and `build_merge_postactions` to write only valid records, skip merge if no valid records
  - Log warning and skip if zero valid records
  - Log merge count on success
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 6.3_

- [x] 6.6 Add try/finally block for staging table cleanup on both success and failure paths
  - Drop staging table in finally block, log error if DROP fails
  - _Requirements: 7.1, 7.2_

- [x] 6.7 Add logging for validation metrics (total, valid, invalid counts), DLQ write count, and merge count
  - _Requirements: 6.1, 6.2, 6.3, 6.4_

## Task 7: Update CloudFormation template

- [x] 7.1 Add `DLQPath` parameter to `cloudformation/template.yaml` for the S3ToRedshiftJob
  - Default value: `s3://<bucket>/dlq/s3toredshift/`
  - _Requirements: 4.1, 4.3_

- [x] 7.2 Update IAM policy to grant S3 write access to the DLQ path
  - Add PutObject permission for the DLQ S3 prefix
  - _Requirements: 4.1_

- [x] 7.3 Add `--DLQ_PATH` and `--extra-py-files` entries to the S3ToRedshiftJob DefaultArguments
  - Include validation_rule.py, validation_engine.py, dlq_writer.py, staging_merger.py in extra-py-files
  - _Requirements: 4.1, 5.1_

## Task 8: Write orchestration-level tests

- [ ]* 8.1 Write unit tests in `tests/test_s3toredshift.py` for zero-records early exit behavior
  - _Requirements: 1.3_

- [ ]* 8.2 Write unit tests for DLQ failure handling: verify merge proceeds when DLQ write fails
  - _Requirements: 4.5_

- [ ]* 8.3 Write unit tests for all-invalid scenario: verify merge is skipped and DLQ is written
  - _Requirements: 3.2_

- [ ]* 8.4 Write unit tests for staging cleanup on failure: verify DROP is attempted in finally block
  - _Requirements: 7.1, 7.2_

## Task 9: Final checkpoint - Ensure all tests pass

- [x] 9. Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
