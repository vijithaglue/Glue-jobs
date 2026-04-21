# Requirements Document

## Introduction

This feature enhances the existing `s3toredshift` AWS Glue job with a robust data quality pipeline. Currently, the job reads CSV data from S3, writes it to a Redshift staging table, and swaps it into the target table via a transaction. The enhancement adds explicit data validation on the staging table (null checks, format validation), conditional merging of only valid records into the target table, and dead-letter queue (DLQ) routing of invalid records to an S3 path. This ensures data integrity in the target table while preserving failed records for investigation and reprocessing.

## Glossary

- **Glue_Job**: The AWS Glue ETL job (`s3toredshift`) that orchestrates reading from S3, validating, and loading into Redshift.
- **Staging_Table**: A temporary Redshift table (`public.taxi_lookup_staging`) used to hold raw ingested data before validation.
- **Target_Table**: The production Redshift table (`public.taxi_lookup`) that holds validated, clean data.
- **Validation_Engine**: The component within the Glue job responsible for executing data quality checks (null checks, format validation) against records in the staging table.
- **DLQ_Writer**: The component responsible for writing invalid records and their error metadata to the S3 dead-letter queue path.
- **DLQ_Path**: An S3 prefix (e.g., `s3://<bucket>/dlq/s3toredshift/`) where invalid records are stored in a partitioned layout.
- **Validation_Rule**: A single data quality check (e.g., a required column is not null, a column matches an expected format).
- **Valid_Record**: A record in the staging table that passes all configured validation rules.
- **Invalid_Record**: A record in the staging table that fails one or more validation rules.
- **Error_Report**: Metadata attached to each invalid record describing which validation rules failed and the failing values.

## Requirements

### Requirement 1: Staging Table Loading

**User Story:** As a data engineer, I want the Glue job to load raw S3 data into a Redshift staging table, so that data can be validated before it reaches the production table.

#### Acceptance Criteria

1. WHEN the Glue_Job executes, THE Glue_Job SHALL read CSV data from the configured S3 source path and load all records into the Staging_Table.
2. WHEN loading into the Staging_Table, THE Glue_Job SHALL drop and recreate the Staging_Table with the same schema as the Target_Table before writing.
3. IF the S3 source path contains zero records, THEN THE Glue_Job SHALL log a warning and exit without modifying the Staging_Table or Target_Table.
4. IF the Staging_Table load fails, THEN THE Glue_Job SHALL log the error and terminate without modifying the Target_Table.

### Requirement 2: Data Validation on Staging Table

**User Story:** As a data engineer, I want the Glue job to validate records in the staging table against configurable rules, so that only clean data is promoted to the production table.

#### Acceptance Criteria

1. WHEN the Staging_Table load completes successfully, THE Validation_Engine SHALL execute all configured Validation_Rules against every record in the Staging_Table.
2. THE Validation_Engine SHALL support null-check rules that verify specified columns contain non-null values.
3. THE Validation_Engine SHALL support format-validation rules that verify column values match a specified regular expression pattern.
4. WHEN a record passes all Validation_Rules, THE Validation_Engine SHALL classify the record as a Valid_Record.
5. WHEN a record fails one or more Validation_Rules, THE Validation_Engine SHALL classify the record as an Invalid_Record and attach an Error_Report listing each failed rule name and the failing column value.
6. THE Validation_Engine SHALL process all records in a single pass, collecting all rule violations per record rather than stopping at the first failure.

### Requirement 3: Conditional Merge of Valid Records

**User Story:** As a data engineer, I want only validated records merged into the production table, so that data quality is maintained in the target table.

#### Acceptance Criteria

1. WHEN the Validation_Engine completes and Valid_Records exist, THE Glue_Job SHALL merge the Valid_Records into the Target_Table using a transactional operation.
2. WHEN the Validation_Engine completes and zero Valid_Records exist, THE Glue_Job SHALL skip the merge step, log a warning, and proceed to DLQ routing.
3. THE Glue_Job SHALL execute the merge as an atomic transaction: delete existing rows from the Target_Table and insert Valid_Records from the Staging_Table within a single transaction block.
4. IF the merge transaction fails, THEN THE Glue_Job SHALL roll back the transaction, log the error, and leave the Target_Table unchanged.

### Requirement 4: Dead-Letter Queue Routing to S3

**User Story:** As a data engineer, I want invalid records written to an S3 dead-letter queue path with error details, so that I can investigate and reprocess failed records.

#### Acceptance Criteria

1. WHEN Invalid_Records exist after validation, THE DLQ_Writer SHALL write all Invalid_Records to the configured DLQ_Path in S3.
2. THE DLQ_Writer SHALL write Invalid_Records in JSON format, with each record containing the original data fields and an additional `_validation_errors` field holding the Error_Report.
3. THE DLQ_Writer SHALL partition DLQ output by job run date using the path pattern `<DLQ_Path>/year=<YYYY>/month=<MM>/day=<DD>/`.
4. WHEN zero Invalid_Records exist after validation, THE DLQ_Writer SHALL skip the DLQ write step and log that no invalid records were found.
5. IF the DLQ write to S3 fails, THEN THE Glue_Job SHALL log the error but still proceed with the merge of Valid_Records into the Target_Table.

### Requirement 5: Validation Rule Configuration

**User Story:** As a data engineer, I want validation rules to be defined in a structured configuration, so that I can add or modify rules without changing job code.

#### Acceptance Criteria

1. THE Glue_Job SHALL accept validation rules as a list of dictionaries, where each dictionary specifies a rule type, target column, and rule-specific parameters.
2. THE Validation_Engine SHALL support a `not_null` rule type that checks a specified column for null or empty string values.
3. THE Validation_Engine SHALL support a `regex` rule type that checks a specified column value against a provided regular expression pattern.
4. IF a Validation_Rule references a column that does not exist in the Staging_Table schema, THEN THE Validation_Engine SHALL log a warning and skip that rule.
5. IF the validation rules list is empty, THEN THE Validation_Engine SHALL treat all records as Valid_Records and log a warning that no validation was performed.

### Requirement 6: Job Observability and Logging

**User Story:** As a data engineer, I want the Glue job to log key metrics at each stage, so that I can monitor pipeline health and debug issues.

#### Acceptance Criteria

1. WHEN the Glue_Job completes validation, THE Glue_Job SHALL log the total record count, the Valid_Record count, and the Invalid_Record count.
2. WHEN the DLQ_Writer completes, THE Glue_Job SHALL log the number of records written to the DLQ_Path.
3. WHEN the merge completes successfully, THE Glue_Job SHALL log the number of records inserted into the Target_Table.
4. IF any stage fails, THEN THE Glue_Job SHALL log the stage name, error message, and stack trace before terminating or continuing as specified by the stage's error handling requirement.

### Requirement 7: Staging Table Cleanup

**User Story:** As a data engineer, I want the staging table cleaned up after the job completes, so that temporary data does not persist in Redshift.

#### Acceptance Criteria

1. WHEN the Glue_Job completes the merge and DLQ steps successfully, THE Glue_Job SHALL drop the Staging_Table.
2. IF the Glue_Job fails at any stage after the Staging_Table is created, THEN THE Glue_Job SHALL attempt to drop the Staging_Table before terminating.
