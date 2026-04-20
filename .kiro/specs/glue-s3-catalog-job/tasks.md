# Implementation Plan

- [x] 1. Set up project structure and dependencies
  - [x] 1.1 Create project directory structure: `glue_job/`, `cloudformation/`, `tests/`
    - Create `glue_job/__init__.py`, `glue_job/glue_job.py` (main script), `glue_job/params.py` (parameter parsing), `glue_job/format_detector.py` (format detection)
    - Create `tests/__init__.py`, `tests/conftest.py` with shared fixtures
    - Create `requirements.txt` with `pytest`, `hypothesis`, `pyspark`, `boto3` dependencies
    - _Requirements: 5.1, 6.1_
  - [x] 1.2 Create `.gitignore` and `README.md`
    - `.gitignore` excludes `__pycache__/`, `*.pyc`, `.env`, `*.egg-info/`, `.pytest_cache/`, `spark-warehouse/`, `metastore_db/`
    - `README.md` documents project structure, setup instructions, job parameters, deployment steps, and GitHub version control configuration
    - _Requirements: 6.1, 6.2_

- [x] 2. Implement parameter parsing and format detection
  - [x] 2.1 Implement `parse_args` in `glue_job/params.py`
    - Accept `sys.argv`-style list, extract `source_path`, `target_path`, `catalog_database`, `catalog_table_name`, and optional `partition_key`
    - Raise `ValueError` with descriptive message for each missing required parameter
    - _Requirements: 4.1, 4.2, 4.3_
  - [ ]* 2.2 Write property test: parameter parsing extracts all provided values
    - **Property 1: Parameter parsing extracts all provided values**
    - **Validates: Requirements 4.1**
  - [ ]* 2.3 Write property test: missing required parameter detection
    - **Property 2: Missing required parameter detection**
    - **Validates: Requirements 4.2**
  - [x] 2.4 Implement `detect_format` in `glue_job/format_detector.py`
    - Return `"csv"` for paths ending in `.csv`, `"json"` for paths ending in `.json`, default to `"csv"`
    - _Requirements: 1.2, 1.3_
  - [x] 2.5 Write unit tests for `parse_args` and `detect_format`
    - Test known-good parameter sets, missing parameter cases, and format detection for `.csv`, `.json`, and ambiguous paths
    - _Requirements: 4.1, 4.2, 1.2, 1.3_

- [x] 3. Checkpoint - Make sure all tests are passing
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement core ETL functions
  - [x] 4.1 Implement `read_source` in `glue_job/glue_job.py`
    - Use `detect_format` to determine CSV or JSON, read from S3 path into a Spark DataFrame
    - Return `None` and log warning if source path is empty
    - Raise and log error if source path is unreachable
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_
  - [x] 4.2 Implement `write_target` in `glue_job/glue_job.py`
    - Write DataFrame to target S3 path in Parquet format
    - Partition by `partition_key` column when provided, write without partitioning when `None`
    - Log error and raise on write failure
    - _Requirements: 2.1, 2.2, 2.3_
  - [x] 4.3 Implement `register_catalog` in `glue_job/glue_job.py`
    - Create Catalog Database if it does not exist using boto3 Glue client
    - Create or update Catalog Table with schema derived from DataFrame, location set to target path, format set to Parquet
    - _Requirements: 3.1, 3.2, 3.3, 3.4_
  - [ ]* 4.4 Write property test: CSV data preservation through ETL pipeline
    - **Property 3: CSV data preservation through ETL pipeline**
    - **Validates: Requirements 7.1, 1.2**
  - [ ]* 4.5 Write property test: JSON data preservation through ETL pipeline
    - **Property 4: JSON data preservation through ETL pipeline**
    - **Validates: Requirements 7.2, 1.3**
  - [ ]* 4.6 Write property test: Parquet serialization round-trip
    - **Property 5: Parquet serialization round-trip**
    - **Validates: Requirements 7.3**
  - [ ]* 4.7 Write property test: partition key controls partitioning behavior
    - **Property 6: Partition key controls partitioning behavior**
    - **Validates: Requirements 2.2, 4.3**
  - [ ]* 4.8 Write unit tests for `read_source`, `write_target`, and `register_catalog`
    - Test empty source path returns None, format detection integration, write with and without partition key, catalog database creation
    - _Requirements: 1.4, 1.5, 2.1, 2.2, 2.3, 3.1, 3.4_

- [x] 5. Checkpoint - Make sure all tests are passing
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Implement main job entry point
  - [x] 6.1 Implement `main()` in `glue_job/glue_job.py`
    - Wire together `parse_args`, `read_source`, `write_target`, and `register_catalog`
    - Handle all error cases with appropriate logging and exit codes
    - Exit with code 0 on success, code 1 on failure
    - _Requirements: 1.4, 1.5, 2.3, 4.1, 4.2_

- [x] 7. Create CloudFormation template
  - [x] 7.1 Create `cloudformation/template.yaml`
    - Define `GlueJob` resource with configurable Glue version, worker type, number of workers, and script location
    - Define `SourceControlDetails` linking to `vijithaglue/Glue-jobs` with PAT from Secrets Manager (`glue/github-pat`)
    - Define `GlueJobRole` IAM role with policies scoped to source bucket, target bucket, script bucket, Glue Catalog, and Secrets Manager
    - Define `ScriptBucket` S3 bucket resource
    - Define template parameters: `SourceBucketArn`, `TargetBucketArn`, `GlueVersion`, `WorkerType`, `NumberOfWorkers`, `GitHubRepository`, `GitHubBranch`
    - _Requirements: 5.1, 5.2, 5.3, 6.3, 6.4, 6.5_
  - [ ]* 7.2 Write unit tests for CloudFormation template validation
    - Validate template structure contains required resources (`GlueJob`, `GlueJobRole`, `ScriptBucket`)
    - Validate IAM policy scoping references source and target bucket ARN parameters
    - Validate `SourceControlDetails` is present with correct provider and auth strategy
    - _Requirements: 5.1, 5.2, 5.3, 6.5_

- [-] 8. Initialize Git repository and configure remote
  - [-] 8.1 Initialize Git repo and configure remote
    - Run `git init`, add `.gitignore`, create initial commit
    - Add remote: `git remote add origin https://github.com/vijithaglue/Glue-jobs.git`
    - _Requirements: 6.1, 6.3, 6.4_

- [ ] 9. Final Checkpoint - Make sure all tests are passing
  - Ensure all tests pass, ask the user if questions arise.
