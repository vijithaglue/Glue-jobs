# Requirements Document

## Introduction

This feature provides a sample AWS Glue ETL job that reads data from an Amazon S3 bucket, transforms it, writes the output back to a target S3 bucket, and registers the resulting dataset in the AWS Glue Data Catalog. The project includes version control integration with GitHub so that the Glue job script and supporting infrastructure code can be pushed to and pulled from a remote repository.

## Glossary

- **Glue_Job**: An AWS Glue ETL job that executes a PySpark script to extract, transform, and load data.
- **Source_Bucket**: The Amazon S3 bucket from which the Glue_Job reads input data.
- **Target_Bucket**: The Amazon S3 bucket to which the Glue_Job writes output data.
- **Glue_Catalog**: The AWS Glue Data Catalog, a centralized metadata repository that stores table definitions and schema information.
- **Catalog_Database**: A logical grouping of tables within the Glue_Catalog.
- **Catalog_Table**: A metadata entry in the Glue_Catalog that describes the schema and location of a dataset.
- **Job_Script**: The PySpark script executed by the Glue_Job.
- **Job_Configuration**: The set of parameters (IAM role, script location, worker type, number of workers) that define how the Glue_Job runs.
- **Version_Control_Repository**: A GitHub repository used to store and track changes to the Job_Script and infrastructure code.
- **Infrastructure_Code**: Code (e.g., CloudFormation or Terraform) that provisions and configures the Glue_Job and related resources.

## Requirements

### Requirement 1

**User Story:** As a data engineer, I want the Glue_Job to read data from the Source_Bucket, so that I can process raw data stored in S3.

#### Acceptance Criteria

1. WHEN the Glue_Job starts, THE Glue_Job SHALL read data from a configurable Source_Bucket path provided as a job parameter.
2. WHEN the Source_Bucket path contains files in CSV format, THE Glue_Job SHALL parse the CSV files into a DynamicFrame with correct column names and types.
3. WHEN the Source_Bucket path contains files in JSON format, THE Glue_Job SHALL parse the JSON files into a DynamicFrame with correct field names and types.
4. IF the Source_Bucket path contains no files, THEN THE Glue_Job SHALL log a descriptive warning message and exit with a success status code.
5. IF the Source_Bucket path is unreachable or access is denied, THEN THE Glue_Job SHALL log the error details and exit with a failure status code.

### Requirement 2

**User Story:** As a data engineer, I want the Glue_Job to write processed data to the Target_Bucket, so that downstream consumers can access the output.

#### Acceptance Criteria

1. WHEN the Glue_Job completes data processing, THE Glue_Job SHALL write the output DynamicFrame to a configurable Target_Bucket path in Parquet format.
2. WHEN writing to the Target_Bucket, THE Glue_Job SHALL partition the output data by a configurable partition key column when the partition key parameter is provided.
3. IF writing to the Target_Bucket fails, THEN THE Glue_Job SHALL log the error details and exit with a failure status code.

### Requirement 3

**User Story:** As a data engineer, I want the Glue_Job to register the output dataset in the Glue_Catalog, so that the data is discoverable and queryable via Athena or other catalog-aware tools.

#### Acceptance Criteria

1. WHEN the Glue_Job writes output data to the Target_Bucket, THE Glue_Job SHALL create or update a Catalog_Table in the specified Catalog_Database with the schema derived from the output DynamicFrame.
2. WHEN registering the Catalog_Table, THE Glue_Job SHALL set the table location to the Target_Bucket output path.
3. WHEN registering the Catalog_Table, THE Glue_Job SHALL set the table format metadata to Parquet.
4. IF the specified Catalog_Database does not exist, THEN THE Glue_Job SHALL create the Catalog_Database before registering the Catalog_Table.

### Requirement 4

**User Story:** As a data engineer, I want the Glue_Job to be configurable through job parameters, so that I can reuse the same job for different datasets without modifying the script.

#### Acceptance Criteria

1. THE Glue_Job SHALL accept the following job parameters: source_path, target_path, catalog_database, catalog_table_name, and partition_key.
2. WHEN a required parameter (source_path, target_path, catalog_database, catalog_table_name) is missing, THE Glue_Job SHALL log a descriptive error message identifying the missing parameter and exit with a failure status code.
3. WHEN the partition_key parameter is omitted, THE Glue_Job SHALL write output data without partitioning.

### Requirement 5

**User Story:** As a data engineer, I want the Glue_Job and its infrastructure defined as code, so that the job can be provisioned repeatably across environments.

#### Acceptance Criteria

1. THE Infrastructure_Code SHALL define the Glue_Job resource including the IAM role, Job_Script location, Glue version, worker type, and number of workers as configurable parameters.
2. THE Infrastructure_Code SHALL define the IAM role with permissions scoped to the Source_Bucket, Target_Bucket, and Glue_Catalog resources.
3. THE Infrastructure_Code SHALL define an S3 bucket resource for storing the Job_Script.
4. IF the Infrastructure_Code is applied, THEN the provisioned Glue_Job SHALL match the Job_Configuration specified in the Infrastructure_Code parameters.

### Requirement 6

**User Story:** As a data engineer, I want the Job_Script and Infrastructure_Code stored in a Version_Control_Repository on GitHub, so that I can track changes, collaborate, and roll back to previous versions.

#### Acceptance Criteria

1. THE project SHALL include a Git repository initialized with a .gitignore file that excludes temporary files, build artifacts, and environment-specific configuration.
2. THE project SHALL include a README file that documents the project structure, setup instructions, job parameters, and deployment steps.
3. WHEN a developer commits changes, THE Version_Control_Repository SHALL accept pushes to the remote GitHub repository at https://github.com/vijithaglue/Glue-jobs.
4. WHEN a developer pulls from the Version_Control_Repository, THE local workspace SHALL receive the latest committed changes from the remote GitHub repository at https://github.com/vijithaglue/Glue-jobs.
5. THE Infrastructure_Code SHALL configure the Glue_Job with SourceControlDetails linking to the GitHub repository using a Personal Access Token stored in AWS Secrets Manager.

### Requirement 7

**User Story:** As a data engineer, I want the Job_Script to serialize and deserialize data correctly between formats, so that data fidelity is maintained throughout the ETL pipeline.

#### Acceptance Criteria

1. WHEN the Glue_Job reads CSV data and writes it as Parquet, THE Glue_Job SHALL preserve all column names and data values from the source CSV in the output Parquet files.
2. WHEN the Glue_Job reads JSON data and writes it as Parquet, THE Glue_Job SHALL preserve all field names and data values from the source JSON in the output Parquet files.
3. WHEN the Glue_Job writes a DynamicFrame as Parquet and the Parquet file is read back, THE resulting DynamicFrame SHALL contain equivalent column names, types, and row data as the original DynamicFrame.
