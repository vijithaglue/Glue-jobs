# AWS Glue S3-to-Catalog ETL Job

An AWS Glue ETL job that reads data from an S3 source bucket (CSV or JSON), writes processed output in Parquet format to a target S3 bucket, and registers the dataset in the AWS Glue Data Catalog.

## Project Structure

```
├── glue_job/
│   ├── __init__.py
│   ├── glue_job.py          # Main ETL script
│   ├── params.py            # Job parameter parsing
│   └── format_detector.py   # Source format detection
├── cloudformation/
│   └── template.yaml        # CloudFormation template
├── tests/
│   ├── __init__.py
│   └── conftest.py          # Shared test fixtures
├── requirements.txt
└── README.md
```

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/vijithaglue/Glue-jobs.git
   cd Glue-jobs
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run tests:
   ```bash
   pytest
   ```

## Job Parameters

| Parameter | Required | Description |
|---|---|---|
| `source_path` | Yes | S3 path to the source data (e.g. `s3://my-bucket/input/`) |
| `target_path` | Yes | S3 path for the Parquet output (e.g. `s3://my-bucket/output/`) |
| `catalog_database` | Yes | Glue Catalog database name |
| `catalog_table_name` | Yes | Glue Catalog table name |
| `partition_key` | No | Column name to partition output by |

## Deployment

1. Store your GitHub PAT in AWS Secrets Manager:
   ```bash
   aws secretsmanager create-secret \
     --name glue/github-pat \
     --secret-string '{"token":"<your-github-pat>"}'
   ```

2. Deploy the CloudFormation stack:
   ```bash
   aws cloudformation deploy \
     --template-file cloudformation/template.yaml \
     --stack-name glue-s3-catalog-job \
     --parameter-overrides \
       SourceBucketArn=arn:aws:s3:::my-source-bucket \
       TargetBucketArn=arn:aws:s3:::my-target-bucket \
     --capabilities CAPABILITY_NAMED_IAM
   ```

## Version Control

- Remote: `https://github.com/vijithaglue/Glue-jobs.git`
- Push changes: `git push origin main`
- Pull latest: `git pull origin main`
