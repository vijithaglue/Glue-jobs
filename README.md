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
│   ├── conftest.py          # Shared test fixtures
│   ├── test_format_detector.py
│   └── test_params.py
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

### 1. Store your GitHub PAT in AWS Secrets Manager

```bash
aws secretsmanager create-secret \
  --name glue/github-pat \
  --secret-string '{"token":"<your-github-pat>"}'
```

### 2. Deploy the CloudFormation stack

```bash
AWS_PAGER="" aws cloudformation deploy \
  --template-file cloudformation/template.yaml \
  --stack-name glue-s3-catalog-job \
  --parameter-overrides \
    SourceBucketArn=arn:aws:s3:::my-source-bucket \
    TargetBucketArn=arn:aws:s3:::my-target-bucket \
  --capabilities CAPABILITY_NAMED_IAM
```

### 3. Get the Script Bucket name

```bash
AWS_PAGER="" aws cloudformation describe-stacks \
  --stack-name glue-s3-catalog-job \
  --query "Stacks[0].Outputs[?OutputKey=='ScriptBucketName'].OutputValue" \
  --output text
```

### 4. Upload the job script and supporting modules to S3

Replace `<SCRIPT_BUCKET>` with the bucket name from step 3.

```bash
aws s3 cp glue_job/glue_job.py s3://<SCRIPT_BUCKET>/glue_job/glue_job.py
aws s3 cp glue_job/params.py s3://<SCRIPT_BUCKET>/glue_job/params.py
aws s3 cp glue_job/format_detector.py s3://<SCRIPT_BUCKET>/glue_job/format_detector.py
aws s3 cp glue_job/__init__.py s3://<SCRIPT_BUCKET>/glue_job/__init__.py
```

### 5. Run the Glue job

```bash
AWS_PAGER="" aws glue start-job-run \
  --job-name glue-s3-catalog-job-etl-job \
  --arguments '{
    "--source_path": "s3://my-source-bucket/path/to/data/",
    "--target_path": "s3://my-target-bucket/output/",
    "--catalog_database": "my_database",
    "--catalog_table_name": "my_table"
  }'
```

To include partitioning, add `"--partition_key": "column_name"` to the arguments.

### 6. Check job run status

```bash
AWS_PAGER="" aws glue get-job-run \
  --job-name glue-s3-catalog-job-etl-job \
  --run-id <RUN_ID>
```

Replace `<RUN_ID>` with the value returned by `start-job-run`.

## Version Control

- Remote: `https://github.com/vijithaglue/Glue-jobs.git`
- Push changes: `git push origin main`
- Pull latest: `git pull origin main`
