# AWS Glue Jobs

A collection of AWS Glue ETL jobs managed via CloudFormation with GitHub version control integration.

## Glue Jobs

| Job Name | Description |
|---|---|
| `glue-s3-catalog-job-etl-job` | Reads CSV/JSON from S3, writes Parquet to S3, registers in Glue Catalog |
| `glue-s3-catalog-job-s3toredshift` | Reads from Glue Catalog table `taxi_join` and writes to Redshift |

## Project Structure

```
├── glue_job/
│   ├── __init__.py
│   ├── glue_job.py            # S3-to-Catalog ETL script
│   ├── s3toredshift.py        # S3-to-Redshift ETL script
│   ├── taxi_join.py           # Taxi join script
│   ├── params.py              # Job parameter parsing
│   └── format_detector.py     # Source format detection
├── cloudformation/
│   └── template.yaml          # CloudFormation template
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_format_detector.py
│   └── test_params.py
├── enable_vcs.py              # Enable GitHub version control on any Glue job
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

### 4. Upload all job scripts to S3

Replace `<SCRIPT_BUCKET>` with the bucket name from step 3.

```bash
aws s3 cp glue_job/glue_job.py s3://<SCRIPT_BUCKET>/glue_job/glue_job.py
aws s3 cp glue_job/s3toredshift.py s3://<SCRIPT_BUCKET>/glue_job/s3toredshift.py
aws s3 cp glue_job/params.py s3://<SCRIPT_BUCKET>/glue_job/params.py
aws s3 cp glue_job/format_detector.py s3://<SCRIPT_BUCKET>/glue_job/format_detector.py
aws s3 cp glue_job/__init__.py s3://<SCRIPT_BUCKET>/glue_job/__init__.py
```

## Running the Jobs

### Important: Enable Version Control After Creating a New Job

CloudFormation does not support `SourceControlDetails` for Glue jobs. After deploying a new job, you must enable version control separately using the included `enable_vcs.py` script:

```bash
python3 enable_vcs.py <job-name> [folder]
```

Examples:

```bash
python3 enable_vcs.py glue-s3-catalog-job-etl-job glue_job
python3 enable_vcs.py glue-s3-catalog-job-s3toredshift glue_job
```

This script:
1. Fetches the GitHub PAT from Secrets Manager (`glue/github-pat`)
2. Gets the current job configuration
3. Updates the job with GitHub source control (repo: `vijithaglue/Glue-jobs`, branch: `main`)

Run this every time you create a new Glue job via CloudFormation.

### S3-to-Catalog ETL Job

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

### S3-to-Redshift Job

Reads from Glue Catalog table `taxi_join` in `parquetdb` database and writes to Redshift `dev` database using Glue connection `testconnection_refshift1`.

```bash
AWS_PAGER="" aws glue start-job-run --job-name glue-s3-catalog-job-s3toredshift
```

### Check job run status

```bash
AWS_PAGER="" aws glue get-job-run \
  --job-name <JOB_NAME> \
  --run-id <RUN_ID>
```

Replace `<RUN_ID>` with the value returned by `start-job-run`.

## Version Control

### Git (local to GitHub)

```bash
git push origin main
git pull origin main
```

### Glue Source Control (Glue job to/from GitHub)

The PAT is fetched automatically from Secrets Manager so you never need to copy/paste it.

Push Glue job script to GitHub:

```bash
AWS_PAGER="" aws glue update-source-control-from-job --job-name glue-s3-catalog-job-etl-job --provider GITHUB --repository-name Glue-jobs --repository-owner vijithaglue --branch-name main --folder glue_job --auth-strategy PERSONAL_ACCESS_TOKEN --auth-token "$(AWS_PAGER="" aws secretsmanager get-secret-value --secret-id glue/github-pat --query SecretString --output text | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')"
```

Pull from GitHub into Glue job:

```bash
AWS_PAGER="" aws glue update-job-from-source-control --job-name glue-s3-catalog-job-etl-job --provider GITHUB --repository-name Glue-jobs --repository-owner vijithaglue --branch-name main --folder glue_job --auth-strategy PERSONAL_ACCESS_TOKEN --auth-token "$(AWS_PAGER="" aws secretsmanager get-secret-value --secret-id glue/github-pat --query SecretString --output text | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')"
```

For the s3toredshift job, replace `--job-name glue-s3-catalog-job-etl-job` with `--job-name glue-s3-catalog-job-s3toredshift` in the commands above.

### Shell Aliases (optional)

Add to `~/.zshrc` for convenience:

```bash
alias glue-push='AWS_PAGER="" aws glue update-source-control-from-job --job-name glue-s3-catalog-job-etl-job --provider GITHUB --repository-name Glue-jobs --repository-owner vijithaglue --branch-name main --folder glue_job --auth-strategy PERSONAL_ACCESS_TOKEN --auth-token "$(AWS_PAGER="" aws secretsmanager get-secret-value --secret-id glue/github-pat --query SecretString --output text | python3 -c '"'"'import sys,json;print(json.load(sys.stdin)["token"])'"'"')"'

alias glue-pull='AWS_PAGER="" aws glue update-job-from-source-control --job-name glue-s3-catalog-job-etl-job --provider GITHUB --repository-name Glue-jobs --repository-owner vijithaglue --branch-name main --folder glue_job --auth-strategy PERSONAL_ACCESS_TOKEN --auth-token "$(AWS_PAGER="" aws secretsmanager get-secret-value --secret-id glue/github-pat --query SecretString --output text | python3 -c '"'"'import sys,json;print(json.load(sys.stdin)["token"])'"'"')"'
```
