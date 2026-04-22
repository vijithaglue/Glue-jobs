# Glue Git Push / Pull Commands

## Pull from GitHub into Glue Job

```bash
AWS_PAGER="" aws glue update-job-from-source-control \
  --job-name glue-s3-catalog-job-s3toredshift \
  --provider GITHUB \
  --repository-name Glue-jobs \
  --repository-owner vijithaglue \
  --branch-name main \
  --folder glue_job \
  --auth-strategy PERSONAL_ACCESS_TOKEN \
  --auth-token "$(AWS_PAGER="" aws secretsmanager get-secret-value --secret-id glue/github-pat --query SecretString --output text | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')"
```

## Push Glue Job to GitHub

```bash
AWS_PAGER="" aws glue update-source-control-from-job \
  --job-name glue-s3-catalog-job-s3toredshift \
  --provider GITHUB \
  --repository-name Glue-jobs \
  --repository-owner vijithaglue \
  --branch-name main \
  --folder glue_job \
  --auth-strategy PERSONAL_ACCESS_TOKEN \
  --auth-token "$(AWS_PAGER="" aws secretsmanager get-secret-value --secret-id glue/github-pat --query SecretString --output text | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')"
```

> The PAT is fetched inline from Secrets Manager so you never need to copy/paste it.
