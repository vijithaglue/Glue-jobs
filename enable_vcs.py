"""Enable GitHub version control on a Glue job.

Usage: python3 enable_vcs.py <job-name> [folder]

Fetches the PAT from Secrets Manager and updates the Glue job with
GitHub source control configuration.
"""

import subprocess, json, os, sys

env = {**os.environ, "AWS_PAGER": ""}

if len(sys.argv) < 2:
    print("Usage: python3 enable_vcs.py <job-name> [folder]")
    print("Example: python3 enable_vcs.py glue-s3-catalog-job-s3toredshift glue_job")
    sys.exit(1)

job_name = sys.argv[1]
folder = sys.argv[2] if len(sys.argv) > 2 else "glue_job"

# Get PAT from Secrets Manager
result = subprocess.run(
    ['aws', 'secretsmanager', 'get-secret-value',
     '--secret-id', 'glue/github-pat',
     '--no-cli-pager', '--output', 'json'],
    capture_output=True, text=True, env=env
)
if result.returncode != 0:
    print("Failed to get secret:", result.stderr)
    sys.exit(1)
pat = json.loads(json.loads(result.stdout)['SecretString'])['token']

# Get current job definition
result = subprocess.run(
    ['aws', 'glue', 'get-job',
     '--job-name', job_name,
     '--no-cli-pager', '--output', 'json'],
    capture_output=True, text=True, env=env
)
if result.returncode != 0:
    print("Failed to get job:", result.stderr)
    sys.exit(1)
job = json.loads(result.stdout)['Job']

# Build update payload
job_update = {
    "Role": job["Role"],
    "Command": job["Command"],
    "DefaultArguments": job.get("DefaultArguments", {}),
    "GlueVersion": job.get("GlueVersion", "4.0"),
    "WorkerType": job.get("WorkerType", "G.1X"),
    "NumberOfWorkers": job.get("NumberOfWorkers", 2),
    "SourceControlDetails": {
        "Provider": "GITHUB",
        "Repository": "Glue-jobs",
        "Owner": "vijithaglue",
        "Branch": "main",
        "Folder": folder,
        "AuthStrategy": "PERSONAL_ACCESS_TOKEN",
        "AuthToken": pat
    }
}

# Include Connections if present
if "Connections" in job and job["Connections"].get("Connections"):
    job_update["Connections"] = job["Connections"]

result = subprocess.run(
    ['aws', 'glue', 'update-job',
     '--job-name', job_name,
     '--job-update', json.dumps(job_update),
     '--no-cli-pager', '--output', 'json'],
    capture_output=True, text=True, env=env
)
if result.returncode != 0:
    print("Failed:", result.stderr)
    sys.exit(1)

print(f"Version control enabled for job: {job_name}")
print(f"  Provider: GITHUB")
print(f"  Repository: vijithaglue/Glue-jobs")
print(f"  Branch: main")
print(f"  Folder: {folder}")
