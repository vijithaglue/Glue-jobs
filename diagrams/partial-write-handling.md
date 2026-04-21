# Handling Partial Writes: PostgreSQL → Glue → Redshift (Direct, No S3)

## Main ETL Flow with Failure Handling

```mermaid
flowchart TD
    Start([🚀 Glue Job Start]) --> Truncate["🧹 Truncate Staging Table\n<i>preactions: TRUNCATE staging_table</i>"]
    Truncate --> Extract["📥 Read from PostgreSQL\n<i>JDBC connection via Glue</i>"]

    Extract -->|Success| WriteStaging["📤 Write to Redshift Staging\n<i>Direct JDBC write, no S3</i>"]
    Extract -->|Failure| FailExtract["❌ Extraction Failed\n<i>PostgreSQL unchanged</i>"]

    WriteStaging -->|Success| Validate["🔍 Run Validation\n<i>Null checks, format, schema</i>"]
    WriteStaging -->|Partial Write| FailStaging["⚠️ Partial Data in Staging\n<i>e.g. 50% rows written</i>"]

    Validate -->|Pass| Merge["🔀 MERGE to Production\n<i>UPSERT via MERGE statement</i>"]
    Validate -->|Fail| DLQ["🗑️ Route Invalid Records to DLQ\n<i>INSERT INTO dlq_table</i>"]

    Merge -->|Success| Metadata["📝 Update Metadata\n<i>Job status, row counts, timestamps</i>"]
    Merge -->|Failure| FailMerge["⚠️ Validated Data Stuck in Staging\n<i>Ready for merge but uncommitted</i>"]

    DLQ --> Metadata
    Metadata --> Done([✅ Job Complete])

    %% Failure Handling Paths
    FailExtract --> Alert["🔔 CloudWatch Alarm\n+ SNS Notification"]
    FailStaging --> Alert
    FailMerge --> Alert

    Alert --> StepFn{"🔄 Step Functions\nRetry Logic"}

    StepFn -->|"Attempt ≤ 3\n(Exponential Backoff)"| Truncate
    StepFn -->|"All Retries\nExhausted"| Manual["🛠️ Manual Recovery"]

    Manual --> FixPath{"Root Cause?"}
    FixPath -->|"Transient Error\n(network, throttle)"| TruncateRetry["Truncate Staging\n& Rerun Job"]
    FixPath -->|"Data Quality\nIssue"| FixData["Fix Data in PostgreSQL\nor Adjust Validation Rules"]
    FixPath -->|"Critical Failure"| Rollback["Truncate Staging\n(Discard Partial Data)"]

    TruncateRetry --> Truncate
    FixData --> Truncate
    Rollback --> Truncate

    %% Styling
    style Start fill:#4CAF50,color:#fff
    style Done fill:#4CAF50,color:#fff
    style FailExtract fill:#f44336,color:#fff
    style FailStaging fill:#ff9800,color:#fff
    style FailMerge fill:#ff9800,color:#fff
    style Alert fill:#e91e63,color:#fff
    style DLQ fill:#9C27B0,color:#fff
    style Manual fill:#FF5722,color:#fff
    style Rollback fill:#f44336,color:#fff
    style StepFn fill:#2196F3,color:#fff
    style Merge fill:#00BCD4,color:#fff
    style Validate fill:#3F51B5,color:#fff
```

## Data State at Each Failure Point

```mermaid
stateDiagram-v2
    [*] --> Extracting: Job Starts

    state "PostgreSQL → Staging" as Extracting {
        [*] --> Reading
        Reading --> PartialWrite: ❌ Crash mid-write
        Reading --> FullWrite: ✅ All rows written
    }

    state "Validation Phase" as Validating {
        [*] --> Checking
        Checking --> Invalid: ❌ Null/format errors
        Checking --> Valid: ✅ All checks pass
    }

    state "Merge Phase" as Merging {
        [*] --> Upserting
        Upserting --> MergeFail: ❌ Concurrency/lock error
        Upserting --> MergeOK: ✅ Production updated
    }

    Extracting --> Validating: Staging populated
    Validating --> Merging: Data validated
    Merging --> [*]: Job complete

    PartialWrite --> [*]: Truncate & Retry
    Invalid --> [*]: Route to DLQ & Retry
    MergeFail --> [*]: Retry merge or escalate
```

## Retry & Recovery Decision Tree

```mermaid
flowchart LR
    Failure["🔴 Job Failed"] --> Detect["CloudWatch\nDetects Failure"]
    Detect --> Classify{"Classify\nError Type"}

    Classify -->|Transient| Retry["🔄 Auto-Retry\n3x with backoff\n30s → 60s → 120s"]
    Classify -->|Data Quality| DLQ["📋 Isolate to DLQ\nFix source data"]
    Classify -->|Critical| Rollback["🗑️ Full Rollback\nTruncate staging"]

    Retry -->|Success| Resume["✅ Job Resumes\n<i>Bookmarks skip\nprocessed rows</i>"]
    Retry -->|All Failed| Escalate["📧 SNS Alert\nManual intervention"]

    DLQ --> Fix["Fix PostgreSQL\nor validation rules"]
    Fix --> Rerun["Rerun Job"]

    Rollback --> Rerun

    style Failure fill:#f44336,color:#fff
    style Resume fill:#4CAF50,color:#fff
    style Escalate fill:#FF5722,color:#fff
    style DLQ fill:#9C27B0,color:#fff
```

## Component Interaction (Sequence)

```mermaid
sequenceDiagram
    participant SF as Step Functions
    participant Glue as AWS Glue Job
    participant PG as PostgreSQL
    participant RS as Redshift Staging
    participant Prod as Redshift Production
    participant DLQ as DLQ Table
    participant CW as CloudWatch/SNS

    SF->>Glue: Start Job
    Glue->>RS: TRUNCATE staging_table
    Glue->>PG: SELECT * FROM source_table (JDBC)
    PG-->>Glue: Return rows

    alt Successful Write
        Glue->>RS: INSERT INTO staging_table (direct JDBC)
        Glue->>RS: Run validation queries
        alt Validation Pass
            Glue->>Prod: MERGE INTO production USING staging
            Glue->>SF: Job Success ✅
        else Validation Fail
            Glue->>DLQ: INSERT failed records
            Glue->>CW: Log validation errors
            Glue->>SF: Job Partial Success ⚠️
        end
    else Partial Write Failure
        Glue--xRS: ❌ Crash (partial rows in staging)
        Glue->>CW: Log error + trigger alarm
        CW->>SF: Notify failure
        SF->>SF: Retry (up to 3x, exponential backoff)
        alt Retry Succeeds
            SF->>Glue: Restart Job (bookmarks resume)
        else All Retries Fail
            SF->>CW: Send SNS alert for manual recovery
        end
    end
```
