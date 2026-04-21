"""Glue job: s3toredshift

Reads CSV data from s3://athena-ctas-result/SampleTaxis/ (hourly
partitioned), validates records against configurable rules, routes
invalid records to an S3 dead-letter queue, and merges only valid
records into the Redshift target table using a staging-table workflow.

Uses a staging table + transaction to handle partial writes safely.
If the job fails mid-write, the staging table is dropped in a finally
block so the production table remains untouched.
"""

import sys
import logging
from datetime import date

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from glue_job.validation_rule import parse_rules
from glue_job.validation_engine import validate, split_valid_invalid
from glue_job.dlq_writer import write_dlq
from glue_job.staging_merger import build_merge_preactions, build_merge_postactions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------
SOURCE_PATH = "s3://athena-ctas-result/SampleTaxis/"
REDSHIFT_CONNECTION = "Redshift-2"
REDSHIFT_DATABASE = "dev"
STAGING_TABLE = "public.taxi_lookup_staging"
TARGET_TABLE = "public.taxi_lookup"

DLQ_PATH = "s3://athena-ctas-result/dlq/s3toredshift/"

VALIDATION_RULES = [
    {"rule_type": "not_null", "column": "vendor_id"},
    {"rule_type": "not_null", "column": "pickup_datetime"},
    {"rule_type": "regex", "column": "vendor_id", "pattern": r"^\d+$"},
]


# ---------------------------------------------------------------------------
# Job orchestration
# ---------------------------------------------------------------------------

def run_job(glue_context, spark, args, validation_rules):
    """Main job orchestration logic.

    1. Read CSV from S3
    2. Check record count (exit if 0)
    3. Write all records to Redshift staging table
    4. Validate staged records
    5. Write invalid records to DLQ (if any)
    6. Merge valid records into target table (if any)
    7. Clean up staging table (in finally block)
    """
    # Step 1: Read CSV data from S3 partitioned path
    logger.info("Reading CSV data from %s", SOURCE_PATH)
    datasource = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [SOURCE_PATH],
            "recurse": True,
        },
        format="csv",
        format_options={"withHeader": True},
        transformation_ctx="datasource",
    )

    record_count = datasource.count()
    logger.info("Read %d records from S3", record_count)

    # Step 2: Early exit on zero records
    if record_count == 0:
        logger.warning("No data found in %s. Exiting.", SOURCE_PATH)
        return

    # Step 3: Write to staging table in Redshift
    preactions = build_merge_preactions(STAGING_TABLE, TARGET_TABLE)
    logger.info(
        "Writing %d records to staging table %s", record_count, STAGING_TABLE
    )
    glue_context.write_dynamic_frame.from_jdbc_conf(
        frame=datasource,
        catalog_connection=REDSHIFT_CONNECTION,
        connection_options={
            "dbtable": STAGING_TABLE,
            "database": REDSHIFT_DATABASE,
            "preactions": preactions,
        },
        redshift_tmp_dir=args["TempDir"],
        transformation_ctx="staging_sink",
    )
    logger.info("Staging load complete for %s", STAGING_TABLE)

    # Convert to DataFrame once for validation and potential cleanup
    staging_df = datasource.toDF()

    # --- try/finally ensures staging table cleanup on any failure path ---
    try:
        # Step 4: Validate staged records
        rules = parse_rules(validation_rules)
        schema_columns = staging_df.columns

        validated_df = validate(staging_df, rules, schema_columns)
        valid_df, invalid_df = split_valid_invalid(validated_df)

        total_count = staging_df.count()
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        logger.info(
            "Validation complete — total: %d, valid: %d, invalid: %d",
            total_count,
            valid_count,
            invalid_count,
        )

        # Step 5: Write invalid records to DLQ
        if invalid_count > 0:
            try:
                dlq_count = write_dlq(invalid_df, DLQ_PATH, date.today())
                logger.info("DLQ write complete — %d records written", dlq_count)
            except Exception:
                logger.error(
                    "Failed to write invalid records to DLQ", exc_info=True
                )
        else:
            logger.info("No invalid records — skipping DLQ write")

        # Step 6: Merge valid records into target table
        if valid_count == 0:
            logger.warning(
                "Zero valid records — skipping merge into %s", TARGET_TABLE
            )
        else:
            valid_dyf = DynamicFrame.fromDF(valid_df, glue_context, "valid_dyf")
            merge_preactions = build_merge_preactions(STAGING_TABLE, TARGET_TABLE)
            merge_postactions = build_merge_postactions(STAGING_TABLE, TARGET_TABLE)

            glue_context.write_dynamic_frame.from_jdbc_conf(
                frame=valid_dyf,
                catalog_connection=REDSHIFT_CONNECTION,
                connection_options={
                    "dbtable": STAGING_TABLE,
                    "database": REDSHIFT_DATABASE,
                    "preactions": merge_preactions,
                    "postactions": merge_postactions,
                },
                redshift_tmp_dir=args["TempDir"],
                transformation_ctx="redshift_merge",
            )
            logger.info(
                "Merge complete — %d valid records inserted into %s",
                valid_count,
                TARGET_TABLE,
            )

    finally:
        # Step 7: Staging table cleanup (best-effort)
        try:
            logger.info("Dropping staging table %s", STAGING_TABLE)
            drop_sql = f"DROP TABLE IF EXISTS {STAGING_TABLE}"
            # Use Glue's JDBC conf to execute cleanup via a minimal write
            # with only the DROP as a preaction on an empty frame.
            empty_df = spark.createDataFrame([], staging_df.schema)
            empty_dyf = DynamicFrame.fromDF(empty_df, glue_context, "empty_cleanup")
            glue_context.write_dynamic_frame.from_jdbc_conf(
                frame=empty_dyf,
                catalog_connection=REDSHIFT_CONNECTION,
                connection_options={
                    "dbtable": STAGING_TABLE,
                    "database": REDSHIFT_DATABASE,
                    "preactions": drop_sql,
                },
                redshift_tmp_dir=args["TempDir"],
                transformation_ctx="staging_cleanup",
            )
            logger.info("Staging table %s dropped successfully", STAGING_TABLE)
        except Exception:
            logger.error(
                "Failed to drop staging table %s", STAGING_TABLE, exc_info=True
            )


# ---------------------------------------------------------------------------
# Module-level initialisation — runs when Glue executes the script
# ---------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "TempDir"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

run_job(glueContext, spark, args, VALIDATION_RULES)

job.commit()
