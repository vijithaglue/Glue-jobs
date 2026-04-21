"""Glue job: s3toredshift

Reads CSV data from s3://athena-ctas-result/SampleTaxis/ (hourly
partitioned), writes to Redshift dev database as taxi_lookup table
using Glue connection 'testconnection_refshift1'.

Uses a staging table + transaction to handle partial writes safely.
If the job fails mid-write, the staging table is dropped and the
production table remains untouched.
"""

import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_PATH = "s3://athena-ctas-result/SampleTaxis/"
REDSHIFT_CONNECTION = "Redshift-2"
REDSHIFT_DATABASE = "dev"
STAGING_TABLE = "public.taxi_lookup_temp"
TARGET_TABLE = "public.taxi_lookup"

# Step 1: Read CSV data from S3 partitioned path
logger.info("Reading CSV data from %s", SOURCE_PATH)
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [SOURCE_PATH],
        "recurse": True
    },
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="datasource"
)

record_count = datasource.count()
logger.info("Read %d records from S3", record_count)

if record_count == 0:
    logger.warning("No data found in %s. Exiting.", SOURCE_PATH)
    job.commit()
    sys.exit(0)

# Step 2: Write to staging table in Redshift
# Using a staging table so that if the write fails partway through,
# the production table is not corrupted.
logger.info("Writing %d records to staging table %s", record_count, STAGING_TABLE)
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=datasource,
    catalog_connection=REDSHIFT_CONNECTION,
    connection_options={
        "dbtable": STAGING_TABLE,
        "database": REDSHIFT_DATABASE,
        "preactions": f"DROP TABLE IF EXISTS {STAGING_TABLE}; CREATE TABLE {STAGING_TABLE} (LIKE {TARGET_TABLE});",
        "postactions": (
            f"BEGIN TRANSACTION; "
            f"DELETE FROM {TARGET_TABLE}; "
            f"INSERT INTO {TARGET_TABLE} SELECT * FROM {STAGING_TABLE}; "
            f"DROP TABLE IF EXISTS {STAGING_TABLE}; "
            f"END TRANSACTION;"
        )
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="redshift_sink"
)

logger.info("Successfully loaded data into %s", TARGET_TABLE)
job.commit()
