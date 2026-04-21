"""Glue job: s3toredshift

Reads data from the Glue Catalog table 'taxi_join' in the 'parquetdb'
database and writes it to a Redshift cluster using the Glue connection
'testconnection_refshift1'.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from Glue Catalog table
source = glueContext.create_dynamic_frame.from_catalog(
    database="parquetdb",
    table_name="taxi_join",
    transformation_ctx="source"
)

# Write to Redshift via Glue connection
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=datasource,
    catalog_connection="testconnection_refshift1",
    connection_options={
        "dbtable": "public.taxi_join",
        "database": "dev"
    },
    redshift_tmp_dir=args.get("TempDir", ""),
    transformation_ctx="redshift_sink"
)

job.commit()
