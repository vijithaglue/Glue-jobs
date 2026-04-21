import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1775176362327 = glueContext.create_dynamic_frame.from_catalog(database="parquetdb", table_name="taxi_zone_lookup", transformation_ctx="AWSGlueDataCatalog_node1775176362327")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1775176406340 = glueContext.create_dynamic_frame.from_catalog(database="parquetdb", table_name="yellow_tripdata", transformation_ctx="AWSGlueDataCatalog_node1775176406340")

# Script generated for node Join
AWSGlueDataCatalog_node1775176362327DF = AWSGlueDataCatalog_node1775176362327.toDF()
AWSGlueDataCatalog_node1775176406340DF = AWSGlueDataCatalog_node1775176406340.toDF()
Join_node1775176396475 = DynamicFrame.fromDF(AWSGlueDataCatalog_node1775176362327DF.join(AWSGlueDataCatalog_node1775176406340DF, (AWSGlueDataCatalog_node1775176362327DF['pulocationid'] == AWSGlueDataCatalog_node1775176406340DF['pulocationid']), "right"), glueContext, "Join_node1775176396475")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1775176396475, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775176352752", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1775176527600 = glueContext.getSink(path="s3://athena-ctas-result/testse/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1775176527600")
AmazonS3_node1775176527600.setCatalogInfo(catalogDatabase="parquetdb",catalogTableName="taxi_join")
AmazonS3_node1775176527600.setFormat("glueparquet", compression="snappy")
AmazonS3_node1775176527600.writeFrame(Join_node1775176396475)
job.commit()
