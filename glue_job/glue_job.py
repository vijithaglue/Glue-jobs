"""Main Glue ETL job script.

Reads data from an S3 source path (CSV or JSON), writes to a target S3
path in Parquet format, and registers the output in the AWS Glue Data
Catalog.
"""

import logging
import sys

import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext

from glue_job.format_detector import detect_format
from glue_job.params import parse_args

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_source(glue_context, source_path):
    """Read data from an S3 source path into a DynamicFrame.

    Uses detect_format to determine whether the source is CSV or JSON,
    then reads accordingly.

    Args:
        glue_context: An awsglue.context.GlueContext instance.
        source_path: S3 path to the source data
            (e.g. "s3://bucket/path/data.csv").

    Returns:
        A DynamicFrame containing the source data, or None if the
        source path contains no files.

    Raises:
        Exception: If the source path is unreachable or access is
            denied. The error is logged before re-raising.
    """
    fmt = detect_format(source_path)
    try:
        dynamic_frame = glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [source_path]},
            format=fmt,
        )
        if dynamic_frame.count() == 0:
            logger.warning(
                "Source path %s contains no files. Returning None.", source_path
            )
            return None
        return dynamic_frame
    except Exception:
        logger.error(
            "Failed to read from source path %s.", source_path, exc_info=True
        )
        raise


def write_target(glue_context, dynamic_frame, target_path, partition_key=None):
    """Write a DynamicFrame to an S3 target path in Parquet format.

    Args:
        glue_context: An awsglue.context.GlueContext instance.
        dynamic_frame: The DynamicFrame to write.
        target_path: S3 path for the output
            (e.g. "s3://bucket/output/").
        partition_key: Optional column name to partition by. When None
            the output is written without partitioning.

    Raises:
        Exception: If writing fails. The error is logged before
            re-raising.
    """
    try:
        connection_options = {"path": target_path}
        if partition_key is not None:
            connection_options["partitionKeys"] = [partition_key]

        glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options=connection_options,
            format="parquet",
        )
    except Exception:
        logger.error(
            "Failed to write to target path %s.", target_path, exc_info=True
        )
        raise


def register_catalog(glue_context, dynamic_frame, catalog_database,
                     catalog_table_name, target_path):
    """Register or update a table in the AWS Glue Data Catalog.

    Creates the Catalog Database if it does not already exist, then
    creates or updates the Catalog Table with the schema derived from
    the DynamicFrame.

    Args:
        glue_context: An awsglue.context.GlueContext instance.
        dynamic_frame: The DynamicFrame whose schema defines the table.
        catalog_database: Name of the Glue Catalog database.
        catalog_table_name: Name of the Glue Catalog table.
        target_path: S3 location of the data the table points to.
    """
    glue_client = boto3.client("glue")

    # Create database if it does not exist
    try:
        glue_client.get_database(Name=catalog_database)
    except glue_client.exceptions.EntityNotFoundException:
        logger.info("Database %s not found. Creating it.", catalog_database)
        glue_client.create_database(
            DatabaseInput={"Name": catalog_database}
        )

    # Derive columns from the DynamicFrame schema
    schema = dynamic_frame.schema()
    columns = []
    for field in schema:
        col_type = field.dataType.jsonValue().get("dataType", "string")
        columns.append({"Name": field.name, "Type": col_type})

    table_input = {
        "Name": catalog_table_name,
        "StorageDescriptor": {
            "Columns": columns,
            "Location": target_path,
            "InputFormat": (
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
            ),
            "OutputFormat": (
                "org.apache.hadoop.hive.ql.io.parquet."
                "MapredParquetOutputFormat"
            ),
            "SerdeInfo": {
                "SerializationLibrary": (
                    "org.apache.hadoop.hive.ql.io.parquet.serde."
                    "ParquetHiveSerDe"
                ),
            },
        },
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet"},
    }

    # Create or update the table
    try:
        glue_client.get_table(
            DatabaseName=catalog_database, Name=catalog_table_name
        )
        # Table exists — update it
        glue_client.update_table(
            DatabaseName=catalog_database, TableInput=table_input
        )
        logger.info(
            "Updated table %s.%s.", catalog_database, catalog_table_name
        )
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(
            DatabaseName=catalog_database, TableInput=table_input
        )
        logger.info(
            "Created table %s.%s.", catalog_database, catalog_table_name
        )


def main():
    """Entry point for the Glue ETL job.

    Wires together parameter parsing, source reading, target writing,
    and catalog registration. Exits with code 0 on success and code 1
    on any failure.
    """
    try:
        params = parse_args(sys.argv)
    except ValueError as exc:
        logger.error("Parameter error: %s", exc)
        sys.exit(1)

    sc = SparkContext()
    glue_context = GlueContext(sc)

    try:
        dynamic_frame = read_source(glue_context, params["source_path"])

        if dynamic_frame is None:
            # Source path was empty — logged inside read_source
            sc.stop()
            sys.exit(0)

        write_target(
            glue_context,
            dynamic_frame,
            params["target_path"],
            partition_key=params["partition_key"],
        )

        register_catalog(
            glue_context,
            dynamic_frame,
            params["catalog_database"],
            params["catalog_table_name"],
            params["target_path"],
        )
    except Exception:
        logger.error("Job failed.", exc_info=True)
        sc.stop()
        sys.exit(1)

    logger.info("Job completed successfully.")
    sc.stop()
    sys.exit(0)


if __name__ == "__main__":
    main()
