"""DLQWriter — writes invalid records to S3 in partitioned JSON format."""

import logging
from datetime import date

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def build_dlq_path(dlq_base_path: str, job_run_date: date) -> str:
    """Construct the partitioned S3 path for DLQ output.

    Path pattern: <dlq_base_path>/year=YYYY/month=MM/day=DD/

    Handles trailing slash on the base path so the result never has
    double slashes between segments.
    """
    base = dlq_base_path.rstrip("/")
    year = f"{job_run_date.year:04d}"
    month = f"{job_run_date.month:02d}"
    day = f"{job_run_date.day:02d}"
    return f"{base}/year={year}/month={month}/day={day}/"


def write_dlq(invalid_df: DataFrame, dlq_base_path: str, job_run_date: date) -> int:
    """Write invalid records to S3 DLQ path and return the record count.

    Format: JSON, one object per line.
    Each record includes original fields + ``_validation_errors``.

    Returns the number of records written.
    """
    count = invalid_df.count()
    if count == 0:
        logger.info("No invalid records to write to DLQ.")
        return 0

    dlq_path = build_dlq_path(dlq_base_path, job_run_date)
    logger.info("Writing %d invalid records to DLQ path: %s", count, dlq_path)

    invalid_df.write.mode("overwrite").json(dlq_path)

    logger.info("Successfully wrote %d records to DLQ.", count)
    return count
