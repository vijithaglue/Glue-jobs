"""Format detection for source data."""

import os


def detect_format(source_path):
    """Detect the data format from an S3 path's file extension.

    Args:
        source_path: S3 path string (e.g. "s3://bucket/data/file.csv").

    Returns:
        "csv" for paths ending in .csv, "json" for paths ending in
        .json, and "csv" as the default for any other extension.
    """
    ext = os.path.splitext(source_path)[1].lower()
    if ext == ".json":
        return "json"
    return "csv"
