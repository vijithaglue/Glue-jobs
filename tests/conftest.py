"""Shared test fixtures for the Glue job test suite."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a shared local SparkSession for the entire test session."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("glue-job-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()
