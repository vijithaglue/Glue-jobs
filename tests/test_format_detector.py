"""Unit tests for glue_job.format_detector.detect_format."""

from glue_job.format_detector import detect_format


class TestDetectFormat:
    """Tests for format detection (Requirements 1.2, 1.3)."""

    def test_csv_extension(self):
        assert detect_format("s3://bucket/data/file.csv") == "csv"

    def test_json_extension(self):
        assert detect_format("s3://bucket/data/file.json") == "json"

    def test_csv_uppercase(self):
        assert detect_format("s3://bucket/data/FILE.CSV") == "csv"

    def test_json_uppercase(self):
        assert detect_format("s3://bucket/data/FILE.JSON") == "json"

    def test_no_extension_defaults_csv(self):
        assert detect_format("s3://bucket/data/file") == "csv"

    def test_unknown_extension_defaults_csv(self):
        assert detect_format("s3://bucket/data/file.parquet") == "csv"

    def test_path_with_dots_in_directory(self):
        assert detect_format("s3://bucket/v1.0/data.json") == "json"
