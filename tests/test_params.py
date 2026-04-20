"""Unit tests for glue_job.params.parse_args."""

import pytest

from glue_job.params import parse_args


class TestParseArgsValidInput:
    """Tests for known-good parameter sets (Requirements 4.1)."""

    def test_all_required_params(self):
        args = [
            "--source_path", "s3://src/data.csv",
            "--target_path", "s3://tgt/out/",
            "--catalog_database", "mydb",
            "--catalog_table_name", "mytable",
        ]
        result = parse_args(args)
        assert result["source_path"] == "s3://src/data.csv"
        assert result["target_path"] == "s3://tgt/out/"
        assert result["catalog_database"] == "mydb"
        assert result["catalog_table_name"] == "mytable"
        assert result["partition_key"] is None

    def test_all_params_with_partition_key(self):
        args = [
            "--source_path", "s3://src/data.json",
            "--target_path", "s3://tgt/out/",
            "--catalog_database", "db",
            "--catalog_table_name", "tbl",
            "--partition_key", "date",
        ]
        result = parse_args(args)
        assert result["partition_key"] == "date"

    def test_params_in_any_order(self):
        args = [
            "--catalog_table_name", "tbl",
            "--source_path", "s3://b/p",
            "--partition_key", "region",
            "--catalog_database", "db",
            "--target_path", "s3://t/o",
        ]
        result = parse_args(args)
        assert result["source_path"] == "s3://b/p"
        assert result["target_path"] == "s3://t/o"
        assert result["catalog_database"] == "db"
        assert result["catalog_table_name"] == "tbl"
        assert result["partition_key"] == "region"


class TestParseArgsMissingParams:
    """Tests for missing required parameters (Requirements 4.2)."""

    def test_missing_source_path(self):
        args = [
            "--target_path", "s3://tgt/",
            "--catalog_database", "db",
            "--catalog_table_name", "tbl",
        ]
        with pytest.raises(ValueError, match="source_path"):
            parse_args(args)

    def test_missing_target_path(self):
        args = [
            "--source_path", "s3://src/",
            "--catalog_database", "db",
            "--catalog_table_name", "tbl",
        ]
        with pytest.raises(ValueError, match="target_path"):
            parse_args(args)

    def test_missing_catalog_database(self):
        args = [
            "--source_path", "s3://src/",
            "--target_path", "s3://tgt/",
            "--catalog_table_name", "tbl",
        ]
        with pytest.raises(ValueError, match="catalog_database"):
            parse_args(args)

    def test_missing_catalog_table_name(self):
        args = [
            "--source_path", "s3://src/",
            "--target_path", "s3://tgt/",
            "--catalog_database", "db",
        ]
        with pytest.raises(ValueError, match="catalog_table_name"):
            parse_args(args)

    def test_empty_args(self):
        with pytest.raises(ValueError):
            parse_args([])
