"""Staging merger SQL builders for Redshift staging-table workflow."""


def build_merge_preactions(staging_table: str, target_table: str) -> str:
    """Build SQL preactions to drop and recreate the staging table.

    Returns a SQL string that drops the staging table if it exists
    and recreates it with the same schema as the target table.
    """
    return (
        f"DROP TABLE IF EXISTS {staging_table}; "
        f"CREATE TABLE {staging_table} (LIKE {target_table});"
    )


def build_merge_postactions(staging_table: str, target_table: str) -> str:
    """Build SQL postactions for a transactional merge into the target table.

    Returns a SQL string that atomically deletes existing rows from the
    target table, inserts all rows from the staging table, and drops
    the staging table, all within a single transaction block.
    """
    return (
        f"BEGIN TRANSACTION; "
        f"DELETE FROM {target_table}; "
        f"INSERT INTO {target_table} SELECT * FROM {staging_table}; "
        f"DROP TABLE IF EXISTS {staging_table}; "
        f"END TRANSACTION;"
    )
