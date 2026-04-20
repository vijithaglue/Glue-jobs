"""Parameter parsing for Glue job."""

REQUIRED_PARAMS = ["source_path", "target_path", "catalog_database", "catalog_table_name"]
OPTIONAL_PARAMS = ["partition_key"]
ALL_PARAMS = REQUIRED_PARAMS + OPTIONAL_PARAMS


def parse_args(sys_args):
    """Parse sys.argv-style argument list into a job parameters dict.

    Accepts arguments in the form --key value. Extracts source_path,
    target_path, catalog_database, catalog_table_name (required) and
    partition_key (optional).

    Args:
        sys_args: List of strings in sys.argv style (e.g.
            ["--source_path", "s3://bucket/path", ...]).

    Returns:
        dict with keys for each recognised parameter. partition_key is
        None when not provided.

    Raises:
        ValueError: When any required parameter is missing. The message
            contains the name of the missing parameter.
    """
    parsed = {}
    i = 0
    while i < len(sys_args):
        arg = sys_args[i]
        if arg.startswith("--"):
            key = arg[2:]
            if key in ALL_PARAMS and i + 1 < len(sys_args):
                parsed[key] = sys_args[i + 1]
                i += 2
                continue
        i += 1

    for param in REQUIRED_PARAMS:
        if param not in parsed:
            raise ValueError(f"Missing required parameter: {param}")

    parsed.setdefault("partition_key", None)
    return parsed
