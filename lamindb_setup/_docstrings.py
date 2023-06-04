# This is here to optimize cold-start of the CLI

# This provides the doc strings for the init function on the
# CLI and the API
# It is located here as it *mostly* parallels the InstanceSettings docstrings.
# Small differences are on purpose, due to the different scope!
class instance_description:
    storage_root = """Storage root. Either local dir, ``s3://bucket_name`` or ``gs://bucket_name``."""  # noqa
    db = """Database connection url, do not pass for SQLite."""
    name = """Instance name."""
    schema = """Comma-separated string of schema modules. None if not set."""
