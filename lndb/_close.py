import os

from ._settings_store import current_instance_settings_file


def close() -> None:
    """Close existing instance.

    Returns `None` if succeeds, otherwise an exception is raised.
    """
    try:
        current_instance_settings_file().unlink()
        os.environ["LAMINDB_INSTANCE_LOADED"] = "0"
    except FileNotFoundError:
        raise
