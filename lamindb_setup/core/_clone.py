"""Utilities to work with Postgres Snapshots.

.. autosummary::
   :toctree:

   upload_sqlite_clone
"""

import gzip
import shutil
from pathlib import Path

from lamindb_setup.core.upath import create_path


def upload_sqlite_clone(
    local_sqlite_path: Path | str | None = None, compress: bool = True
) -> None:
    """Uploads the SQLite clone to the default storage.

    Args:
        local_sqlite_path: Path to the SQLite file.
            Defaults to the local storage path if not specified.
        compress: Whether to compress the database with gzip before uploading.
    """
    import lamindb_setup as ln_setup

    if local_sqlite_path is None:
        local_sqlite_path = ln_setup.settings.instance._sqlite_file_local
    else:
        local_sqlite_path = Path(local_sqlite_path)

    if not local_sqlite_path.exists():
        raise FileNotFoundError(f"Database not found at {local_sqlite_path}")

    cloud_db_path = ln_setup.settings.instance._sqlite_file

    if compress:
        temp_gz_path = local_sqlite_path.with_suffix(".db.gz")
        with (
            open(local_sqlite_path, "rb") as f_in,
            gzip.open(temp_gz_path, "wb") as f_out,
        ):
            shutil.copyfileobj(f_in, f_out)
        cloud_destination = create_path(f"{cloud_db_path}.gz")
        cloud_destination.upload_from(temp_gz_path, print_progress=True)
        temp_gz_path.unlink()
    else:
        cloud_destination = create_path(cloud_db_path)
        cloud_destination.upload_from(local_sqlite_path, print_progress=True)
