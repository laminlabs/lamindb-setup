"""Utilities to copy, clone and load Postgres instances as local SQLite databases.

.. autosummary::
   :toctree:

   init_local_sqlite
   connect_local_sqlite
   upload_sqlite_clone
"""

import gzip
import os
import shutil
from pathlib import Path

from lamindb_setup.core._settings_instance import InstanceSettings
from lamindb_setup.core._settings_load import load_instance_settings
from lamindb_setup.core._settings_store import instance_settings_file
from lamindb_setup.core.django import reset_django
from lamindb_setup.core.upath import create_path


def init_local_sqlite(
    instance: str | None = None, copy_suffix: str | None = None
) -> None:
    """Initialize SQLite copy of an existing Postgres instance.

    Creates a SQLite database with the same schema as the source Postgres instance.
    The copy shares the same storage location as the original instance.

    The copy is intended for read-only access to instance data without requiring a Postgres connection.
    Data synchronization to complete the clone happens via a separate Lambda function.

    Note that essential user, branch and storage tables are missing.
    Therefore, it is not possible to store Artifacts without having replayed these records first.

    Args:
        instance: Pass a slug (`account/name`) or URL (`https://lamin.ai/account/name`).
            If `None`, looks for an environment variable `LAMIN_CURRENT_INSTANCE` to get the instance identifier.
            If it doesn't find this variable, it connects to the instance that was connected with `lamin connect` through the CLI.
        copy_suffix: Optional suffix to append to the local clone name.
    """
    import lamindb_setup as ln_setup

    if instance is None:  # pragma: no cover
        instance = os.environ.get("LAMIN_CURRENT_INSTANCE")

    if instance is None:
        raise ValueError(
            "No instance identifier provided and LAMIN_CURRENT_INSTANCE is not set"
        )

    if ln_setup.settings.instance is None:  # pragma: no cover
        ln_setup.connect(instance)

    name = (
        f"{ln_setup.settings.instance.name}{copy_suffix}"
        if copy_suffix is not None
        else ln_setup.settings.instance.name
    )
    isettings = InstanceSettings(
        id=ln_setup.settings.instance._id,
        owner=ln_setup.settings.instance.owner,  # type: ignore
        name=name,
        storage=ln_setup.settings.storage,
        db=None,
        modules=",".join(ln_setup.settings.instance.modules),
        is_on_hub=False,
        _is_clone=True,
    )

    isettings._persist(write_to_disk=True)

    if not isettings._sqlite_file_local.exists():
        # Reset Django configuration before _init_db() because Django was already configured for the original Postgres instance.
        # Without this reset, the `if not settings.configured`` check in `setup_django()` would skip reconfiguration,
        # causing migrations to run against the old Postgres database instead of the new SQLite clone database.
        reset_django()
        isettings._init_db()


def connect_local_sqlite(
    instance: str,
) -> None:
    """Load a locally stored SQLite instance of which a remote hub Postgres instance exists.

    This function bypasses the hub lookup that `lamin connect` performs, loading the SQLite clone directly from local settings files.
    The clone must first be created via `init_local_sqlite()`.

    Args:
        instance: Instance slug in the form `account/name` (e.g., `laminlabs/privatedata-local`).
    """
    owner, name = instance.split("/")
    settings_file = instance_settings_file(name=name, owner=owner)

    if not settings_file.exists():
        raise ValueError(
            "SQLite clone not found."
            " Run `init_local_sqlite()` to create a local copy or connect to a remote copy using `connect_remote_sqlite`."
        )

    isettings = load_instance_settings(settings_file)
    isettings._persist(write_to_disk=False)

    # Using `setup_django` instead of `_load_db` to not ping AWS RDS
    from lamindb_setup._check_setup import disable_auto_connect

    from .django import setup_django

    disable_auto_connect(setup_django)(isettings)


def connect_remote_sqlite(instance: str, *, copy_suffix: str | None = None) -> None:
    """Load an existing SQLite copy of a hub instance.

    Args:
        instance: Instance slug in the form `account/name` (e.g., `laminlabs/privatedata-local`).
        copy_suffix: Optional suffix of the local clone.
    """
    import lamindb_setup as ln_setup

    owner, name = instance.split("/")

    # Step 1: Create the settings file
    isettings = ln_setup._connect_instance._connect_instance(owner=owner, name=name)
    isettings._db = None
    isettings._is_on_hub = False
    isettings._fine_grained_access = False
    isettings._db_permissions = "read"
    name = (
        f"{isettings.name}{copy_suffix}" if copy_suffix is not None else isettings.name
    )
    isettings._name = name
    isettings._is_clone = True
    isettings._persist(write_to_disk=True)

    # TODO This used to be the full download code that also enabled reading compressed files
    """
    cloud_db_path = str(ln_setup.settings.instance.storage.root) + "/.lamindb/lamin.db"
    sqlite_file_path_gz = create_path(cloud_db_path + ".gz")
    sqlite_file_path = create_path(cloud_db_path)

    local_sqlite_target_path = (
        ln_setup.settings.cache_dir
        / _strip_cloud_prefix(ln_setup.settings.instance.storage.root_as_str)
        / ".lamindb"
        / "lamin.db"
    )

    if not local_sqlite_target_path.exists() or overwrite:
        local_sqlite_target_path.parent.mkdir(parents=True, exist_ok=True)
        cloud_db_path = (
            str(ln_setup.settings.instance.storage.root) + "/.lamindb/lamin.db"
        )
        sqlite_file_path_gz = create_path(cloud_db_path + ".gz")
        sqlite_file_path = create_path(cloud_db_path)

        if sqlite_file_path_gz.exists():
            temp_gz_path = local_sqlite_target_path.with_suffix(".db.gz")
            sqlite_file_path_gz.download_to(temp_gz_path)
            with (
                gzip.open(temp_gz_path, "rb") as f_in,
                open(local_sqlite_target_path, "wb") as f_out,
            ):
                shutil.copyfileobj(f_in, f_out)
            temp_gz_path.unlink()
        else:
            sqlite_file_path.download_to(local_sqlite_target_path)
    """

    connect_local_sqlite(instance=instance + (copy_suffix or ""))


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
