"""Utilities to copy, clone and load Postgres instances as local SQLite databases.

.. autosummary::
   :toctree:

   init_local_sqlite
   connect_local_sqlite
"""

import os

from lamindb_setup.core._settings_instance import InstanceSettings
from lamindb_setup.core._settings_load import load_instance_settings
from lamindb_setup.core._settings_store import instance_settings_file
from lamindb_setup.core.django import reset_django


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
    )

    isettings._persist(write_to_disk=True)

    if not isettings._sqlite_file_local.exists():
        # Reset Django configuration before _init_db() because Django was already configured for the original Postgres instance.
        # Without this reset, the if not settings.configured check in setup_django() would skip reconfiguration,
        # causing migrations to run against the old Postgres database instead of the new SQLite clone database.
        reset_django()
        isettings._init_db()


def connect_local_sqlite(instance: str) -> None:
    """Load a SQLite instance of which a remote hub Postgres instance exists.

    This function bypasses the hub lookup that `lamin connect` performs, loading the SQLite clone directly from local settings files.
    The clone must first be created via `init_local_sqlite()`.

    Args:
        instance: Instance slug in the form `account/name` (e.g., `laminlabs/privatedata-local`).
    """
    owner, name = instance.split("/")
    settings_file = instance_settings_file(name=name, owner=owner)

    if not settings_file.exists():
        raise ValueError("SQLite clone not found. Run init_local_sqlite() first.")

    isettings = load_instance_settings(settings_file)
    isettings._persist(write_to_disk=False)
    isettings._load_db()
