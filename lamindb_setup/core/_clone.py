"""Utilities to copy, clone and load Postgres instances as local SQLite databases.

.. autosummary::
   :toctree:

   init_local_sqlite
"""

import os
from pathlib import Path

from lamindb_setup.core.django import reset_django

from ._settings_instance import InstanceSettings


def init_local_sqlite(instance: str | None = None) -> None:
    """Initialize SQLite copy of an existing Postgres instance.

    Creates a SQLite database with the same schema as the source Postgres instance.
    The copy shares the same storage location as the original instance.

    The copy is intended for read-only access to instance data without requiring a Postgres connection.
    Data synchronization to complete the clone happens via a separate Lambda function.

    Args:
        instance: Pass a slug (`account/name`) or URL (`https://lamin.ai/account/name`).
            If `None`, looks for an environment variable `LAMIN_CURRENT_INSTANCE` to get the instance identifier.
            If it doesn't find this variable, it connects to the instance that was connected with `lamin connect` through the CLI.
    """
    import lamindb_setup as ln_setup

    if instance is None:  # pragma: no cover
        current_instance = os.environ.get("LAMIN_CURRENT_INSTANCE", None)
        if current_instance is None:
            raise ValueError(
                "No instance identifier provided and LAMIN_CURRENT_INSTANCE is not set"
            )

    if instance is not None and ln_setup.settings.instance is None:  # pragma: no cover
        ln_setup.connect(instance)

    isettings = InstanceSettings(
        id=ln_setup.settings.instance._id,
        owner=ln_setup.settings.instance.owner,  # type: ignore
        name=ln_setup.settings.instance.name,
        storage=ln_setup.settings.storage,
        db=None,
        modules=",".join(ln_setup.settings.instance.modules),
        is_on_hub=False,
    )

    isettings._persist(write_to_disk=True)

    if not Path(isettings._sqlite_file_local).exists():
        # Reset Django configuration before _init_db() because Django was already configured for the original Postgres instance.
        # Without this reset, the if not settings.configured check in setup_django() would skip reconfiguration,
        # causing migrations to run against the old Postgres database instead of the new SQLite clone database.
        reset_django()
        isettings._init_db()
