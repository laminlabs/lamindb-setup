"""Utilities to clone and load Postgres instances as local SQLite databases.

.. autosummary::
   :toctree:

   init_clone
   load_clone
"""

import os
from pathlib import Path

from lamindb_setup.core.django import reset_django

from ._settings_instance import InstanceSettings


def init_clone(instance: str | None = None) -> None:
    """Initialize a clone SQLite instance.

    Creates a SQLite database with the same schema as the source Postgres instance.
    The clone shares the same storage location as the original instance.

    The clone is intended for read-only access to instance data without requiring a Postgres connection.
    Data synchronization happens via a separate Lambda function.

    Args:
        instance: Pass a slug (`account/name`) or URL (`https://lamin.ai/account/name`).
            If `None`, looks for an environment variable `LAMIN_CURRENT_INSTANCE` to get the instance identifier.
            If it doesn't find this variable, it connects to the instance that was connected with `lamin connect` through the CLI.
        storage: Optional storage root override. If `None`, uses the same storage as the original instance.
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

    owner = ln_setup.settings.instance.owner
    name = ln_setup.settings.instance.name
    instance_id = ln_setup.settings.instance._id

    isettings = InstanceSettings(
        id=instance_id,
        owner=owner,  # type: ignore
        name=name,
        storage=ln_setup.settings.storage,
        db=None,
        modules=",".join(ln_setup.settings.instance.modules),
        is_on_hub=False,
    )

    isettings._persist(write_to_disk=True)

    # Reset Django configuration before _init_db() because Django was already configured for the original Postgres instance.
    # Without this reset, the if not settings.configured check in setup_django() would skip reconfiguration,
    # causing migrations to run against the old Postgres database instead of the new SQLite clone database.
    if not Path(isettings._sqlite_file_local).exists():
        reset_django()
        isettings._init_db()


def load_clone(instance: str | None = None) -> str | tuple | None:
    """Load a cloned SQLite instance.

    Args:
        instance: Pass a slug (`account/name`) or URL (`https://lamin.ai/account/name`).
            If `None`, looks for an environment variable `LAMIN_CURRENT_INSTANCE` to get the instance identifier.
            If it doesn't find this variable, it connects to the instance that was connected with `lamin connect` through the CLI.
    """
    # use instance slug to find SQLite file and connect to it
    pass
