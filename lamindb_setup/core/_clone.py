"""Clone Postgres instance into SQLite instance.

.. autosummary::
   :toctree:

   init_clone
   load_clone

"""

import os
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5


def init_clone(instance: str | None = None) -> None:
    """Initialize a clone SQLite instance.

    Args:
        instance: Pass a slug (`account/name`) or URL (`https://lamin.ai/account/name`).
            If `None`, looks for an environment variable `LAMIN_CURRENT_INSTANCE` to get the instance identifier.
            If it doesn't find this variable, it connects to the instance that was connected with `lamin connect` through the CLI.
    """
    from lamindb_setup._connect_instance import get_owner_name_from_identifier

    from ._hub_core import connect_instance_hub
    from ._settings import settings
    from ._settings_instance import InstanceSettings
    from ._settings_storage import StorageSettings, init_storage

    if instance is None:
        instance = os.environ.get("LAMIN_CURRENT_INSTANCE")
    if instance is None:
        raise ValueError(
            "No instance identifier provided and LAMIN_CURRENT_INSTANCE is not set"
        )

    import lamindb as ln

    owner = ln.setup.settings.user
    name = ln.setup.settings.instance.name
    instance_id = ln.setup.settings.instance._id
    modules = ln.setup.settings.instance.modules
    storage_root = (
        str(ln.setup.settings.storage.root) + "-test"
    )  # cannot use the same storage location for local tests

    ssettings, _ = init_storage(
        storage_root,
        instance_id=instance_id,
        instance_slug=f"{owner}/{name}",
        register_hub=False,
        init_instance=True,
    )

    # construct InstanceSettings that points to local sqlite (db=None)
    isettings = InstanceSettings(
        id=instance_id,
        owner=owner,
        name=name,
        storage=ssettings,
        db=None,
        modules=",".join(modules),
        is_on_hub=False,
    )

    # persist settings and initialize sqlite DB with same schema
    # if hasattr(isettings, "_sqlite_file_local"):
    #    print(f"Local SQLite file: {isettings._sqlite_file_local}")

    # TODO is this really what we want to do? Write new settings?
    isettings._persist(write_to_disk=True)

    from lamindb_setup.core.django import reset_django

    reset_django()

    # TODO this also connects - we need to discuss whether we want to keep the connection or connect back
    # We probably do NOT want to connect to the clone because it should be empty.
    # Only after the lambda got triggered at least once, it gets populated.
    isettings._init_db()

    from lamindb_setup._init_instance import load_from_isettings

    load_from_isettings(isettings, init=True, write_settings=True)


def load_clone(instance: str | None = None) -> str | tuple | None:
    """Load a cloned SQLite instance.

    Args:
        instance: Pass a slug (`account/name`) or URL (`https://lamin.ai/account/name`).
            If `None`, looks for an environment variable `LAMIN_CURRENT_INSTANCE` to get the instance identifier.
            If it doesn't find this variable, it connects to the instance that was connected with `lamin connect` through the CLI.
    """
    # use instance slug to find SQLite file and connect to it
    pass
