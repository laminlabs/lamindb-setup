"""Utilities to clone and load Postgres instances as local SQLite databases.

.. autosummary::
   :toctree:

   init_clone
   load_clone
"""

import os

from lamindb_setup.core.django import reset_django

from ._settings_instance import InstanceSettings
from ._settings_storage import init_storage


def init_clone(instance: str | None = None, *, storage: str | None = None) -> None:
    """Initialize a clone SQLite instance.

    Args:
        instance: Pass a slug (`account/name`) or URL (`https://lamin.ai/account/name`).
            If `None`, looks for an environment variable `LAMIN_CURRENT_INSTANCE` to get the instance identifier.
            If it doesn't find this variable, it connects to the instance that was connected with `lamin connect` through the CLI.
        storage: Optional storage root override. If `None`, uses the same storage as the original instance.
    """
    import lamindb_setup as ln_setup

    if instance is None:
        current_instance = os.environ.get("LAMIN_CURRENT_INSTANCE", None)
        if current_instance is None:
            raise ValueError(
                "No instance identifier provided and LAMIN_CURRENT_INSTANCE is not set"
            )

    if instance is not None and ln_setup.settings.instance is None:
        ln_setup.connect(instance)

    owner = ln_setup.settings.instance.owner
    name = ln_setup.settings.instance.name
    instance_id = ln_setup.settings.instance._id

    ssettings, _ = init_storage(
        ln_setup.settings.storage.root,
        instance_id=instance_id,
        instance_slug=f"{owner}/{name}",
        register_hub=False,
    )

    isettings = InstanceSettings(
        id=instance_id,
        owner=owner,  # type: ignore
        name=name,
        storage=ssettings,
        db=None,
        modules=",".join(ln_setup.settings.instance.modules),
        is_on_hub=False,
    )

    # persist settings and initialize sqlite DB with same schema
    # TODO is this really what we want to do? Write new settings?
    isettings._persist(write_to_disk=True)

    # Reset Django configuration before _init_db() because Django was already configured for the original Postgres instance.
    # Without this reset, the if not settings.configured check in setup_django() would skip reconfiguration,
    # causing migrations to run against the old Postgres database instead of the new SQLite clone database.
    reset_django()

    # TODO this also connects - we need to discuss whether we want to keep the connection or connect back
    # We probably do NOT want to connect to the clone because it should be empty.
    # Only after the lambda got triggered at least once, it gets populated.
    # At the same time, this is called by the lambda and not user facing
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
