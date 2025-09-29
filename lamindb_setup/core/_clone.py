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
    from lamindb_setup.core._settings_instance import current_instance_settings_file

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

    owner, name = get_owner_name_from_identifier(instance)
    hub_result = connect_instance_hub(owner=owner, name=name)
    # TODO add better error handling because this can also be a string which acts as an error type
    instance_result, storage_result = hub_result  # type: ignore
    modules = instance_result.get("schema_str", None)

    # determine instance id
    # prefer the hub-provided id; if missing, allow an env override (used in tests/CI)
    # otherwise derive a deterministic id from the instance slug (same approach as init)
    instance_id_env = os.getenv("LAMINDB_INSTANCE_ID_INIT")
    if "id" in instance_result and instance_result.get("id"):
        instance_id = UUID(instance_result["id"])
    elif instance_id_env is not None:
        instance_id = UUID(instance_id_env)
    else:
        instance_id = uuid5(NAMESPACE_URL, f"{owner}/{name}")

    # prefer reusing the remote storage metadata (so blobs remain on the same storage)
    ssettings = None
    if isinstance(storage_result, dict) and storage_result.get("root"):
        ssettings = StorageSettings(
            root=storage_result["root"],
            region=storage_result.get("region"),
            uid=storage_result.get("lnid") or storage_result.get("uid"),
            uuid=UUID(storage_result["id"]) if storage_result.get("id") else None,
            instance_id=instance_id,
        )
    else:
        # fallback to creating or initializing a local storage
        # choose local storage root: env -> settings -> cwd
        storage_root = (
            os.environ.get("LAMIN_CLONE_DIR")
            or getattr(settings, "clone_dir", None)
            or Path.cwd()
        )
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
        modules=modules,
        is_on_hub=False,
        api_url=instance_result.get("api_url"),
    )

    # persist settings and initialize sqlite DB with same schema
    if hasattr(isettings, "_sqlite_file_local"):
        print(f"Local SQLite file: {isettings._sqlite_file_local}")

    isettings._persist(write_to_disk=True)
    # TODO this also connects - we need to discuss whether we want to keep the connection or connect back
    isettings._init_db()


def load_clone(instance: str | None = None) -> str | tuple | None:
    """Load a cloned SQLite instance.

    Args:
        instance: Pass a slug (`account/name`) or URL (`https://lamin.ai/account/name`).
            If `None`, looks for an environment variable `LAMIN_CURRENT_INSTANCE` to get the instance identifier.
            If it doesn't find this variable, it connects to the instance that was connected with `lamin connect` through the CLI.
    """
    pass
