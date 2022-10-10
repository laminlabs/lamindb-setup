from typing import Any

import lnschema_core as schema_core
import sqlmodel as sqm
from lamin_logger import logger

from ._settings_load import load_or_create_instance_settings


class upsert:
    @classmethod
    def user(cls, email: str, user_id: str, handle: str, name: str = None):
        settings = load_or_create_instance_settings()
        engine = settings.db_engine()
        with sqm.Session(engine) as session:
            user = session.get(schema_core.user, user_id)
        if user is None:
            user_id = insert.user(email, user_id, handle, name)  # type: ignore
            # do not update sqlite on the cloud as this happens within
            # insert.user
        else:
            # update the user record
            update_email = email != user.email
            update_handle = handle != user.handle
            # name = None is currently understood as no user name yet provided
            update_name = name != user.name and name is not None

            if any((update_email, update_handle, update_name)):
                with sqm.Session(engine) as session:
                    msg = "Updating: "
                    if update_email:
                        msg += f"{user.email} -> {email} "
                        user.email = email
                    if update_handle:
                        msg += f"{user.handle} -> {handle} "
                        user.handle = handle
                    if update_name:
                        msg += f"{user.name} -> {name} "
                        user.name = name
                    logger.info(msg)
                    session.add(user)
                    session.commit()

                settings._update_cloud_sqlite_file()

        return user_id


class insert_if_not_exists:
    """Insert data if it does not yet exist.

    A wrapper around the `insert` class below.
    """

    @classmethod
    def storage(cls, root, region):
        root = str(root)
        settings = load_or_create_instance_settings()
        engine = settings.db_engine()
        with sqm.Session(engine) as session:
            storage = session.exec(
                sqm.select(schema_core.storage).where(schema_core.storage.root == root)
            ).first()
        if storage is None:
            root = insert.storage(root, region)  # type: ignore

        return root


class insert:
    """Insert data."""

    @classmethod
    def version(
        cls,
        *,
        schema_module: Any,
        user_id: str,
        cloud_sqlite: bool = True,
    ):
        """Core schema module version.

        Args:
            schema_module: The schema module.
            user_id: User ID.
            cloud_sqlite: Update cloud SQLite file or not.
        """
        settings = load_or_create_instance_settings()
        engine = settings.db_engine()

        schema_id, version, migration = (
            schema_module._schema_id
            if hasattr(schema_module, "_schema_id")
            else schema_module._schema,  # backward compat
            schema_module.__version__,
            schema_module._migration,
        )

        with sqm.Session(engine) as session:
            version_table = getattr(schema_module, f"version_{schema_id}")
            session.add(version_table(v=version, migration=migration, user_id=user_id))
            # only update migration table if it hasn't already auto-updated
            # by the migration tool and if migration is not None!
            migration_table = getattr(schema_module, f"migration_{schema_id}")
            exists = session.get(migration_table, migration)
            if exists is None and migration is not None:
                session.add(migration_table(version_num=migration))
            session.commit()

        if cloud_sqlite:
            settings._update_cloud_sqlite_file()

    @classmethod
    def user(cls, email, user_id, handle, name):
        """User."""
        settings = load_or_create_instance_settings()
        engine = settings.db_engine()

        with sqm.Session(engine) as session:
            user = schema_core.user(id=user_id, email=email, handle=handle, name=name)
            session.add(user)
            session.commit()
            session.refresh(user)

        settings._update_cloud_sqlite_file()

        return user.id

    @classmethod
    def storage(cls, root, region):
        """Storage."""
        settings = load_or_create_instance_settings()
        engine = settings.db_engine()

        with sqm.Session(engine) as session:
            storage = schema_core.storage(
                root=root, region=region, type=settings.storage.type
            )
            session.add(storage)
            session.commit()
            session.refresh(storage)

        settings._update_cloud_sqlite_file()

        return storage.root
