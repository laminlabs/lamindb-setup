# We see a lot of import statements for lnschema_core below
# This is currently needed as we can only import the schema module
# once settings have been adjusted
from typing import Any

import sqlmodel as sqm
from lamin_logger import logger

from ._settings_load import load_or_create_instance_settings


class upsert:
    @classmethod
    def user(cls, email: str, user_id: str, handle: str, name: str = None):
        import lnschema_core as schema_core

        settings = load_or_create_instance_settings()
        engine = settings.db_engine()
        with sqm.Session(engine) as session:
            user_table = (
                schema_core.User if hasattr(schema_core, "User") else schema_core.user
            )  # noqa
            user = session.get(user_table, user_id)
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
        import lnschema_core as schema_core

        root = str(root)
        settings = load_or_create_instance_settings()
        engine = settings.db_engine()
        with sqm.Session(engine) as session:
            storage_table = (
                schema_core.Storage
                if hasattr(schema_core, "Storage")
                else schema_core.storage
            )  # noqa
            storage = session.exec(
                sqm.select(storage_table).where(storage_table.root == root)
            ).first()
        if storage is None:
            storage = insert.storage(root, region)  # type: ignore

        return storage


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
            schema_module._schema_id,
            schema_module.__version__,
            schema_module._migration,
        )

        with sqm.Session(engine) as session:
            table_loc = (
                schema_module.dev if hasattr(schema_module, "dev") else schema_module
            )
            version_table = getattr(table_loc, f"version_{schema_id}")
            session.add(version_table(v=version, migration=migration, user_id=user_id))
            session.commit()

        if cloud_sqlite:
            settings._update_cloud_sqlite_file()

    @classmethod
    def user(cls, email, user_id, handle, name):
        """User."""
        import lnschema_core as schema_core

        settings = load_or_create_instance_settings()
        engine = settings.db_engine()

        with sqm.Session(engine) as session:
            user_table = (
                schema_core.User if hasattr(schema_core, "User") else schema_core.user
            )  # noqa
            user = user_table(id=user_id, email=email, handle=handle, name=name)
            session.add(user)
            session.commit()
            session.refresh(user)

        settings._update_cloud_sqlite_file()

        return user.id

    @classmethod
    def storage(cls, root, region):
        """Storage."""
        import lnschema_core as schema_core

        settings = load_or_create_instance_settings()
        engine = settings.db_engine()

        with sqm.Session(engine) as session:
            storage_table = (
                schema_core.Storage
                if hasattr(schema_core, "Storage")
                else schema_core.storage
            )  # noqa
            storage = storage_table(
                root=root, region=region, type=settings.storage.type
            )
            session.add(storage)
            session.commit()
            session.refresh(storage)

        settings._update_cloud_sqlite_file()

        return storage
