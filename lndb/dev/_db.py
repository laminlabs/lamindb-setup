# We see a lot of import statements for lnschema_core below
# This is currently needed as we can only import the schema module
# once isettings have been adjusted
from typing import Any, List

import sqlalchemy as sa
import sqlmodel as sqm
from lamin_logger import logger

from .._settings import settings
from ._storage import StorageSettings


class upsert:
    @classmethod
    def user(cls, email: str, user_id: str, handle: str, name: str = None):
        with settings.instance.engine.connect() as conn:
            try:
                table = (
                    "core.user"
                    if settings.instance.dialect == "postgresql"
                    else '"core.user"'
                )
                user = conn.execute(
                    sa.text(f"select * from {table} where id = '{user_id}'")
                ).first()
            except Exception:
                user = conn.execute(
                    sa.text(f"select * from lnschema_core_user where id = '{user_id}'")
                ).first()
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
                with sqm.Session(settings.instance.engine) as session:
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

                settings.instance._update_cloud_sqlite_file()

        return user_id


class insert_if_not_exists:
    """Insert data if it does not yet exist.

    A wrapper around the `insert` class below.
    """

    @classmethod
    def storage(cls, storage_settings: StorageSettings) -> None:
        root_str = storage_settings.root_as_str
        with settings.instance.engine.connect() as conn:
            try:
                table = (
                    "core.storage"
                    if settings.instance.dialect == "postgresql"
                    else '"core.storage"'
                )
                storage = conn.execute(
                    sa.text(f"select * from {table} where root = '{root_str}'")
                ).first()
            except Exception:
                storage = conn.execute(
                    sa.text(
                        f"select * from lnschema_core_storage where root = '{root_str}'"
                    )
                ).first()
        if storage is None:
            storage = insert.storage(root_str, storage_settings.region)


class insert:
    """Insert data."""

    # this here actually first checks for existence
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
        schema_id, version, migration = (
            schema_module._schema_id,
            schema_module.__version__,
            schema_module._migration,
        )

        with sqm.Session(settings.instance.engine) as session:
            table_loc = (
                schema_module.dev if hasattr(schema_module, "dev") else schema_module
            )
            version_table = getattr(table_loc, f"version_{schema_id}")
            version_result = session.exec(
                sqm.select(version_table).where(version_table.v == version)
            ).first()
            if version_result is None:
                session.add(
                    version_table(v=version, migration=migration, user_id=user_id)
                )
                session.commit()
            else:
                logger.info(f"{version} is already live in DB")

        if cloud_sqlite:
            settings.instance._update_cloud_sqlite_file()

    @classmethod
    def user(cls, email, user_id, handle, name):
        """User."""
        import lnschema_core as schema_core

        with sqm.Session(settings.instance.engine) as session:
            user_table = schema_core.User
            user = user_table(id=user_id, email=email, handle=handle, name=name)
            session.add(user)
            session.commit()
            session.refresh(user)

        settings.instance._update_cloud_sqlite_file()

        return user.id

    @classmethod
    def storage(cls, root, region) -> None:
        """Storage."""
        from lnschema_core.dev.id import storage as storage_id

        id = storage_id()
        with settings.instance.engine.begin() as conn:
            try:
                table = (
                    "core.storage"
                    if settings.instance.dialect == "postgresql"
                    else '"core.storage"'
                )
                conn.execute(
                    sa.text(
                        f"insert into {table} (id, root, region, type) values"
                        " (:id, :root, :region, :type)"
                    ).bindparams(
                        id=id,
                        root=root,
                        region=region,
                        type=settings.instance.storage.type,
                    )
                )
            except Exception:
                conn.execute(
                    sa.text(
                        "insert into lnschema_core_storage (id, root, region, type)"
                        " values (:id, :root, :region, :type)"
                    ).bindparams(
                        id=id,
                        root=root,
                        region=region,
                        type=settings.instance.storage.type,
                    )
                )
        settings.instance._update_cloud_sqlite_file()

    @classmethod
    def bionty_versions(cls, records: List[sqm.SQLModel]):
        """Bionty versions."""
        from lnschema_bionty import dev

        with sqm.Session(settings.instance.engine) as session:
            for record in records:
                session.add(record)
                current_record = dev.CurrentBiontyVersions(
                    id=record.id, entity=record.entity
                )
                session.add(current_record)
            session.commit()

        settings.instance._update_cloud_sqlite_file()
