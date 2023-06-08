import os
import shutil
from pathlib import Path
from typing import Literal, Optional, Set, Tuple, Union

from lamin_logger import logger

from ._settings_save import save_instance_settings
from ._settings_store import current_instance_settings_file, instance_settings_file
from ._storage import StorageSettings
from .upath import UPath

# leave commented out until we understand more how to deal with
# migrations in redun
# https://stackoverflow.com/questions/2614984/sqlite-sqlalchemy-how-to-enforce-foreign-keys
# foreign key constraints for sqlite3
# from sqlite3 import Connection as SQLite3Connection
# from sqlalchemy import event
# from sqlalchemy.engine import Engine
# @event.listens_for(Engine, "connect")
# def _set_sqlite_pragma(dbapi_connection, connection_record):
#     if isinstance(dbapi_connection, SQLite3Connection):
#         cursor = dbapi_connection.cursor()
#         cursor.execute("PRAGMA foreign_keys=ON;")
#         cursor.close()


class InstanceSettings:
    """Instance settings."""

    def __init__(
        self,
        owner: str,  # owner handle
        name: str,  # instance name
        storage_root: Union[str, Path, UPath],  # storage location on cloud
        storage_region: Optional[str] = None,
        db: Optional[str] = None,  # DB URI
        schema: Optional[str] = None,  # comma-separated string of schema names
    ):
        self._owner: str = owner
        self._name: str = name
        self._storage: StorageSettings = StorageSettings(
            storage_root, instance_settings=self, region=storage_region
        )
        self._db: Optional[str] = db
        self._schema_str: Optional[str] = schema

    def __repr__(self):
        """Rich string representation."""
        representation = f"Current instance: {self.identifier}"
        attrs = ["owner", "name", "storage", "db", "schema"]
        for attr in attrs:
            value = getattr(self, attr)
            if attr == "storage":
                representation += f"\n- storage root: {value.root_as_str}"
                representation += f"\n- storage region: {value.region}"
            else:
                representation += f"\n- {attr}: {value}"
        return representation

    @property
    def owner(self) -> str:
        """Instance owner. A user or organization account handle."""
        return self._owner

    @property
    def name(self) -> str:
        """Instance name."""
        return self._name

    @property
    def identifier(self) -> str:
        """Unique identifier.

        See remote instances at https://lamin.ai/owner/name.
        """
        return f"{self.owner}/{self.name}"

    @property
    def schema(self) -> Set[str]:
        """Schema modules in addition to core schema."""
        if self._schema_str is None:
            return {}  # type: ignore
        else:
            return {schema for schema in self._schema_str.split(",") if schema != ""}

    @property
    def _sqlite_file(self) -> Union[Path, UPath]:
        """SQLite file.

        Is a UPath if on S3 or GS, otherwise a Path.
        """
        return self.storage.key_to_filepath(f"{self.name}.lndb")

    @property
    def _sqlite_file_local(self) -> Path:
        """Local SQLite file."""
        return self.storage.cloud_to_local_no_update(self._sqlite_file)

    def _update_cloud_sqlite_file(self) -> None:
        """Upload the local sqlite file to the cloud file."""
        if self._is_cloud_sqlite:
            logger.info("Updating & unlocking cloud SQLite")
            sqlite_file = self._sqlite_file
            cache_file = self.storage.cloud_to_local_no_update(sqlite_file)
            sqlite_file.upload_from(cache_file)  # type: ignore
            cloud_mtime = sqlite_file.modified.timestamp()  # type: ignore
            # this seems to work even if there is an open connection
            # to the cache file
            os.utime(cache_file, times=(cloud_mtime, cloud_mtime))
            self._cloud_sqlite_locker.unlock()

    def _update_local_sqlite_file(self) -> None:
        """Download the cloud sqlite file if it is newer than local."""
        if self._is_cloud_sqlite:
            logger.info(
                "Synching cloud SQLite to local (synch back to cloud via: lamin close)"
            )
            sqlite_file = self._sqlite_file
            cache_file = self.storage.cloud_to_local_no_update(sqlite_file)
            sqlite_file.synchronize(cache_file)  # type: ignore
            self._cloud_sqlite_locker.lock()

    @property
    def db(self) -> str:
        """Database URL."""
        if self._db is None:
            # here, we want the updated sqlite file
            # hence, we don't use self._sqlite_file_local()
            return f"sqlite:///{self.storage.cloud_to_local(self._sqlite_file)}"
        else:
            return self._db

    @property
    def dialect(self) -> Literal["sqlite", "postgresql"]:
        """SQL dialect."""
        if self._db is None or self._db.startswith("sqlite://"):
            return "sqlite"
        else:
            assert self._db.startswith("postgresql")
            return "postgresql"

    @property
    def engine(self):
        """Database engine.

        In case of remote sqlite, does not update the local sqlite.
        """
        import sqlalchemy as sa

        return sa.create_engine(self.db, future=True)

    @property
    def session(self):
        raise NotImplementedError

    @property
    def _is_cloud_sqlite(self) -> bool:
        # can we make this a private property, Sergei?
        # as it's not relevant to the user
        """Is this a cloud instance with sqlite db."""
        return self.dialect == "sqlite" and self.storage.is_cloud

    @property
    def _cloud_sqlite_locker(self):
        # avoid circular import
        from ._cloud_sqlite_locker import empty_locker

        if self._is_cloud_sqlite:
            return empty_locker
        else:
            return empty_locker

    @property
    def storage(self) -> StorageSettings:
        """Low-level access to storage location."""
        return self._storage

    @property
    def is_remote(self) -> bool:
        """Boolean indicating if an instance has no local component."""
        if not self.storage.is_cloud:
            return False

        def is_local_uri(uri: str):
            if "@localhost:" in uri:
                return True
            if "@0.0.0.0:" in uri:
                return True
            if "@127.0.0.1" in uri:
                return True
            return False

        if self.dialect == "postgresql":
            if is_local_uri(self.db):
                return False
        # returns True for cloud SQLite
        # and remote postgres
        return True

    def _persist(self) -> None:
        assert self.name is not None
        if self.storage.type == "local":
            self.storage.root.mkdir(parents=True, exist_ok=True)

        filepath = instance_settings_file(self.name, self.owner)
        # persist under filepath for later reference
        save_instance_settings(self, filepath)
        # persist under current file for auto load
        shutil.copy2(filepath, current_instance_settings_file())
        # persist under settings class for same session reference
        # need to import here to avoid circular import
        from .._settings import settings

        settings._instance_settings = self

    def _is_db_setup(self, mute: bool = False) -> Tuple[bool, str]:
        """Is the database available and initialized as LaminDB?"""
        if not self._is_db_reachable(mute=mute):
            if self.dialect == "sqlite":
                return (
                    False,
                    f"SQLite file {self._sqlite_file} does not exist! It should be in"
                    f" the storage root: {self.storage.root}",
                )
            else:
                return False, f"Connection {self.db} not reachable"

        # in order to proceed with the next check, we need the local sqlite
        self._update_local_sqlite_file()

        import sqlalchemy as sa

        engine = sa.create_engine(self.db)
        with engine.connect() as conn:
            try:  # cannot import lnschema_core here, need to use plain SQL
                result = conn.execute(
                    sa.text("select * from lnschema_core_user")
                ).first()
            except Exception as e:
                return False, f"Your DB is not initialized: {e}"
            if result is None:
                return (
                    False,
                    "Your DB is not initialized: lnschema_core_user has no row",
                )
        self._engine = engine
        return True, ""

    def _is_db_reachable(self, mute: bool = False) -> bool:
        if self.dialect == "sqlite":
            if not self._sqlite_file.exists():
                if not mute:
                    logger.warning(f"SQLite file {self._sqlite_file} does not exist")
                return False
        else:
            import sqlalchemy as sa

            engine = sa.create_engine(self.db)
            try:
                engine.connect()
            except Exception:
                if not mute:
                    logger.warning(f"Connection {self.db} not reachable")
                return False
        return True
