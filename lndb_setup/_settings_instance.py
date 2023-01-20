import os
import shutil
from pathlib import Path
from typing import Literal, Optional, Set, Tuple, Union

import sqlalchemy as sa
import sqlmodel as sqm
from cloudpathlib import CloudPath
from pydantic import PostgresDsn
from sqlalchemy.future import Engine

from lndb_setup._storage import Storage

from ._exclusion import Locker, get_locker
from ._settings_save import save_instance_settings
from ._settings_store import current_instance_settings_file, instance_settings_file

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
        storage_root: Union[str, Path, CloudPath],  # storage location on cloud
        storage_region: Optional[str] = None,
        db: Optional[PostgresDsn] = None,  # DB URI
        schema: Optional[str] = None,  # comma-separated string of schema names
    ):
        self._owner: str = owner
        self._name: str = name
        self._storage: Storage = Storage(storage_root, region=storage_region)
        self._db: Optional[str] = db
        self._schema_str: Optional[str] = schema
        self._locker: Optional[Locker] = None
        self._engine: Engine = sqm.create_engine(self.db)

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
    def _sqlite_file(self) -> Union[Path, CloudPath]:
        """SQLite file.

        Is a CloudPath if on S3 or GS, otherwise a Path.
        """
        return self.storage.key_to_filepath(f"{self.name}.lndb")

    @property
    def _sqlite_file_local(self) -> Path:
        """Local SQLite file."""
        return self.storage.cloud_to_local_no_update(self._sqlite_file)

    def _update_cloud_sqlite_file(self) -> None:
        """Unlock; if on cloud storage, update remote file."""
        if self.dialect == "sqlite":
            if self.storage.is_cloud:
                sqlite_file = self._sqlite_file
                cache_file = self.storage.cloud_to_local_no_update(sqlite_file)
                sqlite_file.upload_from(cache_file, force_overwrite_to_cloud=True)  # type: ignore  # noqa
                # doing semi-manually to replace cloudpahlib easily in the future
                cloud_mtime = sqlite_file.stat().st_mtime  # type: ignore
                # this seems to work even if there is an open connection
                # to the cache file
                os.utime(cache_file, times=(cloud_mtime, cloud_mtime))
            locker = self.locker
            if locker is not None:
                locker.unlock()

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
            assert self._db.startswith("postgresql://")
            return "postgresql"

    @property
    def engine(self) -> Engine:
        """Database engine."""
        return self._engine

    @property
    def locker(self):
        if self.dialect == "sqlite" and self._locker is None:
            self._locker = get_locker()

        return self._locker

    def session(self, lock: bool = False) -> sqm.Session:
        """Database session."""
        if lock:
            locker = self.locker
            if locker is not None:
                try:
                    locker.lock()
                except BaseException as e:
                    locker.unlock()
                    raise e

        if self.dialect == "sqlite" and self.storage.is_cloud:
            sqlite_file = self._sqlite_file
            cache_file = self.storage.cloud_to_local_no_update(sqlite_file)
            # checking cloud mtime several times here because of potential changes
            # during the synchronizization process. Maybe better
            # to make these checks dependent on lock,
            # i.e. if locked check cloud mtime only once.
            if not cache_file.exists():
                cache_file.parent.mkdir(parents=True, exist_ok=True)
                cloud_mtime = sqlite_file.stat().st_mtime  # type: ignore
                sqlite_file.download_to(cache_file)  # type: ignore
                os.utime(cache_file, times=(cloud_mtime, cloud_mtime))
            elif sqlite_file.stat().st_mtime > cache_file.stat().st_mtime:  # type: ignore  # noqa
                # checking the time again because
                # it could be changed in the meantime
                # maybe remove this checks when locked
                cloud_mtime = sqlite_file.stat().st_mtime  # type: ignore
                sqlite_file.download_to(cache_file)  # type: ignore
                os.utime(cache_file, times=(cloud_mtime, cloud_mtime))

        return sqm.Session(self.engine, expire_on_commit=False)

    @property
    def storage(self) -> Storage:
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
        # returns True for remote SQLite
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
        from ._settings import settings

        settings._instance_settings = self

    def _is_db_setup(self) -> Tuple[bool, str]:
        """Is the database available and initialized as LaminDB?"""
        if self.dialect == "sqlite":
            if not self._sqlite_file.exists():
                return False, "SQLite file does not exist"
            else:
                return True, ""
        else:  # postgres
            assert self.dialect == "postgresql"
            with self.engine.connect() as conn:
                results = conn.execute(
                    sa.text(
                        """
                    SELECT EXISTS (
                        SELECT FROM
                            information_schema.tables
                        WHERE
                            table_schema LIKE 'public' AND
                            table_name = 'version_yvzi'
                    );
                """
                    )
                ).first()  # returns tuple of boolean
                check = results[0]
                if not check:
                    return False, "Postgres does not seem initialized."
                else:
                    return True, ""
