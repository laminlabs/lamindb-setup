import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Set, Union, get_type_hints

import sqlmodel as sqm
from appdirs import AppDirs

from ._exclusion import Locker, get_locker
from ._settings_save import save_settings
from ._settings_store import (
    InstanceSettingsStore,
    current_instance_settings_file,
    instance_settings_file,
)
from ._upath_ext import UPath

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


_MUTE_SYNC_WARNINGS = False


def _set_mute_sync_warnings(value: bool):
    global _MUTE_SYNC_WARNINGS

    _MUTE_SYNC_WARNINGS = value


DIRS = AppDirs("lamindb", "laminlabs")


class Storage:
    def __init__(self, settings: "InstanceSettings"):
        self.settings = settings

    @property
    def type(self) -> str:
        """AWS S3 vs. Google Cloud vs. local.

        Returns "s3" or "gs" or "local".
        """
        return get_storage_type(self.settings.storage_root)

    def key_to_filepath(self, filekey: Union[Path, UPath, str]) -> Union[Path, UPath]:
        """Cloud or local filepath from filekey."""
        if self.settings.cloud_storage:
            return UPath(self.settings.storage_root / filekey)
        else:
            return self.settings.storage_root / filekey

    def cloud_to_local(self, filepath: Union[Path, UPath]) -> Path:
        """Local (cache) filepath from filepath."""
        local_filepath = self.cloud_to_local_no_update(filepath)  # type: ignore
        if isinstance(filepath, UPath):
            filepath.synchronize(local_filepath, sync_warn=not _MUTE_SYNC_WARNINGS)

        return local_filepath

    # conversion to Path via cloud_to_local() would trigger download
    # of remote file to cache if there already is one
    # in pure write operations that update the cloud, we don't want this
    # hence, we manually construct the local file path
    # using the `.parts` attribute in the following line
    def cloud_to_local_no_update(self, filepath: Union[Path, UPath]) -> Path:
        if self.settings.cloud_storage:
            return self.settings.cache_dir.joinpath(*filepath.parts[1:])  # type: ignore
        return filepath

    def local_filepath(self, filekey: Union[Path, UPath, str]) -> Path:
        """Local (cache) filepath from filekey: `local(filepath(...))`."""
        return self.cloud_to_local(self.key_to_filepath(filekey))


def instance_from_storage(storage):
    return str(storage.stem).lower()


# This provides the doc strings for the init function on the
# CLI and the API
# It is located here as it *mostly* parallels the InstanceSettings docstrings.
# Small differences are on purpose, due to the different scope!
class init_instance_arg_doc:
    storage_root = """Storage root. Either local dir, ``s3://bucket_name`` or ``gs://bucket_name``."""  # noqa
    storage_region = """Cloud storage region for s3 and Google Cloud."""
    url = """Database connection url, do not pass for SQLite."""
    name = """Instance name."""
    _schema = """Comma-separated string of schema modules. None if not set."""


@dataclass
class InstanceSettings:
    """Instance settings written during setup."""

    owner: str
    """Instance owner."""
    name: str
    """Instance name."""
    storage_root: Union[UPath, Path] = None  # None is just for init, can't be None
    """Storage root. Either local dir, ``s3://bucket_name`` or ``gs://bucket_name``."""
    storage_region: Optional[str] = None
    """Cloud storage region for s3 and Google Cloud."""
    url: Optional[str] = None
    """Database connection url, None for SQLite."""
    _schema: str = ""
    """Comma-separated string of schema modules. Empty string if only core schema."""
    _session: Optional[sqm.Session] = None
    _locker: Optional[Locker] = None

    @property
    def schema(self) -> Set[str]:
        """Schema modules in addition to core schema."""
        return {schema for schema in self._schema.split(",") if schema != ""}

    @property
    def cloud_storage(self) -> bool:
        """`True` if `storage_root` is in cloud, `False` otherwise."""
        return isinstance(self.storage_root, UPath)

    @property
    def cache_dir(
        self,
    ) -> Union[Path, None]:
        """Cache root, a local directory to cache cloud files."""
        if self.cloud_storage:
            cache_dir = Path(DIRS.user_cache_dir)
            cache_dir.mkdir(parents=True, exist_ok=True)
        else:
            cache_dir = None
        return cache_dir

    @property
    def _sqlite_file(self) -> Union[Path, UPath]:
        """SQLite file.

        Is a CloudPath if on S3 or GS, otherwise a Path.
        """
        return self.storage.key_to_filepath(f"{self.name}.lndb")

    @property
    def _sqlite_file_local(self) -> Path:
        """Cached local sqlite file."""
        return self.storage.cloud_to_local_no_update(self._sqlite_file)

    def _update_cloud_sqlite_file(self) -> None:
        """Unlock; if on cloud storage, update remote file."""
        if self.dialect == "sqlite":
            if self.cloud_storage:
                sqlite_file = self._sqlite_file
                cache_file = self.storage.cloud_to_local_no_update(sqlite_file)
                sqlite_file.upload_from(cache_file)  # type: ignore  # noqa
                # doing semi-manually to replace cloudpahlib easily in the future
                cloud_mtime = sqlite_file.modified.timestamp()  # type: ignore
                # this seems to work even if there is an open connection
                # to the cache file
                os.utime(cache_file, times=(cloud_mtime, cloud_mtime))
            locker = self._locker
            if locker is not None:
                locker.unlock()

    @property
    def db(self) -> str:
        """Database URL."""
        # the great thing about cloudpathlib is that it downloads the
        # remote file to cache as soon as the time stamp is out of date
        if self.url is None:
            return f"sqlite:///{self.storage.cloud_to_local(self._sqlite_file)}"
        else:
            return self.url

    @property
    def dialect(self):
        return get_db_dialect(self.url)

    @property
    def _dbconfig(self):
        # logger.warning("_dbconfig is deprecated and will be removed soon")
        if self.dialect == "sqlite":
            return "sqlite"
        return self.url

    def db_engine(self, future=True):
        """Database engine."""
        return sqm.create_engine(self.db, future=future)

    def session(self, lock: bool = False) -> sqm.Session:
        """Database session."""
        if lock:
            if self._dbconfig == "sqlite" and self._locker is None:
                self._locker = get_locker()

            locker = self._locker
            if locker is not None:
                try:
                    locker.lock()
                except BaseException as e:
                    locker.unlock()
                    raise e

        if "LAMIN_SKIP_MIGRATION" not in os.environ:
            if self._session is None:
                self._session = sqm.Session(self.db_engine(), expire_on_commit=False)
            elif self.cloud_storage and self.dialect == "sqlite":
                # doing semi-manually for easier replacemnet of cloudpathib
                # in the future
                sqlite_file = self._sqlite_file
                cache_file = self.storage.cloud_to_local_no_update(sqlite_file)
                # checking cloud mtime several times here because of potential changes
                # during the synchronizization process. Maybe better
                # to make these checks dependent on lock,
                # i.e. if locked check cloud mtime only once.

                def _invalidate_session():
                    self._session.invalidate()

                sqlite_file.synchronize(cache_file, callback=_invalidate_session)  # type: ignore # noqa
            # should probably add a different check whether the session is still active
            if not self._session.is_active:
                self._session = sqm.Session(self.db_engine(), expire_on_commit=False)
            return self._session
        else:
            return sqm.Session(self.db_engine(), expire_on_commit=False)

    @property
    def storage(self) -> Storage:
        """Low-level access to storage location."""
        return Storage(self)

    @property
    def is_remote(self) -> bool:
        """Boolean indicating if an instance have no local component."""
        return is_instance_remote(self.storage.settings.storage_root, self.url)

    def _persist(self) -> None:
        assert self.name is not None
        type_hints = get_type_hints(InstanceSettingsStore)
        filepath = instance_settings_file(self.name, self.owner)
        # persist under filepath for later reference
        save_settings(self, filepath, type_hints)
        # persist under current file for auto load
        shutil.copy2(filepath, current_instance_settings_file())
        # persist under settings class for same session reference
        # need to import here to avoid circular import
        from ._settings import settings

        settings._instance_settings = self


def is_local_postgres(url: str):
    if "@localhost:" in url:
        return True
    if "@0.0.0.0:" in url:
        return True
    if "@127.0.0.1" in url:
        return True
    return False


def is_instance_remote(storage_root: Union[UPath, Path], url: Optional[str]):
    dialect = get_db_dialect(url)
    storage_type = get_storage_type(storage_root)
    if storage_type == "local":
        return False
    if dialect == "postgresql":
        assert url is not None, "Postgres db url is none"
        if is_local_postgres(url):
            return False
    return True


def get_db_dialect(url: Optional[str]):
    if url is None or url.startswith("sqlite://"):
        return "sqlite"
    elif url.startswith("postgresql://"):
        return "postgresql"
    return None


def get_storage_type(storage_root):
    if str(storage_root).startswith("s3://"):
        return "s3"
    elif str(storage_root).startswith("gs://"):
        return "gs"
    else:
        return "local"
