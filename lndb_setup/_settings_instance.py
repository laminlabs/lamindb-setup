import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Set, Union, get_type_hints

import sqlmodel as sqm
from appdirs import AppDirs
from cloudpathlib import CloudPath, GSClient, S3Client
from cloudpathlib.exceptions import OverwriteNewerLocalError
from lamin_logger import logger

from ._settings_save import save_settings
from ._settings_store import (
    InstanceSettingsStore,
    current_instance_settings_file,
    instance_settings_file,
)

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


DIRS = AppDirs("lamindb", "laminlabs")


class Storage:
    def __init__(self, settings: "InstanceSettings"):
        self.settings = settings

    @property
    def type(self) -> str:
        """AWS S3 vs. Google Cloud vs. local.

        Returns "s3" or "gs" or "local".
        """
        if str(self.settings.storage_root).startswith("s3://"):
            storage_type = "s3"
        elif str(self.settings.storage_root).startswith("gs://"):
            storage_type = "gs"
        else:
            storage_type = "local"
        return storage_type

    def key_to_filepath(
        self, filekey: Union[Path, CloudPath, str]
    ) -> Union[Path, CloudPath]:
        """Cloud or local filepath from filekey."""
        if self.settings.cloud_storage:
            if self.type == "s3":
                client = S3Client(local_cache_dir=self.settings.cache_dir)
            elif self.type == "gs":
                client = GSClient(local_cache_dir=self.settings.cache_dir)
            else:
                raise RuntimeError(
                    "Currently, only AWS S3 and Google cloud are supported for cloud"
                    " storage."
                )
            return client.CloudPath(self.settings.storage_root / filekey)
        else:
            return self.settings.storage_root / filekey

    def cloud_to_local(self, filepath: Union[Path, CloudPath]) -> Path:
        """Local (cache) filepath from filepath."""
        try:
            # the following will auto-update the local cache if the cloud file is newer
            # if both have the same age, it will keep it as is
            if self.settings.cloud_storage:
                local_filepath = CloudPath(filepath).fspath
            else:
                local_filepath = Path(filepath)
        except OverwriteNewerLocalError:
            local_filepath = self.cloud_to_local_no_update(filepath)  # type: ignore
            logger.warning(
                f"Local file ({local_filepath}) for cloud path ({filepath}) is newer on disk than in cloud.\n"  # noqa
                "It seems you manually updated the database locally and didn't push changes to the cloud.\n"  # noqa
                "This can lead to data loss if somebody else modified the cloud file in"
                " the meantime."
            )
        Path(local_filepath).parent.mkdir(
            parents=True, exist_ok=True
        )  # this should not happen here but is currently needed
        return local_filepath

    # conversion to Path via cloud_to_local() would trigger download
    # of remote file to cache if there already is one
    # in pure write operations that update the cloud, we don't want this
    # hence, we manually construct the local file path
    # using the `.parts` attribute in the following line
    def cloud_to_local_no_update(self, filepath: Union[Path, CloudPath]) -> Path:
        if self.settings.cloud_storage:
            return self.settings.cache_dir.joinpath(*filepath.parts[1:])  # type: ignore
        return filepath

    def local_filepath(self, filekey: Union[Path, CloudPath, str]) -> Path:
        """Local (cache) filepath from filekey: `local(filepath(...))`."""
        return self.cloud_to_local(self.key_to_filepath(filekey))


class instance_description:
    storage_root = """Storage root. Either local dir, ``s3://bucket_name`` or ``gs://bucket_name``."""  # noqa
    storage_region = """Cloud storage region for s3 and Google Cloud."""
    _dbconfig = """Either "sqlite" or postgres connection string."""
    name = """Instance name."""
    _schema = """Comma-separated string of schema modules. None if not set."""


def instance_from_storage(storage):
    return str(storage.stem).lower()


@dataclass
class InstanceSettings:
    """Instance settings written during setup."""

    storage_root: Union[CloudPath, Path] = None  # None is just for init, can't be None
    """Storage root. Either local dir, ``s3://bucket_name`` or ``gs://bucket_name``."""
    storage_region: Optional[str] = None
    """Cloud storage region for s3 and Google Cloud."""
    _dbconfig: str = "sqlite"
    """Either "sqlite" or postgres connection string."""
    _schema: str = ""
    """Comma-separated string of schema modules. Empty string if only core schema."""
    _name: Optional[str] = None
    """Instance name."""
    _session: Optional[sqm.Session] = None

    @property
    def schema(self) -> Set[str]:
        """Schema modules in addition to core schema."""
        return {schema for schema in self._schema.split(",") if schema != ""}

    @property
    def cloud_storage(self) -> bool:
        """`True` if `storage_root` is in cloud, `False` otherwise."""
        return isinstance(self.storage_root, CloudPath)

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
    def _sqlite_file(self) -> Union[Path, CloudPath]:
        """SQLite file.

        Is a CloudPath if on S3 or GS, otherwise a Path.
        """
        filename = instance_from_storage(self.storage_root)  # type: ignore
        return self.storage.key_to_filepath(f"{filename}.lndb")

    @property
    def _sqlite_file_local(self) -> Path:
        """Cached local sqlite file."""
        return self.storage.cloud_to_local_no_update(self._sqlite_file)

    def _update_cloud_sqlite_file(self) -> None:
        """If on cloud storage, update remote file."""
        if self.cloud_storage and self._dbconfig == "sqlite":
            sqlite_file = self._sqlite_file
            cache_file = self.storage.cloud_to_local_no_update(sqlite_file)
            sqlite_file.upload_from(cache_file, force_overwrite_to_cloud=True)  # type: ignore  # noqa
            # doing semi-manually to replace cloudpahlib easily in the future
            cloud_mtime = sqlite_file.stat().st_mtime  # type: ignore
            # this seems to work even if there is an open connection to the cache file
            os.utime(cache_file, times=(cloud_mtime, cloud_mtime))

    @property
    def name(self) -> str:
        """Name of LaminDB instance.

        Every LaminDB instance corresponds to exactly one database.

        The name is unique per instance owner.
        """
        if self._name:
            return self._name
        if self._dbconfig == "sqlite":
            return instance_from_storage(self.storage_root)
        else:
            return self._dbconfig.split("/")[-1]

    @property
    def db(self) -> str:
        """Database URL."""
        # the great thing about cloudpathlib is that it downloads the
        # remote file to cache as soon as the time stamp is out of date
        if self._dbconfig == "sqlite":
            return f"sqlite:///{self.storage.cloud_to_local(self._sqlite_file)}"
        else:
            return self._dbconfig

    def db_engine(self, future=True):
        """Database engine."""
        return sqm.create_engine(self.db, future=future)

    def session(self) -> sqm.Session:
        """Database session."""
        if "LAMIN_SKIP_MIGRATION" not in os.environ:
            if self._session is None:
                self._session = sqm.Session(self.db_engine(), expire_on_commit=False)
            elif self.cloud_storage and self._dbconfig == "sqlite":
                # doing semi-manually for easier replacemnet of cloudpathib
                # in the future
                sqlite_file = self._sqlite_file
                # saving mtime here assuming lock at the beginning of the session
                cloud_mtime = sqlite_file.stat().st_mtime  # type: ignore
                cache_file = self.storage.cloud_to_local_no_update(sqlite_file)

                if not cache_file.exists():
                    cache_file.parent.mkdir(parents=True, exist_ok=True)
                    sqlite_file.download_to(cache_file)  # type: ignore
                    os.utime(cache_file, times=(cloud_mtime, cloud_mtime))
                elif cloud_mtime > cache_file.stat().st_mtime:  # type: ignore  # noqa
                    # no need to recreate session
                    # just need to close current connections
                    # in order to replace the sqlite db file
                    # connections seem to be recreated for every transaction
                    # invalidate because we need to close the connections immediately
                    self._session.invalidate()

                    sqlite_file.download_to(cache_file)  # type: ignore
                    os.utime(cache_file, times=(cloud_mtime, cloud_mtime))

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

    def _persist(self) -> None:
        assert self.name is not None
        type_hints = get_type_hints(InstanceSettingsStore)
        filepath = instance_settings_file(self.name)
        # persist under filepath for later reference
        save_settings(self, filepath, type_hints)
        # persist under current file for auto load
        shutil.copy2(filepath, current_instance_settings_file())
        # persist under settings class for same session reference
        # need to import here to avoid circular import
        from ._settings import settings

        settings._instance_settings = self
