from dataclasses import dataclass
from pathlib import Path
from sqlite3 import Connection as SQLite3Connection
from typing import Optional, Union

import sqlmodel as sqm
from appdirs import AppDirs
from cloudpathlib import CloudPath, GSClient, S3Client
from cloudpathlib.exceptions import OverwriteNewerLocalError
from lamin_logger import logger
from sqlalchemy import event
from sqlalchemy.engine import Engine


# https://stackoverflow.com/questions/2614984/sqlite-sqlalchemy-how-to-enforce-foreign-keys
# foreign key constraints for sqlite3
@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, SQLite3Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


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
    _dbconfig = """Either "sqlite" or "instance_name, postgres_url"."""
    name = """Instance name."""
    schema_modules = """Comma-separated string of schema modules. None if not set."""


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
    """Either "sqlite" or "instance_name, postgres_url"."""
    schema_modules: Optional[str] = None  # type: ignore
    """Comma-separated string of schema modules. None if not set."""

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

        Is a CloudPath if on S3, otherwise a Path.
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

    @property
    def name(self) -> Union[str, None]:
        """Name of LaminDB instance, which corresponds to exactly one database."""
        if self.storage_root is None:  # not yet initialized
            return None
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

    @property
    def storage(self) -> Storage:
        """Low-level access to storage location."""
        return Storage(self)
