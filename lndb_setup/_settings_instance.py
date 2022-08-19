from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import sqlmodel as sqm
from appdirs import AppDirs
from cloudpathlib import CloudPath, S3Client

DIRS = AppDirs("lamindb", "laminlabs")


class Storage:
    def __init__(self, settings: "InstanceSettings"):
        self.settings = settings

    def key_to_filepath(
        self, filekey: Union[Path, CloudPath, str]
    ) -> Union[Path, CloudPath]:
        """Cloud or local filepath from filekey."""
        if self.settings.cloud_storage:
            client = S3Client(local_cache_dir=self.settings.cache_dir)
            return client.CloudPath(self.settings.storage_dir / filekey)
        else:
            return self.settings.storage_dir / filekey

    def cloud_to_local(self, filepath: Union[Path, CloudPath]) -> Path:
        """Local (cache) filepath from filepath."""
        if self.settings.cloud_storage:
            filepath = filepath.fspath  # type: ignore  # mypy misses CloudPath
        Path(filepath).parent.mkdir(
            parents=True, exist_ok=True
        )  # this should not happen here but is currently needed
        return filepath

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
    storage_dir = """Storage root. Either local dir, ``s3://bucket_name`` or ``gs://bucket_name``."""  # noqa
    storage_region = """Cloud storage region for s3 and gcp"""
    _dbconfig = """Either "sqlite" or "instance_name, postgres_url"."""
    name = """Instance name."""
    schema_modules = """Comma-separated string of schema modules."""


def instance_from_storage(storage):
    return str(storage.stem).lower()


@dataclass
class InstanceSettings:
    """Instance settings written during setup."""

    storage_dir: Union[CloudPath, Path] = None
    """Storage root. Either local dir, ``s3://bucket_name`` or ``gs://bucket_name``."""
    storage_region: Optional[str] = None
    """Cloud storage region for s3 and gcp."""
    _dbconfig: str = "sqlite"
    """Either "sqlite" or "instance_name, postgres_url"."""
    schema_modules: str = None  # type: ignore
    """Comma-separated string of schema modules."""

    @property
    def cloud_storage(self) -> bool:
        """`True` if `storage_dir` is in cloud, `False` otherwise."""
        return isinstance(self.storage_dir, CloudPath)

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
        """Database SQLite filepath.

        Is a CloudPath if on S3, otherwise a Path.
        """
        filename = instance_from_storage(self.storage_dir)  # type: ignore
        return self.storage.key_to_filepath(f"{filename}.lndb")

    @property
    def _sqlite_file_local(self):
        """If on cloud storage, update remote file."""
        return self.storage.cloud_to_local_no_update(self._sqlite_file)

    def _update_cloud_sqlite_file(self):
        """If on cloud storage, update remote file."""
        if self.cloud_storage:
            sqlite_file = self._sqlite_file
            cache_file = self.storage.cloud_to_local_no_update(sqlite_file)
            sqlite_file.upload_from(cache_file)

    @property
    def name(self) -> Union[str, None]:
        """Name of LaminDB instance, which corresponds to exactly one database."""
        if self.storage_dir is None:  # not yet initialized
            return None
        if self._dbconfig == "sqlite":
            return instance_from_storage(self.storage_dir)
        else:
            return self._dbconfig.split(",")[0]

    @property
    def db(self) -> str:
        """Database URL."""
        # the great thing about cloudpathlib is that it downloads the
        # remote file to cache as soon as the time stamp is out of date
        return f"sqlite:///{self.storage.cloud_to_local(self._sqlite_file)}"

    def db_engine(self, future=True):
        """Database engine."""
        return sqm.create_engine(self.db, future=future)

    @property
    def storage(self) -> Storage:
        """Low-level access to storage location."""
        return Storage(self)
