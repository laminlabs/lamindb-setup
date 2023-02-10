from pathlib import Path
from typing import Literal, Optional, Union

from appdirs import AppDirs
from cloudpathlib import CloudPath, GSClient, S3Client
from cloudpathlib.exceptions import OverwriteNewerLocalError
from lamin_logger import logger

DIRS = AppDirs("lamindb", "laminlabs")


_MUTE_SYNC_WARNINGS = False


def _set_mute_sync_warnings(value: bool):
    global _MUTE_SYNC_WARNINGS

    _MUTE_SYNC_WARNINGS = value


class Storage:
    """Manage cloud or local storage."""

    def __init__(self, root: Union[str, Path, CloudPath], region: Optional[str] = None):
        if isinstance(root, str):
            root_path = Storage._str_to_path(root)
        else:
            root_path = root
        self._root = root_path
        self._region = region

    @staticmethod
    def _str_to_path(storage: str) -> Union[Path, CloudPath]:
        if str(storage).startswith("s3://"):  # AWS
            storage_root = CloudPath(storage)
        elif str(storage).startswith("gs://"):  # GCP
            # the below seems needed as cloudpathlib on its
            # own fails to initialize when using gcloud auth login
            # and not JSON credentials
            from cloudpathlib import GSClient
            from google.cloud import storage as gstorage

            client = GSClient(storage_client=gstorage.Client())
            storage_root = CloudPath(storage, client)
        else:  # local path
            storage_root = Path(storage).absolute()
        return storage_root

    @property
    def root(self) -> Union[Path, CloudPath]:
        """Root storage location."""
        return self._root

    @property
    def cache_dir(
        self,
    ) -> Union[Path, None]:
        """Cache root, a local directory to cache cloud files."""
        if self.is_cloud:
            cache_dir = Path(DIRS.user_cache_dir)
            cache_dir.mkdir(parents=True, exist_ok=True)
        else:
            cache_dir = None
        return cache_dir

    @property
    def is_cloud(self) -> bool:
        """`True` if `storage_root` is in cloud, `False` otherwise."""
        return isinstance(self.root, CloudPath)

    @property
    def region(self) -> Optional[str]:
        """Storage region."""
        return self._region

    @property
    def client(self) -> Union[S3Client, GSClient]:
        if self.type == "s3":
            return S3Client(local_cache_dir=self.cache_dir)
        elif self.type == "gs":
            # the below seems needed as cloudpathlib on its
            # own fails to initialize when using gcloud auth login
            # and not JSON credentials
            from google.cloud import storage as gstorage

            return GSClient(
                local_cache_dir=self.cache_dir,
                storage_client=gstorage.Client(),
            )
        elif self.type == "local":
            raise RuntimeError("You shouldn't need a client for local storage.")
        else:
            raise RuntimeError(
                "Currently, only AWS S3 and Google cloud are supported for cloud"
                " storage."
            )

    @property
    def type(self) -> Literal["s3", "gs", "local"]:
        """AWS S3 vs. Google Cloud vs. local.

        Returns "s3" or "gs" or "local".
        """
        if str(self.root).startswith("s3://"):
            return "s3"
        elif str(self.root).startswith("gs://"):
            return "gs"
        else:
            return "local"

    def key_to_filepath(
        self, filekey: Union[Path, CloudPath, str]
    ) -> Union[Path, CloudPath]:
        """Cloud or local filepath from filekey."""
        if self.is_cloud:
            return self.client.CloudPath(self.root / filekey)
        else:
            return self.root / filekey

    def cloud_to_local(self, filepath: Union[Path, CloudPath]) -> Path:
        """Local (cache) filepath from filepath."""
        try:
            # the following will auto-update the local cache if the cloud file is newer
            # if both have the same age, it will keep it as is
            if self.is_cloud:
                local_filepath = self.client.CloudPath(filepath).fspath
            else:
                local_filepath = Path(filepath)
        except OverwriteNewerLocalError:
            local_filepath = self.cloud_to_local_no_update(filepath)  # type: ignore
            if not _MUTE_SYNC_WARNINGS:
                logger.warning(
                    f"Local file ({local_filepath}) for cloud path ({filepath}) is newer on disk than in cloud.\n"  # noqa
                    "It seems you manually updated the database locally and didn't push changes to the cloud.\n"  # noqa
                    "This can lead to data loss if somebody else modified the cloud file in"  # noqa
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
        if self.is_cloud:
            return self.cache_dir.joinpath(*filepath.parts[1:])  # type: ignore
        return filepath

    def local_filepath(self, filekey: Union[Path, CloudPath, str]) -> Path:
        """Local (cache) filepath from filekey: `local(filepath(...))`."""
        return self.cloud_to_local(self.key_to_filepath(filekey))
