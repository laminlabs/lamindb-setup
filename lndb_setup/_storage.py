from pathlib import Path
from typing import Literal, Optional, Union

from appdirs import AppDirs

from ._upath_ext import UPath

DIRS = AppDirs("lamindb", "laminlabs")


_MUTE_SYNC_WARNINGS = False


def _set_mute_sync_warnings(value: bool):
    global _MUTE_SYNC_WARNINGS

    _MUTE_SYNC_WARNINGS = value


class Storage:
    """Manage cloud or local storage."""

    def __init__(self, root: Union[str, Path, UPath], region: Optional[str] = None):
        if isinstance(root, str):
            root_path = Storage._str_to_path(root)
        else:
            root_path = root
        self._root = root_path
        self._region = region

    @staticmethod
    def _str_to_path(storage: str) -> Union[Path, UPath]:
        if storage.startswith("s3://") or storage.startswith("gs://"):
            storage_root = UPath(storage)
        else:  # local path
            storage_root = Path(storage).absolute()
        return storage_root

    @property
    def root(self) -> Union[Path, UPath]:
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
        return isinstance(self.root, UPath)

    @property
    def region(self) -> Optional[str]:
        """Storage region."""
        return self._region

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

    def key_to_filepath(self, filekey: Union[Path, UPath, str]) -> Union[Path, UPath]:
        """Cloud or local filepath from filekey."""
        return self.root / filekey

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
        if self.is_cloud:
            return self.cache_dir.joinpath(*filepath.parts[1:])  # type: ignore
        return filepath

    def local_filepath(self, filekey: Union[Path, UPath, str]) -> Path:
        """Local (cache) filepath from filekey: `local(filepath(...))`."""
        return self.cloud_to_local(self.key_to_filepath(filekey))
