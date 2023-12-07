import os
from pathlib import Path
from typing import Any, Optional, Union

from appdirs import AppDirs
from ._settings_save import save_system_storage_settings
from ._settings_store import system_storage_settings_file

from .upath import LocalPathClasses, UPath, create_path

DIRS = AppDirs("lamindb", "laminlabs")


def _process_cache_path(cache_path: Union[str, Path, UPath, None]):
    if cache_path is None or cache_path == "null":
        return None
    cache_dir = UPath(cache_path)
    if not isinstance(cache_dir, LocalPathClasses):
        raise ValueError("cache dir should be a local path.")
    if cache_dir.exists() and not cache_dir.is_dir():
        raise ValueError("cache dir should be a directory.")
    return cache_dir


class StorageSettings:
    """Manage cloud or local storage settings."""

    def __init__(
        self,
        root: Union[str, Path, UPath],
        region: Optional[str] = None,
    ):
        self._root_init = root
        self._root = None
        self._region = region
        # would prefer to type below as Registry, but need to think through import order
        self._record: Optional[Any] = None
        # cache settings
        self._storage_settings_file = system_storage_settings_file()
        if self._storage_settings_file.exists():
            from dotenv import dotenv_values

            cache_path = dotenv_values(self._storage_settings_file)[
                "lamindb_cache_path"
            ]
            self._cache_dir = _process_cache_path(cache_path)
        else:
            self._cache_dir = None

    @property
    def id(self) -> int:
        """Storage id."""
        return self.record.id

    @property
    def record(self) -> Any:
        """Storage record."""
        if self._record is None:
            # dynamic import because of import order
            from lnschema_core.models import Storage

            self._record = Storage.objects.get(root=self.root_as_str)
        return self._record

    @property
    def root(self) -> UPath:
        """Root storage location."""
        if self._root is None:
            root_path = create_path(self._root_init)
            # root_path is either Path or UPath at this point
            if isinstance(root_path, LocalPathClasses):  # local paths
                # resolve fails for nonexisting dir
                root_path.mkdir(parents=True, exist_ok=True)
                root_path = root_path.resolve()
            self._root = root_path
        return self._root

    def _set_fs_kwargs(self, **kwargs):
        """Set additional fsspec arguments for cloud root.

        Example:

        >>> ln.setup.settings.storage._set_fs_kwargs(  # any fsspec args
        >>>    profile="some_profile", cache_regions=True
        >>> )
        """
        if not isinstance(self._root, LocalPathClasses):
            self._root = UPath(self._root, **kwargs)

    @property
    def root_as_str(self) -> str:
        """Formatted root string."""
        return self.root.as_posix().rstrip("/")

    @property
    def cache_dir(
        self,
    ) -> UPath:
        """Cache root, a local directory to cache cloud files."""
        if "LAMIN_CACHE_DIR" in os.environ:
            cache_dir = UPath(os.environ["LAMIN_CACHE_DIR"])
        elif self._cache_dir is None:
            cache_dir = UPath(DIRS.user_cache_dir)
        else:
            cache_dir = self._cache_dir
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    @cache_dir.setter
    def cache_dir(self, cache_dir: Union[str, Path, UPath]):
        """Set cache root."""
        from lamindb_setup import settings

        if settings.instance._is_cloud_sqlite:
            src_sqlite_file = settings.instance._sqlite_file_local
        else:
            src_sqlite_file = None

        save_cache_dir = self._cache_dir

        self._cache_dir = _process_cache_path(cache_dir)

        try:
            if src_sqlite_file is not None:
                dst_sqlite_file = settings.instance._sqlite_file_local
                dst_sqlite_file.parent.mkdir(parents=True, exist_ok=True)
                if dst_sqlite_file.exists():
                    dst_sqlite_file.unlink()
                src_sqlite_file.rename(dst_sqlite_file)
            save_system_storage_settings(self._cache_dir, self._storage_settings_file)
        except Exception as e:
            self._cache_dir = save_cache_dir
            raise e

    @property
    def is_cloud(self) -> bool:
        """`True` if `storage_root` is in cloud, `False` otherwise."""
        return not isinstance(self.root, LocalPathClasses)

    @property
    def region(self) -> Optional[str]:
        """Storage region."""
        return self._region

    @property
    def type(self) -> str:
        """AWS S3 vs. Google Cloud vs. local vs. the other protocols.

        Returns the protocol.
        """
        import fsspec

        convert = {"file": "local"}
        protocol = fsspec.utils.get_protocol(str(self.root))
        return convert.get(protocol, protocol)

    def key_to_filepath(self, filekey: Union[Path, UPath, str]) -> UPath:
        """Cloud or local filepath from filekey."""
        return self.root / filekey

    def cloud_to_local(self, filepath: Union[Path, UPath], **kwargs) -> UPath:
        """Local (cache) filepath from filepath."""
        local_filepath = self.cloud_to_local_no_update(filepath)  # type: ignore
        if isinstance(filepath, UPath) and not isinstance(filepath, LocalPathClasses):
            local_filepath.parent.mkdir(parents=True, exist_ok=True)
            filepath.synchronize(local_filepath, **kwargs)
        return local_filepath

    # conversion to Path via cloud_to_local() would trigger download
    # of remote file to cache if there already is one
    # in pure write operations that update the cloud, we don't want this
    # hence, we manually construct the local file path
    # using the `.parts` attribute in the following line
    def cloud_to_local_no_update(self, filepath: UPath) -> UPath:
        if isinstance(filepath, UPath) and not isinstance(filepath, LocalPathClasses):
            return self.cache_dir.joinpath(filepath._url.netloc, *filepath.parts[1:])  # type: ignore # noqa
        return filepath

    def local_filepath(self, filekey: Union[Path, UPath, str]) -> UPath:
        """Local (cache) filepath from filekey: `local(filepath(...))`."""
        return self.cloud_to_local(self.key_to_filepath(filekey))
