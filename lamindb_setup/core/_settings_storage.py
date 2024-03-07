import os
import shutil
from lamin_utils import logger
from pathlib import Path
from typing import Any, Optional, Union
from ._aws_storage import find_closest_aws_region, get_aws_account_id
from appdirs import AppDirs
from ._settings_save import save_system_storage_settings
from ._settings_store import system_storage_settings_file
from .upath import LocalPathClasses, UPath, create_path, convert_pathlike
from uuid import UUID
import string
import secrets
from .types import UPathStr
from .upath import hosted_regions


DIRS = AppDirs("lamindb", "laminlabs")


def base62(n_char: int) -> str:
    """Like nanoid without hyphen and underscore."""
    alphabet = string.digits + string.ascii_letters.swapcase()
    id = "".join(secrets.choice(alphabet) for i in range(n_char))
    return id


def get_storage_region(storage_root: UPathStr) -> Optional[str]:
    storage_root_str = str(storage_root)
    if storage_root_str.startswith("s3://"):
        import botocore.session as session
        from botocore.config import Config
        from botocore.exceptions import NoCredentialsError

        # strip the prefix and any suffixes of the bucket name
        bucket = storage_root_str.replace("s3://", "").split("/")[0]
        s3_session = session.get_session()
        s3_client = s3_session.create_client("s3")
        try:
            response = s3_client.head_bucket(Bucket=bucket)
        except NoCredentialsError:  # deal with anonymous access
            s3_client = s3_session.create_client(
                "s3", config=Config(signature_version=session.UNSIGNED)
            )
            response = s3_client.head_bucket(Bucket=bucket)
        storage_region = response["ResponseMetadata"].get("HTTPHeaders", {})[
            "x-amz-bucket-region"
        ]
        # if we want to except botcore.exceptions.ClientError to reformat an
        # error message, this is how to do test for the "NoSuchBucket" error:
        #     exc.response["Error"]["Code"] == "NoSuchBucket"
    else:
        storage_region = None
    return storage_region


def init_storage(storage: UPathStr, region: Optional[str] = None) -> "StorageSettings":
    if storage is None:
        raise ValueError("storage argument can't be `None`")
    root = str(storage)  # ensure we have a string
    uid = base62(8)
    if root == "create-s3":
        if region is None:
            region = find_closest_aws_region()
        else:
            if region not in hosted_regions:
                raise ValueError(f"region has to be one of {hosted_regions}")
        lamin_env = os.getenv("LAMIN_ENV")
        if lamin_env is None or lamin_env == "prod":
            root = f"s3://lamin-{region}/{uid}"
        else:
            root = f"s3://lamin-hosted-test/{uid}"
    elif root.startswith(("gs://", "s3://")):
        # check for existence happens in get_storage_region
        pass
    else:  # local path
        try:
            _ = Path(root)
        except Exception as e:
            logger.error(
                "`storage` is neither a valid local, a Google Cloud nor an S3 path."
            )
            raise e
    ssettings = StorageSettings(uid=uid, root=root, region=region)
    if ssettings.is_cloud:
        from ._hub_core import init_storage as init_storage_hub

        if storage == "create-s3":
            ssettings._aws_account_id = 767398070972
        elif root.startswith("s3://"):
            ssettings._aws_account_id = get_aws_account_id()
        ssettings._description = f"Created as default storage for instance {uid}"
        ssettings._uuid = init_storage_hub(ssettings)
        logger.important(f"registered storage: {ssettings.root_as_str}")
    return ssettings


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
        root: UPathStr,
        region: Optional[str] = None,
        uid: Optional[str] = None,
        uuid: Optional[UUID] = None,
        access_token: Optional[str] = None,
    ):
        self._uid = uid
        self._uuid = uuid
        self._root_init = convert_pathlike(root)
        if isinstance(self._root_init, LocalPathClasses):  # local paths
            self._root_init.mkdir(parents=True, exist_ok=True)
            self._root_init = self._root_init.resolve()
        self._root = None
        self._aws_account_id: Optional[int] = None
        self._description: Optional[str] = None
        # we don't yet infer region here to make init fast
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
        # save access_token here for use in self.root
        self.access_token = access_token

    @property
    def id(self) -> int:
        """Storage id."""
        return self.record.id

    @property
    def uuid(self) -> Optional[UUID]:
        """Storage uuid."""
        return self._uuid

    @property
    def uid(self) -> Optional[str]:
        """Storage id."""
        if self._uid is None:
            self._uid = self.record.uid
        return self._uid

    @property
    def record(self) -> Any:
        """Storage record."""
        if self._record is None:
            # dynamic import because of import order
            from lnschema_core.models import Storage
            from ._settings import settings

            self._record = Storage.objects.using(settings._using_key).get(
                root=self.root_as_str
            )
        return self._record

    def __repr__(self):
        """String rep."""
        s = f"root='{self.root_as_str}', uid='{self.uid}'"
        if self.uuid is not None:
            s += f", uuid='{self.uuid.hex}'"
        return f"StorageSettings({s})"

    @property
    def root(self) -> UPath:
        """Root storage location."""
        if self._root is None:
            # below also makes network requests to get credentials
            # right
            root_path = create_path(self._root_init, access_token=self.access_token)
            self._root = root_path
        return self._root

    def _set_fs_kwargs(self, **kwargs):
        """Set additional fsspec arguments for cloud root.

        Example:

        >>> ln.setup.settings.storage._set_fs_kwargs(  # any fsspec args
        >>>    profile="some_profile", cache_regions=True
        >>> )
        """
        if not isinstance(self._root, LocalPathClasses) and kwargs != {}:
            self._root = UPath(self.root, **kwargs)

    @property
    def root_as_str(self) -> str:
        """Formatted root string."""
        return self._root_init.as_posix().rstrip("/")

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
    def cache_dir(self, cache_dir: UPathStr):
        """Set cache root."""
        from lamindb_setup import settings

        if settings.instance._is_cloud_sqlite:
            src_sqlite_file = settings.instance._sqlite_file_local
        else:
            src_sqlite_file = None

        save_cache_dir = self._cache_dir

        new_cache_dir = _process_cache_path(cache_dir)
        if new_cache_dir is not None:
            new_cache_dir.mkdir(parents=True, exist_ok=True)
            new_cache_dir = new_cache_dir.resolve()
        self._cache_dir = new_cache_dir

        try:
            if src_sqlite_file is not None:
                dst_sqlite_file = settings.instance._sqlite_file_local
                dst_sqlite_file.parent.mkdir(parents=True, exist_ok=True)
                if dst_sqlite_file.exists():
                    dst_sqlite_file.unlink()
                shutil.move(src_sqlite_file, dst_sqlite_file)  # type: ignore
            save_system_storage_settings(self._cache_dir, self._storage_settings_file)
        except Exception as e:
            self._cache_dir = save_cache_dir
            raise e

    @property
    def is_cloud(self) -> bool:
        """`True` if `storage_root` is in cloud, `False` otherwise."""
        return self.type != "local"

    @property
    def region(self) -> Optional[str]:
        """Storage region."""
        if self._region is None:
            self._region = get_storage_region(self.root_as_str)
        return self._region

    @property
    def type(self) -> str:
        """AWS S3 vs. Google Cloud vs. local vs. the other protocols.

        Returns the protocol.
        """
        import fsspec

        convert = {"file": "local"}
        protocol = fsspec.utils.get_protocol(self.root_as_str)
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
