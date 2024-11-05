from __future__ import annotations

import os
import secrets
import shutil
import string
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from lamin_utils import logger

from ._aws_credentials import HOSTED_REGIONS, get_aws_credentials_manager
from ._aws_storage import find_closest_aws_region
from .upath import (
    LocalPathClasses,
    UPath,
    create_path,
)

if TYPE_CHECKING:
    from uuid import UUID

    from .types import UPathStr

IS_INITIALIZED_KEY = ".lamindb/_is_initialized"


def base62(n_char: int) -> str:
    """Like nanoid without hyphen and underscore."""
    alphabet = string.digits + string.ascii_letters.swapcase()
    id = "".join(secrets.choice(alphabet) for i in range(n_char))
    return id


def get_storage_region(path: UPathStr) -> str | None:
    path_str = str(path)
    if path_str.startswith("s3://"):
        import botocore.session
        from botocore.config import Config
        from botocore.exceptions import ClientError

        # strip the prefix and any suffixes of the bucket name
        bucket = path_str.replace("s3://", "").split("/")[0]
        session = botocore.session.get_session()
        credentials = session.get_credentials()
        if credentials is None or credentials.access_key is None:
            config = Config(signature_version=botocore.session.UNSIGNED)
        else:
            config = None
        s3_client = session.create_client("s3", config=config)
        try:
            response = s3_client.head_bucket(Bucket=bucket)
        except ClientError as exc:
            response = getattr(exc, "response", {})
            if response.get("Error", {}).get("Code") == "404":
                raise exc
        region = (
            response.get("ResponseMetadata", {})
            .get("HTTPHeaders", {})
            .get("x-amz-bucket-region")
        )
    else:
        region = None
    return region


def mark_storage_root(root: UPathStr, uid: str):
    # we need to touch a 0-byte object in folder-like storage location on S3 to avoid
    # permission errors from leveraging s3fs on an empty hosted storage location
    # for consistency, we write this file everywhere
    root_upath = UPath(root)
    mark_upath = root_upath / IS_INITIALIZED_KEY
    mark_upath.write_text(uid)


def init_storage(
    root: UPathStr,
    instance_id: UUID | None = None,
    register_hub: bool | None = None,
    prevent_register_hub: bool = False,
    init_instance: bool = False,
    created_by: UUID | None = None,
    access_token: str | None = None,
) -> tuple[
    StorageSettings,
    Literal["hub-record-not-created", "hub-record-retireved", "hub-record-created"],
]:
    assert root is not None, "`root` argument can't be `None`"

    root_str = str(root)  # ensure we have a string
    if ".lamindb" in root_str:
        raise ValueError(
            'Please pass a folder name that does not end or contain ".lamindb"'
        )
    uid = os.getenv("LAMINDB_STORAGE_LNID_INIT")
    if uid is None:
        uid = base62(12)
    else:
        # this means we constructed a hosted location of shape s3://bucket-name/uid
        # within LaminHub
        assert root_str.endswith(uid)
    region = None
    lamin_env = os.getenv("LAMIN_ENV")
    if root_str.startswith("create-s3"):
        if root_str != "create-s3":
            assert "--" in root_str, "example: `create-s3--eu-central-1`"
            region = root_str.replace("create-s3--", "")
        if region is None:
            region = find_closest_aws_region()
        else:
            if region not in HOSTED_REGIONS:
                raise ValueError(f"region has to be one of {HOSTED_REGIONS}")
        if lamin_env is None or lamin_env == "prod":
            root_str = f"s3://lamin-{region}/{uid}"
        else:
            root_str = f"s3://lamin-hosted-test/{uid}"
    elif root_str.startswith(("gs://", "s3://", "hf://")):
        pass
    else:  # local path
        try:
            _ = Path(root_str)
        except Exception as e:
            logger.error(
                "`storage` is not a valid local, GCP storage, AWS S3 path or Hugging Face path"
            )
            raise e
    ssettings = StorageSettings(
        uid=uid,
        root=root_str,
        region=region,
        instance_id=instance_id,
        access_token=access_token,
    )
    # this stores the result of init_storage_hub
    hub_record_status: Literal[
        "hub-record-not-created", "hub-record-retireved", "hub-record-created"
    ] = "hub-record-not-created"
    # the below might update the uid with one that's already taken on the hub
    if not prevent_register_hub:
        if ssettings.type_is_cloud or register_hub:
            from ._hub_core import delete_storage_record
            from ._hub_core import init_storage as init_storage_hub

            hub_record_status = init_storage_hub(
                ssettings,
                auto_populate_instance=not init_instance,
                created_by=created_by,
                access_token=access_token,
            )
    # below comes last only if everything else was successful
    try:
        # (federated) credentials for AWS access are provisioned under-the-hood
        # discussion: https://laminlabs.slack.com/archives/C04FPE8V01W/p1719260587167489
        # if access_token was passed in ssettings, it is used here
        mark_storage_root(ssettings.root, ssettings.uid)  # type: ignore
    except Exception:
        logger.important(
            f"due to lack of write access, LaminDB won't manage storage location: {ssettings.root}"
        )
        # we have to check hub_record_status here because
        # _select_storage inside init_storage_hub also populates ssettings._uuid
        # and we don't want to delete an existing storage record here
        # only newly created
        if hub_record_status == "hub-record-created" and ssettings._uuid is not None:
            delete_storage_record(ssettings._uuid, access_token=access_token)  # type: ignore
            hub_record_status = "hub-record-not-created"
        ssettings._instance_id = None
    return ssettings, hub_record_status


class StorageSettings:
    """Settings for a given storage location (local or cloud)."""

    def __init__(
        self,
        root: UPathStr,
        region: str | None = None,
        uid: str | None = None,
        uuid: UUID | None = None,
        instance_id: UUID | None = None,
        # note that passing access_token prevents credentials caching
        access_token: str | None = None,
    ):
        self._uid = uid
        self._uuid_ = uuid
        self._root_init = UPath(root)
        if isinstance(self._root_init, LocalPathClasses):  # local paths
            try:
                (self._root_init / ".lamindb").mkdir(parents=True, exist_ok=True)
                self._root_init = self._root_init.resolve()
            except Exception:
                logger.warning(f"unable to create .lamindb folder in {self._root_init}")
                pass
        self._root = None
        self._instance_id = instance_id
        # we don't yet infer region here to make init fast
        self._region = region
        # would prefer to type below as Registry, but need to think through import order
        self._record: Any | None = None
        # save access_token here for use in self.root
        self.access_token = access_token

        # local storage
        self._has_local = False
        self._local = None

    @property
    def id(self) -> int:
        """Storage id in current instance."""
        return self.record.id

    @property
    def _uuid(self) -> UUID | None:
        """Lamin's internal storage uuid."""
        return self._uuid_

    @property
    def uid(self) -> str | None:
        """Storage id."""
        if self._uid is None:
            self._uid = self.record.uid
        return self._uid

    @property
    def _mark_storage_root(self) -> UPath:
        return self.root / IS_INITIALIZED_KEY

    @property
    def record(self) -> Any:
        """Storage record in current instance."""
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
        if self._uuid is not None:
            s += f", uuid='{self._uuid.hex}'"
        return f"StorageSettings({s})"

    @property
    def root(self) -> UPath:
        """Root storage location."""
        if self._root is None:
            # below makes network requests to get credentials
            self._root = create_path(self._root_init, access_token=self.access_token)
        elif getattr(self._root, "protocol", "") == "s3":
            # this is needed to be sure that the root always has nonexpired credentials
            # this just checks for time of the cached credentials in most cases
            return get_aws_credentials_manager().enrich_path(
                self._root, access_token=self.access_token
            )
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
        from lamindb_setup import settings

        return settings.cache_dir

    @property
    def type_is_cloud(self) -> bool:
        """`True` if `storage_root` is in cloud, `False` otherwise."""
        return self.type != "local"

    @property
    def region(self) -> str | None:
        """Storage region."""
        if self._region is None:
            self._region = get_storage_region(self.root_as_str)
        return self._region

    @property
    def type(self) -> Literal["local", "s3", "gs"]:
        """AWS S3 vs. Google Cloud vs. local.

        Returns the protocol as a string: "local", "s3", "gs".
        """
        import fsspec

        convert = {"file": "local"}
        protocol = fsspec.utils.get_protocol(self.root_as_str)
        return convert.get(protocol, protocol)  # type: ignore

    @property
    def is_on_hub(self) -> bool:
        """Is this instance on the hub.

        Only works if user has access to the instance.
        """
        if self._uuid is None:
            return False
        else:
            return True

    def cloud_to_local(
        self, filepath: UPathStr, cache_key: str | None = None, **kwargs
    ) -> UPath:
        """Local (or local cache) filepath from filepath."""
        from lamindb_setup import settings

        return settings.paths.cloud_to_local(
            filepath=filepath, cache_key=cache_key, **kwargs
        )

    def cloud_to_local_no_update(
        self, filepath: UPathStr, cache_key: str | None = None
    ) -> UPath:
        from lamindb_setup import settings

        return settings.paths.cloud_to_local_no_update(
            filepath=filepath, cache_key=cache_key
        )

    def key_to_filepath(self, filekey: UPathStr) -> UPath:
        """Cloud or local filepath from filekey."""
        return self.root / filekey

    def local_filepath(self, filekey: UPathStr) -> UPath:
        """Local (cache) filepath from filekey: `local(filepath(...))`."""
        return self.cloud_to_local(self.key_to_filepath(filekey))
