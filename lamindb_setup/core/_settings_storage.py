from __future__ import annotations

import os
import secrets
import string
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

import fsspec
from lamin_utils import logger

from lamindb_setup.errors import StorageAlreadyManaged

from ._aws_options import (
    HOSTED_REGIONS,
    LAMIN_ENDPOINTS,
    get_aws_options_manager,
)
from ._aws_storage import find_closest_aws_region
from ._deprecated import deprecated
from .hashing import hash_and_encode_as_b62
from .upath import (
    LocalPathClasses,
    UPath,
    _split_path_query,
    create_path,
    get_storage_region,
)

if TYPE_CHECKING:
    from lamindb_setup.types import StorageType, UPathStr

STORAGE_UID_FILE_KEY = ".lamindb/storage_uid.txt"
LEGACY_STORAGE_UID_FILE_KEY = ".lamindb/_is_initialized"

# a list of supported fsspec protocols
# rename file to local before showing to a user
VALID_PROTOCOLS = ("file", "gs", "s3", "hf", "http", "https")


def base62(n_char: int) -> str:
    """Like nanoid without hyphen and underscore."""
    alphabet = string.digits + string.ascii_letters.swapcase()
    id = "".join(secrets.choice(alphabet) for i in range(n_char))
    return id


def instance_uid_from_uuid(instance_id: UUID) -> str:
    return hash_and_encode_as_b62(instance_id.hex)[:12]


def get_storage_type(root_as_str: str) -> StorageType:
    import fsspec

    convert = {"file": "local"}
    # init_storage checks that the root protocol belongs to VALID_PROTOCOLS
    protocol = fsspec.utils.get_protocol(root_as_str)
    return convert.get(protocol, protocol)  # type: ignore


def mark_storage_root(
    root: UPathStr, uid: str, instance_id: UUID, instance_slug: str
) -> Literal["__marked__"] | str:
    # we need a file in folder-like storage locations on S3 to avoid
    # permission errors from leveraging s3fs on an empty hosted storage location
    # (path.fs.find raises a PermissionError)
    # we also need it in case a storage location is ambiguous because a server / local environment
    # doesn't have a globally unique identifier, then we screen for this file to map the
    # path on a storage location in the registry

    root_upath = UPath(root)
    existing_uid = ""
    legacy_mark_upath = root_upath / LEGACY_STORAGE_UID_FILE_KEY
    mark_upath = root_upath / STORAGE_UID_FILE_KEY
    if legacy_mark_upath.exists():
        legacy_mark_upath.rename(mark_upath)
    if mark_upath.exists():
        existing_uid = mark_upath.read_text().splitlines()[0]
    if existing_uid == "":
        instance_uid = instance_uid_from_uuid(instance_id)
        text = f"{uid}\ncreation info:\ninstance_slug={instance_slug}\ninstance_id={instance_id.hex}\ninstance_uid={instance_uid}"
        mark_upath.write_text(text)
    elif existing_uid != uid:
        return uid
    # covers the case in which existing uid is the same as uid
    # and the case in which there was no existing uid
    return "__is_marked__"


def init_storage(
    root: UPathStr,
    instance_id: UUID,
    instance_slug: str,
    register_hub: bool | None = None,
    init_instance: bool = False,
    created_by: UUID | None = None,
    access_token: str | None = None,
    region: str | None = None,
    space_uuid: UUID | None = None,
) -> tuple[
    StorageSettings,
    Literal["hub-record-not-created", "hub-record-retrieved", "hub-record-created"],
]:
    from ._hub_core import delete_storage_record, init_storage_hub

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
            root = f"s3://lamin-{region}/{uid}"
        else:
            root = f"s3://lamin-hosted-test/{uid}"
    elif (input_protocol := fsspec.utils.get_protocol(root_str)) not in VALID_PROTOCOLS:
        valid_protocols = ("local",) + VALID_PROTOCOLS[1:]  # show local instead of file
        raise ValueError(
            f"Protocol {input_protocol} is not supported, valid protocols are {', '.join(valid_protocols)}"
        )
    ssettings = StorageSettings(
        uid=uid,
        root=root,
        region=region,
        instance_id=instance_id,
        access_token=access_token,
    )
    # this retrieves the storage record if it exists already in the hub
    # and updates uid and instance_id in ssettings
    if register_hub and not ssettings.type_is_cloud and ssettings.host is None:
        raise ValueError(
            "`host` must be set for local storage locations that are registered on the hub"
        )
    hub_record_status = init_storage_hub(
        ssettings,
        created_by=created_by,
        access_token=access_token,
        prevent_creation=not register_hub,
        is_default=init_instance,
        space_id=space_uuid,
    )
    # we check the write access here if the storage record has not been retrieved from the hub
    if hub_record_status != "hub-record-retrieved":
        try:
            # (federated) credentials for AWS access are provisioned under-the-hood
            # discussion: https://laminlabs.slack.com/archives/C04FPE8V01W/p1719260587167489
            # if access_token was passed in ssettings, it is used here
            marking_result = mark_storage_root(
                root=ssettings.root,
                uid=ssettings.uid,
                instance_id=instance_id,
                instance_slug=instance_slug,
            )
        except Exception:
            marking_result = "no-write-access"
        if marking_result != "__is_marked__":
            if marking_result == "no-write-access":
                logger.important(
                    f"due to lack of write access, LaminDB won't manage this storage location: {ssettings.root_as_str}"
                )
                ssettings._instance_id = None  # indicate that this storage location is not managed by the instance
            else:
                s = "S" if init_instance else "s"  # upper case for error message
                message = (
                    f"{s}torage location {ssettings.root_as_str} is already marked with uid {marking_result}, meaning that it is managed by another LaminDB instance -- "
                    "if you manage your instance with LaminHub you get an overview of all your storage locations"
                )
                if init_instance:
                    raise StorageAlreadyManaged(message)
                logger.warning(message)
                ssettings._instance_id = UUID(
                    "00000000000000000000000000000000"
                )  # indicate not known
                ssettings._uid = marking_result
            # this condition means that the hub record was created
            if ssettings._uuid is not None:
                delete_storage_record(ssettings, access_token=access_token)  # type: ignore
                ssettings._uuid_ = None
                hub_record_status = "hub-record-not-created"
    return ssettings, hub_record_status


class StorageSettings:
    """Settings for a storage location (local or cloud).

    Do not instantiate this class yourself, use `ln.Storage` instead.
    """

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
        self._root_init = UPath(root).expanduser()
        if isinstance(self._root_init, LocalPathClasses):  # local paths
            try:
                (self._root_init / ".lamindb").mkdir(parents=True, exist_ok=True)
                self._root_init = self._root_init.resolve()
            except Exception:
                logger.warning(
                    f"unable to create .lamindb/ folder in {self._root_init}"
                )
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
    @deprecated("_id")
    def id(self) -> int:
        return self._id

    @property
    def _id(self) -> int:
        """Storage id.

        This id is only valid in the current instance and not globally unique. Only for internal use.
        """
        return self.record.id

    @property
    def _uuid(self) -> UUID | None:
        """Lamin's internal storage uuid."""
        return self._uuid_

    @property
    def uid(self) -> str:
        """Storage uid."""
        if self._uid is None:
            self._uid = self.record.uid
        return self._uid

    @property
    def instance_uid(self) -> str | None:
        """The `uid` of the managing LaminDB instance.

        If `None`, the storage location is not managed by any LaminDB instance.
        """
        if self._instance_id is not None:
            if self._instance_id.hex == "00000000000000000000000000000000":
                instance_uid = "__unknown__"
            else:
                instance_uid = instance_uid_from_uuid(self._instance_id)
        else:
            instance_uid = None
        return instance_uid

    @property
    def _mark_storage_root(self) -> UPath:
        marker_path = self.root / STORAGE_UID_FILE_KEY
        legacy_filepath = self.root / LEGACY_STORAGE_UID_FILE_KEY
        if legacy_filepath.exists():
            logger.warning(
                f"found legacy marker file, renaming it from {legacy_filepath} to {marker_path}"
            )
            legacy_filepath.rename(marker_path)
        return marker_path

    @property
    def record(self) -> Any:
        """Storage record in the current instance."""
        if self._record is None:
            # dynamic import because of import order
            from lamindb.models import Storage

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
            return get_aws_options_manager().enrich_path(
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
        # embed endpoint_url into path string for storing and displaying
        if self._root_init.protocol == "s3":
            endpoint_url = self._root_init.storage_options.get("endpoint_url", None)
            # LAMIN_ENDPOINTS include None
            if endpoint_url not in LAMIN_ENDPOINTS:
                return f"s3://{self._root_init.path.rstrip('/')}?endpoint_url={endpoint_url}"
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
    def host(self) -> str | None:
        """Host identifier for local storage locations.

        Is `None` for locations with `type != "local"`.

        A globally unique user-defined host identifier (cluster, server, laptop, etc.).
        """
        if self.type != "local":
            return None
        return self.region

    @property
    def region(self) -> str | None:
        """Storage region."""
        if self._region is None:
            self._region = get_storage_region(self.root_as_str)
        return self._region

    @property
    def type(self) -> StorageType:
        """AWS S3 vs. Google Cloud vs. local.

        Returns the protocol as a stringe, e.g., "local", "s3", "gs", "http", "https".
        """
        return get_storage_type(self.root_as_str)

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
        """Local (cache) filepath from filekey."""
        return self.cloud_to_local(self.key_to_filepath(filekey))
