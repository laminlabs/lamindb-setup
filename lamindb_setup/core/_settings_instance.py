from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from lamin_utils import logger

from ._hub_client import call_with_fallback
from ._hub_crud import select_account_handle_name_by_lnid
from ._hub_utils import LaminDsn, LaminDsnModel
from ._settings_save import save_instance_settings
from ._settings_storage import StorageSettings, init_storage, mark_storage_root
from ._settings_store import current_instance_settings_file, instance_settings_file
from .cloud_sqlite_locker import (
    EXPIRATION_TIME,
    InstanceLockedException,
)
from .upath import LocalPathClasses, UPath, convert_pathlike

if TYPE_CHECKING:
    from uuid import UUID


def sanitize_git_repo_url(repo_url: str) -> str:
    assert repo_url.startswith("https://")
    return repo_url.replace(".git", "")


class InstanceSettings:
    """Instance settings."""

    def __init__(
        self,
        id: UUID,  # instance id/uuid
        owner: str,  # owner handle
        name: str,  # instance name
        storage: StorageSettings,  # storage location
        keep_artifacts_local: bool = False,  # default to local storage
        uid: str | None = None,  # instance uid/lnid
        db: str | None = None,  # DB URI
        schema: str | None = None,  # comma-separated string of schema names
        git_repo: str | None = None,  # a git repo URL
    ):
        from ._hub_utils import validate_db_arg

        self._id_: UUID = id
        self._owner: str = owner
        self._name: str = name
        self._uid: str | None = uid
        self._storage: StorageSettings = storage
        validate_db_arg(db)
        self._db: str | None = db
        self._schema_str: str | None = schema
        self._git_repo = None if git_repo is None else sanitize_git_repo_url(git_repo)
        # local storage
        self._keep_artifacts_local = keep_artifacts_local
        self._local_storage = None

    def __repr__(self):
        """Rich string representation."""
        representation = f"Current instance: {self.slug}"
        attrs = ["owner", "name", "storage", "db", "schema", "git_repo"]
        for attr in attrs:
            value = getattr(self, attr)
            if attr == "storage":
                representation += f"\n- storage root: {value.root_as_str}"
                representation += f"\n- storage region: {value.region}"
            elif attr == "db":
                if self.dialect != "sqlite":
                    model = LaminDsnModel(db=value)
                    db_print = LaminDsn.build(
                        scheme=model.db.scheme,
                        user=model.db.user,
                        password="***",
                        host="***",
                        port=model.db.port,
                        database=model.db.database,
                    )
                else:
                    db_print = value
                representation += f"\n- {attr}: {db_print}"
            else:
                representation += f"\n- {attr}: {value}"
        return representation

    @property
    def owner(self) -> str:
        """Instance owner. A user or organization account handle."""
        return self._owner

    @property
    def name(self) -> str:
        """Instance name."""
        return self._name

    def _search_local_root(self):
        from lnschema_core.models import Storage

        local_records = Storage.objects.filter(type="local")
        found = False
        for record in local_records.all():
            root_path = Path(record.root)
            if root_path.exists():
                marker_path = root_path / ".lamindb/_is_initialized"
                if marker_path.exists():
                    uid = marker_path.read_text()
                    if uid == self.uid:
                        found = True
                        break
                    elif uid == "":
                        # legacy instance that was not yet marked properly
                        mark_storage_root(record.root, record.uid)
                    else:
                        continue
                else:
                    # legacy instance that was not yet marked at all
                    mark_storage_root(record.root, record.uid)
                    break
        if found:
            self._local_storage = StorageSettings(record.root)
            logger.important(f"defaulting to local storage: {record}")
        else:
            logger.warning(
                f"none of the registered local storage locations were found in your environment: {local_records}"
                "\n\nplease register a new local storage location via `ln.settings.storage = storage_path` "
                "and re-load/connect the instance"
            )

    @property
    def keep_artifacts_local(self) -> bool:
        """Keep artifacts in a default local storage.

        Only needed for cloud instances. Local instances keep their artifacts local, anyway.

        Change this setting on lamin.ai.

        If enabled, it makes it easy to, by default, keep artifacts local and
        maintain an overview of local storage locations managed by an instance
        on lamin.ai.
        """
        return self._keep_artifacts_local

    @property
    def local_storage(self) -> StorageSettings:
        """Default local storage.

        Warning: Only enable if you're sure you want to use the more complicated
        storage mode across local & cloud locations.

        As an admin, enable via: `ln.setup.settings.instance.local_storage =
        local_root`.

        If enabled, you'll save artifacts to a default local storage
        location.

        Upon passing `upload=True` in `artifact.save(upload=True)`, you upload the
        artifact to the default cloud storage location.
        """
        if not self._keep_artifacts_local:
            raise ValueError("`keep_artifacts_local` is not enabled for this instance.")
        if self._local_storage is None:
            self._search_local_root()
        if self._local_storage is None:
            raise ValueError()
        return self._local_storage

    @local_storage.setter
    def local_storage(self, local_root: Path):
        from lamindb_setup._init_instance import register_storage_in_instance

        if not self._keep_artifacts_local:
            raise ValueError("`keep_artifacts_local` is not enabled for this instance.")
        self._search_local_root()
        if self._local_storage is not None:
            raise ValueError(
                "You already configured a local storage root for this instance in this"
                f" environment: {self.local_storage.root}"
            )
        local_root = convert_pathlike(local_root)
        assert isinstance(local_root, LocalPathClasses)
        self._local_storage = init_storage(local_root)  # type: ignore
        register_storage_in_instance(self._local_storage, self)  # type: ignore

    @property
    def slug(self) -> str:
        """Unique semantic identifier of form `"{account_handle}/{instance_name}"`."""
        return f"{self.owner}/{self.name}"

    @property
    def git_repo(self) -> str | None:
        """Sync transforms with scripts in git repository.

        Provide the full git repo URL.
        """
        return self._git_repo

    # @property
    # def id(self) -> UUID:
    #     """The internal instance id."""
    #     logger.warning("is deprecated, use _id instead")
    #     return self._id_

    @property
    def _id(self) -> UUID:
        """The internal instance id."""
        return self._id_

    @property
    def uid(self) -> str:
        """The user-facing instance id."""
        from .hashing import hash_and_encode_as_b62

        return hash_and_encode_as_b62(self._id.hex)[:12]

    @property
    def schema(self) -> set[str]:
        """Schema modules in addition to core schema."""
        if self._schema_str is None:
            return {}  # type: ignore
        else:
            return {schema for schema in self._schema_str.split(",") if schema != ""}

    @property
    def _sqlite_file(self) -> UPath:
        """SQLite file."""
        return self.storage.key_to_filepath(f"{self._id.hex}.lndb")

    @property
    def _sqlite_file_local(self) -> Path:
        """Local SQLite file."""
        return self.storage.cloud_to_local_no_update(self._sqlite_file)

    def _update_cloud_sqlite_file(self, unlock_cloud_sqlite: bool = True) -> None:
        """Upload the local sqlite file to the cloud file."""
        if self._is_cloud_sqlite:
            sqlite_file = self._sqlite_file
            logger.warning(
                f"updating & unlocking cloud SQLite '{sqlite_file}' of instance"
                f" '{self.slug}'"
            )
            cache_file = self.storage.cloud_to_local_no_update(sqlite_file)
            sqlite_file.upload_from(cache_file, print_progress=True)  # type: ignore
            cloud_mtime = sqlite_file.modified.timestamp()  # type: ignore
            # this seems to work even if there is an open connection
            # to the cache file
            os.utime(cache_file, times=(cloud_mtime, cloud_mtime))
            if unlock_cloud_sqlite:
                self._cloud_sqlite_locker.unlock()

    def _update_local_sqlite_file(self, lock_cloud_sqlite: bool = True) -> None:
        """Download the cloud sqlite file if it is newer than local."""
        if self._is_cloud_sqlite:
            logger.warning(
                "updating local SQLite & locking cloud SQLite (sync back & unlock:"
                " lamin close)"
            )
            if lock_cloud_sqlite:
                self._cloud_sqlite_locker.lock()
                self._check_sqlite_lock()
            sqlite_file = self._sqlite_file
            cache_file = self.storage.cloud_to_local_no_update(sqlite_file)
            sqlite_file.synchronize(cache_file, print_progress=True)  # type: ignore

    def _check_sqlite_lock(self):
        if not self._cloud_sqlite_locker.has_lock:
            locked_by = self._cloud_sqlite_locker._locked_by
            lock_msg = "Cannot load the instance, it is locked by "
            user_info = call_with_fallback(
                select_account_handle_name_by_lnid,
                lnid=locked_by,
            )
            if user_info is None:
                lock_msg += f"uid: '{locked_by}'."
            else:
                lock_msg += (
                    f"'{user_info['handle']}' (uid: '{locked_by}', name:"
                    f" '{user_info['name']}')."
                )
            lock_msg += (
                " The instance will be automatically unlocked after"
                f" {int(EXPIRATION_TIME/3600/24)}d of no activity."
            )
            raise InstanceLockedException(lock_msg)

    @property
    def db(self) -> str:
        """Database connection string (URI)."""
        if self._db is None:
            # here, we want the updated sqlite file
            # hence, we don't use self._sqlite_file_local()
            # error_no_origin=False because on instance init
            # the sqlite file is not yet in the cloud
            sqlite_filepath = self.storage.cloud_to_local(
                self._sqlite_file, error_no_origin=False
            )
            return f"sqlite:///{sqlite_filepath}"
        else:
            return self._db

    @property
    def dialect(self) -> Literal["sqlite", "postgresql"]:
        """SQL dialect."""
        if self._db is None or self._db.startswith("sqlite://"):
            return "sqlite"
        else:
            assert self._db.startswith("postgresql"), f"Unexpected DB value: {self._db}"
            return "postgresql"

    @property
    def _is_cloud_sqlite(self) -> bool:
        # can we make this a private property, Sergei?
        # as it's not relevant to the user
        """Is this a cloud instance with sqlite db."""
        return self.dialect == "sqlite" and self.storage.type_is_cloud

    @property
    def _cloud_sqlite_locker(self):
        # avoid circular import
        from .cloud_sqlite_locker import empty_locker, get_locker

        if self._is_cloud_sqlite:
            try:
                return get_locker(self)
            except PermissionError:
                logger.warning("read-only access - did not access locker")
                return empty_locker
        else:
            return empty_locker

    @property
    def storage(self) -> StorageSettings:
        """Low-level access to storage location."""
        return self._storage

    @property
    def is_remote(self) -> bool:
        """Boolean indicating if an instance has no local component."""
        if not self.storage.type_is_cloud:
            return False

        def is_local_uri(uri: str):
            if "@localhost:" in uri:
                return True
            if "@0.0.0.0:" in uri:
                return True
            if "@127.0.0.1" in uri:
                return True
            return False

        if self.dialect == "postgresql":
            if is_local_uri(self.db):
                return False
        # returns True for cloud SQLite
        # and remote postgres
        return True

    def _get_settings_file(self) -> Path:
        return instance_settings_file(self.name, self.owner)

    def _persist(self) -> None:
        assert self.name is not None

        filepath = self._get_settings_file()
        # persist under filepath for later reference
        save_instance_settings(self, filepath)
        # persist under current file for auto load
        shutil.copy2(filepath, current_instance_settings_file())
        # persist under settings class for same session reference
        # need to import here to avoid circular import
        from ._settings import settings

        settings._instance_settings = self

    def _init_db(self):
        from .django import setup_django

        setup_django(self, init=True)

    def _load_db(
        self, do_not_lock_for_laminapp_admin: bool = False
    ) -> tuple[bool, str]:
        # Is the database available and initialized as LaminDB?
        # returns a tuple of status code and message
        if self.dialect == "sqlite" and not self._sqlite_file.exists():
            legacy_file = self.storage.key_to_filepath(f"{self.name}.lndb")
            if legacy_file.exists():
                raise RuntimeError(
                    "The SQLite file has been renamed!\nPlease rename your SQLite file"
                    f" {legacy_file} to {self._sqlite_file}"
                )
            return False, f"SQLite file {self._sqlite_file} does not exist"
        from lamindb_setup import settings  # to check user

        from .django import setup_django

        # lock in all cases except if do_not_lock_for_laminapp_admin is True and
        # user is `laminapp-admin`
        # value doesn't matter if not a cloud sqlite instance
        lock_cloud_sqlite = self._is_cloud_sqlite and (
            not do_not_lock_for_laminapp_admin
            or settings.user.handle != "laminapp-admin"
        )
        # we need the local sqlite to setup django
        self._update_local_sqlite_file(lock_cloud_sqlite=lock_cloud_sqlite)
        # setting up django also performs a check for migrations & prints them
        # as warnings
        # this should fail, e.g., if the db is not reachable
        setup_django(self)
        return True, ""
