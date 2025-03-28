from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from django.db.utils import ProgrammingError
from lamin_utils import logger

from ._deprecated import deprecated
from ._hub_client import call_with_fallback
from ._hub_crud import select_account_handle_name_by_lnid
from ._hub_utils import LaminDsn, LaminDsnModel
from ._settings_save import save_instance_settings
from ._settings_storage import (
    LEGACY_STORAGE_UID_FILE_KEY,
    STORAGE_UID_FILE_KEY,
    StorageSettings,
    init_storage,
)
from ._settings_store import current_instance_settings_file, instance_settings_file
from .cloud_sqlite_locker import (
    EXPIRATION_TIME,
    InstanceLockedException,
)
from .upath import LocalPathClasses, UPath

if TYPE_CHECKING:
    from uuid import UUID

    from ._settings_user import UserSettings


def sanitize_git_repo_url(repo_url: str) -> str:
    assert repo_url.startswith("https://")
    return repo_url.replace(".git", "")


def is_local_db_url(db_url: str) -> bool:
    if "@localhost:" in db_url:
        return True
    if "@0.0.0.0:" in db_url:
        return True
    if "@127.0.0.1" in db_url:
        return True
    return False


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
        modules: str | None = None,  # comma-separated string of module names
        git_repo: str | None = None,  # a git repo URL
        is_on_hub: bool | None = None,  # initialized from hub
        api_url: str | None = None,
        schema_id: UUID | None = None,
        fine_grained_access: bool = False,
        db_permissions: str | None = None,
        _locker_user: UserSettings | None = None,  # user to lock for if cloud sqlite
    ):
        from ._hub_utils import validate_db_arg

        self._id_: UUID = id
        self._owner: str = owner
        self._name: str = name
        self._uid: str | None = uid
        self._storage: StorageSettings = storage
        validate_db_arg(db)
        self._db: str | None = db
        self._schema_str: str | None = modules
        self._git_repo = None if git_repo is None else sanitize_git_repo_url(git_repo)
        # local storage
        self._keep_artifacts_local = keep_artifacts_local
        self._storage_local: StorageSettings | None = None
        self._is_on_hub = is_on_hub
        # private, needed for api requests
        self._api_url = api_url
        self._schema_id = schema_id
        # private, whether fine grained access is used
        # needed to be set to request jwt etc
        self._fine_grained_access = fine_grained_access
        # permissions for db such as jwt, read, write etc.
        self._db_permissions = db_permissions
        # if None then settings.user is used
        self._locker_user = _locker_user

    def __repr__(self):
        """Rich string representation."""
        representation = f"Current instance: {self.slug}"
        attrs = ["owner", "name", "storage", "db", "modules", "git_repo"]
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

    def _search_local_root(
        self, local_root: str | None = None, mute_warning: bool = False
    ) -> StorageSettings | None:
        from lamindb.models import Storage

        if local_root is not None:
            local_records = Storage.objects.filter(root=local_root)
        else:
            # only search local managed storage locations (instance_uid=self.uid)
            local_records = Storage.objects.filter(type="local", instance_uid=self.uid)
        all_local_records = local_records.all()
        try:
            # trigger an error in case of a migration issue
            all_local_records.first()
        except ProgrammingError:
            logger.error("not able to load Storage registry: please migrate")
            return None
        found = False
        for record in all_local_records:
            root_path = Path(record.root)
            if root_path.exists():
                marker_path = root_path / STORAGE_UID_FILE_KEY
                if not marker_path.exists():
                    legacy_filepath = root_path / LEGACY_STORAGE_UID_FILE_KEY
                    if legacy_filepath.exists():
                        logger.warning(
                            f"found legacy marker file, renaming it from {legacy_filepath} to {marker_path}"
                        )
                        legacy_filepath.rename(marker_path)
                    else:
                        raise ValueError(
                            f"local storage location '{root_path}' is corrupted, cannot find marker file with storage uid"
                        )
                try:
                    uid = marker_path.read_text()
                except PermissionError:
                    logger.warning(
                        f"ignoring the following location because no permission to read it: {marker_path}"
                    )
                    continue
                if uid == record.uid:
                    found = True
                    break
        if found:
            return StorageSettings(record.root)
        elif not mute_warning:
            logger.warning(
                "none of the registered local storage locations were found:\n    "
                + "\n    ".join(r.root for r in all_local_records)
            )
            logger.important(
                "please register a new local storage location via `ln.settings.storage_local = local_root_path` and re-load/connect the instance"
            )
        return None

    @property
    def keep_artifacts_local(self) -> bool:
        """Default to keeping artifacts local.

        Enable this optional setting for cloud instances on lamin.ai.

        Guide: :doc:`faq/keep-artifacts-local`
        """
        return self._keep_artifacts_local

    @property
    def storage(self) -> StorageSettings:
        """Default storage.

        For a cloud instance, this is cloud storage. For a local instance, this
        is a local directory.
        """
        return self._storage

    @property
    def storage_local(self) -> StorageSettings:
        """An additional local default storage.

        Is only available if :attr:`keep_artifacts_local` is enabled.

        Guide: :doc:`faq/keep-artifacts-local`
        """
        if not self._keep_artifacts_local:
            raise ValueError("`keep_artifacts_local` is not enabled for this instance.")
        if self._storage_local is None:
            self._storage_local = self._search_local_root()
        if self._storage_local is None:
            # raise an error, there was a warning just before in search_local_root
            raise ValueError()
        return self._storage_local

    @storage_local.setter
    def storage_local(self, local_root: Path | str):
        from lamindb_setup._init_instance import register_storage_in_instance

        local_root = Path(local_root)
        if not self._keep_artifacts_local:
            raise ValueError("`keep_artifacts_local` is not enabled for this instance.")
        storage_local = self._search_local_root(
            local_root=StorageSettings(local_root).root_as_str, mute_warning=True
        )
        if storage_local is not None:
            # great, we're merely switching storage location
            self._storage_local = storage_local
            logger.important(f"defaulting to local storage: {storage_local.root}")
            return None
        storage_local = self._search_local_root(mute_warning=True)
        if storage_local is not None:
            if os.getenv("LAMIN_TESTING") == "true":
                response = "y"
            else:
                response = input(
                    "You already configured a local storage root for this instance in this"
                    f" environment: {self.storage_local.root}\nDo you want to register another one? (y/n)"
                )
            if response != "y":
                return None
        local_root = UPath(local_root)
        assert isinstance(local_root, LocalPathClasses)
        self._storage_local, _ = init_storage(local_root, self._id, register_hub=True)  # type: ignore
        register_storage_in_instance(self._storage_local)  # type: ignore
        logger.important(f"defaulting to local storage: {self._storage_local.root}")

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
    def modules(self) -> set[str]:
        """The set of modules that defines the database schema.

        The core schema contained in lamindb is not included in this set.
        """
        if self._schema_str is None:
            return set()
        else:
            return {module for module in self._schema_str.split(",") if module != ""}

    @property
    @deprecated("modules")
    def schema(self) -> set[str]:
        return self.modules

    @property
    def _sqlite_file(self) -> UPath:
        """SQLite file."""
        filepath = self.storage.root / ".lamindb/lamin.db"
        return filepath

    @property
    def _sqlite_file_local(self) -> Path:
        """Local SQLite file."""
        return self.storage.cloud_to_local_no_update(self._sqlite_file)

    def _update_cloud_sqlite_file(self, unlock_cloud_sqlite: bool = True) -> None:
        """Upload the local sqlite file to the cloud file."""
        if self._is_cloud_sqlite:
            sqlite_file = self._sqlite_file
            logger.warning(
                f"updating{' & unlocking' if unlock_cloud_sqlite else ''} cloud SQLite "
                f"'{sqlite_file}' of instance"
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
                " lamin disconnect)"
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
        if "LAMINDB_DJANGO_DATABASE_URL" in os.environ:
            logger.warning(
                "LAMINDB_DJANGO_DATABASE_URL env variable "
                f"is set to {os.environ['LAMINDB_DJANGO_DATABASE_URL']}. "
                "It overwrites all db connections and is used instead of `instance.db`."
            )
        if self._db is None:
            # here, we want the updated sqlite file
            # hence, we don't use self._sqlite_file_local()
            # error_no_origin=False because on instance init
            # the sqlite file is not yet in the cloud
            sqlite_filepath = self.storage.cloud_to_local(
                self._sqlite_file, error_no_origin=False
            )
            return f"sqlite:///{sqlite_filepath.as_posix()}"
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
                # if _locker_user is None then settings.user is used
                return get_locker(self, self._locker_user)
            except PermissionError:
                logger.warning("read-only access - did not access locker")
                return empty_locker
        else:
            return empty_locker

    @property
    def is_remote(self) -> bool:
        """Boolean indicating if an instance has no local component."""
        if not self.storage.type_is_cloud:
            return False

        if self.dialect == "postgresql":
            if is_local_db_url(self.db):
                return False
        # returns True for cloud SQLite
        # and remote postgres
        return True

    @property
    def is_on_hub(self) -> bool:
        """Is this instance on the hub?

        Can only reliably establish if user has access to the instance. Will
        return `False` in case the instance isn't found.
        """
        if self._is_on_hub is None:
            from ._hub_client import call_with_fallback_auth
            from ._hub_crud import select_instance_by_id
            from ._settings import settings

            if settings.user.handle != "anonymous":
                response = call_with_fallback_auth(
                    select_instance_by_id, instance_id=self._id.hex
                )
            else:
                response = call_with_fallback(
                    select_instance_by_id, instance_id=self._id.hex
                )
                logger.warning("calling anonymously, will miss private instances")
            if response is None:
                self._is_on_hub = False
            else:
                self._is_on_hub = True
        return self._is_on_hub

    def _get_settings_file(self) -> Path:
        return instance_settings_file(self.name, self.owner)

    def _persist(self, write_to_disk: bool = True) -> None:
        """Set these instance settings as the current instance.

        Args:
            write_to_disk: Save these instance settings to disk and
                overwrite the current instance settings file.
        """
        if write_to_disk:
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
        from lamindb_setup._check_setup import disable_auto_connect

        from .django import setup_django

        disable_auto_connect(setup_django)(self, init=True)

    def _load_db(self) -> tuple[bool, str]:
        # Is the database available and initialized as LaminDB?
        # returns a tuple of status code and message
        if self.dialect == "sqlite" and not self._sqlite_file.exists():
            legacy_file = self.storage.key_to_filepath(f"{self._id.hex}.lndb")
            if legacy_file.exists():
                logger.warning(
                    f"The SQLite file is being renamed from {legacy_file} to {self._sqlite_file}"
                )
                legacy_file.rename(self._sqlite_file)
            else:
                return False, f"SQLite file {self._sqlite_file} does not exist"
        # we need the local sqlite to setup django
        self._update_local_sqlite_file()
        # setting up django also performs a check for migrations & prints them
        # as warnings
        # this should fail, e.g., if the db is not reachable
        from lamindb_setup._check_setup import disable_auto_connect

        from .django import setup_django

        disable_auto_connect(setup_django)(self)

        return True, ""
