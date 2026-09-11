from __future__ import annotations

import os
import sys
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING

import jwt
from lamin_utils import logger
from platformdirs import user_cache_dir

from ._deprecated import deprecated
from ._settings_load import (
    load_cache_path_from_settings,
    load_instance_settings,
    load_or_create_user_settings,
)
from ._settings_store import (
    current_instance_settings_file,
    current_modules_file,
    get_settings_file_name_prefix,
    local_current_branch_file,
    local_current_instance_file,
    remove_local_current_instance,
    settings_dir,
    system_settings_dir,
    write_local_current_instance,
)

if TYPE_CHECKING:
    from lamindb.models import Branch, Space

    from lamindb_setup.core import InstanceSettings, StorageSettings, UserSettings
    from lamindb_setup.core.django import DBToken, DBTokenManager
    from lamindb_setup.types import AnyPathStr

    from .upath import UPath


DEFAULT_CACHE_DIR = Path(user_cache_dir(appname="lamindb", appauthor="laminlabs"))
_WORKTREE_ROOT_ENTRIES = {".agents", ".claude", ".lamin", ".vscode"}


def _confirm_worktree_migration() -> bool:
    response = input("Continue? [Y/n]: ").strip().lower()
    return response in {"", "y", "yes"}


def _is_lamindb_storage(path: Path) -> bool:
    return path.is_dir() and (path / ".lamindb" / "storage_uid.txt").is_file()


def _default_cache_dir():
    from .upath import UPath

    return UPath(user_cache_dir(appname="lamindb", appauthor="laminlabs"))


def _process_cache_path(cache_path: AnyPathStr | None) -> UPath | None:
    from .upath import LocalPathClasses, UPath

    if cache_path is None or cache_path == "null":
        return None
    cache_dir = UPath(cache_path)
    if not isinstance(cache_dir, LocalPathClasses):
        raise ValueError("cache dir should be a local path.")
    if cache_dir.exists() and not cache_dir.is_dir():
        raise ValueError("cache dir should be a directory.")
    if not cache_dir.is_absolute():
        raise ValueError("A path to the cache dir should be absolute.")
    return cache_dir


# returned by settings.branch for none/none instance
class MainBranchMock:
    id = 1
    name = "main"


class SetupSettings:
    """Setup settings."""

    _user_settings: UserSettings | None = None
    _instance_settings: InstanceSettings | None = None

    _user_settings_env: str | None = None
    _instance_settings_env: str | None = None

    _auto_connect_path: Path = settings_dir / "auto_connect"
    _private_django_api_path: Path = settings_dir / "private_django_api"

    _cache_dir: Path | None = None
    modules_warning: str | None = None

    _branch = None  # do not have types here
    _space = None  # do not have types here

    def _clear_instance_context_cache(self) -> None:
        """Clear cached instance context."""
        self._branch = None
        self._space = None

    @property
    def _instance_settings_path(self) -> Path:
        return current_instance_settings_file()

    @property
    def _modules_path(self) -> Path:
        return current_modules_file()

    @property
    def modules(self) -> set[str]:
        """The set of configured schema modules for this environment.

        Instance modules take precedence if an instance is configured.
        Otherwise, `LAMINDB_MODULES` overrides a global setting.
        """
        # if a current instance is configured in the environment,
        # return the instance modules directly
        if self._instance_settings_path.exists():
            return self.instance.modules
        # Explicit env var override for ephemeral configuration.
        env_modules = os.environ.get("LAMINDB_MODULES")
        if env_modules is not None:
            return {
                module.strip()
                for module in env_modules.split(",")
                if module.strip() != ""
            }
        if not self._modules_path.exists():
            candidates = {"bionty"}
            return {c for c in candidates if find_spec(c) is not None}
        schema_str = self._modules_path.read_text().strip()
        if schema_str == "":
            return set()
        return {module for module in schema_str.split(",") if module != ""}

    @modules.setter
    def modules(self, value: set[str] | str | None) -> None:
        if value is None:
            self._modules_path.unlink(missing_ok=True)
            return
        if isinstance(value, str):
            schema_str = value
        else:
            schema_str = ",".join(sorted(value))
        self._modules_path.write_text(schema_str)

    @property
    def settings_dir(self) -> Path:
        """The directory that holds locally persisted settings."""
        return settings_dir

    @property
    def auto_connect(self) -> bool:
        """Auto-connect to current instance upon `import lamindb`.

        This setting is always `True` and will be removed in a future version.
        """
        return True

    @auto_connect.setter
    def auto_connect(self, value: bool) -> None:
        logger.warning(
            "setting auto_connect to `False` no longer has an effect and the setting will likely be removed in the future",
        )
        if value:
            self._auto_connect_path.touch()
        else:
            self._auto_connect_path.unlink(missing_ok=True)

    @property
    def _dev_dir_path(self) -> Path:
        return (
            settings_dir / f"dev-dir--{self.instance.owner}--{self.instance.name}.txt"
        )

    @property
    def _worktree_path(self) -> Path:
        return (
            settings_dir / f"worktree--{self.instance.owner}--{self.instance.name}.txt"
        )

    @property
    def dev_dir(self) -> Path | None:
        """Get or set the local development directory for the current instance.

        If setting it to `None`, the working development directory is unset.
        Setting a directory also marks that directory for local auto-connect.
        """
        if not self._dev_dir_path.exists():
            return None
        return Path(self._dev_dir_path.read_text())

    @dev_dir.setter
    def dev_dir(self, value: str | Path | None) -> None:
        previous_dev_dir = self.dev_dir
        instance_slug = self.instance.slug
        previous_branch_marker = (
            local_current_branch_file(previous_dev_dir.resolve())
            if previous_dev_dir is not None
            else None
        )

        if value is None:
            if self._dev_dir_path.exists():
                self._dev_dir_path.unlink()
            if previous_branch_marker is not None:
                previous_branch_marker.unlink(missing_ok=True)
            if previous_dev_dir is not None:
                remove_local_current_instance(
                    marker=local_current_instance_file(previous_dev_dir.resolve()),
                    expected_instance_slug=instance_slug,
                )
        else:
            value_path = Path(value).expanduser().resolve()
            value_str = value_path.as_posix()
            self._dev_dir_path.write_text(value_str)
            if instance_slug != "none/none":
                write_local_current_instance(value_path, instance_slug)
            if (
                previous_dev_dir is not None
                and previous_dev_dir.resolve() != value_path
            ):
                if previous_branch_marker is not None:
                    previous_branch_marker.unlink(missing_ok=True)
                remove_local_current_instance(
                    marker=local_current_instance_file(previous_dev_dir.resolve()),
                    expected_instance_slug=instance_slug,
                )

    @property
    def worktree(self) -> bool:
        """Whether `dev_dir` is treated like a Git-worktree parent.

        When enabled, `dev_dir` is a parent directory and each child directory is a
        branch-specific workspace (analogous to a Git worktree checkout). LaminDB then
        resolves branch context from the child's `.lamin/current_branch` and derives
        keys relative to that child root. When disabled, `dev_dir` itself is the active
        root for branch lookup and key derivation.
        """
        if not self._worktree_path.exists():
            return False
        value = self._worktree_path.read_text().strip().lower()
        return value in {"1", "true", "yes"}

    @worktree.setter
    def worktree(self, value: bool) -> None:
        if value:
            self._enable_worktree()
        else:
            self._disable_worktree()

    def _enable_worktree(self) -> None:
        if self.worktree:
            return

        dev_dir = self.dev_dir
        if dev_dir is None:
            raise RuntimeError("worktree mode requires a configured dev-dir")

        dev_dir = dev_dir.resolve()
        dev_dir.mkdir(parents=True, exist_ok=True)
        # Agent/editor configuration applies to every branch and stays at the root.
        entries = [
            entry
            for entry in dev_dir.iterdir()
            if entry.name not in _WORKTREE_ROOT_ENTRIES
            # Storage roots are registered by path and must not move with branch files.
            and not _is_lamindb_storage(entry)
        ]
        if not entries:
            self._worktree_path.write_text("true")
            return

        branch_idlike, branch_name = self._read_branch_idlike_name()
        branch_dir = dev_dir / branch_name
        if branch_dir.exists():
            raise RuntimeError(
                f"Cannot enable worktree mode because '{branch_dir}' already exists."
            )

        print(
            f"Existing files in '{dev_dir}' will be moved to branch directory "
            f"'{branch_dir}'."
        )
        if not _confirm_worktree_migration():
            raise RuntimeError("Aborted.")

        branch_dir.mkdir()
        moved_entries: list[tuple[Path, Path]] = []
        try:
            for source in entries:
                destination = branch_dir / source.name
                source.replace(destination)
                moved_entries.append((source, destination))

            branch_marker = local_current_branch_file(branch_dir)
            branch_marker.parent.mkdir(parents=True, exist_ok=True)
            branch_marker.write_text(f"{branch_idlike}\n{branch_name}")
            self._worktree_path.write_text("true")
        except Exception:
            self._worktree_path.unlink(missing_ok=True)
            for source, destination in reversed(moved_entries):
                if destination.exists() and not source.exists():
                    destination.replace(source)
            branch_marker = local_current_branch_file(branch_dir)
            branch_marker.unlink(missing_ok=True)
            if branch_marker.parent.exists() and not any(
                branch_marker.parent.iterdir()
            ):
                branch_marker.parent.rmdir()
            if branch_dir.exists() and not any(branch_dir.iterdir()):
                branch_dir.rmdir()
            raise

        print(f"Moved existing files to '{branch_dir}'.")

    def _disable_worktree(self) -> None:
        if not self.worktree:
            return

        dev_dir = self.dev_dir
        if dev_dir is None:
            self._worktree_path.unlink(missing_ok=True)
            return

        dev_dir = dev_dir.resolve()
        branch_dirs = [
            path
            for path in dev_dir.iterdir()
            if path.is_dir() and local_current_branch_file(path).exists()
        ]
        if not branch_dirs:
            self._worktree_path.unlink(missing_ok=True)
            return
        # Never guess which branch should become the manual dev-dir or merge branches.
        if len(branch_dirs) > 1:
            names = ", ".join(sorted(path.name for path in branch_dirs))
            raise RuntimeError(
                "Cannot disable worktree mode while multiple branch directories "
                f"exist: {names}. Merge or remove the other branch directories first."
            )

        branch_dir = branch_dirs[0]
        # The branch marker is metadata; only user files return to the dev-dir root.
        entries = [entry for entry in branch_dir.iterdir() if entry.name != ".lamin"]
        collisions = [
            entry.name for entry in entries if (dev_dir / entry.name).exists()
        ]
        if collisions:
            raise RuntimeError(
                "Cannot disable worktree mode because these paths already exist in "
                f"the dev-dir: {', '.join(sorted(collisions))}."
            )

        print(
            f"Files in branch directory '{branch_dir}' will be moved back to "
            f"'{dev_dir}'."
        )
        if not _confirm_worktree_migration():
            raise RuntimeError("Aborted.")

        moved_entries: list[tuple[Path, Path]] = []
        try:
            for source in entries:
                destination = dev_dir / source.name
                source.replace(destination)
                moved_entries.append((source, destination))
            self._worktree_path.unlink(missing_ok=True)
        except Exception:
            self._worktree_path.write_text("true")
            for source, destination in reversed(moved_entries):
                if destination.exists() and not source.exists():
                    destination.replace(source)
            raise

        branch_marker = local_current_branch_file(branch_dir)
        branch_marker.unlink(missing_ok=True)
        branch_lamin_dir = branch_marker.parent
        # Remove only empty metadata/wrapper directories, never user data.
        if branch_lamin_dir.exists() and not any(branch_lamin_dir.iterdir()):
            branch_lamin_dir.rmdir()
        if branch_dir.exists() and not any(branch_dir.iterdir()):
            branch_dir.rmdir()
        print(f"Restored files from '{branch_dir}' to '{dev_dir}'.")

    def _resolve_active_worktree_root(
        self, *, cwd: Path | None = None, raise_on_error: bool = False
    ) -> Path | None:
        if not self.worktree:
            return self.dev_dir.resolve() if self.dev_dir is not None else None

        from lamindb_setup.errors import WorktreePathError

        dev_dir = self.dev_dir
        if dev_dir is None:
            if raise_on_error:
                raise WorktreePathError(
                    "worktree mode requires a configured dev-dir. "
                    "Run: lamin settings dev-dir set <path>"
                )
            return None

        root = dev_dir.resolve()
        location = (cwd or Path.cwd()).resolve()
        if not location.is_relative_to(root) or location == root:
            if raise_on_error:
                raise WorktreePathError(
                    "worktree mode is enabled: run this command inside a child "
                    "directory under the configured dev-dir."
                )
            return None

        rel = location.relative_to(root)
        return root / rel.parts[0]

    @property
    def effective_dev_dir(self) -> Path | None:
        """Root directory used for relative transform/script key derivation.

        This is needed because in worktree mode `dev_dir` is only a parent container.
        The effective key root must be the active child workspace so branch-local runs
        produce stable, isolated keys. Returns `dev_dir` in normal mode; in worktree
        mode returns the active child root and raises `WorktreePathError` if the current
        directory is not inside a valid child workspace.
        """
        return self._resolve_active_worktree_root(raise_on_error=True)

    @property
    def _branch_path(self) -> Path:
        if self.worktree:
            worktree_root = self._resolve_active_worktree_root(raise_on_error=False)
            if worktree_root is not None:
                return local_current_branch_file(worktree_root)
        if self.dev_dir is not None:
            return local_current_branch_file(self.dev_dir.resolve())
        return (
            settings_dir
            / f"current-branch--{self.instance.owner}--{self.instance.name}.txt"
        )

    @property
    def _legacy_branch_path(self) -> Path:
        return (
            settings_dir
            / f"{get_settings_file_name_prefix()}current-branch--{self.instance.owner}--{self.instance.name}.txt"
        )

    def _read_branch_idlike_name(self) -> tuple[int | str, str]:
        idlike: str | int = 1
        name: str = "main"
        try:
            branch_path = self._branch_path
        except SystemExit:  # in case no instance setup
            return idlike, name
        if branch_path.exists():
            idlike, name = branch_path.read_text().split("\n")
        elif self.dev_dir is not None and self._legacy_branch_path.exists():
            # Backward compat for sessions that only wrote branch state globally.
            idlike, name = self._legacy_branch_path.read_text().split("\n")
        return idlike, name

    @property
    # TODO: refactor so that it returns a BranchMock object
    # and we never need a DB request
    def branch(self) -> Branch:
        """Default branch."""
        # this is needed for .filter() with non-default connections
        if not self.is_configured:
            return MainBranchMock()

        if self._branch is None:
            from lamindb import Branch
            from lamindb.errors import DoesNotExist

            idlike, _ = self._read_branch_idlike_name()
            try:
                self._branch = Branch.get(idlike)
            except DoesNotExist:
                # The local branch marker can become stale if the referenced
                # branch was deleted. Fall back to `main` and refresh marker.
                branch_record = Branch.filter(name="main").one()
                self._branch_path.parent.mkdir(parents=True, exist_ok=True)
                self._branch_path.write_text(
                    f"{branch_record.uid}\n{branch_record.name}"
                )
                self._branch = branch_record
        return self._branch

    @branch.setter
    def branch(self, value: str | Branch) -> None:
        from lamindb import Branch, Q
        from lamindb.errors import DoesNotExist

        if isinstance(value, Branch):
            assert value._state.adding is False, "Branch must be saved"
            branch_record = value
        else:
            branch_record = Branch.filter(Q(name=value) | Q(uid=value)).one_or_none()
            if branch_record is None:
                raise DoesNotExist(
                    f"Branch '{value}', please check on the hub UI whether you have the correct `uid` or `name`."
                )
        # we are sure that the current instance is setup because
        # it will error on lamindb import otherwise
        self._branch_path.parent.mkdir(parents=True, exist_ok=True)
        self._branch_path.write_text(f"{branch_record.uid}\n{branch_record.name}")
        self._branch = branch_record

    @property
    def _space_path(self) -> Path:
        return (
            settings_dir
            / f"current-space--{self.instance.owner}--{self.instance.name}.txt"
        )

    def _read_space_idlike_name(self) -> tuple[int | str, str]:
        idlike: str | int = 1
        name: str = "all"
        try:
            space_path = self._space_path
        except SystemExit:  # in case no instance setup
            return idlike, name
        if space_path.exists():
            idlike, name = space_path.read_text().split("\n")
        return idlike, name

    @property
    # TODO: refactor so that it returns a BranchMock object
    # and we never need a DB request
    def space(self) -> Space:
        """Default space."""
        if self._space is None:
            from lamindb import Space

            idlike, _ = self._read_space_idlike_name()
            self._space = Space.get(idlike)
        return self._space

    @space.setter
    def space(self, value: str | Space) -> None:
        from lamindb import Q, Space
        from lamindb.errors import DoesNotExist

        if isinstance(value, Space):
            assert value._state.adding is False, "Space must be saved"
            space_record = value
        else:
            space_record = Space.filter(Q(name=value) | Q(uid=value)).one_or_none()
            if space_record is None:
                raise DoesNotExist(
                    f"Space '{value}', please check on the hub UI whether you have the correct `uid` or `name`."
                )
        # we are sure that the current instance is setup because
        # it will error on lamindb import otherwise
        self._space_path.write_text(f"{space_record.uid}\n{space_record.name}")
        self._space = space_record

    @property
    def is_connected(self) -> bool:
        """Determine whether the current instance is fully connected and ready to use.

        If `True`, the current instance is connected, meaning that the db and other settings
        are properly configured for use.
        """
        from . import django

        return self.is_configured and django.IS_SETUP

    @property
    def is_configured(self) -> bool:
        """Whether an instance is configured in this environment.

        `True` means the current instance is a real instance (not `none/none`).
        """
        return self.instance.slug != "none/none"

    @property
    def private_django_api(self) -> bool:
        """Turn internal Django API private to clean up the API (default `False`).

        This patches your local pip-installed django installation.
        You can undo the patch by setting this back to `False`.
        """
        return self._private_django_api_path.exists()

    @private_django_api.setter
    def private_django_api(self, value: bool) -> None:
        from ._private_django_api import private_django_api

        # we don't want to call private_django_api() twice
        if value and not self.private_django_api:
            private_django_api()
            self._private_django_api_path.touch()
        elif not value and self.private_django_api:
            private_django_api(reverse=True)
            self._private_django_api_path.unlink(missing_ok=True)

    @property
    def user(self) -> UserSettings:
        """Settings of current user."""
        env_changed = (
            self._user_settings_env is not None
            and self._user_settings_env != get_env_name()
        )
        if self._user_settings is None or env_changed:
            # only uses LAMIN_API_KEY if there is no current_user.env
            self._user_settings = load_or_create_user_settings(
                api_key=os.environ.get("LAMIN_API_KEY")
            )
            self._user_settings_env = get_env_name()
            if self._user_settings and self._user_settings.uid is None:
                raise RuntimeError("Need to login, first: lamin login")
        return self._user_settings  # type: ignore

    @property
    def instance(self) -> InstanceSettings:
        """Settings of current LaminDB instance."""
        env_changed = (
            self._instance_settings_env is not None
            and self._instance_settings_env != get_env_name()
        )
        if self._instance_settings is None or env_changed:
            self._instance_settings = load_instance_settings()
            self._instance_settings_env = get_env_name()
        return self._instance_settings  # type: ignore

    @property
    def storage(self) -> StorageSettings:
        """Settings of default storage."""
        return self.instance.storage

    @property
    @deprecated("is_configured")
    def _instance_exists(self):
        return self.is_configured

    @property
    def cache_dir(self) -> UPath:
        """Cache root, a local directory to cache cloud files."""
        from .upath import UPath

        if "LAMIN_CACHE_DIR" in os.environ:
            cache_dir = UPath(os.environ["LAMIN_CACHE_DIR"])
            if not cache_dir.is_absolute():
                raise ValueError("LAMIN_CACHE_DIR must be a valid absolute path.")
        elif self._cache_dir is None:
            cache_path = load_cache_path_from_settings()
            cache_dir = _process_cache_path(cache_path)
            if cache_dir is None:
                cache_dir = _default_cache_dir()
            self._cache_dir = cache_dir
        else:
            cache_dir = self._cache_dir
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
        # we don not want this to error
        # beause no actual writing happens on just getting the cache dir
        # in cloud_to_local_no_update for example
        # so it should not fail on read-only systems
        except Exception as e:
            logger.warning(
                f"Failed to create lamin cache directory at {cache_dir}: {e}"
            )
        return cache_dir

    @property
    def paths(self) -> type[SetupPaths]:
        """Convert cloud paths to lamindb local paths.

        Use `settings.paths.cloud_to_local_no_update` or `settings.paths.cloud_to_local`.
        """
        return SetupPaths

    @property
    def _db_token_manager(self) -> DBTokenManager:
        from lamindb_setup.core.django import db_token_manager

        return db_token_manager

    def _get_db_token(self, connection_name: str = "default") -> DBToken | None:
        return self._db_token_manager.tokens.get(connection_name, None)

    def _debug_db_access(self):
        """Debug database access problems."""
        instance = self.instance
        db_permissions = instance._db_permissions
        print("db connection: ", instance.db)
        print("db permissions: ", db_permissions)
        if db_permissions != "jwt":
            return
        # sets the token if not present yet
        print("available spaces: ", instance.available_spaces)

        tokens = self._db_token_manager.tokens
        if tokens:
            for conn, token in tokens.items():
                token_encoded = token._token
                if token_encoded is None:
                    token._refresh_token()
                    token_encoded = token._token
                token_decoded = jwt.decode(
                    token_encoded, options={"verify_signature": False}
                )
                print(
                    f"db token for the connection '{conn}' is '{token_encoded}': {token_decoded}"
                )
        else:
            print("no db tokens are present")

    def __repr__(self) -> str:
        """Rich string representation."""
        from lamin_utils import colors

        # do not show current setting representation when building docs
        if "sphinx" in sys.modules:
            return object.__repr__(self)

        repr = ""
        if self.is_configured:
            instance_rep = self.instance.__repr__().split("\n")
            _, branch_name = self._read_branch_idlike_name()
            repr += f"{colors.cyan('Instance:')} {instance_rep[0].replace('Instance: ', '')}\n"
            repr += f" - branch: {branch_name}\n"
            repr += f" - space: {self._read_space_idlike_name()[1]}\n"
            repr += f" - dev-dir: {self.dev_dir}"
            repr += f"\n - worktree: {self.worktree}"
            repr += f"\n{colors.yellow('Details:')}\n"
            repr += "\n".join(instance_rep[1:])
        else:
            repr += f"{colors.cyan('Instance:')} None"
        modules_schema_str = ",".join(sorted(self.modules))
        modules_display = modules_schema_str if modules_schema_str != "" else '""'
        repr += f"\n{colors.blue('Settings:')}\n"
        repr += f" - modules: {modules_display}\n"
        repr += f" - cache: {self.cache_dir.as_posix()}\n"
        repr += f" - user settings: {settings_dir.as_posix()}\n"
        repr += f" - system settings: {system_settings_dir.as_posix()}"
        repr += f"\n{colors.green('User:')} {self.user.handle}"
        return repr


class SetupPaths:
    """A static class for conversion of cloud paths to lamindb local paths."""

    @staticmethod
    def cloud_to_local_no_update(
        filepath: AnyPathStr, cache_key: str | None = None
    ) -> UPath:
        """Local (or local cache) filepath from filepath without synchronization."""
        from .upath import LocalPathClasses, UPath

        if not isinstance(filepath, UPath):
            filepath = UPath(filepath)
        # cache_key is ignored if filepath is a local path
        if not isinstance(filepath, LocalPathClasses):
            # settings is defined further in this file
            if cache_key is None:
                local_key = filepath.path  # type: ignore
                protocol = filepath.protocol  # type: ignore
                if protocol in {"http", "https"}:
                    local_key = local_key.removeprefix(protocol + "://")
            else:
                local_key = cache_key
            cache_dir = settings.cache_dir
            local_filepath = (cache_dir / local_key).resolve()
            # a key containing ".." or an absolute key would otherwise
            # resolve to a path outside the cache directory
            if not local_filepath.is_relative_to(cache_dir):
                raise ValueError(
                    f"cache key {local_key} resolves outside the cache directory."
                )
        else:
            local_filepath = filepath
        return local_filepath

    @staticmethod
    def cloud_to_local(
        filepath: AnyPathStr, cache_key: str | None = None, **kwargs
    ) -> UPath:
        """Local (or local cache) filepath from filepath."""
        from .upath import LocalPathClasses, UPath

        if not isinstance(filepath, UPath):
            filepath = UPath(filepath)
        # cache_key is ignored in cloud_to_local_no_update if filepath is local
        local_filepath = SetupPaths.cloud_to_local_no_update(filepath, cache_key)
        if not isinstance(filepath, LocalPathClasses):
            local_filepath.parent.mkdir(parents=True, exist_ok=True)
            filepath.synchronize_to(local_filepath, **kwargs)  # type: ignore
        return local_filepath


def get_env_name():
    if "LAMIN_ENV" in os.environ:
        return os.environ["LAMIN_ENV"]
    else:
        return "prod"


settings = SetupSettings()
