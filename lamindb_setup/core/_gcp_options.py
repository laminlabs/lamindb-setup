from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from upath import UPath

_HubGcsCredentialsCls: type | None = None


def _normalize_gs_root(path_str: str) -> str:
    """Return a `gs://` root with a trailing slash.

    Hub storage roots are stored as `gs://`, so `gcs://` has to be rewritten
    before the prefix comparison in `get_storage_api_info_from_path`.
    """
    if path_str.startswith("gcs://"):
        path_str = "gs://" + path_str.removeprefix("gcs://")
    return path_str if path_str.endswith("/") else path_str + "/"


def _expiry_to_naive_utc(expiry: str | datetime) -> datetime:
    """Return a naive UTC expiry.

    `google.auth` compares expiry with a naive UTC clock. Aware values are
    converted; `Z` is accepted on Python 3.10, whose `fromisoformat` rejects it.
    """
    if isinstance(expiry, datetime):
        parsed = expiry
    else:
        parsed = datetime.fromisoformat(str(expiry).replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _hub_gcs_credentials_class() -> type:
    """Build a refreshable `google.auth` credentials class once google-auth is importable."""
    global _HubGcsCredentialsCls
    if _HubGcsCredentialsCls is not None:
        return _HubGcsCredentialsCls

    try:
        from google.auth.credentials import Credentials
        from google.auth.exceptions import RefreshError
    except ImportError as error:
        raise RuntimeError(
            "Google Cloud credentials require google-auth. "
            "Install it with the gcp extra: pip install 'lamindb-setup[gcp]'"
        ) from error

    class HubGcsCredentials(Credentials):
        """Cloud Storage token minted by laminhub.

        gcsfs calls `refresh` when the token is close to expiry. Refresh asks
        laminhub for a new downscoped token instead of using a Google refresh token.
        """

        def __init__(
            self,
            token: str,
            expiry: datetime,
            refresh_handler: Callable[[], tuple[str, datetime]],
        ):
            super().__init__()
            self.token = token
            self.expiry = expiry
            self._refresh_handler = refresh_handler
            self._lock = threading.Lock()

        def refresh(self, request):
            del request
            with self._lock:
                try:
                    token, expiry = self._refresh_handler()
                except Exception as error:
                    raise RefreshError(
                        "Failed to refresh Google Cloud Storage credentials"
                    ) from error
                if not token:
                    raise RefreshError(
                        "Failed to refresh Google Cloud Storage credentials"
                    )
                self.token = token
                self.expiry = expiry

    _HubGcsCredentialsCls = HubGcsCredentials
    return HubGcsCredentials


class GCPOptionsManager:
    def __init__(self):
        # None means the root is not a managed Cloud Storage location
        self._credentials_cache: dict[str, Any] = {}
        self._parameters_cache: dict[str, dict | None] = {}

    def _find_root(self, path_str: str) -> str | None:
        if path_str in self._credentials_cache:
            return path_str
        for root in sorted(self._credentials_cache, key=len, reverse=True):
            if path_str.startswith(root):
                return root
        return None

    def _create_managed_credentials(
        self,
        storage_root: str,
        credentials: dict,
        access_token: str | None = None,
    ):
        """Create a refreshable token for a managed root."""
        credentials_cls = _hub_gcs_credentials_class()

        def _refresh() -> tuple[str, datetime]:
            from ._hub_core import access_gcp

            storage_root_info = access_gcp(storage_root, access_token=access_token)
            refreshed = storage_root_info["credentials"]
            token = refreshed.get("token")
            expiry_time = refreshed.get("expiry_time")
            if not token or not expiry_time:
                raise RuntimeError(f"Failed to refresh credentials for {storage_root}")
            return token, _expiry_to_naive_utc(expiry_time)

        return credentials_cls(
            credentials["token"],
            _expiry_to_naive_utc(credentials["expiry_time"]),
            _refresh,
        )

    def _path_inject_options(
        self,
        path: UPath,
        managed_credentials=None,
        extra_parameters: dict | None = None,
    ) -> UPath:
        from upath import UPath

        connection_options: dict = {}
        storage_options = path.storage_options
        # a caller-supplied token is left in place for non-managed paths;
        # managed credentials replace it, as the AWS session does
        if managed_credentials is not None:
            connection_options["token"] = managed_credentials

        if "use_listings_cache" not in storage_options:
            connection_options["use_listings_cache"] = True
        # ignore object versions unless the caller asked for them
        if "version_aware" not in storage_options:
            connection_options["version_aware"] = False

        if extra_parameters:
            connection_options.update(extra_parameters)

        if not connection_options:
            return path
        return UPath(path, **connection_options)

    def enrich_path(self, path: UPath, access_token: str | None = None) -> UPath:
        # an explicit endpoint is not lamin-managed Cloud Storage
        if path.storage_options.get("endpoint_url", None) is not None:
            return path

        path_str = _normalize_gs_root(path.as_posix())

        if access_token is not None:
            root = None
            need_fetch = True
            set_cache = False
        else:
            # trailing slash is needed to avoid returning incorrect results with .startswith
            root = self._find_root(path_str)
            need_fetch = root is None
            set_cache = need_fetch

        if need_fetch:
            from ._hub_core import access_gcp

            storage_root_info = access_gcp(path_str, access_token=access_token)

            accessibility = storage_root_info["accessibility"]
            credentials = storage_root_info["credentials"]

            is_managed = accessibility.get("is_managed", False)
            extra_parameters = accessibility.get("extra_parameters")

            if set_cache:
                resolved_root = accessibility.get("storage_root")
                # just to be safe
                resolved_root = None if resolved_root == "" else resolved_root
                if resolved_root is None:
                    # cache the bucket so a miss is not repeated for every key
                    resolved_root = "gs://" + path.drive
                resolved_root = _normalize_gs_root(resolved_root)
            else:
                # this is not cached, but used only for this specific path
                resolved_root = path_str

            if (
                is_managed
                and credentials.get("token")
                and credentials.get("expiry_time")
            ):
                managed_credentials = self._create_managed_credentials(
                    resolved_root,
                    credentials,
                    access_token,
                )
            else:
                managed_credentials = None

            if set_cache:
                self._credentials_cache[resolved_root] = managed_credentials
                self._parameters_cache[resolved_root] = extra_parameters
        else:
            assert root is not None
            managed_credentials = self._credentials_cache[root]
            extra_parameters = self._parameters_cache.get(root)

        return self._path_inject_options(path, managed_credentials, extra_parameters)


_gcp_options_manager_dict: dict[str, GCPOptionsManager] = {}


def get_user_gcp_options_manager() -> GCPOptionsManager:
    from lamindb_setup import settings

    user_handle = settings.user.handle

    global _gcp_options_manager_dict
    if user_handle not in _gcp_options_manager_dict:
        _gcp_options_manager_dict[user_handle] = GCPOptionsManager()

    return _gcp_options_manager_dict[user_handle]


def reset_user_gcp_options_cache():
    from lamindb_setup import settings

    # to avoid triggering user reload
    user_handle = getattr(settings._user_settings, "handle", None)

    if user_handle is not None:
        _gcp_options_manager_dict.pop(user_handle, None)
