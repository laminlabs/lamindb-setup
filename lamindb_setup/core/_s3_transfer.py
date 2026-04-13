from typing import TYPE_CHECKING

from ._hub_core import access_aws_transfer
from .types import UPathStr

if TYPE_CHECKING:
    from aiobotocore.session import AioSession  # noqa
    from s3fs import S3FileSystem  # noqa


def _normalize_s3_transfer_path(path: UPathStr) -> str:
    path_str = str(path).rstrip("/")
    assert path_str.startswith("s3://")
    return path_str


def _canonical_transfer_pair(
    source_path: UPathStr, target_path: UPathStr
) -> tuple[str, str]:
    source_path_str = _normalize_s3_transfer_path(source_path)
    target_path_str = _normalize_s3_transfer_path(target_path)
    # the order is not important so we sort them to avoid duplicate cache entries
    if source_path_str <= target_path_str:
        return source_path_str, target_path_str
    return target_path_str, source_path_str


class S3TransferOptionsManager:
    def __init__(self) -> None:
        self._sessions_cache: dict[tuple[str, str], AioSession | None] = {}

    @staticmethod
    def _make_refresh_callback(
        source_path: str, target_path: str, access_token: str | None = None
    ):
        def _refresh():
            transfer_info = access_aws_transfer(
                source_path, target_path, access_token=access_token
            )
            credentials = transfer_info["credentials"]
            if not credentials:
                raise RuntimeError(
                    f"Failed to refresh transfer credentials for {source_path} and {target_path}"
                )
            return {
                "access_key": credentials["key"],
                "secret_key": credentials["secret"],
                "token": credentials["token"],
                "expiry_time": credentials["expiry_time"],
            }

        return _refresh

    def _create_managed_session(
        self,
        source_path: str,
        target_path: str,
        credentials: dict,
        access_token: str | None = None,
    ) -> AioSession:
        from aiobotocore.credentials import AioRefreshableCredentials
        from aiobotocore.session import AioSession

        metadata = {
            "access_key": credentials["key"],
            "secret_key": credentials["secret"],
            "token": credentials["token"],
            "expiry_time": credentials["expiry_time"],
        }

        refresh_callback = self._make_refresh_callback(
            source_path, target_path, access_token
        )
        refreshable_credentials = AioRefreshableCredentials.create_from_metadata(
            metadata,
            refresh_using=refresh_callback,
            method="lamin-hub",
        )

        session = AioSession(profile="lamindb_empty_profile")
        session.full_config["profiles"]["lamindb_empty_profile"] = {}
        session._credentials = refreshable_credentials

        return session

    def transfer_session(
        self,
        source_path: UPathStr,
        target_path: UPathStr,
        access_token: str | None = None,
    ) -> AioSession | None:
        canonical_source, canonical_target = _canonical_transfer_pair(
            source_path, target_path
        )
        cache_key = (canonical_source, canonical_target)

        if access_token is None and cache_key in self._sessions_cache:
            return self._sessions_cache[cache_key]

        transfer_info = access_aws_transfer(
            canonical_source, canonical_target, access_token=access_token
        )
        credentials = transfer_info["credentials"]
        assert isinstance(credentials, dict)

        if credentials:
            managed_session = self._create_managed_session(
                canonical_source, canonical_target, credentials, access_token
            )
        else:
            managed_session = None

        if access_token is None:
            self._sessions_cache[cache_key] = managed_session

        return managed_session


_s3_transfer_manager_dict: dict[str, S3TransferOptionsManager] = {}


def get_user_s3_transfer_manager() -> S3TransferOptionsManager:
    from lamindb_setup import settings

    user_handle = settings.user.handle

    global _s3_transfer_manager_dict
    if user_handle not in _s3_transfer_manager_dict:
        _s3_transfer_manager_dict[user_handle] = S3TransferOptionsManager()
    return _s3_transfer_manager_dict[user_handle]


def s3_transfer_fs(
    source_path: UPathStr, target_path: UPathStr, access_token: str | None = None
) -> S3FileSystem:
    from s3fs import S3FileSystem

    manager = get_user_s3_transfer_manager()
    managed_session = manager.transfer_session(source_path, target_path, access_token)
    s3fs_kwargs = {"session": managed_session} if managed_session is not None else {}
    return S3FileSystem(**s3fs_kwargs)
