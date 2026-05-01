from __future__ import annotations

from typing import TYPE_CHECKING

from ._hub_core import access_aws_for_moving

if TYPE_CHECKING:
    from aiobotocore.session import AioSession
    from s3fs import S3FileSystem
    from upath.implementations.cloud import S3Path

    from .types import AnyPathStr


def _normalize_s3_path_for_moving(path: S3Path | str) -> str:
    path_str = str(path).rstrip("/")
    assert path_str.startswith("s3://")
    return path_str


def _canonical_pair_for_moving(
    source_path: S3Path | str, target_path: S3Path | str
) -> tuple[str, str]:
    source_path_str = _normalize_s3_path_for_moving(source_path)
    target_path_str = _normalize_s3_path_for_moving(target_path)
    # the order is not important so we sort them to avoid duplicated cache entries
    if source_path_str <= target_path_str:
        return source_path_str, target_path_str
    return target_path_str, source_path_str


class S3MovingOptionsManager:
    def __init__(self) -> None:
        self._sessions_cache: dict[tuple[str, str], AioSession | None] = {}

    @staticmethod
    def _make_refresh_callback(
        source_path: str, target_path: str, access_token: str | None = None
    ):
        def _refresh():
            info_for_moving = access_aws_for_moving(
                source_path, target_path, access_token=access_token
            )
            credentials = info_for_moving["credentials"]
            if not credentials:
                raise RuntimeError(
                    f"Failed to refresh move credentials for {source_path} and {target_path}"
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

    def session_for_moving(
        self,
        source_path: S3Path | str,
        target_path: S3Path | str,
        access_token: str | None = None,
    ) -> AioSession | None:
        canonical_source, canonical_target = _canonical_pair_for_moving(
            source_path, target_path
        )
        cache_key = (canonical_source, canonical_target)

        if access_token is None and cache_key in self._sessions_cache:
            return self._sessions_cache[cache_key]

        move_info = access_aws_for_moving(
            canonical_source, canonical_target, access_token=access_token
        )
        credentials = move_info["credentials"]
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


_s3_moving_manager_dict: dict[str, S3MovingOptionsManager] = {}


def get_user_s3_moving_manager() -> S3MovingOptionsManager:
    from lamindb_setup import settings

    user_handle = settings.user.handle

    global _s3_moving_manager_dict
    if user_handle not in _s3_moving_manager_dict:
        _s3_moving_manager_dict[user_handle] = S3MovingOptionsManager()
    return _s3_moving_manager_dict[user_handle]


def s3_fs_for_moving(
    source_path: S3Path | str,
    target_path: S3Path | str,
    access_token: str | None = None,
) -> S3FileSystem | None:
    from s3fs import S3FileSystem

    manager = get_user_s3_moving_manager()
    managed_session = manager.session_for_moving(source_path, target_path, access_token)
    if managed_session is None:
        return None
    return S3FileSystem(cache_regions=True, session=managed_session)
