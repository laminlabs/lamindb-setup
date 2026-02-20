from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from lamin_utils import logger
from upath import UPath

if TYPE_CHECKING:
    from aiobotocore.session import AioSession

HOSTED_REGIONS = [
    "eu-central-1",
    "eu-west-2",
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
]
lamin_env = os.getenv("LAMIN_ENV")
if lamin_env is None or lamin_env == "prod":
    HOSTED_BUCKETS = tuple([f"s3://lamin-{region}" for region in HOSTED_REGIONS])
else:
    logger.warning(f"loaded LAMIN_ENV: {lamin_env}")
    HOSTED_BUCKETS = ("s3://lamin-hosted-test",)  # type: ignore


def _keep_trailing_slash(path_str: str) -> str:
    return path_str if path_str[-1] == "/" else path_str + "/"


FALLBACK_CREDENTIALS_LIFETIME = timedelta(hours=12)


# set anon=True for these buckets if credentials fail for a public bucket to be expanded
PUBLIC_BUCKETS: tuple[str, ...] = ("cellxgene-data-public", "bionty-assets")


# s3-comaptible endpoints managed by lamin
# None means the standard aws s3 endpoint
LAMIN_ENDPOINTS: tuple[str | None] = (None,)


class NoTracebackFilter(logging.Filter):
    def filter(self, record):
        record.exc_info = None  # Remove traceback info from the log record.
        return True


class AWSOptionsManager:
    # suppress giant traceback logs from aiobotocore when failing to refresh sso etc
    @staticmethod
    def _suppress_aiobotocore_traceback_logging():
        logger = logging.getLogger("aiobotocore.credentials")
        logger.addFilter(NoTracebackFilter())

    def __init__(self):
        self._sessions_cache: dict[str, AioSession | None] = {}
        self._parameters_cache = {}  # this is not refreshed

        from aiobotocore.session import AioSession
        from packaging import version as packaging_version

        # takes 100ms to import, so keep it here to avoid delaying the import of the main module
        from s3fs import S3FileSystem
        from s3fs import __version__ as s3fs_version

        if packaging_version.parse(s3fs_version) < packaging_version.parse("2023.12.2"):
            raise RuntimeError(
                f"The version of s3fs you have ({s3fs_version}) is impompatible "
                "with lamindb, please upgrade it: pip install s3fs>=2023.12.2"
            )

        anon_env = os.getenv("LAMIN_S3_ANON") == "true"
        # this is cached so will be resued with the connection initialized
        # these options are set for paths in _path_inject_options
        # here we set the same options to cache the filesystem
        fs = S3FileSystem(
            cache_regions=True,
            use_listings_cache=True,
            version_aware=False,
            config_kwargs={"max_pool_connections": 64},
            anon=anon_env,
        )

        self._suppress_aiobotocore_traceback_logging()

        if anon_env:
            self.anon: bool = True
            logger.warning(
                "`anon` mode will be used for all non-managed buckets "
                "because the environment variable LAMIN_S3_ANON was set to 'true'"
            )
        else:
            try:
                fs.connect()
                self.anon = fs.session._credentials is None
            except Exception as e:
                logger.warning(
                    f"There is a problem with your default AWS Credentials: {e}\n"
                    "`anon` mode will be used for all non-managed buckets"
                )
                self.anon = True

        self.anon_public: bool | None = None
        if not self.anon:
            try:
                # use lamindata public bucket for this test
                fs.call_s3("head_bucket", Bucket="lamindata")
                self.anon_public = False
            except Exception:
                self.anon_public = True

        empty_session = AioSession(profile="lamindb_empty_profile")
        empty_session.full_config["profiles"]["lamindb_empty_profile"] = {}
        # this is set downstream to avoid using local configs when we provide credentials
        # or when we set anon=True
        self.empty_session = empty_session

    def _find_root(self, path_str: str) -> str | None:
        if path_str in self._sessions_cache:
            return path_str
        for root in sorted(self._sessions_cache, key=len, reverse=True):
            if path_str.startswith(root):
                return root
        return None

    @staticmethod
    def _make_refresh_callback(storage_root: str, access_token: str | None = None):
        """Create a credential refresh callable for AioRefreshableCredentials."""

        def _refresh():
            from ._hub_core import access_aws

            storage_root_info = access_aws(storage_root, access_token=access_token)
            creds = storage_root_info["credentials"]
            if not creds:
                raise RuntimeError(f"Failed to refresh credentials for {storage_root}")
            expiry_time = creds.get("expiry_time")
            if expiry_time is None:
                expiry_time = (
                    datetime.now(timezone.utc) + FALLBACK_CREDENTIALS_LIFETIME
                ).isoformat()
            return {
                "access_key": creds["key"],
                "secret_key": creds["secret"],
                "token": creds["token"],
                "expiry_time": expiry_time,
            }

        return _refresh

    def _create_managed_session(
        self,
        storage_root: str,
        credentials: dict,
        access_token: str | None = None,
    ) -> AioSession:
        """Create an AioSession with AioRefreshableCredentials for a managed root."""
        from aiobotocore.credentials import AioRefreshableCredentials
        from aiobotocore.session import AioSession

        expiry_time = credentials.get("expiry_time")
        if expiry_time is None:
            expiry_time = (
                datetime.now(timezone.utc) + FALLBACK_CREDENTIALS_LIFETIME
            ).isoformat()

        metadata = {
            "access_key": credentials["key"],
            "secret_key": credentials["secret"],
            "token": credentials["token"],
            "expiry_time": expiry_time,
        }

        refresh_callback = self._make_refresh_callback(storage_root, access_token)
        refreshable_credentials = AioRefreshableCredentials.create_from_metadata(
            metadata,
            refresh_using=refresh_callback,
            method="lamin-hub",
        )

        session = AioSession(profile="lamindb_empty_profile")
        session.full_config["profiles"]["lamindb_empty_profile"] = {}
        session._credentials = refreshable_credentials

        return session

    def _path_inject_options(
        self,
        path: UPath,
        managed_session: AioSession | None = None,
        extra_parameters: dict | None = None,
    ) -> UPath:
        connection_options: dict[str, Any] = {}
        storage_options = path.storage_options
        if managed_session is None:
            # otherwise credentials were specified manually for the path
            if "anon" not in storage_options and (
                path.fs.key is None or path.fs.secret is None
            ):
                anon = self.anon
                if not anon and self.anon_public and path.drive in PUBLIC_BUCKETS:
                    anon = True
                if anon:
                    connection_options["anon"] = anon
                    connection_options["session"] = self.empty_session
        else:
            connection_options["session"] = managed_session

        if "cache_regions" in storage_options:
            connection_options["cache_regions"] = storage_options["cache_regions"]
        else:
            connection_options["cache_regions"] = (
                storage_options.get("endpoint_url", None) is None
            )
        # we use cache to avoid some uneeded downloads or credential problems
        # see in upload_from
        connection_options["use_listings_cache"] = storage_options.get(
            "use_listings_cache", True
        )
        # normally we want to ignore objects vsrsions in a versioned bucket
        connection_options["version_aware"] = storage_options.get(
            "version_aware", False
        )
        # this is for better concurrency as the default batch_size is 128
        # read https://github.com/laminlabs/lamindb-setup/pull/1146
        if "config_kwargs" not in storage_options:
            connection_options["config_kwargs"] = {"max_pool_connections": 64}
        elif "max_pool_connections" not in (
            config_kwargs := storage_options["config_kwargs"]
        ):
            config_kwargs["max_pool_connections"] = 64
            connection_options["config_kwargs"] = config_kwargs

        if extra_parameters:
            connection_options.update(extra_parameters)

        return UPath(path, **connection_options)

    def enrich_path(self, path: UPath, access_token: str | None = None) -> UPath:
        # ignore paths with non-lamin-managed endpoints
        if (
            endpoint_url := path.storage_options.get("endpoint_url", None)
        ) not in LAMIN_ENDPOINTS:
            if "r2.cloudflarestorage.com" in endpoint_url:
                # fixed_upload_size should always be True for R2
                # this option is needed for correct uploads to R2
                # TODO: maybe set max_pool_connections=64 here also
                path = UPath(path, fixed_upload_size=True)
            return path
        # trailing slash is needed to avoid returning incorrect results with .startswith
        # for example s3://lamindata-eu should not receive cache for s3://lamindata
        path_str = _keep_trailing_slash(path.as_posix())
        root = self._find_root(path_str)

        need_fetch = root is None or access_token is not None

        if need_fetch:
            from ._hub_core import access_aws

            storage_root_info = access_aws(path_str, access_token=access_token)
            accessibility = storage_root_info["accessibility"]
            is_managed = accessibility.get("is_managed", False)
            if is_managed:
                credentials = storage_root_info["credentials"]
                extra_parameters = accessibility["extra_parameters"]
            else:
                credentials = {}
                extra_parameters = None

            resolved_root = root
            if access_token is None:
                if "storage_root" in accessibility:
                    resolved_root = accessibility["storage_root"]
                # just to be safe
                resolved_root = None if resolved_root == "" else resolved_root
                if resolved_root is None:
                    # heuristic
                    # do not write the first level for the known hosted buckets
                    if path_str.startswith(HOSTED_BUCKETS):
                        resolved_root = "/".join(path.path.rstrip("/").split("/")[:2])
                    else:
                        # write the bucket for everything else
                        resolved_root = path.drive
                    resolved_root = "s3://" + resolved_root

                resolved_root = _keep_trailing_slash(resolved_root)
                assert isinstance(resolved_root, str)

            if is_managed:
                managed_session = self._create_managed_session(
                    resolved_root or path_str,
                    credentials,
                    access_token,
                )
            else:
                managed_session = None

            if access_token is None:
                assert resolved_root is not None
                self._sessions_cache[resolved_root] = managed_session
                self._parameters_cache[resolved_root] = extra_parameters
        else:
            assert root is not None
            managed_session = self._sessions_cache[root]
            extra_parameters = self._parameters_cache.get(root)

        return self._path_inject_options(path, managed_session, extra_parameters)


_aws_options_manager_dict: dict[str, AWSOptionsManager] = {}


def get_user_aws_options_manager() -> AWSOptionsManager:
    from lamindb_setup import settings

    user_handle = settings.user.handle

    global _aws_options_manager_dict
    if user_handle not in _aws_options_manager_dict:
        _aws_options_manager_dict[user_handle] = AWSOptionsManager()

    return _aws_options_manager_dict[user_handle]


def reset_user_aws_options_cache():
    from lamindb_setup import settings

    # to avoid triggering user reload
    user_handle = getattr(settings._user_settings, "handle", None)

    if user_handle is not None:
        _aws_options_manager_dict.pop(user_handle, None)
