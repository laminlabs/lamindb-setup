from __future__ import annotations

import logging
import os
import time
from typing import Any

from lamin_utils import logger
from upath import UPath

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
    logger.warning("loaded LAMIN_ENV: staging")
    HOSTED_BUCKETS = ("s3://lamin-hosted-test",)  # type: ignore


def _keep_trailing_slash(path_str: str) -> str:
    return path_str if path_str[-1] == "/" else path_str + "/"


AWS_CREDENTIALS_EXPIRATION: int = 11 * 60 * 60  # refresh credentials after 11 hours


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
        self._credentials_cache = {}
        self._parameters_cache = {}  # this is not refreshed

        from aiobotocore.session import AioSession
        from s3fs import S3FileSystem

        # this is cached so will be resued with the connection initialized
        # these options are set for paths in _path_inject_options
        # here we set the same options to cache the filesystem
        fs = S3FileSystem(
            cache_regions=True,
            use_listings_cache=True,
            version_aware=False,
            config_kwargs={"max_pool_connections": 64},
        )

        self._suppress_aiobotocore_traceback_logging()

        try:
            fs.connect()
            self.anon: bool = fs.session._credentials is None
        except Exception as e:
            logger.warning(
                f"There is a problem with your default AWS Credentials: {e}\n"
                "`anon` mode will be used for all non-managed buckets."
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
        roots = self._credentials_cache.keys()
        if path_str in roots:
            return path_str
        roots = sorted(roots, key=len, reverse=True)
        for root in roots:
            if path_str.startswith(root):
                return root
        return None

    def _is_active(self, root: str) -> bool:
        return (
            time.time() - self._credentials_cache[root]["time"]
        ) < AWS_CREDENTIALS_EXPIRATION

    def _set_cached_credentials(self, root: str, credentials: dict):
        if root not in self._credentials_cache:
            self._credentials_cache[root] = {}
        self._credentials_cache[root]["credentials"] = credentials
        self._credentials_cache[root]["time"] = time.time()

    def _get_cached_credentials(self, root: str) -> dict:
        return self._credentials_cache[root]["credentials"]

    def _path_inject_options(
        self, path: UPath, credentials: dict, extra_parameters: dict | None = None
    ) -> UPath:
        connection_options: dict[str, Any] = {}
        storage_options = path.storage_options
        if credentials == {}:
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
            connection_options.update(credentials)
            connection_options["session"] = self.empty_session

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

        if root is not None:
            set_cache = False
            credentials = self._get_cached_credentials(root)
            extra_parameters = self._parameters_cache.get(root)
            if access_token is not None:
                set_cache = True
            elif credentials != {}:
                # update credentials
                if not self._is_active(root):
                    set_cache = True
        else:
            set_cache = True

        if set_cache:
            from ._hub_core import access_aws
            from ._settings import settings

            storage_root_info = access_aws(path_str, access_token=access_token)
            accessibility = storage_root_info["accessibility"]
            is_managed = accessibility.get("is_managed", False)
            if is_managed:
                credentials = storage_root_info["credentials"]
                extra_parameters = accessibility["extra_parameters"]
            else:
                credentials = {}
                extra_parameters = None

            if access_token is None:
                if "storage_root" in accessibility:
                    root = accessibility["storage_root"]
                # just to be safe
                root = None if root == "" else root
                if root is None:
                    # heuristic
                    # do not write the first level for the known hosted buckets
                    if path_str.startswith(HOSTED_BUCKETS):
                        root = "/".join(path.path.rstrip("/").split("/")[:2])
                    else:
                        # write the bucket for everything else
                        root = path.drive
                    root = "s3://" + root

                root = _keep_trailing_slash(root)
                assert isinstance(root, str)
                self._set_cached_credentials(root, credentials)
                self._parameters_cache[root] = extra_parameters

        return self._path_inject_options(path, credentials, extra_parameters)


_aws_options_manager: AWSOptionsManager | None = None


def get_aws_options_manager() -> AWSOptionsManager:
    global _aws_options_manager

    if _aws_options_manager is None:
        _aws_options_manager = AWSOptionsManager()

    return _aws_options_manager


def reset_aws_options_cache():
    global _aws_options_manager

    if _aws_options_manager is not None:
        _aws_options_manager._credentials_cache = {}
        _aws_options_manager._parameters_cache = {}
