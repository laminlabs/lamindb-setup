from __future__ import annotations

import logging
import os
import time

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
    HOSTED_BUCKETS = ("s3://lamin-hosted-test",)  # type: ignore


def _keep_trailing_slash(path_str: str):
    return path_str if path_str[-1] == "/" else path_str + "/"


AWS_CREDENTIALS_EXPIRATION: int = 11 * 60 * 60  # refresh credentials after 11 hours


# set anon=True for these buckets if credentials fail for a public bucket
# to be expanded
PUBLIC_BUCKETS: tuple[str] = ("cellxgene-data-public",)


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

        from s3fs import S3FileSystem

        # this is cached so will be resued with the connection initialized
        fs = S3FileSystem(
            cache_regions=True, use_listings_cache=True, version_aware=False
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
            except Exception as e:
                self.anon_public = isinstance(e, PermissionError)

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

    def _path_inject_options(self, path: UPath, credentials: dict) -> UPath:
        if credentials == {}:
            # credentials were specified manually for the path
            if "anon" in path.storage_options:
                anon = path.storage_options["anon"]
            elif path.fs.key is not None and path.fs.secret is not None:
                anon = False
            else:
                anon = self.anon
                if not anon and self.anon_public and path.drive in PUBLIC_BUCKETS:
                    anon = True
            connection_options = {"anon": anon}
        else:
            connection_options = credentials

        if "cache_regions" in path.storage_options:
            connection_options["cache_regions"] = path.storage_options["cache_regions"]
        else:
            connection_options["cache_regions"] = (
                path.storage_options.get("endpoint_url", None) is None
            )
        # we use cache to avoid some uneeded downloads or credential problems
        # see in upload_from
        connection_options["use_listings_cache"] = path.storage_options.get(
            "use_listings_cache", True
        )
        # normally we want to ignore objects vsrsions in a versioned bucket
        connection_options["version_aware"] = path.storage_options.get(
            "version_aware", False
        )

        return UPath(path, **connection_options)

    def enrich_path(self, path: UPath, access_token: str | None = None) -> UPath:
        # ignore paths with non-lamin-managed endpoints
        if (
            endpoint_url := path.storage_options.get("endpoint_url", None)
        ) not in LAMIN_ENDPOINTS:
            if "r2.cloudflarestorage.com" in endpoint_url:
                # fixed_upload_size should always be True for R2
                # this option is needed for correct uploads to R2
                path = UPath(path, fixed_upload_size=True)
            return path
        # trailing slash is needed to avoid returning incorrect results
        # with .startswith
        # for example s3://lamindata-eu should not receive cache for s3://lamindata
        path_str = _keep_trailing_slash(path.as_posix())
        root = self._find_root(path_str)

        if root is not None:
            set_cache = False
            credentials = self._get_cached_credentials(root)

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

            if settings.user.handle != "anonymous" or access_token is not None:
                storage_root_info = access_aws(path_str, access_token=access_token)
            else:
                storage_root_info = {"credentials": {}, "accessibility": {}}

            accessibility = storage_root_info["accessibility"]
            is_managed = accessibility.get("is_managed", False)
            if is_managed:
                credentials = storage_root_info["credentials"]
            else:
                credentials = {}

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
                self._set_cached_credentials(_keep_trailing_slash(root), credentials)

        return self._path_inject_options(path, credentials)


_aws_options_manager: AWSOptionsManager | None = None


def get_aws_options_manager() -> AWSOptionsManager:
    global _aws_options_manager

    if _aws_options_manager is None:
        _aws_options_manager = AWSOptionsManager()

    return _aws_options_manager
