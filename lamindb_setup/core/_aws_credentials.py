from __future__ import annotations

import os
import time

from upath.implementations.cloud import S3Path

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


AWS_CREDENTIALS_EXPIRATION = 11 * 60 * 60  # refresh credentials after 11 hours


# set anon=True for these buckets if credentials fail for a public bucket
# to be expanded
PUBLIC_BUCKETS = ("cellxgene-data-public",)


class AWSCredentialsManager:
    def __init__(self):
        self._credentials_cache = {}

        from s3fs import S3FileSystem

        # this is cached so will be resued with the connection initialized
        fs = S3FileSystem(cache_regions=True)
        fs.connect()
        self.anon: bool = fs.session._credentials is None
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

    def _path_inject_options(self, path: S3Path, credentials: dict) -> S3Path:
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
            cache_regions = path.storage_options["cache_regions"]
        else:
            cache_regions = True

        return S3Path(path, cache_regions=cache_regions, **connection_options)

    def enrich_path(self, path: S3Path, access_token: str | None = None) -> S3Path:
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


_aws_credentials_manager: AWSCredentialsManager | None = None


def get_aws_credentials_manager() -> AWSCredentialsManager:
    global _aws_credentials_manager

    if _aws_credentials_manager is None:
        _aws_credentials_manager = AWSCredentialsManager()

    return _aws_credentials_manager
