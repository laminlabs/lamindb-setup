from __future__ import annotations

import time

from upath.implementations.cloud import S3Path

AWS_CREDENTIALS_EXPIRATION = 7 * 60 * 60  # 7 hours


class AWSCredentialsManager:
    def __init__(self):
        self._credentials_cache = {}

        from s3fs import S3FileSystem

        # this is cached so will be resued with the connection initialized
        fs = S3FileSystem(cache_regions=True)
        fs.connect()
        self.anon = fs.session._credentials is None

    def _find_root(self, path_str: str):
        roots = self._credentials_cache.keys()
        if path_str in roots:
            return path_str
        roots = sorted(roots, key=len, reverse=True)
        for root in roots:
            if path_str.startswith(root):
                return root
        return None

    def _is_active(self, root: str):
        return (
            time.time() - self._credentials_cache[root]["time"]
        ) < AWS_CREDENTIALS_EXPIRATION

    def _set_cached_credentials(self, root: str, credentials: dict):
        if root not in self._credentials_cache:
            self._credentials_cache[root] = {}
        self._credentials_cache[root]["credentials"] = credentials
        self._credentials_cache[root]["time"] = time.time()

    def _get_cached_credentials(self, root: str):
        return self._credentials_cache[root]["credentials"]

    def _path_with_options(self, path: S3Path, credentials: dict) -> S3Path:
        if credentials == {}:
            # credentials were specified manually for the path
            if path.fs.key is not None and path.fs.secret is not None:
                anon = False
            else:
                anon = self.anon
            connection_options = {"anon": anon}
        else:
            connection_options = credentials
        return S3Path(path, cache_regions=True, **connection_options)

    def create_path(self, path: S3Path, access_token: str | None = None) -> S3Path:
        path_str = path.path.rstrip("/")
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
            # also check for the second level if the returned root is a bucket
            # elif "/" not in root and "/" in path_str:
            # set_cache = True
            # root = None
        else:
            set_cache = True

        if set_cache:
            from ._hub_core import access_aws

            if root is None:
                root = "/".join(path_str.split("/")[:2])
            credentials = access_aws(f"s3://{root}", access_token=access_token)
            if access_token is None:
                self._set_cached_credentials(root, credentials)

        return self._path_with_options(path, credentials)


_aws_credentials_manager: AWSCredentialsManager | None = None


def get_aws_credentials_manager() -> AWSCredentialsManager:
    global _aws_credentials_manager

    if _aws_credentials_manager is None:
        _aws_credentials_manager = AWSCredentialsManager()

    return _aws_credentials_manager
