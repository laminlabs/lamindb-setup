from typing import TYPE_CHECKING

from s3fs import S3FileSystem

from ._hub_core import access_aws_transfer


def s3_transfer_fs(
    source_path: str, target_path: str, access_token: str | None = None
) -> S3FileSystem:
    credentials = access_aws_transfer(source_path, target_path, access_token)[
        "credentials"
    ]
    assert isinstance(credentials, dict)

    if credentials:
        s3fs_kwargs = {
            "key": credentials["key"],
            "secret": credentials["secret"],
            "token": credentials["token"],
        }
    else:
        s3fs_kwargs = {}

    return S3FileSystem(**s3fs_kwargs)
