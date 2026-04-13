from typing import TYPE_CHECKING

from ._hub_core import access_aws_transfer
from .types import UPathStr

if TYPE_CHECKING:
    from s3fs import S3FileSystem  # noqa


def s3_transfer_fs(
    source_path: UPathStr, target_path: UPathStr, access_token: str | None = None
) -> S3FileSystem:
    source_path = str(source_path)
    target_path = str(target_path)

    assert source_path.startswith("s3://")
    assert target_path.startswith("s3://")

    from s3fs import S3FileSystem

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
