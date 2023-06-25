from typing import Optional
from unittest.mock import patch

import boto3
import pytest
from moto import mock_s3

from lamindb_setup.dev._hub_core import add_storage

S3_REGION = "us-east-1"


def mock_validate_storage_root_arg(storage_root: str) -> None:
    return None


def mock_get_storage_region(storage_root: str) -> Optional[str]:
    return "us-east-1"


@pytest.fixture(scope="session")
@mock_s3
def s3_bucket_1(instance_name_1):
    conn = boto3.resource("s3", region_name=S3_REGION)
    response = conn.create_bucket(Bucket=instance_name_1)
    return response


def test_add_storage(instance_name_1, user_account_1, auth_1, s3_bucket_1):
    with patch(
        "lamindb_setup.dev._hub_utils.validate_storage_root_arg",
        new=mock_validate_storage_root_arg,
    ):
        with patch(
            "lamindb_setup.dev._hub_utils.get_storage_region",
            new=mock_get_storage_region,
        ):
            storage_id, message = add_storage(
                root=f"s3://{instance_name_1}",
                account_handle=user_account_1["handle"],
                _access_token=auth_1["access_token"],
            )
            print("STORAGE MESSAGE", message)
            assert storage_id
            assert message is None


@pytest.mark.skip("This test assumes we have AWS credentials set")
def test_add_storage_with_non_existing_bucket(auth_1):
    non_existing_storage_root = "s3://non_existing_storage_root"

    storage_id, message = add_storage(
        root=non_existing_storage_root,
        account_handle=auth_1["handle"],
        _access_token=auth_1["access_token"],
    )
    assert storage_id is None
    assert message == "bucket-does-not-exists"
