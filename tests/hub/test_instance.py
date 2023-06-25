from typing import Optional
from unittest.mock import patch

import boto3
import pytest
from moto import mock_s3

from lamindb_setup.dev._hub_core import init_instance, load_instance
from lamindb_setup.dev._hub_crud import sb_select_instance_by_name

S3_REGION = "us-east-1"


def mock_validate_storage_root_arg(storage_root: str) -> None:
    return None


def mock_get_storage_region(storage_root: str) -> Optional[str]:
    return "us-east-1"


def db_name(test_instance_name):
    return f"postgresql://postgres:pwd@fakeserver.xyz:5432/{test_instance_name}"


@pytest.fixture(scope="session")
@mock_s3
def s3_bucket_1(instance_name_1):
    conn = boto3.resource("s3", region_name=S3_REGION)
    response = conn.create_bucket(Bucket=instance_name_1)
    return response


@pytest.fixture(scope="session")
def instance_1(auth_1, instance_name_1, user_account_1, account_hub_1, s3_bucket_1):
    with patch(
        "lamindb_setup.dev._hub_utils.validate_storage_root_arg",
        new=mock_validate_storage_root_arg,
    ):
        with patch(
            "lamindb_setup.dev._hub_utils.get_storage_region",
            new=mock_get_storage_region,
        ):
            response = init_instance(
                owner=auth_1["handle"],
                name=instance_name_1,
                storage=f"s3://{instance_name_1}",
                db=db_name(instance_name_1),
                _access_token=auth_1["access_token"],
            )
            print("INIT RESPONSE", response)
            instance = sb_select_instance_by_name(
                account_id=user_account_1["id"],
                name=instance_name_1,
                supabase_client=account_hub_1,
            )
            return instance


def test_load_instance(auth_1, instance_1):
    result = load_instance(
        owner=auth_1["handle"],
        name=instance_1["name"],
        _access_token=auth_1["access_token"],
    )
    loaded_instance, _ = result
    assert loaded_instance.name == instance_1["name"]
    assert loaded_instance.db
