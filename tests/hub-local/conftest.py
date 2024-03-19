import os

from lamin_utils import logger

from laminhub_rest.dev._setup_local_hub import setup_local_hub
from laminhub_rest.test.fixtures.s3_bucket import TestS3Bucket
from time import sleep
import boto3
import pytest
from laminhub_rest.utils._id import base62
from laminhub_rest.orm._instance import sb_delete_instance
from laminhub_rest.orm._storage import sb_delete_storage
from laminhub_rest.test.utils.instance import create_test_instance_in_hub
from laminhub_rest.test.utils.storage import create_test_storage_in_hub
from laminhub_rest.test.utils.access_aws import get_upath_from_access_token
from laminhub_rest.test.utils.user import TestUser
from laminhub_rest.utils._supabase_client import SbClientAdmin
from lamindb_setup.core._settings_storage import IS_INITIALIZED_KEY

pytest_plugins = [
    "laminhub_rest.test.fixtures.user",
    "laminhub_rest.test.fixtures.run_id",
]

local_setup_state = setup_local_hub()


def pytest_configure():
    if os.environ["LAMIN_ENV"] == "local":
        local_setup_state.__enter__()
    else:
        logger.warning("you're running non-local tests")


def pytest_unconfigure():
    if os.environ["LAMIN_ENV"] == "local":
        local_setup_state.__exit__(None, None, None)


@pytest.fixture(scope="session")
def hosted_s3_bucket():
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = f"lamin-hosted-test-{base62(6)}".lower()
    s3_client.create_bucket(Bucket=bucket_name)
    sleep(2)  # needed because only then permissions can be issued

    yield TestS3Bucket(bucket_name, f"s3://{bucket_name}")

    bucket_objects = s3_client.list_objects_v2(Bucket=bucket_name)
    if "Contents" in bucket_objects:
        for obj in bucket_objects["Contents"]:
            s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
    s3_client.delete_bucket(Bucket=bucket_name)


@pytest.fixture(scope="function")
def test_hosted_instance_hub_only(
    user_account_1: TestUser, hosted_s3_bucket: TestS3Bucket
):
    storage = create_test_storage_in_hub(
        user_account_1.access_token, root=hosted_s3_bucket.root, public=False
    )
    instance = create_test_instance_in_hub(
        storage_id=storage["id"], access_token=user_account_1.access_token, public=False
    )
    # leveraging s3fs to list objects in a subdirectory leads to permission errors
    # if the bucket is empty (even if the appropriate s3:ListBucket permission
    # is granted)
    root_upath = get_upath_from_access_token(
        user_account_1.access_token, storage["root"]
    )
    touch_file_upath = root_upath / IS_INITIALIZED_KEY
    touch_file_upath.touch()

    yield instance
    with SbClientAdmin().connect() as client:
        sb_delete_instance(instance.id.hex, client)
        sb_delete_storage(storage["id"], client)
