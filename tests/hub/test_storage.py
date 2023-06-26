import pytest

from lamindb_setup.dev._hub_core import add_storage


@pytest.fixture(scope="session")
def s3_bucket_1():
    return "lndb-setup-ci"


def test_add_storage(user_account_1, auth_1, s3_bucket_1):
    storage_id, message = add_storage(
        root=f"s3://{s3_bucket_1}",
        account_handle=user_account_1["handle"],
        _access_token=auth_1["access_token"],
    )

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
