import pytest

from lamindb_setup.dev._hub_core import init_instance, load_instance
from lamindb_setup.dev._hub_crud import sb_select_instance_by_name


def db_name(test_instance_name):
    return f"postgresql://postgres:pwd@fakeserver.xyz:5432/{test_instance_name}"


@pytest.fixture(scope="session")
def s3_bucket_1():
    return "lndb-setup-ci"


@pytest.fixture(scope="session")
def instance_1(auth_1, instance_name_1, user_account_1, account_hub_1, s3_bucket_1):
    init_instance(
        owner=auth_1["handle"],
        name=instance_name_1,
        storage=f"s3://{s3_bucket_1}",
        db=db_name(instance_name_1),
        _access_token=auth_1["access_token"],
    )
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
    assert loaded_instance["name"] == instance_1["name"]
    assert loaded_instance["db"]
