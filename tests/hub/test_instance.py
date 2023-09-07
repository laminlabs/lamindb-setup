import pytest

import lamindb_setup as ln_setup
from lamindb_setup.dev._hub_client import connect_hub_with_auth
from lamindb_setup.dev._hub_core import init_instance, load_instance
from lamindb_setup.dev._hub_crud import (
    sb_select_collaborator,
    sb_select_db_user_by_instance,
    sb_select_instance_by_name,
)
from lamindb_setup.dev._hub_utils import LaminDsn, base62


def db_name(test_instance_name):
    return f"postgresql://postgres:pwd@fakeserver.xyz:5432/{test_instance_name}"


@pytest.fixture(scope="session")
def s3_bucket_1():
    return "lndb-setup-ci"


@pytest.fixture(scope="session")
def signup_testuser1():
    email = "testuser1@gmail.com"
    ln_setup.signup(email)
    account_id = ln_setup.settings.user.uuid.hex
    account = {
        "id": account_id,
        "user_id": account_id,
        "lnid": base62(8),
        "handle": "testuser1",
    }
    hub = connect_hub_with_auth(access_token=ln_setup.settings.user.access_token)
    hub.table("account").insert(account).execute()
    yield hub
    hub.auth.sign_out()


@pytest.fixture(scope="session")
def instance_1(signup_testuser1, instance_name_1, s3_bucket_1):
    init_instance(
        name=instance_name_1,
        storage=f"s3://{s3_bucket_1}",
        db=db_name(instance_name_1),
    )
    hub = signup_testuser1
    instance = sb_select_instance_by_name(
        account_id=ln_setup.settings.user.uuid,
        name=instance_name_1,
        supabase_client=hub,
    )
    yield instance


def test_connection_string(instance_1, signup_testuser1):
    hub = signup_testuser1
    db_user = sb_select_db_user_by_instance(
        instance_id=instance_1["id"],
        supabase_client=hub,
    )
    assert instance_1["db_scheme"] == "postgresql"
    assert instance_1["db_host"] == "fakeserver.xyz"
    assert instance_1["db_port"] == 5432
    assert instance_1["db_database"] == instance_1["name"]
    assert db_user["db_user_name"] == "postgres"
    assert db_user["db_user_password"] == "pwd"

    db_collaborator = sb_select_collaborator(
        instance_id=instance_1["id"],
        account_id=ln_setup.settings.user.uuid.hex,
        supabase_client=hub,
    )
    assert db_collaborator["db_user_id"] == db_user["id"]


def test_load_instance(instance_1, signup_testuser1):
    result = load_instance(
        owner="testuser1",
        name=instance_1["name"],
        _access_token=ln_setup.settings.user.access_token,
    )
    hub = signup_testuser1
    db_user = sb_select_db_user_by_instance(
        instance_id=instance_1["id"],
        supabase_client=hub,
    )
    expected_dsn = LaminDsn.build(
        scheme=instance_1["db_scheme"],
        user=db_user["db_user_name"],
        password=db_user["db_user_password"],
        host=instance_1["db_host"],
        port=str(instance_1["db_port"]),
        database=instance_1["db_database"],
    )
    loaded_instance, _ = result
    assert loaded_instance["name"] == instance_1["name"]
    assert loaded_instance["db"] == expected_dsn
