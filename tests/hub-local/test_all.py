import os
from uuid import UUID

import pytest
from gotrue.errors import AuthApiError
import lamindb_setup as ln_setup
from lamindb_setup.dev._hub_client import (
    Environment,
    connect_hub_with_auth,
)
from lamindb_setup.dev._hub_core import (
    add_storage,
    init_instance,
    load_instance,
    sign_in_hub,
    sign_up_hub,
)
from lamindb_setup.dev._hub_crud import (
    sb_select_collaborator,
    sb_select_db_user_by_instance,
    sb_select_instance_by_name,
)

# typing
# from lamindb.dev import UserSettings
# from supabase import Client
from lamindb_setup.dev._hub_utils import LaminDsn, base62


def test_runs_locally():
    assert os.environ["LAMIN_ENV"] == "local"
    assert Environment().lamin_env == "local"


def test_incomplete_signup():
    email = "testuser-incomplete-signup@gmail.com"
    response = sign_up_hub(email)
    assert isinstance(response, tuple) and len(response) == 3
    response = sign_in_hub(email, response[0])
    assert response == "complete-signup"


@pytest.fixture(scope="session")
def create_testuser1_session():  # -> Tuple[Client, UserSettings]
    email = "testuser1@gmail.com"
    response = ln_setup.signup(email)
    assert response is None
    account_id = ln_setup.settings.user.uuid.hex
    account = {
        "id": account_id,
        "user_id": account_id,
        "lnid": base62(8),
        "handle": "testuser1",
    }
    # uses ln_setup.settings.user.access_token
    client = connect_hub_with_auth()
    client.table("account").insert(account).execute()
    yield client, ln_setup.settings.user
    client.auth.sign_out()


@pytest.fixture(scope="session")
def create_myinstance(create_testuser1_session):  # -> Dict
    init_instance(
        name="myinstance",
        storage="s3://lndb-setup-ci",
        db="postgresql://postgres:pwd@fakeserver.xyz:5432/mydb",
    )
    client, _ = create_testuser1_session
    instance = sb_select_instance_by_name(
        account_id=ln_setup.settings.user.uuid,
        name="myinstance",
        client=client,
    )
    yield instance


def test_connection_string_decomp(create_myinstance, create_testuser1_session):
    client, _ = create_testuser1_session
    db_user = sb_select_db_user_by_instance(
        instance_id=create_myinstance["id"],
        client=client,
    )
    assert create_myinstance["db_scheme"] == "postgresql"
    assert create_myinstance["db_host"] == "fakeserver.xyz"
    assert create_myinstance["db_port"] == 5432
    assert create_myinstance["db_database"] == "mydb"
    assert db_user["db_user_name"] == "postgres"
    assert db_user["db_user_password"] == "pwd"

    db_collaborator = sb_select_collaborator(
        instance_id=create_myinstance["id"],
        account_id=ln_setup.settings.user.uuid.hex,
        client=client,
    )
    assert db_collaborator["db_user_id"] == db_user["id"]


def test_load_instance(create_myinstance, create_testuser1_session):
    # trigger return for inexistent handle
    assert "account-not-exists" == load_instance(
        owner="testusr1",  # testuser1 with a typo
        name=create_myinstance["name"],
    )
    # trigger misspelled name
    assert "instance-not-reachable" == load_instance(
        owner="testuser1",
        name="inexistent-name",  # inexistent name
    )
    # now supply correct data
    result = load_instance(
        owner="testuser1",
        name=create_myinstance["name"],
    )
    client, _ = create_testuser1_session
    db_user = sb_select_db_user_by_instance(
        instance_id=create_myinstance["id"],
        client=client,
    )
    expected_dsn = LaminDsn.build(
        scheme=create_myinstance["db_scheme"],
        user=db_user["db_user_name"],
        password=db_user["db_user_password"],
        host=create_myinstance["db_host"],
        port=str(create_myinstance["db_port"]),
        database=create_myinstance["db_database"],
    )
    loaded_instance, _ = result
    assert loaded_instance["name"] == create_myinstance["name"]
    assert loaded_instance["db"] == expected_dsn


def test_load_instance_corrupted_or_expired_credentials(
    create_myinstance, create_testuser1_session
):
    # assume token & password are corrupted or expired
    ln_setup.settings.user.access_token = "corrupted_or_expired_token"
    correct_password = ln_setup.settings.user.password
    ln_setup.settings.user.password = "corrupted_password"
    with pytest.raises(AuthApiError):
        load_instance(
            owner="testuser1",
            name=create_myinstance["name"],
        )
    # now, let's assume only the token is expired or corrupted
    # re-creating the auth client triggers a re-generated token because it
    # excepts the error assuming the token is expired
    ln_setup.settings.user.access_token = "corrupted_or_expired_token"
    ln_setup.settings.user.password = correct_password
    load_instance(
        owner="testuser1",
        name=create_myinstance["name"],
    )


def test_add_storage(create_testuser1_session):
    client, usettings = create_testuser1_session
    storage_id = add_storage(
        root="s3://lndb-setup-ci",
        account_id=usettings.uuid,
        hub=client,
    )
    assert isinstance(storage_id, UUID)


def test_add_storage_with_non_existing_bucket(create_testuser1_session):
    client, usettings = create_testuser1_session
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as error:
        add_storage(
            root="s3://non_existing_storage_root",
            account_id=usettings.uuid,
            hub=client,
        )
    assert error.exconly().endswith("Not Found")
