from __future__ import annotations

import os
from uuid import UUID, uuid4

import lamindb_setup as ln_setup
import pytest
from gotrue.errors import AuthApiError
from lamindb_setup.core._hub_client import (
    Environment,
    connect_hub,
    connect_hub_with_auth,
)
from lamindb_setup.core._hub_core import (
    _connect_instance_hub,
    connect_instance_hub,
    init_instance,
    init_storage,
    sign_in_hub,
    sign_up_local_hub,
)
from lamindb_setup.core._hub_crud import (
    insert_db_user,
    select_collaborator,
    select_db_user_by_instance,
    select_instance_by_name,
    update_instance,
)

# typing
# from lamindb.dev import UserSettings
# from supabase import Client
from lamindb_setup.core._hub_utils import LaminDsn, LaminDsnModel
from lamindb_setup.core._settings_instance import InstanceSettings
from lamindb_setup.core._settings_save import save_user_settings
from lamindb_setup.core._settings_storage import base62
from lamindb_setup.core._settings_storage import init_storage as init_storage_base
from lamindb_setup.core._settings_user import UserSettings
from laminhub_rest.core.instance.collaborator import InstanceCollaboratorHandler
from postgrest.exceptions import APIError
from supafunc.errors import FunctionsHttpError


def sign_up_user(email: str, handle: str, save_as_settings: bool = False):
    """Sign up user."""
    from lamindb_setup.core._hub_core import sign_up_local_hub

    result_or_error = sign_up_local_hub(email)
    if result_or_error == "user-exists":  # user already exists
        return "user-exists"
    account_id = UUID(result_or_error[1])
    access_token = result_or_error[2]
    user_settings = UserSettings(
        handle=handle,
        email=email,
        password=result_or_error[0],
        _uuid=account_id,
        access_token=access_token,
    )
    if save_as_settings:
        save_user_settings(user_settings)
    return user_settings


def test_runs_locally():
    assert os.environ["LAMIN_ENV"] == "local"
    assert Environment().lamin_env == "local"


def test_incomplete_signup():
    email = "testuser-incomplete-signup@gmail.com"
    response = sign_up_local_hub(email)
    assert isinstance(response, tuple) and len(response) == 3
    response = sign_in_hub(email, response[0])
    assert response == "complete-signup"


@pytest.fixture(scope="session")
def create_testadmin1_session():  # -> Tuple[Client, UserSettings]
    email = "testadmin1@gmail.com"
    sign_up_user(email, "testadmin1", save_as_settings=True)
    with pytest.raises(AuthApiError):
        # test error with "User already registered"
        sign_up_user(email, "testadmin1")
    account_id = ln_setup.settings.user._uuid
    account = {
        "id": account_id.hex,
        "user_id": account_id.hex,
        "lnid": base62(8),
        "handle": "testadmin1",
    }
    # uses ln_setup.settings.user.access_token
    client = connect_hub_with_auth()
    client.table("account").insert(account).execute()
    yield client, ln_setup.settings.user
    client.auth.sign_out()


@pytest.fixture(scope="session")
def create_testreader1_session():  # -> Tuple[Client, UserSettings]
    email = "testreader1@gmail.com"
    user_settings = sign_up_user(email, "testreader1")
    account = {
        "id": user_settings._uuid.hex,
        "user_id": user_settings._uuid.hex,
        "lnid": base62(8),
        "handle": "testreader1",
    }
    client = connect_hub_with_auth(access_token=user_settings.access_token)
    client.table("account").insert(account).execute()
    yield client, user_settings
    client.auth.sign_out()


@pytest.fixture(scope="session")
def create_myinstance(create_testadmin1_session):  # -> Dict
    admin_client, usettings = create_testadmin1_session
    instance_id = uuid4()
    db_str = "postgresql://postgres:pwd@fakeserver.xyz:5432/mydb"
    isettings = InstanceSettings(
        id=instance_id,
        owner=usettings.handle,
        name="myinstance",
        # cannot yet pass instance_id here as it does not yet exist
        storage=init_storage_base(
            "s3://lamindb-ci/myinstance",
        )[0],
        db=db_str,
    )
    init_instance(isettings)
    # test loading it
    with pytest.raises(PermissionError) as error:
        ln_setup.connect("testadmin1/myinstance", _test=True)
    assert error.exconly().startswith(
        "PermissionError: No database access, please ask your admin"
    )
    db_collaborator = select_collaborator(
        instance_id=instance_id.hex,
        account_id=ln_setup.settings.user._uuid.hex,
        client=admin_client,
    )
    assert db_collaborator["role"] == "admin"
    assert db_collaborator["db_user_id"] is None
    db_dsn = LaminDsnModel(db=db_str)
    db_user_name = db_dsn.db.user
    db_user_password = db_dsn.db.password
    insert_db_user(
        name="write",
        db_user_name=db_user_name,
        db_user_password=db_user_password,
        instance_id=instance_id,
        client=admin_client,
    )
    instance = select_instance_by_name(
        account_id=ln_setup.settings.user._uuid,
        name="myinstance",
        client=admin_client,
    )
    yield instance


def test_connection_string_decomp(create_myinstance, create_testadmin1_session):
    client, _ = create_testadmin1_session
    assert create_myinstance["db_scheme"] == "postgresql"
    assert create_myinstance["db_host"] == "fakeserver.xyz"
    assert create_myinstance["db_port"] == 5432
    assert create_myinstance["db_database"] == "mydb"
    db_collaborator = select_collaborator(
        instance_id=create_myinstance["id"],
        account_id=ln_setup.settings.user._uuid.hex,
        client=client,
    )
    assert db_collaborator["role"] == "admin"
    assert db_collaborator["db_user_id"] is None


def test_db_user(
    create_myinstance, create_testadmin1_session, create_testreader1_session
):
    admin_client, admin_settings = create_testadmin1_session
    instance_id = UUID(create_myinstance["id"])
    db_user = select_db_user_by_instance(
        instance_id=instance_id,
        client=admin_client,
    )
    assert db_user["db_user_name"] == "postgres"
    assert db_user["db_user_password"] == "pwd"
    assert db_user["name"] == "write"
    reader_client, reader_settings = create_testreader1_session
    db_user = select_db_user_by_instance(
        instance_id=instance_id,
        client=reader_client,
    )
    assert db_user is None
    # check that testreader1 is not yet a collaborator
    db_collaborator = select_collaborator(
        instance_id=instance_id.hex,
        account_id=reader_settings._uuid.hex,
        client=admin_client,
    )
    assert db_collaborator is None
    # now add testreader1 as a collaborator
    InstanceCollaboratorHandler(admin_client).add(
        account_id=reader_settings._uuid,
        instance_id=instance_id,
        role="read",
        schema_id=None,
        skip_insert_user_table=True,
    )
    # check that this was successful and can be read by the reader
    db_collaborator = select_collaborator(
        instance_id=instance_id.hex,
        account_id=reader_settings._uuid.hex,
        client=reader_client,
    )
    assert db_collaborator["role"] == "read"
    assert UUID(db_collaborator["instance_id"]) == instance_id
    assert UUID(db_collaborator["account_id"]) == reader_settings._uuid
    assert db_collaborator["db_user_id"] is None
    # this alone doesn't set a db_user
    db_user = select_db_user_by_instance(
        instance_id=instance_id,
        client=reader_client,
    )
    assert db_user is None
    # now set the db_user
    insert_db_user(
        name="read",
        db_user_name="dbreader",
        db_user_password="1234",
        instance_id=instance_id,
        client=admin_client,
    )
    # admin can access all db users
    data = (
        admin_client.table("db_user")
        .select("*")
        .eq("instance_id", instance_id)
        .execute()
        .data
    )
    assert len(data) == 2
    # reader can only access the read-level db user
    db_user = select_db_user_by_instance(
        instance_id=instance_id,
        client=reader_client,
    )
    assert db_user["db_user_name"] == "dbreader"
    assert db_user["db_user_password"] == "1234"
    assert db_user["name"] == "read"
    # admin still gets the write-level connection string
    db_user = select_db_user_by_instance(
        instance_id=instance_id,
        client=admin_client,
    )
    assert db_user["db_user_name"] == "postgres"
    assert db_user["db_user_password"] == "pwd"
    assert db_user["name"] == "write"


# This tests lamindb_setup.core._hub_core.connect_instance_hub
# This functions makes a request to execute get-instance-settings-v1 edge function
# see how to make a request without using supabase in hub-cloud tests, in test_edge_request.py
def test_connect_instance_hub(create_myinstance, create_testadmin1_session):
    admin_client, _ = create_testadmin1_session

    owner, name = ln_setup.settings.user.handle, create_myinstance["name"]
    instance, storage = connect_instance_hub(owner=owner, name=name)
    assert instance["name"] == name
    assert instance["owner"] == owner
    assert instance["api_url"] is None
    assert instance["db_permissions"] == "write"
    assert storage["root"] == "s3://lamindb-ci/myinstance"
    assert "schema_id" in instance
    assert "lnid" in instance

    db_user = select_db_user_by_instance(
        instance_id=create_myinstance["id"],
        client=admin_client,
    )
    expected_dsn = LaminDsn.build(
        scheme=create_myinstance["db_scheme"],
        user=db_user["db_user_name"],
        password=db_user["db_user_password"],
        host=create_myinstance["db_host"],
        port=str(create_myinstance["db_port"]),
        database=create_myinstance["db_database"],
    )
    assert instance["name"] == create_myinstance["name"]
    assert instance["db"] == expected_dsn

    # add db_server from seed_local_test
    admin_client.table("instance").update(
        {"db_server_id": "e36c7069-2129-4c78-b2c6-323e2354b741"}
    ).eq("id", instance["id"]).execute()

    instance, _ = connect_instance_hub(owner=owner, name=name)
    assert instance["api_url"] == "http://localhost:8000"

    # test anon access to public instance
    update_instance(
        instance_id=create_myinstance["id"],
        instance_fields={"public": True},
        client=admin_client,
    )

    anon_client = connect_hub()
    instance, _ = _connect_instance_hub(owner=owner, name=name, client=anon_client)
    assert instance["name"] == name
    assert instance["owner"] == owner

    update_instance(
        instance_id=create_myinstance["id"],
        instance_fields={"public": False},
        client=admin_client,
    )
    # test non-existent
    result = connect_instance_hub(owner="user-not-exists", name=name)
    assert result == "account-not-exists"
    result = connect_instance_hub(owner=owner, name="instance-not-exists")
    assert result == "instance-not-found"


def test_connect_instance_hub_corrupted_or_expired_credentials(
    create_myinstance, create_testadmin1_session
):
    # assume token & password are corrupted or expired
    ln_setup.settings.user.access_token = "corrupted_or_expired_token"
    correct_password = ln_setup.settings.user.password
    ln_setup.settings.user.password = "corrupted_password"
    with pytest.raises(FunctionsHttpError):
        connect_instance_hub(
            owner="testadmin1",
            name=create_myinstance["name"],
        )
    # now, let's assume only the token is expired or corrupted
    # re-creating the auth client triggers a re-generated token because it
    # excepts the error assuming the token is expired
    ln_setup.settings.user.access_token = "corrupted_or_expired_token"
    ln_setup.settings.user.password = correct_password
    connect_instance_hub(
        owner="testadmin1",
        name=create_myinstance["name"],
    )
    # check access_token renewal
    access_token = ln_setup.settings.user.access_token
    assert access_token != "corrupted_or_expired_token"
    # check that the access_token was written to the settings
    ln_setup.settings._user_settings = None
    assert ln_setup.settings.user.access_token == access_token


def test_init_storage_with_non_existing_bucket(create_testadmin1_session):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as error:
        init_storage(
            ssettings=init_storage_base(
                "s3://non_existing_storage_root", instance_id=uuid4()
            )[0]
        )
    assert error.exconly().endswith("Not Found")
