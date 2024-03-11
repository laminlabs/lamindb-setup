import os
from uuid import UUID, uuid4
from typing import Optional
import pytest
from supabase import Client
from lamindb_setup.core._hub_utils import LaminDsnModel
from gotrue.errors import AuthApiError
import lamindb_setup as ln_setup
from lamindb_setup.core._settings_instance import InstanceSettings
from lamindb_setup.core._hub_client import (
    Environment,
    connect_hub_with_auth,
    call_with_fallback_auth,
)
from lamindb_setup.core._hub_core import (
    init_storage,
    init_instance,
    connect_instance,
    sign_in_hub,
    sign_up_local_hub,
)
from lamindb_setup.core._hub_crud import (
    select_collaborator,
    select_db_user_by_instance,
    select_instance_by_name,
    update_instance,
)

# typing
# from lamindb.dev import UserSettings
# from supabase import Client
from lamindb_setup.core._hub_utils import LaminDsn
from lamindb_setup.core._settings_storage import base62
from lamindb_setup.core._settings_storage import init_storage as init_storage_base
from lamindb_setup.core._settings_save import save_user_settings
from lamindb_setup.core._settings_user import UserSettings


def insert_db_user(db_user_fields: dict, client: Client):
    try:
        data = client.table("db_user").insert(db_user_fields).execute().data
    except Exception as e:
        if str(e) == str("Expecting value: line 1 column 1 (char 0)"):
            pass
        else:
            raise e
    return data[0]


def update_db_user(db_user_id: str, db_user_fields: dict, client: Client):
    data = (
        client.table("db_user")
        .update(db_user_fields)
        .eq("id", db_user_id)
        .execute()
        .data
    )
    if len(data) == 0:
        return None
    return data[0]


def set_db_user(
    *,
    db: str,
    instance_id: UUID,
) -> None:
    return call_with_fallback_auth(_set_db_user, db=db, instance_id=instance_id)


def _set_db_user(
    *,
    db: str,
    instance_id: UUID,
    client: Client,
) -> None:
    db_dsn = LaminDsnModel(db=db)
    db_user = select_db_user_by_instance(instance_id.hex, client)
    if db_user is None:
        insert_db_user(
            {
                "id": uuid4().hex,
                "instance_id": instance_id.hex,
                "db_user_name": db_dsn.db.user,
                "db_user_password": db_dsn.db.password,
            },
            client,
        )
    else:
        update_db_user(
            db_user["id"],
            {
                "instance_id": instance_id.hex,
                "db_user_name": db_dsn.db.user,
                "db_user_password": db_dsn.db.password,
            },
            client,
        )


def sign_up_user(email: str, handle: str) -> Optional[str]:
    """Sign up user."""
    from lamindb_setup.core._hub_core import sign_up_local_hub

    result_or_error = sign_up_local_hub(email)
    if result_or_error == "user-exists":  # user already exists
        return "user-exists"
    user_settings = UserSettings(
        handle=handle,
        email=email,
        password=result_or_error[0],
        uuid=UUID(result_or_error[1]),
        access_token=result_or_error[2],
    )
    save_user_settings(user_settings)
    return None  # user needs to confirm email now


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
def create_testuser1_session():  # -> Tuple[Client, UserSettings]
    email = "testuser1@gmail.com"
    response = sign_up_user(email, "testuser1")
    assert response is None
    # test repeated sign up
    with pytest.raises(AuthApiError):
        # test error with "User already registered"
        sign_up_user(email, "testuser1")
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
    _, usettings = create_testuser1_session
    instance_id = uuid4()
    isettings = InstanceSettings(
        id=instance_id,
        owner=usettings.handle,
        name="myinstance",
        storage=init_storage_base("s3://lamindb-ci/myinstance"),
        db="postgresql://postgres:pwd@fakeserver.xyz:5432/mydb",
    )
    init_instance(isettings)
    # test loading it
    with pytest.raises(PermissionError) as error:
        ln_setup.connect("testuser1/myinstance", _test=True)
    assert error.exconly().startswith(
        "PermissionError: No database access, please ask your admin"
    )
    set_db_user(
        db="postgresql://postgres:pwd@fakeserver.xyz:5432/mydb", instance_id=instance_id
    )
    client, _ = create_testuser1_session
    instance = select_instance_by_name(
        account_id=ln_setup.settings.user.uuid,
        name="myinstance",
        client=client,
    )
    yield instance


def test_connection_string_decomp(create_myinstance, create_testuser1_session):
    client, _ = create_testuser1_session
    db_user = select_db_user_by_instance(
        instance_id=create_myinstance["id"],
        client=client,
    )
    assert create_myinstance["db_scheme"] == "postgresql"
    assert create_myinstance["db_host"] == "fakeserver.xyz"
    assert create_myinstance["db_port"] == 5432
    assert create_myinstance["db_database"] == "mydb"
    assert db_user["db_user_name"] == "postgres"
    assert db_user["db_user_password"] == "pwd"

    db_collaborator = select_collaborator(
        instance_id=create_myinstance["id"],
        account_id=ln_setup.settings.user.uuid.hex,
        client=client,
    )
    assert db_collaborator["db_user_id"] is None


def test_connect_instance(create_myinstance, create_testuser1_session):
    # trigger return for inexistent handle
    assert "account-not-exists" == connect_instance(
        owner="testusr1",  # testuser1 with a typo
        name=create_myinstance["name"],
    )
    # trigger misspelled name
    assert "instance-not-reachable" == connect_instance(
        owner="testuser1",
        name="inexistent-name",  # inexistent name
    )
    # make instance public so that we can also test connection string
    client, _ = create_testuser1_session
    update_instance(
        instance_id=create_myinstance["id"],
        instance_fields={"public": True},
        client=client,
    )
    # now supply correct data and make instance public
    result = connect_instance(
        owner="testuser1",
        name=create_myinstance["name"],
    )
    db_user = select_db_user_by_instance(
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
    # make instance private again
    update_instance(
        instance_id=create_myinstance["id"],
        instance_fields={"public": False},
        client=client,
    )


def test_connect_instance_corrupted_or_expired_credentials(
    create_myinstance, create_testuser1_session
):
    # assume token & password are corrupted or expired
    ln_setup.settings.user.access_token = "corrupted_or_expired_token"
    correct_password = ln_setup.settings.user.password
    ln_setup.settings.user.password = "corrupted_password"
    with pytest.raises(AuthApiError):
        connect_instance(
            owner="testuser1",
            name=create_myinstance["name"],
        )
    # now, let's assume only the token is expired or corrupted
    # re-creating the auth client triggers a re-generated token because it
    # excepts the error assuming the token is expired
    ln_setup.settings.user.access_token = "corrupted_or_expired_token"
    ln_setup.settings.user.password = correct_password
    connect_instance(
        owner="testuser1",
        name=create_myinstance["name"],
    )


def test_init_storage(create_testuser1_session):
    client, _ = create_testuser1_session
    storage_id = init_storage(ssettings=init_storage_base("s3://lamindb-ci/myinstance"))
    assert isinstance(storage_id, UUID)


def test_init_storage_with_non_existing_bucket(create_testuser1_session):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as error:
        init_storage(ssettings=init_storage_base("s3://non_existing_storage_root"))
    assert error.exconly().endswith("Not Found")
