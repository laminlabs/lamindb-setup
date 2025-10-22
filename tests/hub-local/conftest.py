from __future__ import annotations

import os
from uuid import UUID, uuid4

import lamindb_setup as ln_setup
import pytest
from gotrue.errors import AuthApiError
from lamindb_setup.core._hub_client import (
    connect_hub_with_auth,
)
from lamindb_setup.core._hub_core import (
    init_instance_hub,
)
from lamindb_setup.core._hub_crud import (
    insert_db_user,
    select_collaborator,
    select_instance_by_name,
)
from lamindb_setup.core._hub_utils import LaminDsnModel
from lamindb_setup.core._settings_instance import InstanceSettings
from lamindb_setup.core._settings_save import save_user_settings
from lamindb_setup.core._settings_storage import base62
from lamindb_setup.core._settings_storage import init_storage as init_storage_base
from lamindb_setup.core._settings_user import UserSettings
from laminhub_rest.core._central_client import SupabaseClientWrapper
from laminhub_rest.dev import (
    SupabaseResources,
    remove_lamin_local_settings,
    seed_local_test,
)
from laminhub_rest.test.instance import create_instance

supabase_resources = SupabaseResources()


def pytest_configure():
    os.environ["LAMIN_ENV"] = "local"
    os.environ["LAMIN_CLOUD_VERSION"] = "0.1"
    os.environ["LAMIN_TEST_INSTANCE_SCHEMA_STR"] = ""
    os.environ["LAMIN_TEST_INSTANCE_PUBLIC"] = "false"
    os.environ["LAMIN_TEST_INSTANCE_SCHEMA_STR"] = "bionty"
    # Disable redis, it is not deployed here
    os.environ["EXTERNAL_CACHE_DISABLED"] = "true"
    remove_lamin_local_settings()
    supabase_resources.start_local()
    supabase_resources.reset_local()
    supabase_resources.migrate()
    seed_local_test()


def pytest_unconfigure():
    if supabase_resources.edge_function_process:
        supabase_resources.stop_local_edge_functions()


def sign_up_user(email: str, handle: str, save_as_settings: bool = False):
    """Sign up user."""
    from lamindb_setup.core._hub_core import sign_up_local_hub

    result_or_error = sign_up_local_hub(email)
    if result_or_error == "user-exists":  # user already exists
        return "user-exists"
    account_id = UUID(result_or_error[1])
    access_token = result_or_error[2]
    user_settings = UserSettings(
        uid=base62(8),
        handle=handle,
        email=email,
        password=result_or_error[0],
        _uuid=account_id,
        access_token=access_token,
    )
    if save_as_settings:
        save_user_settings(user_settings)
    return user_settings


@pytest.fixture(scope="session")
def create_testadmin1_session():  # -> Tuple[Client, UserSettings]
    email = "testadmin1@gmail.com"
    sign_up_user(email, "testadmin1", save_as_settings=True)
    with pytest.raises(AuthApiError):
        # test error with "User already registered"
        sign_up_user(email, "testadmin1")

    handle = ln_setup.settings.user.handle
    assert handle == "testadmin1"

    account_id = ln_setup.settings.user._uuid.hex
    account = {
        "id": account_id,
        "user_id": account_id,
        "lnid": ln_setup.settings.user.uid,
        "handle": handle,
    }
    # uses ln_setup.settings.user.access_token
    client = connect_hub_with_auth()
    client.table("account").insert(account).execute()
    yield SupabaseClientWrapper(client), ln_setup.settings.user
    client.auth.sign_out()


@pytest.fixture(scope="session")
def create_testreader1_session():  # -> Tuple[Client, UserSettings]
    email = "testreader1@gmail.com"
    user_settings = sign_up_user(email, "testreader1")
    assert user_settings.handle == "testreader1"

    account = {
        "id": user_settings._uuid.hex,
        "user_id": user_settings._uuid.hex,
        "lnid": user_settings.uid,
        "handle": user_settings.handle,
    }
    client = connect_hub_with_auth(access_token=user_settings.access_token)
    client.table("account").insert(account).execute()
    yield SupabaseClientWrapper(client), user_settings
    client.auth.sign_out()


@pytest.fixture(scope="session")
def create_myinstance(create_testadmin1_session):  # -> Dict
    admin_client, usettings = create_testadmin1_session
    instance_id = uuid4()
    db_str = "postgresql://postgres:pwd@fakeserver.xyz:5432/mydb"
    instance_name = "myinstance"
    instance_slug = f"{usettings.handle}/{instance_name}"
    isettings = InstanceSettings(
        id=instance_id,
        owner=usettings.handle,
        name=instance_name,
        db=db_str,
    )
    init_instance_hub(isettings)
    storage = init_storage_base(
        "s3://lamindb-ci/myinstance",
        instance_id=instance_id,
        instance_slug=instance_slug,
        init_instance=True,
        register_hub=True,
    )[0]
    isettings._storage = storage
    # add resource_db_server from seed_local_test
    admin_client.table("instance").update(
        {"resource_db_server_id": "e36c7069-2129-4c78-b2c6-323e2354b741"}
    ).eq("id", instance_id.hex).execute()
    # test loading it
    with pytest.raises(PermissionError) as error:
        ln_setup.connect("testadmin1/myinstance", _test=True)
    assert error.exconly().startswith(
        "PermissionError: No database access, please ask your admin"
    )
    db_collaborator = select_collaborator(
        instance_id=instance_id.hex,
        account_id=ln_setup.settings.user._uuid.hex,
        fine_grained_access=True,
        client=admin_client,
    )
    assert db_collaborator["role"] == "admin"
    db_dsn = LaminDsnModel(db=db_str)
    db_user_name = db_dsn.db.user
    db_user_password = db_dsn.db.password
    # fine-grained access db user
    insert_db_user(
        name="jwt",
        db_user_name=db_user_name,
        db_user_password=db_user_password,
        instance_id=instance_id,
        fine_grained_access=True,
        client=admin_client,
    )
    # non-fine-grained access db user
    insert_db_user(
        name="write",
        db_user_name=db_user_name,
        db_user_password=db_user_password,
        instance_id=instance_id,
        fine_grained_access=False,
        client=admin_client,
    )
    # the instance doesn't have fine_grained_access set to True yet
    instance = select_instance_by_name(
        account_id=ln_setup.settings.user._uuid,
        name="myinstance",
        client=admin_client,
    )
    yield instance
    ln_setup.delete(instance_slug, force=True)


@pytest.fixture(scope="function")
def create_instance_fine_grained_access(create_testadmin1_session):
    client, _ = create_testadmin1_session

    instance = create_instance("instance_test", client=client, connect=False)

    yield instance

    ln_setup.delete(instance.name, force=True)
