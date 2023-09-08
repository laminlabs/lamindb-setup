from typing import Optional

import pytest
from faker import Faker
from fastapi.testclient import TestClient
from supabase.lib.client_options import ClientOptions

from laminhub_rest.connector import connect_hub, connect_hub_with_auth
from laminhub_rest.core.account import create_user_account
from laminhub_rest.main import app
from laminhub_rest.utils._test import (
    create_test_account,
    create_test_auth,
    create_test_storage,
)

FAKE = Faker()


def mock_validate_storage_root_arg(storage_root: str) -> None:
    return None


def mock_get_storage_region(storage_root: str) -> Optional[str]:
    return "us-east-1"


@pytest.fixture(scope="session")
def client():
    client = TestClient(app)
    return client


@pytest.fixture(scope="session")
def hub():
    client_options = ClientOptions(persist_session=False)
    return connect_hub(client_options)


@pytest.fixture(scope="session")
def auth_1():
    auth_1 = create_test_auth()
    return auth_1


@pytest.fixture(scope="session")
def auth_2():
    auth_2 = create_test_auth()
    return auth_2


@pytest.fixture(scope="session")
def auth_3():
    auth_3 = create_test_auth()
    return auth_3


@pytest.fixture(scope="session")
def auth_4():
    auth_4 = create_test_auth()
    return auth_4


@pytest.fixture(scope="session")
def user_account_1(auth_1):
    user_account = create_test_account(
        handle=auth_1["handle"], access_token=auth_1["access_token"]
    )
    return user_account


@pytest.fixture(scope="session")
def user_account_2(auth_2):
    user_account = create_test_account(
        handle=auth_2["handle"], access_token=auth_2["access_token"]
    )
    return user_account


@pytest.fixture(scope="session")
def user_account_3(auth_3):
    user_account = create_test_account(
        handle=auth_3["handle"], access_token=auth_3["access_token"]
    )
    return user_account


@pytest.fixture(scope="session")
def user_account_4(auth_4):
    user_account = create_test_account(
        handle=auth_4["handle"], access_token=auth_4["access_token"]
    )
    return user_account


@pytest.fixture(scope="session")
def account_hub_1(auth_1):
    hub = connect_hub_with_auth(access_token=auth_1["access_token"])
    yield hub
    hub.auth.sign_out()


@pytest.fixture(scope="session")
def account_hub_2(auth_2):
    hub = connect_hub_with_auth(access_token=auth_2["access_token"])
    yield hub
    hub.auth.sign_out()


@pytest.fixture(scope="session")
def account_hub_3(auth_3):
    hub = connect_hub_with_auth(access_token=auth_3["access_token"])
    yield hub
    hub.auth.sign_out()


@pytest.fixture(scope="session")
def account_hub_4(auth_4):
    hub = connect_hub_with_auth(access_token=auth_4["access_token"])
    yield hub
    hub.auth.sign_out()


@pytest.fixture
def account_to_test_deletion():
    auth = create_test_auth()
    hub = connect_hub_with_auth(access_token=auth["access_token"])
    user_account = create_user_account(
        handle=auth["handle"], _access_token=auth["access_token"]
    )
    yield auth, hub, user_account
    hub.auth.sign_out()


@pytest.fixture(scope="session")
def storage_1(auth_1, user_account_1):
    storage = create_test_storage(auth_1["access_token"])
    return storage


@pytest.fixture(scope="session")
def test_instance_name():
    return FAKE.lexify("????????")


def db_name(test_instance_name):
    return f"postgresql://postgres:pwd@fakeserver.xyz:5432/{test_instance_name}"


@pytest.fixture(scope="session")
def instance_name_1():
    return FAKE.lexify("????????")


@pytest.fixture(scope="session")
def instance_name_2():
    return FAKE.lexify("????????")
