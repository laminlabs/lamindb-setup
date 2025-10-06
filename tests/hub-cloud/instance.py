import os
from uuid import UUID

import pytest
from laminhub_rest._config import get_default_db_server_name
from laminhub_rest.core._managed_s3_bucket import BucketPolicyHandler
from laminhub_rest.core.instance import InstanceHandler, _InstanceCreationHandler
from laminhub_rest.core.storage import StorageHandler
from laminhub_rest.test.account import TestAccount
from laminhub_rest.utils._id import base62
from supabase import Client

pytest_plugins = [
    "laminhub_rest.test.account",
]


def create_instance(name: str, client: Client, *, connect: bool = False):
    TEST_INSTANCE_SCHEMA_STR = os.environ["LAMIN_TEST_INSTANCE_SCHEMA_STR"]
    TEST_INSTANCE_PUBLIC = os.environ["LAMIN_TEST_INSTANCE_PUBLIC"] == "true"

    if TEST_INSTANCE_PUBLIC:
        name += "__public"

    instance_handler = InstanceHandler(client)
    instance_id, root_db_url = instance_handler.create(
        db_server_name=get_default_db_server_name(),
        name=name,
        schema_str=TEST_INSTANCE_SCHEMA_STR,
        register=os.environ["LAMIN_ENV"] == "local",
        public=TEST_INSTANCE_PUBLIC,
    )
    instance_handler._update_record(
        instance_id,
        # We use the same db_server_id across all environments for tests
        db_server_id=UUID("e36c7069-2129-4c78-b2c6-323e2354b741"),
    )
    StorageHandler(client)._update_records(
        instance_handler.get_storage(instance_id).id,
        public=TEST_INSTANCE_PUBLIC,
        # Make as if the storage was in the AWS prod account
        aws_account_id=767398070972,
    )

    bucket_name = _InstanceCreationHandler(client)._get_default_bucket_root()
    BucketPolicyHandler(client)._get_or_create_record(
        bucket_name,
        extra_parameters={"s3_additional_kwargs": {"ServerSideEncryption": "AES256"}},
    )

    if connect:
        import lamindb as ln

        ln.connect(instance_handler.get(instance_id).slug)
        ln.settings.creation.search_names = False
        ln.settings.track_run_inputs = False

    return instance_handler.get(instance_id)


@pytest.fixture(scope="session")
def instance(run_id: str, account_lnci: TestAccount):
    TEST_INSTANCE_SCHEMA_STR = os.environ["LAMIN_TEST_INSTANCE_SCHEMA_STR"]
    name = f"{run_id}__{'_'.join(TEST_INSTANCE_SCHEMA_STR.split(','))}"
    client = account_lnci.client
    assert client is not None
    instance = create_instance(name, client, connect=True)
    yield instance
    InstanceHandler(client).delete(instance.id)


@pytest.fixture(scope="function")
def instance_hub_only(run_id: str, new_account_1: TestAccount, client_admin: Client):
    name = f"{run_id}__hub_only"
    yield from _create_hub_only_instance(
        new_account_1, client_admin, name, public=False
    )


@pytest.fixture(scope="function")
def instance_hub_only_public(
    run_id: str, new_account_1: TestAccount, client_admin: Client
):
    name = f"{run_id}__hub_only__public"
    yield from _create_hub_only_instance(new_account_1, client_admin, name, public=True)


def _create_hub_only_instance(
    account: TestAccount, client_admin: Client, name: str, *, public: bool
):
    client = account.client
    storage = StorageHandler(client)._create_record(
        root=f"s3://{base62(8)}",
        region="us-east-1",
        created_by_id=account.account.id,
        aws_account_id=586130067823,
        public=True,
    )
    instance = InstanceHandler(client)._create_record(
        account.account.id,
        name,
        public=public,
    )
    StorageHandler(client)._update_records(
        storage.id.hex, instance_id=instance.id, is_default=True
    )
    yield instance

    from lamindb_setup.core._hub_core import _delete_instance

    _delete_instance(instance.id.hex, require_empty=False, client=client)
    StorageHandler(client_admin)._delete_records(storage.id.hex)
