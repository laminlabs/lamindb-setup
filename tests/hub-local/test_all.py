from __future__ import annotations

import os
import subprocess
from unittest.mock import patch
from uuid import UUID

import lamindb_setup as ln_setup
import pytest
from lamindb_setup.core._hub_client import (
    Environment,
    connect_hub,
)
from lamindb_setup.core._hub_core import (
    _connect_instance_hub,
    connect_instance_hub,
    init_storage_hub,
    select_storage_or_parent,
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
from lamindb_setup.core._hub_utils import LaminDsn
from lamindb_setup.core._settings_storage import init_storage as init_storage_base
from lamindb_setup.core._settings_store import instance_settings_file
from laminhub_rest.core.instance_collaborator import InstanceCollaboratorHandler
from laminhub_rest.core.organization import OrganizationMemberHandler
from sqlalchemy.exc import OperationalError as SQLAlchemy_OperationalError
from supafunc.errors import FunctionsHttpError


def test_runs_locally():
    assert os.environ["LAMIN_ENV"] == "local"
    assert Environment().lamin_env == "local"


def test_incomplete_signup():
    email = "testuser-incomplete-signup@gmail.com"
    response = sign_up_local_hub(email)
    assert isinstance(response, tuple) and len(response) == 3
    response = sign_in_hub(email, response[0])
    assert response == "complete-signup"


def test_connection_string_decomp(create_myinstance, create_testadmin1_session):
    client, _ = create_testadmin1_session
    assert create_myinstance["db_scheme"] == "postgresql"
    assert create_myinstance["db_host"] == "fakeserver.xyz"
    assert create_myinstance["db_port"] == 5432
    assert create_myinstance["db_database"] == "mydb"
    db_collaborator = select_collaborator(
        instance_id=create_myinstance["id"],
        account_id=ln_setup.settings.user._uuid.hex,
        fine_grained_access=True,
        client=client,
    )
    assert db_collaborator["role"] == "admin"


def test_db_user(
    create_myinstance, create_testadmin1_session, create_testreader1_session
):
    admin_client, admin_settings = create_testadmin1_session

    instance_id_hex = create_myinstance["id"]
    instance_id = UUID(instance_id_hex)

    # check non-fine-grained access db user
    db_user = select_db_user_by_instance(
        instance_id=instance_id_hex, client=admin_client, fine_grained_access=False
    )
    assert db_user["db_user_name"] == "postgres"
    assert db_user["db_user_password"] == "pwd"
    assert db_user["name"] == "write"
    # check fine-grained access db user
    db_user = select_db_user_by_instance(
        instance_id=instance_id_hex, client=admin_client, fine_grained_access=True
    )
    assert db_user["name"] == "postgres"
    assert db_user["password"] == "pwd"
    assert db_user["type"] == "jwt"
    reader_client, reader_settings = create_testreader1_session
    db_user = select_db_user_by_instance(
        instance_id=instance_id_hex,
        fine_grained_access=True,
        client=reader_client,
    )
    assert db_user is None
    # check that testreader1 is not yet a collaborator
    db_collaborator = select_collaborator(
        instance_id=instance_id_hex,
        account_id=reader_settings._uuid.hex,
        fine_grained_access=True,
        client=admin_client,
    )
    assert db_collaborator is None
    # now add testreader1 as a collaborator
    OrganizationMemberHandler(admin_client).add(
        organization_id=UUID(create_myinstance["account_id"]),
        account_id=reader_settings._uuid,
        role="member",
    )
    try:
        InstanceCollaboratorHandler(admin_client).add(
            account_id=reader_settings._uuid,
            instance_id=instance_id,
            role="read",
        )
    except SQLAlchemy_OperationalError:
        # the function above tries to write to the db
        # but the db for this instance is dummy
        # the insertion in the supabase table succeeds anyways
        pass
    # check that this was successful and can be read by the reader
    db_collaborator = select_collaborator(
        instance_id=instance_id_hex,
        account_id=reader_settings._uuid.hex,
        fine_grained_access=True,
        client=reader_client,
    )
    assert db_collaborator["role"] == "read"
    assert db_collaborator["instance_id"] == instance_id_hex
    assert UUID(db_collaborator["account_id"]) == reader_settings._uuid
    # reader is a collaborator now, can see the jwt db user
    db_user = select_db_user_by_instance(
        instance_id=instance_id_hex,
        fine_grained_access=True,
        client=reader_client,
    )
    assert db_user["name"] == "postgres"
    assert db_user["password"] == "pwd"
    assert db_user["type"] == "jwt"
    # now set the public db_user
    insert_db_user(
        name="public",
        db_user_name="dbreader",
        db_user_password="1234",
        instance_id=instance_id,
        fine_grained_access=True,
        client=admin_client,
    )
    # admon and reader both still get the jwt db user
    db_user = select_db_user_by_instance(
        instance_id=instance_id_hex,
        fine_grained_access=True,
        client=reader_client,
    )
    assert db_user["name"] == "postgres"
    assert db_user["password"] == "pwd"
    assert db_user["type"] == "jwt"
    db_user = select_db_user_by_instance(
        instance_id=instance_id_hex,
        fine_grained_access=True,
        client=admin_client,
    )
    assert db_user["name"] == "postgres"
    assert db_user["password"] == "pwd"
    assert db_user["type"] == "jwt"


# This tests lamindb_setup.core._hub_core.connect_instance_hub
# This functions makes a request to execute get-instance-settings-v1 edge function
# see how to make a request without using supabase in hub-cloud tests, in test_edge_request.py
def test_connect_instance_hub(create_myinstance, create_testadmin1_session):
    admin_client, _ = create_testadmin1_session

    owner, name = ln_setup.settings.user.handle, create_myinstance["name"]
    instance, storage = connect_instance_hub(owner=owner, name=name)
    assert instance["name"] == name
    assert instance["owner"] == owner
    assert instance["api_url"] == "http://localhost:8000"
    assert (
        instance["db_permissions"] == "write"
    )  # the instance has fine_grained_access=False
    assert storage["root"] == "s3://lamindb-ci/myinstance"
    assert "schema_id" in instance
    assert "lnid" in instance
    # switch the instance to fine-grained access
    update_instance(
        instance_id=create_myinstance["id"],
        instance_fields={"fine_grained_access": True},
        client=admin_client,
    )
    instance, _ = connect_instance_hub(owner=owner, name=name)
    assert instance["db_permissions"] == "jwt"

    db_user = select_db_user_by_instance(
        instance_id=create_myinstance["id"],
        fine_grained_access=True,
        client=admin_client,
    )
    expected_dsn = LaminDsn.build(
        scheme=create_myinstance["db_scheme"],
        user=db_user["name"],
        password=db_user["password"],
        host=create_myinstance["db_host"],
        port=str(create_myinstance["db_port"]),
        database=create_myinstance["db_database"],
    )
    assert instance["name"] == create_myinstance["name"]
    assert instance["db"] == expected_dsn

    # test anon access to public instance
    update_instance(
        instance_id=create_myinstance["id"],
        instance_fields={"public": True},
        client=admin_client,
    )

    anon_client = connect_hub()
    instance, _ = _connect_instance_hub(
        owner=owner,
        name=name,
        use_root_db_user=False,
        use_proxy_db=False,
        client=anon_client,
    )
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
    # make realisticly looking token that passes
    # supafunc is_valid_jwt but is actually not a real token
    invalid_token = "header1.payload1.signature1"
    ln_setup.settings.user.access_token = invalid_token
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
    ln_setup.settings.user.access_token = invalid_token
    ln_setup.settings.user.password = correct_password
    connect_instance_hub(
        owner="testadmin1",
        name=create_myinstance["name"],
    )
    # check access_token renewal
    access_token = ln_setup.settings.user.access_token
    assert access_token != invalid_token
    # check that the access_token was written to the settings
    ln_setup.settings._user_settings = None
    assert ln_setup.settings.user.access_token == access_token


def test_init_storage_with_non_existing_bucket(
    create_myinstance, create_testadmin1_session
):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as error:
        init_storage_hub(
            ssettings=init_storage_base(
                root="s3://non_existing_storage_root",
                instance_id=UUID(create_myinstance["id"]),
                instance_slug=f"testadmin1/{create_myinstance['id']}",
                init_instance=True,
                register_hub=True,
            )[0]
        )
    assert error.exconly().endswith("Not Found")


def test_init_storage_incorrect_protocol(create_myinstance):
    with pytest.raises(ValueError) as error:
        init_storage_base(
            root="incorrect-protocol://some-path/some-path-level",
            instance_id=create_myinstance["id"],
            instance_slug=f"testadmin1/{create_myinstance['id']}",
            init_instance=True,
            register_hub=True,
        )
    assert "Protocol incorrect-protocol is not supported" in error.exconly()


def test_select_storage_or_parent(create_myinstance):
    # check not exisitng
    assert select_storage_or_parent("s3://does-not-exist") is None

    root = "s3://lamindb-ci/myinstance"

    result = select_storage_or_parent(root)
    assert result["root"] == root
    # check with a child path and anonymous user
    with patch.object(ln_setup.settings.user, "handle", new="anonymous"):
        result = select_storage_or_parent(root + "/subfolder")
    assert result["root"] == root


def test_fine_grained_access(
    create_testadmin1_session, create_instance_fine_grained_access
):
    admin_client, testadmin1 = create_testadmin1_session
    instance = create_instance_fine_grained_access
    # check api_url is set up correctly through the new tables
    instance_record = select_instance_by_name(
        account_id=testadmin1._uuid,
        name=instance.name,
        client=admin_client,
    )
    assert instance_record["resource_db_server_id"] is not None

    isettings_file = instance_settings_file(
        instance.name, ln_setup.settings.user.handle
    )
    # the file is written by create_instance_fine_grained_access
    # has fine_grained_access=False because create_instance_fine_grained_access
    # updates the instance after init without updating the settings file
    assert isettings_file.exists()
    # need to delete it, because otheriwse
    # isettings.is_remote evaluates to False
    # and ln_setup.connect doesn't make a hub request
    # thus fine_grained_access stays False
    isettings_file.unlink()
    # run from a script because test_update_schema_in_hub.py has ln_setup.init
    # which fails if we connect here
    result = subprocess.run(
        "python ./tests/hub-local/scripts/script-connect-fine-grained-access.py",
        shell=True,
        capture_output=True,
    )
    print(result.stdout.decode())
    print(result.stderr.decode())
    assert result.returncode == 0
