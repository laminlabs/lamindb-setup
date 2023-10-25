from pathlib import Path
from typing import Dict, Optional, Tuple
from uuid import UUID

import pytest

import lamindb_setup as ln_setup
from lamindb_setup.dev._hub_client import connect_hub_with_auth
from lamindb_setup.dev._hub_crud import (
    Client,
    sb_delete_instance,
    sb_select_account_by_handle,
    sb_select_db_user_by_instance,
    sb_select_instance_by_name,
)

pgurl = "postgresql://postgres:pwd@0.0.0.0:5432/pgtest"


@pytest.fixture
def get_hub_client():
    hub = connect_hub_with_auth()
    yield hub
    hub.auth.sign_out()


def get_instance_and_dbuser_from_hub(
    instance_name: str, hub: Client
) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, str]]]:
    assert ln_setup.settings.user.handle == "testuser2"
    account = sb_select_account_by_handle(handle="testuser2", client=hub)
    instance = sb_select_instance_by_name(
        account_id=account["id"],
        name=instance_name,
        client=hub,
    )
    if instance is None:
        return None, None
    db_user = sb_select_db_user_by_instance(instance_id=instance["id"], client=hub)
    return instance, db_user


def test_init_instance_postgres_default_name(get_hub_client):
    hub = get_hub_client
    instance_name = "pgtest"
    instance, _ = get_instance_and_dbuser_from_hub(instance_name, hub)
    # if instance exists, delete it
    if instance is not None:
        sb_delete_instance(instance["id"], hub)
    # now, run init
    ln_setup.init(storage="./mydatapg", db=pgurl, _test=True)
    ln_setup.register()
    # and check
    instance, db_user = get_instance_and_dbuser_from_hub(instance_name, hub)
    # hub checks
    assert db_user is None
    assert instance["name"] == "pgtest"
    assert instance["db_scheme"] == "postgresql"
    assert instance["db_host"] == "0.0.0.0"
    assert instance["db_port"] == 5432
    assert instance["db_database"] == "pgtest"
    # client checks
    assert ln_setup.settings.instance.id == UUID(instance["id"])
    assert ln_setup.settings.instance.name == "pgtest"
    assert not ln_setup.settings.instance.storage.is_cloud
    assert ln_setup.settings.instance.owner == ln_setup.settings.user.handle
    assert ln_setup.settings.instance.dialect == "postgresql"
    assert ln_setup.settings.instance.db == pgurl
    assert (
        ln_setup.settings.instance.storage.root.as_posix()
        == Path("mydatapg").absolute().as_posix()
    )
    sb_delete_instance(instance["id"], hub)
    ln_setup.delete("pgtest", force=True)


def test_init_instance_postgres_custom_name():
    ln_setup.init(storage="./mystorage", name="mydata2", db=pgurl, _test=True)
    assert ln_setup.settings.instance.name == "mydata2"
    assert not ln_setup.settings.instance.storage.is_cloud
    assert ln_setup.settings.instance.owner == ln_setup.settings.user.handle
    assert ln_setup.settings.instance.dialect == "postgresql"
    assert ln_setup.settings.instance.db == pgurl
    assert (
        ln_setup.settings.instance.storage.root.as_posix()
        == Path("mystorage").absolute().as_posix()
    )
    ln_setup.delete("mydata2", force=True)


def test_init_instance_cloud_aws_us():
    ln_setup.init(storage="s3://lndb-setup-ci", _test=True)
    hub = connect_hub_with_auth()
    account = sb_select_account_by_handle(
        handle=ln_setup.settings.instance.owner, client=hub
    )
    instance = sb_select_instance_by_name(
        account_id=account["id"],
        name=ln_setup.settings.instance.name,
        client=hub,
    )
    assert ln_setup.settings.instance.id == UUID(instance["id"])
    assert ln_setup.settings.storage.is_cloud
    assert str(ln_setup.settings.storage.root) == "s3://lndb-setup-ci/"
    assert ln_setup.settings.storage.root_as_str == "s3://lndb-setup-ci"
    assert ln_setup.settings.storage.region == "us-east-1"
    assert (
        str(ln_setup.settings.instance._sqlite_file)
        == "s3://lndb-setup-ci/lndb-setup-ci.lndb"
    )


def test_init_instance_cloud_aws_europe():
    # do the same for an S3 bucket in Europe
    ln_setup.init(
        storage="s3://lndb-setup-ci-eu-central-1",
        name="lndb-setup-ci-europe",
        _test=True,
    )
    assert ln_setup.settings.instance._id is not None
    assert ln_setup.settings.storage.region == "eu-central-1"
    assert ln_setup.settings.instance.name == "lndb-setup-ci-europe"
    assert (
        str(ln_setup.settings.instance._sqlite_file)
        == "s3://lndb-setup-ci-eu-central-1/lndb-setup-ci-europe.lndb"
    )


def test_init_instance_sqlite():
    ln_setup.init(
        storage="./mydatasqlite",
        name="local-sqlite-instance",
        _test=True,
    )
    ln_setup.register()
    hub = connect_hub_with_auth()
    account = sb_select_account_by_handle(
        handle=ln_setup.settings.instance.owner, client=hub
    )
    instance = sb_select_instance_by_name(
        account_id=account["id"],
        name=ln_setup.settings.instance.name,
        client=hub,
    )
    assert ln_setup.settings.instance.id == UUID(instance["id"])
    assert ln_setup.settings.instance.name == "local-sqlite-instance"
    assert not ln_setup.settings.instance.storage.is_cloud
    assert ln_setup.settings.instance.owner == ln_setup.settings.user.handle
    assert ln_setup.settings.instance.dialect == "sqlite"
    ln_setup.delete("local-sqlite-instance", force=True)
    sb_delete_instance(instance["id"], hub)


def test_init_invalid_name():
    with pytest.raises(ValueError) as error:
        ln_setup.init(storage="./invalidname", name="invalid/name")
    assert (
        error.exconly()
        == "ValueError: Invalid instance name: '/' delimiter not allowed."
    )


# def test_db_unique_error():
#     ln_setup.login("testuser2")

#     # postgres


# #     with pytest.raises(RuntimeError):
# #         ln_setup.init(
# #             storage="s3://lndb-setup-ci",
# #             schema="retro, bionty",
# #             db="postgresql://batman:robin@35.222.187.204:5432/retro",
# #         )

# # sqlite
# # this fails because there is already an sqlite with the same name in that bucket
# # hence, the sqlite file would clash

# # with pytest.raises(RuntimeError):
# #     ln_setup.init(storage="s3://lamindb-ci")


# def test_value_error_schema():
#     with pytest.raises(ModuleNotFoundError):
#         ln_setup.init(storage="tmpstorage1", schema="bionty, xyz1")
