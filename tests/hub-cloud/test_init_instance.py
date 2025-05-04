from __future__ import annotations

import os
from pathlib import Path
from uuid import UUID

import lamindb_setup as ln_setup
import pytest
from lamindb_setup._connect_instance import InstanceNotFoundError
from lamindb_setup._init_instance import infer_instance_name
from lamindb_setup.core._hub_client import connect_hub_with_auth
from lamindb_setup.core._hub_core import _connect_instance_hub
from lamindb_setup.core._hub_crud import (
    Client,
    select_account_by_handle,
    select_default_storage_by_instance_id,
    select_instance_by_name,
)

pgurl = "postgresql://postgres:pwd@0.0.0.0:5432/pgtest"


@pytest.fixture
def get_hub_client():
    ln_setup.login("testuser2")
    hub = connect_hub_with_auth()
    yield hub
    hub.auth.sign_out()


def test_infer_instance_name():
    assert infer_instance_name(storage="s3://bucket/key") == "key"
    assert infer_instance_name(storage="s3://bucket/") == "bucket"
    assert infer_instance_name(storage="s3://bucket/", name="name") == "name"
    assert (
        infer_instance_name(
            storage="s3://bucket/key?endpoint_url=http://localhost:8000/s3"
        )
        == "key"
    )
    assert (
        infer_instance_name(
            storage="s3://bucket/?endpoint_url=http://localhost:8000/s3"
        )
        == "bucket"
    )
    assert infer_instance_name(storage="create-s3", name="name") == "name"
    assert infer_instance_name(storage="some/localpath") == "localpath"
    with pytest.raises(ValueError):
        infer_instance_name(storage="create-s3")


def test_init_instance_postgres_default_name(get_hub_client):
    hub = get_hub_client
    instance_name = "pgtest"
    try:
        ln_setup.delete(instance_name, force=True)
    except InstanceNotFoundError:
        pass
    # test init with storage=None
    # this happens when calling CLI lamin init or lamin init --name smth
    with pytest.raises(SystemExit):
        ln_setup.init(storage=None, _test=True)
    with pytest.raises(SystemExit):
        ln_setup.init(storage=None, name="init-to-fail", _test=True)
    # now, run init
    ln_setup.init(storage="./mydatapg", db=pgurl, _test=True)
    assert ln_setup.settings.instance.slug == "testuser2/pgtest"
    ln_setup.register(_test=True)
    assert ln_setup.settings.instance.slug == "testuser2/pgtest"
    # and check
    instance, storage = _connect_instance_hub(
        owner="testuser2", name=instance_name, client=hub
    )
    # hub checks
    assert instance["db"].startswith("postgresql://none:none")
    assert instance["name"] == instance_name
    assert instance["db_scheme"] == "postgresql"
    assert instance["db_host"] == "0.0.0.0"
    assert instance["db_port"] == 5432
    assert instance["db_database"] == "pgtest"
    # client checks
    assert ln_setup.settings.instance._id == UUID(instance["id"])
    assert ln_setup.settings.instance.name == "pgtest"
    assert not ln_setup.settings.instance.storage.type_is_cloud
    assert ln_setup.settings.instance.owner == ln_setup.settings.user.handle
    assert ln_setup.settings.instance.dialect == "postgresql"
    assert ln_setup.settings.instance.db == pgurl
    assert (
        ln_setup.settings.instance.storage.root.as_posix()
        == Path("mydatapg").absolute().as_posix()
    )
    ln_setup.delete(instance_name, force=True)


def test_init_instance_postgres_custom_name():
    ln_setup.init(storage="./mystorage", name="mydata2", db=pgurl, _test=True)
    assert ln_setup.settings.instance.name == "mydata2"
    assert not ln_setup.settings.instance.storage.type_is_cloud
    assert ln_setup.settings.instance.owner == ln_setup.settings.user.handle
    assert ln_setup.settings.instance.dialect == "postgresql"
    assert ln_setup.settings.instance.db == pgurl
    assert (
        ln_setup.settings.instance.storage.root.as_posix()
        == Path("mystorage").absolute().as_posix()
    )
    ln_setup.delete("mydata2", force=True)


def test_init_instance_cwd():
    # can't make it via fixture because need to chnage dir back before ln_setup.delete
    prev_wd = Path.cwd()
    storage = Path("./mystorage_cwd")
    storage.mkdir()
    storage = storage.resolve()
    os.chdir(storage)
    assert Path.cwd() == storage
    ln_setup.init(storage=".", _test=True)
    assert ln_setup.settings.instance.name == "mystorage_cwd"
    assert not ln_setup.settings.instance.storage.type_is_cloud
    assert ln_setup.settings.instance.storage.root.as_posix() == Path.cwd().as_posix()
    os.chdir(prev_wd)
    ln_setup.delete("mystorage_cwd", force=True)


def test_init_instance_cloud_aws_us():
    storage = (
        f"s3://lamindb-ci/{os.environ['LAMIN_ENV']}_test/init_instance_cloud_aws_us"
    )
    ln_setup.init(storage=storage, _test=True)
    # run for the second time
    # just loads an already existing instance
    ln_setup.init(storage=storage, _test=True)
    hub = connect_hub_with_auth()
    account = select_account_by_handle(
        handle=ln_setup.settings.instance.owner, client=hub
    )
    instance = select_instance_by_name(
        account_id=account["id"],
        name=ln_setup.settings.instance.name,
        client=hub,
    )
    # test default storage record is correct
    storage_record = select_default_storage_by_instance_id(instance["id"], hub)
    assert storage_record["root"] == storage
    # test instance settings
    assert ln_setup.settings.instance._id == UUID(instance["id"])
    assert ln_setup.settings.storage.type_is_cloud
    assert str(ln_setup.settings.storage.root) == storage
    assert ln_setup.settings.storage.root_as_str == storage
    assert ln_setup.settings.storage.region == "us-west-1"
    assert (
        str(ln_setup.settings.instance._sqlite_file) == f"{storage}/.lamindb/lamin.db"
    )
    ln_setup.delete("init_instance_cloud_aws_us", force=True)


def test_init_instance_cloud_aws_europe():
    # do the same for an S3 bucket in Europe
    storage = f"s3://lndb-setup-ci-eu-central-1/{os.environ['LAMIN_ENV']}_test"
    ln_setup.init(
        storage=storage,
        name="lamindb-ci-europe",
        _test=True,
    )
    assert ln_setup.settings.instance._id is not None
    assert ln_setup.settings.storage.region == "eu-central-1"
    assert ln_setup.settings.instance.name == "lamindb-ci-europe"
    assert (
        str(ln_setup.settings.instance._sqlite_file) == f"{storage}/.lamindb/lamin.db"
    )
    ln_setup.delete("lamindb-ci-europe", force=True)


def test_init_instance_sqlite():
    user_settings_original = ln_setup.settings.user
    # here we test dynamically switching the user
    # it will lead to an error
    ln_setup.settings._user_settings = ln_setup.core._settings_user.UserSettings(
        handle="my_special_test_user"
    )
    with pytest.raises(ValueError) as error:
        ln_setup.init(
            storage="./mydatasqlite",
            name="local-sqlite-instance",
            _test=True,
        )
    assert (
        "Neither bearer token or basic authentication scheme is provided"
        in error.exconly()
    )
    ln_setup.settings._user_settings = user_settings_original
    ln_setup.init(
        storage="./mydatasqlite",
        name="local-sqlite-instance",
        _test=True,
    )
    assert ln_setup.settings.instance.name == "local-sqlite-instance"
    assert not ln_setup.settings.instance.storage.type_is_cloud
    assert ln_setup.settings.instance.owner == user_settings_original.handle
    assert ln_setup.settings.instance.dialect == "sqlite"
    ln_setup.delete("local-sqlite-instance", force=True)


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
# #             storage="s3://lamindb-ci",
# #             modules="retro, bionty",
# #             db="postgresql://batman:robin@35.222.187.204:5432/retro",
# #         )

# # sqlite
# # this fails because there is already an sqlite with the same name in that bucket
# # hence, the sqlite file would clash

# # with pytest.raises(RuntimeError):
# #     ln_setup.init(storage="s3://lamindb-ci")


# def test_value_error_modules():
#     with pytest.raises(ModuleNotFoundError):
#         ln_setup.init(storage="tmpstorage1", modules="bionty, xyz1")
