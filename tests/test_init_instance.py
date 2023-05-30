from pathlib import Path

import pytest

import lndb as lndb

pgurl = "postgresql://postgres:pwd@0.0.0.0:5432/pgtest"


def test_init_instance_postgres_default_name():
    lndb.init(storage="./mydatapg", db=pgurl, _test=True)
    assert lndb.settings.instance.name == "pgtest"
    assert not lndb.settings.instance.storage.is_cloud
    assert lndb.settings.instance.owner == lndb.settings.user.handle
    assert lndb.settings.instance.dialect == "postgresql"
    assert lndb.settings.instance.db == pgurl
    assert (
        lndb.settings.instance.storage.root.as_posix()
        == Path("mydatapg").absolute().as_posix()
    )
    assert lndb.settings.instance.storage.cache_dir is None


def test_init_instance_postgres_custom_name():
    lndb.init(storage="./mystorage", name="mydata2", db=pgurl, _test=True)
    assert lndb.settings.instance.name == "mydata2"
    assert not lndb.settings.instance.storage.is_cloud
    assert lndb.settings.instance.owner == lndb.settings.user.handle
    assert lndb.settings.instance.dialect == "postgresql"
    assert lndb.settings.instance.db == pgurl
    assert (
        lndb.settings.instance.storage.root.as_posix()
        == Path("mystorage").absolute().as_posix()
    )
    assert lndb.settings.instance.storage.cache_dir is None


def test_init_instance_postgres_cloud_aws_us():
    lndb.init(storage="s3://lndb-setup-ci", _test=True)
    assert lndb.settings.storage.is_cloud
    assert str(lndb.settings.storage.root) == "s3://lndb-setup-ci/"
    assert lndb.settings.storage.root_as_str == "s3://lndb-setup-ci"
    assert lndb.settings.storage.region == "us-east-1"
    assert (
        str(lndb.settings.instance._sqlite_file)
        == "s3://lndb-setup-ci/lndb-setup-ci.lndb"
    )


def test_init_instance_postgres_cloud_aws_europe():
    # do the same for an S3 bucket in Europe
    lndb.init(
        storage="s3://lndb-setup-ci-eu-central-1",
        name="lndb-setup-ci-europe",
        _test=True,
    )
    assert lndb.settings.storage.region == "eu-central-1"
    assert lndb.settings.instance.name == "lndb-setup-ci-europe"
    assert (
        str(lndb.settings.instance._sqlite_file)
        == "s3://lndb-setup-ci-eu-central-1/lndb-setup-ci-europe.lndb"
    )


# def test_db_unique_error():
#     lndb.login("testuser2")

#     # postgres


# #     with pytest.raises(RuntimeError):
# #         lndb.init(
# #             storage="s3://lndb-setup-ci",
# #             schema="retro, bionty",
# #             db="postgresql://batman:robin@35.222.187.204:5432/retro",
# #         )

# # sqlite
# # this fails because there is already an sqlite with the same name in that bucket
# # hence, the sqlite file would clash

# # with pytest.raises(RuntimeError):
# #     lndb.init(storage="s3://lamindb-ci")


def test_value_error_schema():
    with pytest.raises(ValueError):
        lndb.init(storage="tmpstorage1", schema="bionty, xyz1")
