from pathlib import Path

import pytest

import lamindb_setup as ln_setup

pgurl = "postgresql://postgres:pwd@0.0.0.0:5432/pgtest"


def test_init_instance_postgres_default_name():
    ln_setup.init(storage="./mydatapg", db=pgurl, _test=True)
    assert ln_setup.settings.instance.name == "pgtest"
    assert not ln_setup.settings.instance.storage.is_cloud
    assert ln_setup.settings.instance.owner == ln_setup.settings.user.handle
    assert ln_setup.settings.instance.dialect == "postgresql"
    assert ln_setup.settings.instance.db == pgurl
    assert (
        ln_setup.settings.instance.storage.root.as_posix()
        == Path("mydatapg").absolute().as_posix()
    )
    assert ln_setup.settings.instance.storage.cache_dir is None


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
    assert ln_setup.settings.instance.storage.cache_dir is None


def test_init_instance_postgres_cloud_aws_us():
    ln_setup.init(storage="s3://lndb-setup-ci", _test=True)
    assert ln_setup.settings.storage.is_cloud
    assert str(ln_setup.settings.storage.root) == "s3://lndb-setup-ci/"
    assert ln_setup.settings.storage.root_as_str == "s3://lndb-setup-ci"
    assert ln_setup.settings.storage.region == "us-east-1"
    assert (
        str(ln_setup.settings.instance._sqlite_file)
        == "s3://lndb-setup-ci/lndb-setup-ci.lndb"
    )


def test_init_instance_postgres_cloud_aws_europe():
    # do the same for an S3 bucket in Europe
    ln_setup.init(
        storage="s3://lndb-setup-ci-eu-central-1",
        name="lndb-setup-ci-europe",
        _test=True,
    )
    assert ln_setup.settings.storage.region == "eu-central-1"
    assert ln_setup.settings.instance.name == "lndb-setup-ci-europe"
    assert (
        str(ln_setup.settings.instance._sqlite_file)
        == "s3://lndb-setup-ci-eu-central-1/lndb-setup-ci-europe.lndb"
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


def test_value_error_schema():
    with pytest.raises(ValueError):
        ln_setup.init(storage="tmpstorage1", schema="bionty, xyz1")
