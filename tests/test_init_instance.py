import pytest

import lndb as lndb


def test_db_unique_error():
    lndb.login("testuser2")

    # postgres
    with pytest.raises(RuntimeError):
        lndb.init(
            storage="s3://lndb-setup-ci",
            schema="retro,bionty",
            db="postgresql://batman:robin@35.222.187.204:5432/retro",
        )

    # sqlite
    # this fails because there is already an sqlite with the same name in that bucket
    # hence, the sqlite file would clash

    # with pytest.raises(RuntimeError):
    #     lndb.init(storage="s3://lamindb-ci")


def test_value_error_schema():
    with pytest.raises(ValueError):
        lndb.init(storage="tmpstorage1", schema="bionty, xyz1")
