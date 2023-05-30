from pathlib import Path

import lndb


def test_load_add_storage_location():
    lndb.delete("mydata")
    lndb.init(storage="mydata", _test=True)
    # assume we move the storage location
    Path("./mydata").rename("./mydata_new_loc")
    # with pytest.raises(
    #     RuntimeError
    # ):  # triggers because it does not find the SQLite file anymore
    #     lndb.load("mydata", _test=True)
    # now account for the new storage location
    lndb.load("mydata", storage="./mydata_new_loc", _test=True)
    assert (
        lndb.settings.instance.storage.root.as_posix()
        == Path("./mydata_new_loc").resolve().as_posix()
    )
    assert lndb.settings.instance.storage.cache_dir is None
    assert (
        lndb.settings.instance.db
        == f"sqlite:///{Path('./mydata_new_loc').resolve().as_posix()}/mydata.lndb"
    )


def test_load_remote_instance():
    lndb.delete("lndb-setup-ci")
    lndb.init(storage="s3://lndb-setup-ci")
    # ensure that the locally cached env file is deleted
    from lndb.dev._settings_store import instance_settings_file

    instance_settings_file("lndb-setup-ci", "testuser1").unlink()
    lndb.load("lndb-setup-ci", _test=False)
    assert lndb.settings.instance.storage.is_cloud
    assert lndb.settings.instance.storage.root_as_str == "s3://lndb-setup-ci"
    assert (
        lndb.settings.instance._sqlite_file.as_posix()
        == "s3://lndb-setup-ci/lndb-setup-ci.lndb"
    )
