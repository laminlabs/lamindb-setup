import lndb


def test_load_remote_instance():
    lndb.delete("lndb-setup-ci")
    lndb.init(storage="s3://lndb-setup-ci")
    # ensure that the locally cached env file is deleted
    from lndb.dev._settings_store import instance_settings_file

    instance_settings_file("lndb-setup-ci", "testuser1").unlink()
    lndb.load("testuser1/lndb-setup-ci", _test=False)
    assert lndb.settings.instance.storage.is_cloud
    assert lndb.settings.instance.storage.root_as_str == "s3://lndb-setup-ci"
    assert (
        lndb.settings.instance._sqlite_file.as_posix()
        == "s3://lndb-setup-ci/lndb-setup-ci.lndb"
    )
