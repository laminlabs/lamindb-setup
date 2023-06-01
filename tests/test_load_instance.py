import lamindb_setup as ln_setup
from lamindb_setup.dev._settings_store import instance_settings_file


def test_load_remote_instance():
    ln_setup.login("testuser1")
    ln_setup.delete("lndb-setup-ci")
    ln_setup.init(storage="s3://lndb-setup-ci", _test=True)
    instance_settings_file("lndb-setup-ci", "testuser1").unlink()
    ln_setup.load("testuser1/lndb-setup-ci", _test=True)
    assert ln_setup.settings.instance.storage.is_cloud
    assert ln_setup.settings.instance.storage.root_as_str == "s3://lndb-setup-ci"
    assert (
        ln_setup.settings.instance._sqlite_file.as_posix()
        == "s3://lndb-setup-ci/lndb-setup-ci.lndb"
    )
