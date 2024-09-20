import lamindb_setup as ln_setup
from lamindb_setup.core._settings_load import load_user_settings
from lamindb_setup.core._settings_store import user_settings_file_handle

ln_setup.logout()
assert ln_setup.settings.user.handle == "anonymous"

testuser1 = load_user_settings(user_settings_file_handle("testuser1"))
assert testuser1.handle == "testuser1"

ln_setup.init(
    storage="create-s3",
    name="test-init-no-writes",
    _user=testuser1,
    _write_settings=False,
)

assert ln_setup.settings.instance.name == "test-init-no-writes"
assert ln_setup.settings.instance.owner == "testuser1"
assert not ln_setup.settings.instance._get_settings_file().exists()
