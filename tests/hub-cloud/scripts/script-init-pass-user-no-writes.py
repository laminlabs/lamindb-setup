import os

import lamindb_setup as ln_setup
from lamindb_setup.core._settings_load import load_instance_settings, load_user_settings
from lamindb_setup.core._settings_store import (
    current_instance_settings_file,
    user_settings_file_handle,
)

ln_setup.logout()
assert ln_setup.settings.user.handle == "anonymous"

current_instance_file = current_instance_settings_file()
current_instance_exists = current_instance_file.exists()

testuser1 = load_user_settings(user_settings_file_handle("testuser1"))
assert testuser1.handle == "testuser1"

storage = f"s3://lamindb-ci/{os.environ['LAMIN_ENV']}_test/test-init-no-writes"

ln_setup.init(
    storage=storage,
    _user=testuser1,
    _write_settings=False,
)

assert ln_setup.settings.instance.name == "test-init-no-writes"
assert ln_setup.settings.instance.owner == "testuser1"
assert not ln_setup.settings.instance._get_settings_file().exists()
assert ln_setup.settings.instance._cloud_sqlite_locker.user == testuser1.uid
# check that there were no change in current_instance.env
# here in CI tests current_instance.env is just not present at the time of execution
assert current_instance_exists == current_instance_file.exists()
if current_instance_exists:
    current_instance = load_instance_settings(current_instance_file)
    assert current_instance.name != "test-init-no-writes"
