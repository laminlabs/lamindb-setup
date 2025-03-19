import os

import lamindb_setup as ln_setup
import pytest
from lamindb_setup.core._hub_core import access_db

assert os.environ["LAMIN_ENV"] == "local"

ln_setup.connect("instance_access_v2")

assert ln_setup.settings.instance._fine_grained_access
assert ln_setup.settings.instance._db_permissions == "jwt"

# check calling access_db with anonymous user
ln_setup.settings.user.handle = "anonymous"
with pytest.raises(RuntimeError):
    access_db(ln_setup.settings.instance)
# check with providing access_token explicitly
access_db(ln_setup.settings.instance, ln_setup.settings.user.access_token)
