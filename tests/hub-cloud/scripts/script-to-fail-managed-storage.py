from pathlib import Path

import lamindb_setup as ln_setup
import pytest
from django.db.utils import ProgrammingError
from lamindb_setup._set_managed_storage import set_managed_storage
from lamindb_setup.core._hub_client import connect_hub_with_auth

# a user should have read-only access to laminlabs/lamin-site-assets
ln_setup.login("testuser1")
ln_setup.connect("laminlabs/lamin-site-assets")

from lamindb.errors import NoWriteAccess

test_root = Path("./test_script_ci_storage").resolve().as_posix()

with pytest.raises(NoWriteAccess) as error:
    set_managed_storage(test_root, host="test-host-1234")
assert "You're not allowed to write to the space 'all'" in str(error)

hub_client = connect_hub_with_auth()

records = hub_client.table("storage").select("*").eq("root", test_root).execute().data
assert len(records) == 0

hub_client.auth.sign_out()
