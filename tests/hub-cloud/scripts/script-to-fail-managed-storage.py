from pathlib import Path

import lamindb_setup as ln_setup
import pytest
from django.db.utils import ProgrammingError
from lamindb_setup._set_managed_storage import set_managed_storage
from lamindb_setup.core._hub_client import connect_hub_with_auth

# a user should have read-only access to laminlabs/lamin-site-assets
ln_setup.login("testuser1")
ln_setup.connect("laminlabs/lamin-site-assets")

test_root = Path("./test_script_ci_storage").resolve().as_posix()

with pytest.raises(ProgrammingError) as error:
    set_managed_storage(test_root)
assert error.exconly().endswith(
    "ProgrammingError: permission denied for table lamindb_storage"
)

hub_client = connect_hub_with_auth()

records = hub_client.table("storage").select("*").eq("root", test_root).execute().data
assert len(records) == 0

hub_client.auth.sign_out()
