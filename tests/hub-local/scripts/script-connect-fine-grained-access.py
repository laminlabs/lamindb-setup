import os

import lamindb_setup as ln_setup
from lamindb_setup.core._hub_core import access_db

assert os.environ["LAMIN_ENV"] == "local"

ln_setup.connect("instance_access_v2")

assert ln_setup.settings.instance._fine_grained_access

# check token renewal in access_db
invalid_token = "header1.payload1.signature1"
ln_setup.settings.user.access_token = invalid_token
access_db(ln_setup.settings.instance)
