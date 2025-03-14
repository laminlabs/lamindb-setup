import os

import lamindb_setup as ln_setup

assert os.environ["LAMIN_ENV"] == "local"
# check token renewal in _hub_client.request_get_auth
invalid_token = "header1.payload1.signature1"
ln_setup.settings.user.access_token = invalid_token

ln_setup.connect("instance_access_v2")

assert ln_setup.settings.instance._fine_grained_access
