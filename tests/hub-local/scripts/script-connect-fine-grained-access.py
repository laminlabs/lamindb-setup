import os

import lamindb_setup as ln_setup

assert os.environ["LAMIN_ENV"] == "local"

ln_setup.connect("instance_access_v2")

assert ln_setup.settings.instance._fine_grained_access
