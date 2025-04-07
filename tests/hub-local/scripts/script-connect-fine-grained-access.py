import os

import lamindb_setup as ln_setup
import pytest
from django.db import transaction
from lamindb_setup.core._hub_core import access_db
from lamindb_setup.core.django import db_token_manager

assert os.environ["LAMIN_ENV"] == "local"

ln_setup.connect("instance_access_v2")

isettings = ln_setup.settings.instance

assert isettings._fine_grained_access
assert isettings._db_permissions == "jwt"
assert isettings._api_url is not None

# test querying
db_token_manager.debug = True

storage_record = isettings.storage.record

isettings.storage._record = None
with transaction.atomic():
    assert isettings.storage.record.id == storage_record.id

# check calling access_db with a dict
instance_dict = {
    "owner": isettings.owner,
    "name": isettings.name,
    "id": isettings._id.hex,
    "api_url": isettings._api_url,
}
access_db(instance_dict)
# check calling access_db with anonymous user
ln_setup.settings.user.handle = "anonymous"
with pytest.raises(RuntimeError):
    access_db(isettings)
# check with providing access_token explicitly
access_db(isettings, ln_setup.settings.user.access_token)
