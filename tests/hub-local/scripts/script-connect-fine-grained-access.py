import os

import lamindb_setup as ln_setup
import pytest
from django.db import connection, transaction
from django.db.utils import ProgrammingError
from lamindb_setup.core._hub_core import access_db
from lamindb_setup.core.django import db_token_manager

assert os.environ["LAMIN_ENV"] == "local"

ln_setup.connect("instance_test")

isettings = ln_setup.settings.instance

# check extra parameters for s3 managed buckets
# this is populated by create_instance imported from laminhub
assert (
    isettings.storage.root.storage_options["s3_additional_kwargs"][
        "ServerSideEncryption"
    ]
    == "AES256"
)

assert isettings._fine_grained_access
assert isettings._db_permissions == "jwt"
assert isettings._api_url is not None

storage_record = isettings.storage.record

isettings.storage._record = None
with transaction.atomic():
    assert isettings.storage.record.id == storage_record.id

# check directly
assert db_token_manager.tokens

with connection.cursor() as cur:
    cur.execute("SELECT * FROM check_access();")

# check reset
db_token_manager.reset()
assert not db_token_manager.tokens

# check after reset
with pytest.raises(ProgrammingError) as error, connection.cursor() as cur:
    cur.execute("SELECT * FROM check_access();")
assert "JWT is not set" in error.exconly()
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
# check specifying an env token via an env variable
os.environ["LAMIN_DB_TOKEN"] = "test_db_token"
assert access_db(isettings) == os.environ["LAMIN_DB_TOKEN"]
