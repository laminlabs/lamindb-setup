import os
from uuid import UUID
import pytest
import lamindb_setup as ln_setup
from lamindb_setup.core._hub_client import connect_hub_with_auth
from lamindb_setup.core._hub_crud import (
    select_account_by_handle,
    select_instance_by_name,
)


def test_connect_instance_with_public_storage():
    # this loads a persistent instance created with a public s3 bucket
    # with s3:GetObject and s3:ListBucket policies enabled for all
    # the bucket is s3://lamin-site-assets
    ln_setup.login("testuser1@lamin.ai")
    ln_setup.connect("laminlabs/lamin-site-assets")
    # Alex doesn't fully understand why we're testing the load from hub, here, but OK
    client = connect_hub_with_auth()
    account = select_account_by_handle("laminlabs", client)
    instance = select_instance_by_name(
        account["id"], ln_setup.settings.instance.name, client
    )
    client.auth.sign_out()
    assert ln_setup.settings.instance.id == UUID(instance["id"])
    ln_setup.close()


def test_connect_instance_with_private_storage_and_no_storage_access():
    ln_setup.login("testuser1@lamin.ai")
    # this should fail
    with pytest.raises(PermissionError):
        ln_setup.connect("laminlabs/static-test-instance-private-sqlite")
    # loading a postgres instance should work:
    ln_setup.connect(
        "laminlabs/test-instance-private-postgres",
        db=os.environ["TEST_INSTANCE_PRIVATE_POSTGRES"],
    )
    # accessing storage in the instance should fail:
    with pytest.raises(PermissionError):
        path = ln_setup.settings.storage.root
        path.fs.call_s3("head_bucket", Bucket=path._url.netloc)
