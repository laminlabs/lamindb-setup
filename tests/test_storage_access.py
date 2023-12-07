import os
from uuid import UUID
import pytest
import lamindb_setup as ln_setup
from lamindb_setup.dev._hub_client import connect_hub_with_auth
from lamindb_setup.dev._hub_crud import select_instance_by_owner_name


def test_load_instance_with_public_storage():
    # start out by having AWS_CREDENTIALS_PRESENT be undetermined
    assert ln_setup.dev.upath.AWS_CREDENTIALS_PRESENT is None
    # this loads a persistent instance created with a public s3 bucket
    # with s3:GetObject and s3:ListBucket policies enabled for all
    # the bucket is s3://lamin-site-assets
    ln_setup.login("testuser1@lamin.ai")
    ln_setup.load("laminlabs/static-testinstance1")
    # upon load, it's determined that AWS_CREDENTIALS_PRESENT is False (because
    # this is run in an environment that doesn't have them)
    # Alex doesn't fully understand why we're testing the load from hub, here, but OK
    hub = connect_hub_with_auth()
    instance = select_instance_by_owner_name(
        owner="laminlabs",
        name=ln_setup.settings.instance.name,
        client=hub,
    )
    hub.auth.sign_out()
    assert ln_setup.settings.instance.id == UUID(instance["id"])
    # test that AWS credentials are in fact not present
    assert not ln_setup.dev.upath.AWS_CREDENTIALS_PRESENT
    ln_setup.close()


def test_load_instance_with_private_storage_and_no_storage_access():
    ln_setup.login("testuser1@lamin.ai")
    # this should fail
    with pytest.raises(PermissionError):
        ln_setup.load("laminlabs/static-test-instance-private-sqlite")
    # loading a postgres instance should work:
    ln_setup.load(
        "laminlabs/test-instance-private-postgres",
        db=os.environ["TEST_INSTANCE_PRIVATE_POSTGRES"],
    )
    # accessing storage in the instance should fail:
    with pytest.raises(PermissionError):
        path = ln_setup.settings.storage.root
        path.fs.call_s3("head_bucket", Bucket=path._url.netloc)
