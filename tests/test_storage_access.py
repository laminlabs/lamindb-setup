from uuid import UUID

import lamindb_setup as ln_setup
from lamindb_setup.dev._hub_client import connect_hub_with_auth
from lamindb_setup.dev._hub_crud import select_instance_by_owner_name


def test_load_instance_with_public_storage():
    # this loads a persistent instance created with a public s3 bucket
    # with s3:GetObject and s3:ListBucket policies enabled for all
    # the bucket is s3://lamin-site-assets

    # start out by having AWS_CREDENTIALS_PRESENT be undetermined
    assert ln_setup.dev.upath.AWS_CREDENTIALS_PRESENT is None
    ln_setup.login("static-testuser1@lamin.ai", password="static-testuser1-password")
    # upon load, it's determined that AWS_CREDENTIALS_PRESENT is False (because
    # this is run in an environment that doesn't have them)
    ln_setup.load("static-testuser1/static-testinstance1")
    # Alex doesn't fully understand why we're testing the load from hub, here, but OK
    hub = connect_hub_with_auth()
    instance = select_instance_by_owner_name(
        owner=ln_setup.settings.instance.owner,
        name=ln_setup.settings.instance.name,
        client=hub,
    )
    hub.auth.sign_out()
    assert ln_setup.settings.instance.id == UUID(instance["id"])
    # test that AWS credentials are in fact not present
    assert not ln_setup.dev.upath.AWS_CREDENTIALS_PRESENT
    ln_setup.close()


def test_load_instance_with_private_storage():
    pass
