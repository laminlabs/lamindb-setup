# this tries to load a persistent instance created in a public s3 bucket
# with s3:GetObject and s3:ListBucket policies enabled for all
# the bucket is s3://lamin-site-assets

from uuid import UUID

import lamindb_setup as ln_setup
from lamindb_setup.dev._hub_client import connect_hub_with_auth
from lamindb_setup.dev._hub_crud import select_instance_by_owner_name


def test_load_persistent_instance():
    assert ln_setup.dev.upath.AWS_CREDENTIALS_PRESENT is None
    ln_setup.login("static-testuser1@lamin.ai", password="static-testuser1-password")
    ln_setup.load("static-testuser1/static-testinstance1")
    hub = connect_hub_with_auth()
    instance = select_instance_by_owner_name(
        owner=ln_setup.settings.instance.owner,
        name=ln_setup.settings.instance.name,
        client=hub,
    )
    hub.auth.sign_out()
    assert ln_setup.settings.instance.id == UUID(instance["id"])
    assert not ln_setup.dev.upath.AWS_CREDENTIALS_PRESENT
    ln_setup.close()
