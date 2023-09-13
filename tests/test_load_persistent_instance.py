# this tries to load a persistent instance created in a public s3 bucket
# with s3:GetObject and s3:ListBucket policies enabled for all
# the bucket is s3://lamin-site-assets

from uuid import UUID

import lamindb_setup as ln_setup
from lamindb_setup.dev._hub_client import connect_hub_with_auth
from lamindb_setup.dev._hub_crud import (
    sb_select_account_by_handle,
    sb_select_instance_by_name,
)


def test_load_persistent_instance():
    assert ln_setup.dev.upath.AWS_CREDENTIALS_PRESENT is None
    ln_setup.load("testuser1/lamin-site-assets")
    hub = connect_hub_with_auth()
    account = sb_select_account_by_handle(
        handle=ln_setup.settings.instance.owner, client=hub
    )
    instance = sb_select_instance_by_name(
        account_id=account["id"],
        name=ln_setup.settings.instance.name,
        client=hub,
    )
    assert ln_setup.settings.instance.id == UUID(instance["id"])
    assert not ln_setup.dev.upath.AWS_CREDENTIALS_PRESENT
    ln_setup.close()
