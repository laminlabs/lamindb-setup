from lamindb_setup.core.upath import check_s3_storage_location_empty
from laminhub_rest.orm._account_instance import sb_insert_collaborator
from laminhub_rest.test.utils.instance import TestInstance
from laminhub_rest.test.utils.user import TestUser
from laminhub_rest.test.utils.access_aws import get_upath_from_access_token
from laminhub_rest.utils._supabase_client import SbClientAdmin
import pytest


def test_check_s3_storage_location_empty(
    test_instance_hub_only: TestInstance, user_account_2: TestUser
):
    # Provide the appropriate storage credentials for user_account_2
    with SbClientAdmin().connect() as client:
        sb_insert_collaborator(
            {
                "account_id": user_account_2.id.hex,
                "instance_id": test_instance_hub_only.id.hex,
                "role": "admin",
            },
            client,
        )

    # Make sure gating function does not error on empty storage location
    path = get_upath_from_access_token(
        user_account_2.access_token, test_instance_hub_only.storage_root
    )
    assert check_s3_storage_location_empty(path) is None  # type: ignore

    # Add object and make sure gating function errors
    path.fs.touch(str(path / "new_file"))
    with pytest.raises(ValueError):
        check_s3_storage_location_empty(path)
