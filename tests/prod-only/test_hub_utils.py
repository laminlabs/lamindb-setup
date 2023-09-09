from lamindb_setup.dev._hub_crud import sb_select_account_name_handle_by_lnid
from lamindb_setup.dev._hub_client import connect_hub


def test_select_account():
    client = connect_hub()
    # testuser1
    name, handle = sb_select_account_name_handle_by_lnid("DzTjkKse", client)

    assert name == "Test User1"
    assert handle == "testuser1"
