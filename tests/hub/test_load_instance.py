from lamindb_setup.dev._hub_core import load_instance
from lamindb_setup.dev._hub_crud import sb_select_instance_by_name


def test_load_instance(user_account_1, instance_name_1, account_hub_1, auth_1):
    instance = sb_select_instance_by_name(
        account_id=user_account_1["id"],
        name=instance_name_1,
        supabase_client=account_hub_1,
    )
    result = load_instance(
        owner=auth_1["handle"],
        name=instance["name"],
        _access_token=auth_1["access_token"],
    )
    loaded_instance, _ = result
    assert loaded_instance.name == instance["name"]
    assert loaded_instance.db
