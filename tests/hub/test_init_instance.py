import lamindb_setup as ln_setup


def db_name(test_instance_name):
    return f"postgresql://postgres:pwd@fakeserver.xyz:5432/{test_instance_name}"


def test_init_instance(auth_1, instance_name_1, user_account_1, account_hub_1):
    ln_setup.dev._hub_core.init_instance(
        owner=auth_1["handle"],
        name=instance_name_1,
        storage=f"s3://{instance_name_1}",
        db=db_name(instance_name_1),
        _access_token=auth_1["access_token"],
    )
    instance = ln_setup.dev._hub_crud.sb_select_instance_by_name(
        account_id=user_account_1["id"],
        name=instance_name_1,
        supabase_client=account_hub_1,
    )
    return instance
