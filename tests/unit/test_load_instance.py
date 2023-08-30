import lamindb_setup as ln_setup

# from lamindb_setup.dev._hub_client import connect_hub_with_auth
# from lamindb_setup.dev._hub_crud import (
#     sb_delete_instance,
#     sb_select_account_by_handle,
#     sb_select_instance_by_name,
#     sb_update_instance,
# )
from lamindb_setup.dev._settings_store import instance_settings_file


def test_load_remote_instance():
    ln_setup.login("testuser1")
    ln_setup.delete("lndb-setup-ci", force=True)
    ln_setup.init(storage="s3://lndb-setup-ci", _test=True)
    instance_settings_file("lndb-setup-ci", "testuser1").unlink()
    ln_setup.load("testuser1/lndb-setup-ci", _test=True)
    assert ln_setup.settings.instance._id is not None
    assert ln_setup.settings.instance.storage.is_cloud
    assert ln_setup.settings.instance.storage.root_as_str == "s3://lndb-setup-ci"
    assert (
        ln_setup.settings.instance._sqlite_file.as_posix()
        == "s3://lndb-setup-ci/lndb-setup-ci.lndb"
    )
    # ln_setup.delete("lndb-setup-ci")


# def test_load_public_connection_string():
#     # Admin connection strings are currently assigned to non-collaborator
#     # users of public instances
#     pgurl = "postgresql://postgres:pwd@0.0.0.0:5432/pgtest"
#     ln_setup.login("testuser1")
#     ln_setup.init(storage="./mydatapg", db=pgurl, _test=True)
#     ln_setup.register()
#     # Get hub instance entries and make it public
#     hub = connect_hub_with_auth(access_token=ln_setup.settings.user.access_token)
#     account = sb_select_account_by_handle(
#         handle=ln_setup.settings.instance.owner, supabase_client=hub
#     )
#     instance = sb_select_instance_by_name(
#         account_id=account["id"],
#         name=ln_setup.settings.instance.name,
#         supabase_client=hub,
#     )
#     sb_update_instance(
#         instance_id=instance["id"],
#         instance_fields={"public": True},
#         supabase_client=hub,
#     )
#     # Load instance with non-collaborator user
#     ln_setup.login("testuser2")
#     instance_settings_file("pgtest", "testuser1").unlink()
#     ln_setup.load("testuser1/pgtest", _test=True)
#     assert ln_setup.settings.instance.db == pgurl
#     ln_setup.login("testuser1")
#     ln_setup.delete("pgtest")
#     sb_delete_instance(instance["id"], hub)
