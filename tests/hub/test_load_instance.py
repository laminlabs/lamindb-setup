from lamindb_setup.dev._hub_core import load_instance


def test_load_instance(auth_1, instance_1):
    result = load_instance(
        owner=auth_1["handle"],
        name=instance_1["name"],
        _access_token=auth_1["access_token"],
    )
    loaded_instance, _ = result
    assert loaded_instance.name == instance_1["name"]
    assert loaded_instance.db
