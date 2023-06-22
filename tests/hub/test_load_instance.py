from lamindb_setup.dev._hub_core import load_instance


def test_load_instance(instance_1, auth_1):
    result = load_instance(
        owner=auth_1["handle"],
        name=instance_1["name"],
        _access_token=auth_1["access_token"],
    )
    instance, _ = result
    assert instance.name == instance_1["name"]
    assert instance.db
