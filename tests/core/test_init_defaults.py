import inspect

from lamindb_setup._init_instance import init


def test_init_storage_default_is_storage_dir():
    signature = inspect.signature(init)
    assert signature.parameters["storage"].default == "./storage"
