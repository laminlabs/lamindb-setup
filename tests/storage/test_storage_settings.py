from lamindb_setup.core._settings_storage import StorageSettings
from lamindb_setup.core.upath import S3Path, UPath


def test_endpoint_url():
    # test passing endpoint_url in the path string
    ssettings = StorageSettings("s3://bucket/key?endpoint_url=http://localhost:8000/s3")
    assert (
        ssettings.root_as_str == "s3://bucket/key?endpoint_url=http://localhost:8000/s3"
    )
    assert ssettings.root.as_posix() == "s3://bucket/key"
    assert ssettings._root_init.as_posix() == "s3://bucket/key"
    assert ssettings.root.storage_options["endpoint_url"] == "http://localhost:8000/s3"
    assert isinstance(ssettings.root, S3Path)
    assert ssettings.type == "s3"
    # test passing endpoint_url in storage_options
    test_root = UPath("s3://bucket/", endpoint_url="http://localhost:8000/s3")
    ssettings = StorageSettings(test_root)
    assert ssettings.root_as_str == "s3://bucket?endpoint_url=http://localhost:8000/s3"
    assert ssettings.root.as_posix() == "s3://bucket/"
    assert ssettings._root_init.as_posix() == "s3://bucket/"
    assert ssettings.root.storage_options["endpoint_url"] == "http://localhost:8000/s3"
    assert isinstance(ssettings.root, S3Path)
    assert ssettings.type == "s3"
