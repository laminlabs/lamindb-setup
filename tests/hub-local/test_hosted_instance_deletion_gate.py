from laminhub_rest.test.utils.instance import (
    create_hosted_test_instance,
    delete_test_instance,
)
from lamindb_setup.core.upath import create_path
from lamindb_setup.core._settings_storage import TOUCH_FILE_PATH
from lamindb_setup.core._hub_core import delete_instance
from lamindb_setup import settings
import pytest


def test_hosted_instance_deletion_gate(run_id, s3_bucket):
    test_instance = create_hosted_test_instance(
        f"test_instance_{run_id}", s3_bucket.name
    )

    # Make sure 0-byte file is touched upon storage initialization
    path = create_path(
        test_instance.storage_root, access_token=settings.user.access_token
    )
    object_string_paths = path.fs.find(path.as_posix())
    assert len(object_string_paths) == 1
    assert (
        object_string_paths[0]
        == f"{test_instance.storage_root.replace('s3://', '')}/{TOUCH_FILE_PATH}"
    )

    # Make sure gating function blocks instance and storage deletion if
    # storage location is not empty
    path.fs.touch(str(path / "new_file"))
    instance_slug = f"{settings.user.handle}/{test_instance.name}"
    with pytest.raises(ValueError):
        delete_instance(instance_slug)

    # Make sure instance and storage deletion is possible with empty storage
    path.fs.rm(str(path / "new_file"))
    delete_instance(instance_slug)

    # Clean up other assets
    delete_test_instance(test_instance)
