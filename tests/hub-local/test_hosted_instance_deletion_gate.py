from lamindb_setup.core.upath import InstanceNotEmpty
from lamindb_setup.core._settings_storage import IS_INITIALIZED_KEY
from lamindb_setup.core._hub_core import delete_instance
from lamindb_setup import settings
import lamindb_setup as ln_setup
import pytest


def test_hosted_instance_deletion_gate():
    ln_setup.init(name="my-hosted", storage="create-s3")

    # Make sure 0-byte file is touched upon storage initialization
    root_upath = settings.storage.root
    root_string = root_upath.as_posix()
    object_string_paths = root_upath.fs.find(root_string)
    assert (
        f"{root_string.replace('s3://', '')}/{IS_INITIALIZED_KEY}"
        in object_string_paths
    )

    # Make sure gating function blocks instance and storage deletion if
    # storage location is not empty
    new_file = root_upath / "new_file"
    new_file.touch()
    instance_id = settings.instance.id
    with pytest.raises(InstanceNotEmpty):
        delete_instance(instance_id)

    # Make sure instance and storage deletion is possible with empty storage
    new_file.unlink()
    ln_setup.delete("my-hosted", force=True)
    delete_instance(instance_id)
