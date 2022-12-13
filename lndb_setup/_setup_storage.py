from pathlib import Path
from typing import Optional, Union

from cloudpathlib import CloudPath

from ._settings_load import load_instance_settings, setup_storage_root
from ._settings_store import current_instance_settings_file, instance_settings_file


def get_storage_region(storage_root):
    storage_root_str = str(storage_root)
    storage_region = None

    if storage_root_str.startswith("s3://"):
        import boto3

        response = boto3.client("s3").get_bucket_location(
            Bucket=storage_root_str.replace("s3://", "")
        )
        # returns `None` for us-east-1
        # returns a string like "eu-central-1" etc. for all other regions
        storage_region = response["LocationConstraint"]
        if storage_region is None:
            storage_region = "us-east-1"

    return storage_region


def set_storage(
    storage: Union[str, Path, CloudPath], instance_name: Optional[str] = None
):
    settings_file = (
        instance_settings_file(instance_name)
        if instance_name
        else current_instance_settings_file()
    )
    isettings = load_instance_settings(settings_file)
    storage_root = setup_storage_root(storage)
    storage_region = get_storage_region(storage_root)
    isettings.storage_root = storage_root
    isettings.storage_region = storage_region
