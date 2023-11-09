# we are not documenting UPath here because it's documented at lamindb.UPath
"""Paths & file systems."""

import os
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Union, Optional

from botocore.exceptions import NoCredentialsError
from dateutil.parser import isoparse  # type: ignore
from lamin_utils import logger
from upath import UPath
from upath.implementations.cloud import CloudPath, S3Path  # noqa
from upath.implementations.local import LocalPath, PosixUPath, WindowsUPath

LocalPathClasses = (PosixUPath, WindowsUPath, LocalPath)


AWS_CREDENTIALS_PRESENT = None


def set_aws_credentials_present(path: S3Path) -> None:
    global AWS_CREDENTIALS_PRESENT
    try:
        path.fs.call_s3("head_bucket", Bucket=path._url.netloc)
        AWS_CREDENTIALS_PRESENT = True
    except NoCredentialsError:
        logger.debug("did not find aws credentials, using anonymous")
        AWS_CREDENTIALS_PRESENT = False


def infer_filesystem(path: Union[Path, UPath, str]):
    import fsspec  # improve cold start

    path_str = str(path)

    if isinstance(path, UPath):
        fs = path.fs
    else:
        protocol = fsspec.utils.get_protocol(path_str)
        if protocol == "s3":
            fs_kwargs = {"cache_regions": True}
        else:
            fs_kwargs = {}
        fs = fsspec.filesystem(protocol, **fs_kwargs)

    return fs, path_str


def download_to(self, path, **kwargs):
    """Download to a path."""
    self.fs.download(str(self), str(path), **kwargs)


def upload_from(self, path, **kwargs):
    """Upload from a path."""
    self.fs.upload(str(path), str(self), **kwargs)


def synchronize(self, filepath: Path, **kwargs):
    """Sync to a local destination path."""
    if not self.exists():
        return None

    if not filepath.exists():
        filepath.parent.mkdir(parents=True, exist_ok=True)
        mts = self.modified.timestamp()  # type: ignore
        self.download_to(filepath, **kwargs)
        os.utime(filepath, times=(mts, mts))
        return None

    cloud_mts = self.modified.timestamp()  # type: ignore
    local_mts = filepath.stat().st_mtime
    if cloud_mts > local_mts:
        mts = self.modified.timestamp()  # type: ignore
        self.download_to(filepath, **kwargs)
        os.utime(filepath, times=(mts, mts))
    elif cloud_mts < local_mts:
        pass
        # these warnings are out-dated because it can be normal to have a more up-to-date version locally  # noqa
        # logger.warning(
        #     f"Local file ({filepath}) for cloud path ({self}) is newer on disk than in cloud.\n"  # noqa
        #     "It seems you manually updated the database locally and didn't push changes to the cloud.\n"  # noqa
        #     "This can lead to data loss if somebody else modified the cloud file in"  # noqa
        #     " the meantime."
        # )


def modified(self) -> Optional[datetime]:
    """Return modified time stamp."""
    path = str(self)
    if "gcs" not in self.fs.protocol:
        mtime = self.fs.modified(path)
    else:
        stat = self.fs.stat(path)
        if "updated" in stat:
            mtime = stat["updated"]
            mtime = isoparse(mtime)
        else:
            return None
    # always convert to the local timezone before returning
    # assume in utc if the time zone is not specified
    if mtime.tzinfo is None:
        mtime = mtime.replace(tzinfo=timezone.utc)
    return mtime.astimezone().replace(tzinfo=None)


# Why aren't we subclassing?
#
# The problem is that UPath defines a type system of paths
# Its __new__ method returns instances of different subclasses rather than a
# UPath object
# If we create a custom subclass naively, subclasses of the parent UPath won't
# be subclasses of our custom subclass
# This makes life really hard in type checks involving local to cloud
# comparisons, etc.
# Hence, we extend the existing UPath and amend the docs
# Some of this might end up in the original UPath implementation over time,
# we'll see.


# add custom functions
UPath.modified = property(modified)
UPath.synchronize = synchronize
UPath.upload_from = upload_from
UPath.download_to = download_to


# fix docs
UPath.glob.__doc__ = Path.glob.__doc__
UPath.rglob.__doc__ = Path.rglob.__doc__
UPath.stat.__doc__ = Path.stat.__doc__
UPath.iterdir.__doc__ = Path.iterdir.__doc__
UPath.resolve.__doc__ = Path.resolve.__doc__
UPath.relative_to.__doc__ = Path.relative_to.__doc__
UPath.exists.__doc__ = Path.exists.__doc__
UPath.is_dir.__doc__ = Path.is_dir.__doc__
UPath.is_file.__doc__ = Path.is_file.__doc__
UPath.unlink.__doc__ = Path.unlink.__doc__
UPath.__doc__ = """Paths: low-level key-value access to files & objects.

Paths are keys that offer the typical access patterns of file systems and object
stores. The ``key`` field in the `File` registry is a relative path in the
storage location of the record.

If you don't care about validating & linking extensive metadata to a file, you
can store it as a path.

For instance, if you have a folder with 1M images on S3 and you don't want to
create file records for each of them, create a Dataset object like so:

>>> dataset = Dataset("s3://my-bucket/my-folder", file="s3://my-bucket/meta.parquet")

Passing the ``file`` parameter is optional. This way, you can iterate over path
objects through

>>> dataset.path
>>> assert dataset.path.is_dir()

Args:
    pathlike: A string or Path to a local/cloud file/directory/folder.
"""


def create_path(pathlike: Union[str, Path, UPath]) -> UPath:
    """Convert pathlike to Path or UPath inheriting options from root."""
    if isinstance(pathlike, (str, UPath)):
        path = UPath(pathlike)
    elif isinstance(pathlike, Path):
        path = UPath(str(pathlike))  # UPath applied on Path gives Path back
    else:
        raise ValueError("pathlike should be of type Union[str, Path, UPath]")

    if isinstance(path, S3Path):
        path = UPath(path, cache_regions=True)
        if AWS_CREDENTIALS_PRESENT is None:
            set_aws_credentials_present(path)
        if not AWS_CREDENTIALS_PRESENT:
            path = UPath(path, anon=True)

    return path
