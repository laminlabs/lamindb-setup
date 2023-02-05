import os
from datetime import timezone
from pathlib import Path

from dateutil.parser import isoparse  # type: ignore
from lamin_logger import logger
from upath import UPath


def download_to(self, path, **kwargs):
    self.fs.download(str(self), str(path), **kwargs)


def upload_from(self, path, **kwargs):
    self.fs.upload(str(path), str(self), **kwargs)


def synchronize(self, filepath: Path, sync_warn=True):
    if not filepath.exists():
        filepath.parent.mkdir(parents=True, exist_ok=True)
        mts = self.modified.timestamp()
        self.download_to(filepath)
        os.utime(filepath, times=(mts, mts))
    elif self.modified.timestamp() > filepath.stat().st_mtime:
        mts = self.modified.timestamp()
        self.download_to(filepath)
        os.utime(filepath, times=(mts, mts))
    elif sync_warn:
        logger.warning(
            f"Local file ({filepath}) for cloud path ({self}) is newer on disk than in cloud.\n"  # noqa
            "It seems you manually updated the database locally and didn't push changes to the cloud.\n"  # noqa
            "This can lead to data loss if somebody else modified the cloud file in"  # noqa
            " the meantime."
        )


def modified(self):
    path = str(self)
    try:
        mtime = self.fs.modified(path)
    except NotImplementedError:
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


UPath.download_to = download_to
UPath.upload_from = upload_from
UPath.synchronize = synchronize
UPath.modified = property(modified)
