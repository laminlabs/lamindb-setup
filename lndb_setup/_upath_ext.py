from datetime import timezone

from dateutil.parser import isoparse  # type: ignore
from upath import UPath


def download_to(self, path, **kwargs):
    self.fs.download(str(self), path, **kwargs)


def upload_from(self, path, **kwargs):
    self.fs.upload(path, str(self), **kwargs)


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
UPath.modified = property(modified)
