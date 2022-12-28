from upath import UPath


def download_to(self, path, **kwargs):
    self.fs.download(str(self), path, **kwargs)


def upload_from(self, path, **kwargs):
    self.fs.upload(path, str(self), **kwargs)


UPath.download_to = download_to
UPath.upload_from = upload_from
