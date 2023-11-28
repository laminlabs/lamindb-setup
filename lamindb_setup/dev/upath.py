# we are not documenting UPath here because it's documented at lamindb.UPath
"""Paths & file systems."""

import os
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Literal
import fsspec
from itertools import islice
from typing import Union, Optional, Set, Any
from collections import defaultdict
from botocore.exceptions import NoCredentialsError
from dateutil.parser import isoparse  # type: ignore
from lamin_utils import logger
from upath import UPath
from upath.implementations.cloud import CloudPath, S3Path  # noqa
from upath.implementations.local import LocalPath, PosixUPath, WindowsUPath

LocalPathClasses = (PosixUPath, WindowsUPath, LocalPath)


AWS_CREDENTIALS_PRESENT = None

# also see https://gist.github.com/securifera/e7eed730cbe1ce43d0c29d7cd2d582f4
#    ".gz" is not listed here as it typically occurs with another suffix
KNOWN_SUFFIXES = {
    #
    # without readers
    #
    ".fasta",
    ".fastq",
    ".jpg",
    ".mtx",
    ".obo",
    ".pdf",
    ".png",
    ".tar",
    ".tiff",
    ".txt",
    ".tsv",
    ".zip",
    ".xml",
    #
    # with readers (see below)
    #
    ".h5ad",
    ".parquet",
    ".csv",
    ".fcs",
    ".xslx",
    ".zarr",
    ".zrad",
}


def extract_suffix_from_path(
    path: Union[UPath, Path], arg_name: Optional[str] = None
) -> str:
    if len(path.suffixes) <= 1:
        return path.suffix
    else:
        print_warning = True
        arg_name = "file" if arg_name is None else arg_name  # for the warning
        msg = f"{arg_name} has more than one suffix (path.suffixes), "
        # first check the 2nd-to-last suffix because it might be followed by .gz
        # or another compression-related suffix
        # Alex thought about adding logic along the lines of path.suffixes[-1]
        # in COMPRESSION_SUFFIXES to detect something like .random.gz and then
        # add ".random.gz" but concluded it's too dangerous it's safer to just
        # use ".gz" in such a case
        if path.suffixes[-2] in KNOWN_SUFFIXES:
            suffix = "".join(path.suffixes[-2:])
            msg += f"inferring: '{suffix}'"
            # do not print a warning for things like .tar.gz, .fastq.gz
            if path.suffixes[-1] == ".gz":
                print_warning = False
        else:
            suffix = path.suffixes[-1]  # this is equivalent to path.suffix!!!
            msg += (
                f"using only last suffix: '{suffix}' - if you want your file format to"
                " be recognized, make an issue:"
                " https://github.com/laminlabs/lamindb/issues/new"
            )
        if print_warning:
            logger.warning(msg)
        return suffix


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


def print_hook(size: int, value: int, **kwargs):
    progress_in_percent = (value / size) * 100
    out = (
        f"... {kwargs['action']} {Path(kwargs['filepath']).name}:"
        f" {min(progress_in_percent, 100):4.1f}%"
    )
    if progress_in_percent >= 100:
        out += "\n"
    if "NBPRJ_TEST_NBPATH" not in os.environ:
        print(out, end="\r")


class ProgressCallback(fsspec.callbacks.Callback):
    def __init__(self, action: Literal["uploading", "downloading"]):
        super().__init__()
        self.action = action

    def branch(self, path_1, path_2, kwargs):
        kwargs["callback"] = fsspec.callbacks.Callback(
            hooks=dict(print_hook=print_hook), filepath=path_1, action=self.action
        )

    def call(self, *args, **kwargs):
        return None


def download_to(self, path, print_progress: bool = False, **kwargs):
    """Download to a path."""
    if print_progress:
        if path.suffix not in {".zarr", ".zrad"}:
            cb = ProgressCallback("downloading")
        else:
            # todo: make proper progress bar for zarr
            cb = fsspec.callbacks.NoOpCallback()
        kwargs["callback"] = cb
    self.fs.download(str(self), str(path), **kwargs)


def upload_from(self, path, print_progress: bool = False, **kwargs):
    """Upload from a local path."""
    if print_progress:
        if path.suffix not in {".zarr", ".zrad"}:
            cb = ProgressCallback("uploading")
        else:
            # todo: make proper progress bar for zarr
            cb = fsspec.callbacks.NoOpCallback()
        kwargs["callback"] = cb
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


# adapted from: https://stackoverflow.com/questions/9727673
def view_tree(
    path: Union[str, Path, UPath] = None,
    *,
    level: int = -1,
    only_dirs: bool = False,
    limit: int = 1000,
    include_paths: Optional[Set[Any]] = None,
) -> None:
    """Print a visual tree structure of files & directories.

    Args:
        level: If `1`, only iterate through one level, if `2` iterate through 2
            levels, if `-1` iterate through entire hierarchy.
        only_dirs: Only iterate through directories.
        limit: Display limit. Will only show this many files. Doesn't affect count.
        include_paths: Restrict to these paths.

    Examples:
        >>> dir_path = ln.dev.datasets.generate_cell_ranger_files(
        >>>     "sample_001", ln.settings.storage
        >>> )
        >>> ln.UPath(dir_path).view_tree()
        3 subdirectories, 15 files
        sample_001
        ├── web_summary.html
        ├── metrics_summary.csv
        ├── molecule_info.h5
        ├── filtered_feature_bc_matrix
        │   ├── features.tsv.gz
        │   ├── barcodes.tsv.gz
        │   └── matrix.mtx.gz
        ├── analysis
        │   └── analysis.csv
        ├── raw_feature_bc_matrix
        │   ├── features.tsv.gz
        │   ├── barcodes.tsv.gz
        │   └── matrix.mtx.gz
        ├── possorted_genome_bam.bam.bai
        ├── cloupe.cloupe
        ├── possorted_genome_bam.bam
        ├── filtered_feature_bc_matrix.h5
        └── raw_feature_bc_matrix.h5
    """
    space = "    "
    branch = "│   "
    tee = "├── "
    last = "└── "
    max_files_per_dir_per_type = 7

    dir_path = create_path(path)  # returns Path for local
    n_files = 0
    n_directories = 0

    # by default only including registered files
    # need a flag and a proper implementation
    suffixes = set()
    include_dirs = set()
    if include_paths is not None:
        include_dirs = {d for p in include_paths for d in p.parents}
    else:
        include_paths = set()

    def inner(dir_path: Union[Path, UPath], prefix: str = "", level: int = -1):
        nonlocal n_files, n_directories, suffixes
        if level == 0:
            return
        stripped_dir_path = dir_path.as_posix().rstrip("/")
        # do not iterate through zarr directories
        if stripped_dir_path.endswith((".zarr", ".zrad")):
            return
        # this is needed so that the passed folder is not listed
        contents = [
            i
            for i in dir_path.iterdir()
            if i.as_posix().rstrip("/") != stripped_dir_path
        ]
        if only_dirs:
            contents = [d for d in contents if d.is_dir()]
        pointers = [tee] * (len(contents) - 1) + [last]
        n_files_per_dir_per_type = defaultdict(lambda: 0)  # type: ignore
        for pointer, path in zip(pointers, contents):
            if path.is_dir():
                if include_dirs and path not in include_dirs:
                    continue
                yield prefix + pointer + path.name
                n_directories += 1
                n_files_per_dir_per_type = defaultdict(lambda: 0)
                extension = branch if pointer == tee else space
                yield from inner(path, prefix=prefix + extension, level=level - 1)
            elif not only_dirs:
                if include_paths and path not in include_paths:
                    continue
                suffix = extract_suffix_from_path(path)
                suffixes.add(suffix)
                n_files_per_dir_per_type[suffix] += 1
                n_files += 1
                if n_files_per_dir_per_type[suffix] == max_files_per_dir_per_type:
                    yield prefix + "..."
                elif n_files_per_dir_per_type[suffix] > max_files_per_dir_per_type:
                    continue
                else:
                    yield prefix + pointer + path.name

    folder_tree = ""
    iterator = inner(dir_path, level=level)
    for line in islice(iterator, limit):
        folder_tree += f"\n{line}"
    if next(iterator, None):
        folder_tree += f"\n... only showing {limit} out of {n_files} files"
    directory_info = "directory" if n_directories == 1 else "directories"
    display_suffixes = ", ".join([f"{suffix!r}" for suffix in suffixes])
    suffix_message = f" with suffixes {display_suffixes}" if n_files > 0 else ""
    message = (
        f"{dir_path.name} ({n_directories} sub-{directory_info} &"
        f" {n_files} files{suffix_message}): {folder_tree}"
    )
    logger.print(message)


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
UPath.view_tree = view_tree
# unfortunately, we also have to do this for the subclasses
Path.view_tree = view_tree  # type: ignore


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

    # ensures there's no trailing slash for directories
    path = UPath(path.as_posix().rstrip("/"))

    if isinstance(path, S3Path):
        path = UPath(path, cache_regions=True)
        if AWS_CREDENTIALS_PRESENT is None:
            set_aws_credentials_present(path)
        if not AWS_CREDENTIALS_PRESENT:
            path = UPath(path, anon=True)

    return path
