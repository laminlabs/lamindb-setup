# we are not documenting UPath here because it's documented at lamindb.UPath
"""Paths & file systems."""

from __future__ import annotations

import os
import warnings
from collections import defaultdict
from datetime import datetime, timezone
from functools import partial
from itertools import islice
from pathlib import Path, PosixPath, PurePosixPath, WindowsPath
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import parse_qs, urlsplit

import fsspec
from lamin_utils import logger
from upath import UPath
from upath.implementations.cloud import CloudPath, S3Path  # keep CloudPath!
from upath.implementations.local import LocalPath
from upath.registry import register_implementation

from lamindb_setup.errors import StorageNotEmpty

from ._aws_options import HOSTED_BUCKETS, get_aws_options_manager
from ._deprecated import deprecated
from .hashing import HASH_LENGTH, b16_to_b64, hash_from_hashes_list, hash_string

if TYPE_CHECKING:
    from lamindb_setup.types import UPathStr

LocalPathClasses = (PosixPath, WindowsPath, LocalPath)

# also see https://gist.github.com/securifera/e7eed730cbe1ce43d0c29d7cd2d582f4
#    ".gz" is not listed here as it typically occurs with another suffix
# the complete list is at lamindb.core.storage._suffixes
VALID_SIMPLE_SUFFIXES = {
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
    ".qs",  # https://cran.r-project.org/web/packages/qs/vignettes/vignette.html
    ".rds",
    ".pt",
    ".pth",
    ".ckpt",
    ".state_dict",
    ".keras",
    ".pb",
    ".pbtxt",
    ".savedmodel",
    ".pkl",
    ".pickle",
    ".bin",
    ".safetensors",
    ".model",
    ".mlmodel",
    ".mar",
    #
    # with readers (see below)
    #
    ".h5ad",
    ".parquet",
    ".csv",
    ".fcs",
    ".xslx",
    ".zarr",
    ".json",
}
# below gets updated within lamindb because it's frequently changing
VALID_COMPOSITE_SUFFIXES = {".anndata.zarr"}

TRAILING_SEP = (os.sep, os.altsep) if os.altsep is not None else os.sep


def extract_suffix_from_path(path: Path, arg_name: str | None = None) -> str:
    def process_digits(suffix: str):
        if suffix[1:].isdigit():  # :1 to skip the dot
            return ""  # digits are no valid suffixes
        else:
            return suffix

    if len(path.suffixes) <= 1:
        return process_digits(path.suffix)

    total_suffix = "".join(path.suffixes)
    if total_suffix in VALID_SIMPLE_SUFFIXES:
        return total_suffix
    elif total_suffix.endswith(tuple(VALID_COMPOSITE_SUFFIXES)):
        # below seems slow but OK for now
        for suffix in VALID_COMPOSITE_SUFFIXES:
            if total_suffix.endswith(suffix):
                break
        return suffix
    else:
        print_hint = True
        arg_name = "file" if arg_name is None else arg_name  # for the warning
        msg = f"{arg_name} has more than one suffix (path.suffixes), "
        # first check the 2nd-to-last suffix because it might be followed by .gz
        # or another compression-related suffix
        # Alex thought about adding logic along the lines of path.suffixes[-1]
        # in COMPRESSION_SUFFIXES to detect something like .random.gz and then
        # add ".random.gz" but concluded it's too dangerous it's safer to just
        # use ".gz" in such a case
        if path.suffixes[-2] in VALID_SIMPLE_SUFFIXES:
            suffix = "".join(path.suffixes[-2:])
            msg += f"inferring: '{suffix}'"
            # do not print a warning for things like .tar.gz, .fastq.gz
            if path.suffixes[-1] == ".gz":
                print_hint = False
        else:
            suffix = path.suffixes[-1]  # this is equivalent to path.suffix
            msg += (
                f"using only last suffix: '{suffix}' - if you want your composite"
                " suffix to be recognized add it to"
                " lamindb.core.storage.VALID_SIMPLE_SUFFIXES.add()"
            )
        if print_hint:
            logger.hint(msg)
        return process_digits(suffix)


def infer_filesystem(path: UPathStr):
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


# this is needed to avoid CreateBucket permission
class S3FSMap(fsspec.FSMap):
    def __setitem__(self, key, value):
        """Store value in key."""
        key = self._key_to_str(key)
        self.fs.pipe_file(key, fsspec.mapping.maybe_convert(value))


def create_mapper(
    fs,
    url="",
    check=False,
    create=False,
    missing_exceptions=None,
):
    if fsspec.utils.get_protocol(url) == "s3":
        return S3FSMap(
            url, fs, check=check, create=False, missing_exceptions=missing_exceptions
        )
    else:
        return fsspec.FSMap(
            url, fs, check=check, create=create, missing_exceptions=missing_exceptions
        )


def print_hook(size: int, value: int, objectname: str, action: str):
    if size == 0:
        progress_in_percent = 100.0
    else:
        progress_in_percent = (value / size) * 100
    out = f"... {action} {objectname}:" f" {min(progress_in_percent, 100):4.1f}%"
    if "NBPRJ_TEST_NBPATH" not in os.environ:
        end = "\n" if progress_in_percent >= 100 else "\r"
        print(out, end=end)


class ProgressCallback(fsspec.callbacks.Callback):
    def __init__(
        self,
        objectname: str,
        action: Literal["uploading", "downloading", "synchronizing"],
        adjust_size: bool = False,
    ):
        assert action in {"uploading", "downloading", "synchronizing"}

        super().__init__()

        self.action = action
        print_progress = partial(print_hook, objectname=objectname, action=action)
        self.hooks = {"print_progress": print_progress}

        self.adjust_size = adjust_size

    def absolute_update(self, value):
        pass

    def relative_update(self, inc=1):
        pass

    def update_relative_value(self, inc=1):
        if inc != 0:
            self.value += inc
            self.call()
        else:
            # this is specific to http filesystem
            # for some reason the last update is 0 always
            # sometimes the reported result is less that 100%
            # here 100% is forced manually in this case
            if self.value < 1.0 and self.value >= 0.999:
                self.value = self.size
                self.call()

    def branch(self, path_1, path_2, kwargs):
        if self.adjust_size:
            if Path(path_2 if self.action != "uploading" else path_1).is_dir():
                self.size -= 1
        kwargs["callback"] = ChildProgressCallback(self)

    def branched(self, path_1, path_2, **kwargs):
        self.branch(path_1, path_2, kwargs)
        return kwargs["callback"]

    def wrap(self, iterable):
        if self.adjust_size:
            paths = []
            for lpath, rpath in iterable:
                paths.append((lpath, rpath))
                if Path(lpath).is_dir():
                    self.size -= 1
            self.adjust_size = False
            return paths
        else:
            return iterable

    @classmethod
    def requires_progress(
        cls,
        maybe_callback: fsspec.callbacks.Callback | None,
        print_progress: bool,
        objectname: str,
        action: Literal["uploading", "downloading", "synchronizing"],
        **kwargs,
    ):
        if maybe_callback is None:
            if print_progress:
                return cls(objectname, action, **kwargs)
            else:
                return fsspec.callbacks.NoOpCallback()
        return maybe_callback


class ChildProgressCallback(fsspec.callbacks.Callback):
    def __init__(self, parent: ProgressCallback):
        super().__init__()

        self.parent = parent

    def parent_update(self, inc=1):
        self.parent.update_relative_value(inc)

    def relative_update(self, inc=1):
        if self.size != 0:
            self.parent_update(inc / self.size)
        else:
            self.parent_update(1)


def download_to(self, local_path: UPathStr, print_progress: bool = True, **kwargs):
    """Download from self (a destination in the cloud) to the local path."""
    if "recursive" not in kwargs:
        kwargs["recursive"] = True
    if print_progress and "callback" not in kwargs:
        callback = ProgressCallback(
            PurePosixPath(local_path).name, "downloading", adjust_size=True
        )
        kwargs["callback"] = callback

    cloud_path_str = str(self)
    local_path_str = str(local_path)
    # needed due to https://github.com/fsspec/filesystem_spec/issues/1766
    # otherwise fsspec calls fs._ls_real where it reads the body and parses links
    # so the file is downloaded 2 times
    # upath doesn't call fs.ls to infer type, so it is safe to call
    if self.protocol in {"http", "https"} and self.stat().as_info()["type"] == "file":
        self.fs.use_listings_cache = True
        self.fs.dircache[cloud_path_str] = []

    self.fs.download(cloud_path_str, local_path_str, **kwargs)


def upload_from(
    self,
    local_path: UPathStr,
    create_folder: bool | None = None,
    print_progress: bool = True,
    **kwargs,
) -> UPath:
    """Upload from the local path to `self` (a destination in the cloud).

    If the local path is a directory, recursively upload its contents.

    Args:
        local_path: A local path of a file or directory.
        create_folder: Only applies if `local_path` is a directory and then
            defaults to `True`. If `True`, make a new folder in the destination
            using the directory name of `local_path`. If `False`, upload the
            contents of the directory to to the root-level of the destination.
        print_progress: Print progress.

    Returns:
        The destination path.
    """
    local_path = Path(local_path)
    local_path_is_dir = local_path.is_dir()
    if create_folder is None:
        create_folder = local_path_is_dir
    if create_folder and not local_path_is_dir:
        raise ValueError("create_folder can only be True if local_path is a directory")

    if print_progress and "callback" not in kwargs:
        callback = ProgressCallback(local_path.name, "uploading")
        kwargs["callback"] = callback

    source: str | list[str] = local_path.as_posix()
    destination: str | list[str] = self.as_posix()
    if local_path_is_dir:
        size: int = 0
        files: list[str] = []
        for file in (path for path in local_path.rglob("*") if path.is_file()):
            size += file.stat().st_size
            files.append(file.as_posix())
        # see https://github.com/fsspec/s3fs/issues/897
        # here we reduce batch_size for folders bigger than 8 GiB
        # to avoid the problem in the issue
        # the default batch size for this case is 128
        if "batch_size" not in kwargs and size >= 8 * 2**30:
            kwargs["batch_size"] = 64

        if not create_folder:
            source = files
            destination = fsspec.utils.other_paths(
                files, self.as_posix(), exists=False, flatten=False
            )

    # the below lines are to avoid s3fs triggering create_bucket in upload if
    # dirs are present, it allows to avoid the permission error
    if self.protocol == "s3" and local_path_is_dir and create_folder:
        bucket = self.drive
        if bucket not in self.fs.dircache:
            self.fs.dircache[bucket] = [{}]
            assert isinstance(destination, str)
            if not destination.endswith(TRAILING_SEP):  # type: ignore
                destination += "/"
            cleanup_cache = True
        else:
            cleanup_cache = False
    else:
        cleanup_cache = False

    self.fs.upload(source, destination, recursive=create_folder, **kwargs)

    if cleanup_cache:
        # normally this is invalidated after the upload but still better to check
        if bucket in self.fs.dircache:
            del self.fs.dircache[bucket]

    if local_path_is_dir and create_folder:
        return self / local_path.name
    else:
        return self


def synchronize_to(
    origin: UPath,
    destination: Path,
    error_no_origin: bool = True,
    print_progress: bool = False,
    just_check: bool = False,
    **kwargs,
) -> bool:
    """Sync to a local destination path."""
    destination = destination.resolve()
    protocol = origin.protocol
    stat_kwargs = {"expand_info": True} if protocol == "hf" else {}
    origin_str = str(origin)
    try:
        cloud_info = origin.fs.stat(origin_str, **stat_kwargs)
        exists = True
        is_dir = cloud_info["type"] == "directory"
    except FileNotFoundError:
        exists = False

    if not exists:
        warn_or_error = f"The original path {origin} does not exist anymore."
        if destination.exists():
            warn_or_error += (
                f"\nHowever, the local path {destination} still exists, you might want"
                " to reupload the object back."
            )
            logger.warning(warn_or_error)
        elif error_no_origin:
            warn_or_error += "\nIt is not possible to synchronize."
            raise FileNotFoundError(warn_or_error)
        return False

    use_size: bool = False
    # use casting to int to avoid problems when the local filesystem
    # discards fractional parts of timestamps
    if protocol == "s3":
        get_modified = lambda file_stat: int(file_stat["LastModified"].timestamp())
    elif protocol == "gs":
        get_modified = lambda file_stat: int(file_stat["mtime"].timestamp())
    elif protocol == "hf":
        get_modified = lambda file_stat: int(file_stat["last_commit"].date.timestamp())
    else:  #  http etc
        use_size = True
        get_modified = lambda file_stat: file_stat["size"]

    if use_size:
        is_sync_needed = lambda cloud_size, local_stat: cloud_size != local_stat.st_size
    else:
        # no need to cast local_stat.st_mtime to int
        # because if it has the fractional part and cloud_mtime doesn't
        # and they have the same integer part then cloud_mtime can't be bigger
        is_sync_needed = (
            lambda cloud_mtime, local_stat: cloud_mtime > local_stat.st_mtime
        )

    local_paths: list[Path] = []
    cloud_stats: dict[str, int]
    if is_dir:
        cloud_stats = {
            file: get_modified(stat)
            for file, stat in origin.fs.find(
                origin_str, detail=True, **stat_kwargs
            ).items()
        }
        for cloud_path in cloud_stats:
            file_key = PurePosixPath(cloud_path).relative_to(origin.path).as_posix()
            local_paths.append(destination / file_key)
    else:
        cloud_stats = {origin.path: get_modified(cloud_info)}
        local_paths.append(destination)

    local_paths_all: dict[Path, os.stat_result] = {}
    if destination.exists():
        if is_dir:
            local_paths_all = {
                path: path.stat() for path in destination.rglob("*") if path.is_file()
            }
            if not use_size:
                # cast to int to remove the fractional parts
                # there is a problem when a fractional part is allowed on one filesystem
                # but not on the other
                # so just normalize both to int
                cloud_mts_max: int = max(cloud_stats.values())
                local_mts_max: int = int(
                    max(stat.st_mtime for stat in local_paths_all.values())
                )
                if local_mts_max > cloud_mts_max:
                    return False
                elif local_mts_max == cloud_mts_max:
                    if len(local_paths_all) == len(cloud_stats):
                        return False
                    elif just_check:
                        return True
        else:
            local_paths_all = {destination: destination.stat()}

        cloud_files_sync = []
        local_files_sync = []
        for i, (cloud_file, cloud_stat) in enumerate(cloud_stats.items()):
            local_path = local_paths[i]
            if local_path not in local_paths_all or is_sync_needed(
                cloud_stat, local_paths_all[local_path]
            ):
                cloud_files_sync.append(cloud_file)
                local_files_sync.append(local_path.as_posix())
    else:
        cloud_files_sync = list(cloud_stats.keys())
        local_files_sync = [local_path.as_posix() for local_path in local_paths]

    if cloud_files_sync:
        if just_check:
            return True

        callback = ProgressCallback.requires_progress(
            maybe_callback=kwargs.pop("callback", None),
            print_progress=print_progress,
            objectname=destination.name,
            action="synchronizing",
            adjust_size=False,
        )
        origin.fs.download(
            cloud_files_sync,
            local_files_sync,
            recursive=False,
            callback=callback,
            **kwargs,
        )
        if not use_size:
            for i, cloud_file in enumerate(cloud_files_sync):
                cloud_mtime = cloud_stats[cloud_file]
                os.utime(local_files_sync[i], times=(cloud_mtime, cloud_mtime))
    else:
        return False

    if is_dir and local_paths_all:
        for path in (path for path in local_paths_all if path not in local_paths):
            path.unlink()
            parent = path.parent
            if next(parent.iterdir(), None) is None:
                parent.rmdir()

    return True


def modified(self) -> datetime | None:
    """Return modified time stamp."""
    mtime = self.fs.modified(str(self))
    if mtime.tzinfo is None:
        mtime = mtime.replace(tzinfo=timezone.utc)
    return mtime.astimezone().replace(tzinfo=None)


def compute_file_tree(
    path: UPath,
    *,
    level: int = -1,
    only_dirs: bool = False,
    n_max_files_per_dir_and_type: int = 100,
    n_max_files: int = 1000,
    include_paths: set[Any] | None = None,
    skip_suffixes: list[str] | None = None,
) -> tuple[str, int]:
    # .exists() helps to separate files from folders for gcsfs
    # otherwise sometimes it has is_dir() True and is_file() True
    if path.protocol == "gs" and not path.exists():
        raise FileNotFoundError

    space = "    "
    branch = "│   "
    tee = "├── "
    last = "└── "
    if skip_suffixes is None:
        skip_suffixes_tuple = ()
    else:
        skip_suffixes_tuple = tuple(skip_suffixes)  # type: ignore
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

    def inner(dir_path: Path, prefix: str = "", level: int = -1):
        nonlocal n_files, n_directories, suffixes
        if level == 0:
            return
        stripped_dir_path = dir_path.as_posix().rstrip("/")
        # do not iterate through zarr directories
        if stripped_dir_path.endswith(skip_suffixes_tuple):
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
        n_files_per_dir_and_type = defaultdict(lambda: 0)  # type: ignore
        # TODO: pass strict=False to zip with python > 3.9
        for pointer, child_path in zip(pointers, contents, strict=False):  # type: ignore
            if child_path.is_dir():
                if include_dirs and child_path not in include_dirs:
                    continue
                yield prefix + pointer + child_path.name + "/"
                n_directories += 1
                n_files_per_dir_and_type = defaultdict(lambda: 0)
                extension = branch if pointer == tee else space
                yield from inner(child_path, prefix=prefix + extension, level=level - 1)
            elif not only_dirs:
                if include_paths and child_path not in include_paths:
                    continue
                suffix = extract_suffix_from_path(child_path)
                suffixes.add(suffix)
                n_files_per_dir_and_type[suffix] += 1
                n_files += 1
                if n_files_per_dir_and_type[suffix] == n_max_files_per_dir_and_type:
                    yield prefix + "..."
                elif n_files_per_dir_and_type[suffix] > n_max_files_per_dir_and_type:
                    continue
                else:
                    yield prefix + pointer + child_path.name

    folder_tree = ""
    iterator = inner(path, level=level)
    for line in islice(iterator, n_max_files):
        folder_tree += f"\n{line}"
    if next(iterator, None):
        folder_tree += f"\n... only showing {n_max_files} out of {n_files} files"
    directory_info = "directory" if n_directories == 1 else "directories"
    display_suffixes = ", ".join([f"{suffix!r}" for suffix in suffixes])
    suffix_message = f" with suffixes {display_suffixes}" if n_files > 0 else ""
    message = (
        f"{n_directories} sub-{directory_info} &"
        f" {n_files} files{suffix_message}\n{path.resolve()}{folder_tree}"
    )
    return message, n_files


# adapted from: https://stackoverflow.com/questions/9727673
def view_tree(
    path: Path,
    *,
    level: int = 2,
    only_dirs: bool = False,
    n_max_files_per_dir_and_type: int = 100,
    n_max_files: int = 1000,
    include_paths: set[Any] | None = None,
    skip_suffixes: list[str] | None = None,
) -> None:
    """Print a visual tree structure of files & directories.

    Args:
        level: If `1`, only iterate through one level, if `2` iterate through 2
            levels, if `-1` iterate through entire hierarchy.
        only_dirs: Only iterate through directories.
        n_max_files: Display limit. Will only show this many files. Doesn't affect count.
        include_paths: Restrict to these paths.
        skip_suffixes: Skip directories with these suffixes.

    Examples:
        >>> dir_path = ln.core.datasets.generate_cell_ranger_files(
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
    message, _ = compute_file_tree(
        path,
        level=level,
        only_dirs=only_dirs,
        n_max_files=n_max_files,
        n_max_files_per_dir_and_type=n_max_files_per_dir_and_type,
        include_paths=include_paths,
        skip_suffixes=skip_suffixes,
    )
    logger.print(message)


def to_url(upath):
    """Public storage URL.

    Generates a public URL for an object in an S3 bucket using fsspec's UPath,
    considering the bucket's region.

    Args:
    - upath: A UPath object representing an S3 path.

    Returns:
    - A string containing the public URL to the S3 object.
    """
    if upath.protocol != "s3":
        raise ValueError("The provided UPath must be an S3 path.")
    key = "/".join(upath.parts[1:])
    bucket = upath.drive
    region = get_storage_region(upath)
    if region == "us-east-1":
        return f"https://{bucket}.s3.amazonaws.com/{key}"
    else:
        return f"https://{bucket}.s3-{region}.amazonaws.com/{key}"


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
UPath.synchronize = deprecated("synchronize_to")(synchronize_to)
UPath.synchronize_to = synchronize_to
UPath.upload_from = upload_from
UPath.to_url = to_url
UPath.download_to = download_to
UPath.view_tree = view_tree
# unfortunately, we also have to do this for the subclasses
Path.view_tree = view_tree  # type: ignore

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
UPath.rename.__doc__ = """Move file, see fsspec.AbstractFileSystem.mv.

>>> upath = Upath("s3://my-bucket/my-file")
>>> upath.rename(UPath("s3://my-bucket/my-file-renamed"))
>>> upath.rename("my-file-renamed")

>>> upath = Upath("local-folder/my-file")
>>> upath.rename("local-folder/my-file-renamed")
"""
UPath.__doc__ = """Paths: low-level key-value access to files/objects.

Paths are based on keys that offer the typical access patterns of file systems
 and object stores.

>>> upath = UPath("s3://my-bucket/my-folder")
>>> upath.exists()

Args:
    pathlike: A string or Path to a local/cloud file/directory/folder.
"""

logger.debug("upath.UPath has been patched")

# suppress the warning from upath about hf (huggingface) filesystem
# not being explicitly implemented in upath
warnings.filterwarnings(
    "ignore", module="upath", message=".*'hf' filesystem not explicitly implemented.*"
)


# split query params from path string
def _split_path_query(url: str) -> tuple[str, dict]:
    split_result = urlsplit(url)
    query = parse_qs(split_result.query)
    path = split_result._replace(query="").geturl()
    return path, query


class S3QueryPath(S3Path):
    @classmethod
    def _transform_init_args(cls, args, protocol, storage_options):
        args, protocol, storage_options = super()._transform_init_args(
            args, protocol, storage_options
        )
        arg0 = args[0]
        path, query = _split_path_query(str(arg0))
        for param, param_values in query.items():
            if len(param_values) > 1:
                raise ValueError(f"Multiple values for {param} query parameter")
            else:
                param_value = param_values[0]
                if param in storage_options and param_value != storage_options[param]:
                    raise ValueError(
                        f"Incompatible {param} in query and storage_options"
                    )
                storage_options.setdefault(param, param_value)
        if hasattr(arg0, "storage_options"):
            storage_options = {**arg0.storage_options, **storage_options}

        return (path, *args[1:]), protocol, storage_options


register_implementation("s3", S3QueryPath, clobber=True)


def get_storage_region(path: UPathStr) -> str | None:
    upath = UPath(path)

    if upath.protocol != "s3":
        return None

    bucket = upath.drive

    if bucket == "scverse-spatial-eu-central-1":
        return "eu-central-1"
    elif f"s3://{bucket}" in HOSTED_BUCKETS:
        return bucket.replace("lamin-", "")

    from botocore.exceptions import ClientError

    if isinstance(path, str):
        import botocore.session
        from botocore.config import Config

        path_part = path.replace("s3://", "")
        # check for endpoint_url in the path string
        if "?" in path_part:
            path_part, query = _split_path_query(path_part)
            endpoint_url = query.get("endpoint_url", [None])[0]
        else:
            endpoint_url = None
        session = botocore.session.get_session()
        credentials = session.get_credentials()
        if credentials is None or credentials.access_key is None:
            config = Config(signature_version=botocore.session.UNSIGNED)
        else:
            config = None
        s3_client = session.create_client(
            "s3", endpoint_url=endpoint_url, config=config
        )
        try:
            response = s3_client.head_bucket(Bucket=bucket)
        except ClientError as exc:
            response = getattr(exc, "response", {})
            if response.get("Error", {}).get("Code") == "404":
                raise exc
    else:
        upath = get_aws_options_manager()._path_inject_options(upath, {})
        try:
            response = upath.fs.call_s3("head_bucket", Bucket=bucket)
        except Exception as exc:
            cause = getattr(exc, "__cause__", None)
            if not isinstance(cause, ClientError):
                raise exc
            response = getattr(cause, "response", {})
            if response.get("Error", {}).get("Code") == "404":
                raise exc

    region = (
        response.get("ResponseMetadata", {})
        .get("HTTPHeaders", {})
        .get("x-amz-bucket-region", None)
    )
    return region


def create_path(path: UPathStr, access_token: str | None = None) -> UPath:
    upath = UPath(path).expanduser()

    if upath.protocol == "s3":
        # add managed credentials and other options for AWS s3 paths
        return get_aws_options_manager().enrich_path(upath, access_token)

    if upath.protocol in {"http", "https"}:
        # this is needed because by default aiohttp drops a connection after 5 min
        # so it is impossible to download large files
        storage_options = {}
        client_kwargs = upath.storage_options.get("client_kwargs", {})
        if "timeout" not in client_kwargs:
            from aiohttp import ClientTimeout

            client_kwargs = {
                **client_kwargs,
                "timeout": ClientTimeout(sock_connect=30, sock_read=30),
            }
            storage_options["client_kwargs"] = client_kwargs
        # see download_to for the reason
        if "use_listings_cache" not in upath.storage_options:
            storage_options["use_listings_cache"] = True
        if len(storage_options) > 0:
            return UPath(upath, **storage_options)
    return upath


def get_stat_file_cloud(stat: dict) -> tuple[int, str | None, str | None]:
    size = stat["size"]
    hash, hash_type = None, None
    # gs, use md5Hash instead of etag for now
    if "md5Hash" in stat:
        # gs hash is already in base64
        hash = stat["md5Hash"].strip('"=')
        hash_type = "md5"
    # hf
    elif "blob_id" in stat:
        hash = b16_to_b64(stat["blob_id"])
        hash_type = "sha1"
    # s3
    # StorageClass is checked to be sure that it is indeed s3
    # because http also has ETag
    elif "ETag" in stat:
        etag = stat["ETag"]
        if "mimetype" in stat:
            # http
            hash = hash_string(etag.strip('"'))
            hash_type = "md5-etag"
        else:
            # s3
            # small files
            if "-" not in etag:
                # only store hash for non-multipart uploads
                # we can't rapidly validate multi-part uploaded files client-side
                # we can add more logic later down-the-road
                hash = b16_to_b64(etag)
                hash_type = "md5"
            else:
                stripped_etag, suffix = etag.split("-")
                suffix = suffix.strip('"')
                hash = b16_to_b64(stripped_etag)
                hash_type = f"md5-{suffix}"  # this is the S3 chunk-hashing strategy
    if hash is not None:
        hash = hash[:HASH_LENGTH]
    return size, hash, hash_type


def get_stat_dir_cloud(path: UPath) -> tuple[int, str | None, str | None, int]:
    objects = path.fs.find(path.as_posix(), detail=True)
    hash, hash_type = None, None
    compute_list_hash = True
    if path.protocol == "s3":
        accessor = "ETag"
    elif path.protocol == "gs":
        accessor = "md5Hash"
    elif path.protocol == "hf":
        accessor = "blob_id"
    else:
        compute_list_hash = False
    sizes = []
    hashes = []
    for object in objects.values():
        sizes.append(object["size"])
        if compute_list_hash:
            hashes.append(object[accessor].strip('"='))
    size = sum(sizes)
    n_files = len(sizes)
    if compute_list_hash:
        hash, hash_type = hash_from_hashes_list(hashes), "md5-d"
    return size, hash, hash_type, n_files


# is as fast as boto3: https://lamin.ai/laminlabs/lamin-site-assets/transform/krGp3hT1f78N5zKv
def check_storage_is_empty(
    root: UPathStr, *, raise_error: bool = True, account_for_sqlite_file: bool = False
) -> int:
    from ._settings_storage import STORAGE_UID_FILE_KEY

    root_upath = UPath(root)
    root_string = root_upath.as_posix()  # type: ignore
    n_offset_objects = 1  # because of storage_uid.txt file, see mark_storage_root()
    # if the storage_uid.txt was somehow deleted, we restore a dummy version of it
    # because we need it to count files in an empty directory on S3 (otherwise permission error)
    if not (root_upath / STORAGE_UID_FILE_KEY).exists():
        try:
            (root_upath / STORAGE_UID_FILE_KEY).write_text(
                "was deleted, restored during delete"
            )
        except FileNotFoundError:
            # this can happen if the root is a local non-existing path
            pass
    if account_for_sqlite_file:
        n_offset_objects += 1  # the SQLite file is in the ".lamindb" directory
    if root_string.startswith(HOSTED_BUCKETS):
        # in hosted buckets, count across entire root
        directory_string = root_string
    else:
        # in any other storage location, only count in .lamindb
        if not root_string.endswith("/"):
            root_string += "/"
        directory_string = root_string + ".lamindb"
    objects = root_upath.fs.find(directory_string)
    n_files = len(objects)
    n_diff = n_files - n_offset_objects
    ask_for_deletion = (
        "delete them prior to deleting the storage location"
        if raise_error
        else "consider deleting them"
    )
    message = (
        f"'{directory_string}' contains {n_files - n_offset_objects} objects"
        f" - {ask_for_deletion}"
    )
    if n_diff > 0:
        if raise_error:
            raise StorageNotEmpty(message) from None
        else:
            logger.warning(message)
    return n_diff
