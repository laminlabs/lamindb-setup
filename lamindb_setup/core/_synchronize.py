import os
from pathlib import Path, PurePosixPath

import fsspec
from lamin_utils import logger
from upath import UPath

from .upath import ProgressCallback


def synchronize(
    origin: UPath,
    destination: Path,
    error_no_origin: bool = True,
    print_progress: bool = False,
    just_check: bool = False,
) -> bool:
    """Sync to a local destination path."""
    protocol = origin.protocol
    try:
        cloud_info = origin.stat().as_info()
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
    if protocol == "s3":
        get_modified = lambda file_stat: file_stat["LastModified"].timestamp()
    elif protocol == "gs":
        get_modified = lambda file_stat: file_stat["mtime"].timestamp()
    elif protocol == "hf":
        get_modified = lambda file_stat: file_stat["last_commit"].date.timestamp()
    else:  #  http etc
        use_size = True

    if use_size:
        is_sync_needed = (
            lambda cloud_stat, local_stat: cloud_stat["size"] != local_stat.st_size
        )
    else:
        is_sync_needed = (
            lambda cloud_stat, local_stat: get_modified(cloud_stat)
            > local_stat.st_mtime
        )

    local_paths: list[Path] = []
    cloud_stats: dict
    if is_dir:
        cloud_stats = origin.fs.find(origin.as_posix(), detail=True)
        for cloud_path in cloud_stats:
            file_key = PurePosixPath(cloud_path).relative_to(origin.path).as_posix()
            local_paths.append(destination / file_key)
    else:
        cloud_stats = {origin.path: cloud_info}
        local_paths.append(destination)

    local_paths_all: list[Path] | None = None
    if destination.exists():
        if is_dir:
            local_paths_all = [
                path for path in destination.rglob("*") if path.is_file()
            ]
            if not use_size:
                cloud_mts_max = max(
                    get_modified(cloud_stat) for cloud_stat in cloud_stats.values()
                )
                local_mts_max = max(
                    file.stat().st_mtime
                    for file in destination.rglob("*")
                    if file.is_file()
                )
                if local_mts_max > cloud_mts_max:
                    return False
                elif local_mts_max == cloud_mts_max:
                    if len(local_paths_all) == len(cloud_stats):
                        return False
                    elif just_check:
                        return True

        cloud_files_sync = []
        local_files_sync = []
        for i, (cloud_file, cloud_stat) in enumerate(cloud_stats.items()):
            local_path = local_paths[i]
            if not local_path.exists() or is_sync_needed(cloud_stat, local_path.stat()):
                cloud_files_sync.append(cloud_file)
                local_files_sync.append(local_path.as_posix())
    else:
        cloud_files_sync = list(cloud_stats.keys())
        local_files_sync = [local_path.as_posix() for local_path in local_paths]

    if len(cloud_files_sync) > 0:
        if just_check:
            return True
        if print_progress:
            callback = ProgressCallback(
                destination.name, "synchronizing", adjust_size=False
            )
        else:
            callback = fsspec.callbacks.NoOpCallback()
        origin.fs.download(
            cloud_files_sync, local_files_sync, recursive=False, callback=callback
        )
        if not use_size:
            for i, cloud_file in enumerate(cloud_files_sync):
                cloud_mtime = get_modified(cloud_stats[cloud_file])
                os.utime(local_files_sync[i], times=(cloud_mtime, cloud_mtime))
    else:
        return False

    if (
        is_dir
        and local_paths_all is not None
        and len(local_paths_all) > len(local_paths)
    ):
        redundant_paths = (path for path in local_paths_all if path not in local_paths)
        for path in redundant_paths:
            path.unlink()
            parent = path.parent
            if next(parent.iterdir(), None) is None:
                parent.rmdir()

    return True
