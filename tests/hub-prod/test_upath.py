from __future__ import annotations

from pathlib import Path

from lamindb_setup.core.upath import ProgressCallback, UPath, create_path


def test_create_path():
    upath = UPath("s3://lamindb-ci/xyz/", default_fill_cache=False)
    assert "default_fill_cache" in upath._kwargs

    upath = create_path(upath)
    # test option inheritance
    assert "default_fill_cache" in upath._kwargs
    # test cache_regions setting for s3
    assert "cache_regions" in upath._kwargs
    # test removal of training slash
    assert upath.as_posix()[-1] != "/"
    assert (
        UPath("s3://lamindb-ci/xyz").as_posix()
        == create_path("s3://lamindb-ci/xyz/").as_posix()
    )


def test_progress_callback_size():
    pcb = ProgressCallback("test", "downloading", adjust_size=True)
    pcb.set_size(10)

    cwd = str(Path.cwd())
    paths = zip([cwd, cwd], [cwd, cwd])
    # adjust size for directories in path list
    assert pcb.wrap(paths) == [(cwd, cwd), (cwd, cwd)]
    assert pcb.size == 8
    assert not pcb.adjust_size

    pcb.adjust_size = True
    pcb.branch(cwd, cwd, {})

    assert pcb.size == 7
