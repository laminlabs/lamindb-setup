from __future__ import annotations

from pathlib import Path

import pytest
from lamindb_setup.core.upath import ProgressCallback, UPath, create_path


def test_view_tree():
    with pytest.raises(FileNotFoundError):
        UPath("gs://no-such-bucket-surely-145/folder").view_tree()


def test_trailing_slash():
    assert UPath("s3://bucket/key/").path[-1] != "/"
    assert (UPath("s3://bucket/") / "key/").path[-1] != "/"


def test_storage_options_s3():
    upath = UPath("s3://bucket/key?option2=option2", option1="option1")
    assert upath.storage_options["option1"] == "option1"
    assert upath.storage_options["option2"] == "option2"
    upath = UPath(upath, option2="option2_c", option3="option3")
    assert upath.storage_options["option1"] == "option1"
    assert upath.storage_options["option2"] == "option2_c"
    assert upath.storage_options["option3"] == "option3"

    with pytest.raises(ValueError):
        UPath("s3://bucket?option=option1", option="option2")
    with pytest.raises(ValueError):
        UPath("s3://bucket?option=option1&option=option2")


def test_create_path():
    upath = UPath("s3://lamindb-ci/xyz/", default_fill_cache=False)
    assert "default_fill_cache" in upath.storage_options

    upath = create_path(upath)
    # test option inheritance
    assert not upath.storage_options["default_fill_cache"]
    # test storage_option settings for s3 added inside create_path
    assert upath.storage_options["cache_regions"]
    assert not upath.storage_options["version_aware"]
    assert upath.storage_options["use_listings_cache"]
    # test removal of training slash
    assert upath.as_posix()[-1] != "/"
    assert (
        UPath("s3://lamindb-ci/xyz").as_posix()
        == create_path("s3://lamindb-ci/xyz/").as_posix()
    )
    # test endpoint_url
    upath = create_path("s3://bucket/key?endpoint_url=http://localhost:8000/s3")
    assert upath.as_posix() == "s3://bucket/key"
    assert upath.storage_options["endpoint_url"] == "http://localhost:8000/s3"
    # test http
    upath = create_path("http://some_url.com/")
    assert upath.storage_options["use_listings_cache"]
    assert "timeout" in upath.storage_options["client_kwargs"]
    # test R2
    upath = create_path("s3://bucket/key?endpoint_url=https://r2.cloudflarestorage.com")
    assert upath.as_posix() == "s3://bucket/key"
    assert upath.storage_options["fixed_upload_size"]


def test_progress_callback_size():
    pcb = ProgressCallback("test", "downloading", adjust_size=True)
    pcb.set_size(10)

    cwd = str(Path.cwd())
    paths = zip([cwd, cwd], [cwd, cwd], strict=False)
    # adjust size for directories in path list
    assert pcb.wrap(paths) == [(cwd, cwd), (cwd, cwd)]
    assert pcb.size == 8
    assert not pcb.adjust_size

    pcb.adjust_size = True
    pcb.branch(cwd, cwd, {})

    assert pcb.size == 7
