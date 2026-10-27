from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from lamindb_setup.core.upath import synchronize_to

# A gcsfs find() entry for a directory marker has no mtime. Including it in the
# sync stats would raise. A zero-byte key ending in "/" is the S3 form.
MARKER_LISTINGS = [
    {
        "bucket/folder": {
            "name": "bucket/folder",
            "size": 0,
            "type": "directory",
        }
    },
    {"bucket/folder/nested/": {"name": "bucket/folder/nested/", "size": 0}},
]


class _FS:
    def __init__(self, listing: dict):
        self.listing = listing
        self.downloaded: list[str] | None = None

    def stat(self, _path: str, **_kwargs):
        return {"type": "directory"}

    def find(self, _path: str, detail: bool = True, **_kwargs):
        return self.listing

    def download(self, sources, dests, **_kwargs):
        self.downloaded = list(sources)
        for dest in dests:
            path = Path(dest)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"data")


class _Origin:
    protocol = "gs"
    path = "bucket/folder"

    def __init__(self, listing: dict):
        self.fs = _FS(listing)

    def __str__(self) -> str:
        return "gs://bucket/folder"


def _assert_empty_dir(path: Path) -> None:
    assert path.is_dir()
    assert next(path.iterdir(), None) is None


@pytest.mark.parametrize("listing", MARKER_LISTINGS)
def test_synchronize_to_marker_only_replaces_local_tree(tmp_path: Path, listing: dict):
    destination = tmp_path / "local"
    nested = destination / "sub"
    nested.mkdir(parents=True)
    (nested / "old.txt").write_text("stale")
    origin = _Origin(listing)

    assert synchronize_to(origin, destination) is True

    _assert_empty_dir(destination)
    assert origin.fs.downloaded is None


@pytest.mark.parametrize("listing", MARKER_LISTINGS)
def test_synchronize_to_marker_only_keeps_empty_dir(tmp_path: Path, listing: dict):
    destination = tmp_path / "local"
    destination.mkdir()

    assert synchronize_to(_Origin(listing), destination) is False
    assert synchronize_to(_Origin(listing), destination, just_check=True) is False
    _assert_empty_dir(destination)


@pytest.mark.parametrize("listing", MARKER_LISTINGS)
def test_synchronize_to_marker_only_creates_parents(tmp_path: Path, listing: dict):
    destination = tmp_path / "missing" / "parents" / "local"
    origin = _Origin(listing)

    assert synchronize_to(origin, destination, just_check=True) is True
    assert not destination.exists()

    assert synchronize_to(origin, destination) is True
    _assert_empty_dir(destination)
    assert origin.fs.downloaded is None


def test_synchronize_to_marker_only_replaces_file(tmp_path: Path):
    destination = tmp_path / "local"
    destination.write_text("not a directory")
    origin = _Origin(MARKER_LISTINGS[0])

    assert synchronize_to(origin, destination, just_check=True) is True
    assert destination.is_file()

    assert synchronize_to(origin, destination) is True
    _assert_empty_dir(destination)


def test_synchronize_to_marker_only_just_check_leaves_files(tmp_path: Path):
    destination = tmp_path / "local"
    destination.mkdir()
    (destination / "old.txt").write_text("stale")
    origin = _Origin(MARKER_LISTINGS[0])

    assert synchronize_to(origin, destination, just_check=True) is True
    assert (destination / "old.txt").read_text() == "stale"
    assert origin.fs.downloaded is None


def test_synchronize_to_skips_markers_and_downloads_real_file(tmp_path: Path):
    destination = tmp_path / "local"
    mtime = datetime(2024, 1, 2, tzinfo=timezone.utc)
    listing = {
        "bucket/folder": {"name": "bucket/folder", "size": 0, "type": "directory"},
        "bucket/folder/nested/": {"name": "bucket/folder/nested/", "size": 0},
        "bucket/folder/data.txt": {
            "name": "bucket/folder/data.txt",
            "size": 4,
            "type": "file",
            "mtime": mtime,
        },
    }
    origin = _Origin(listing)

    assert synchronize_to(origin, destination) is True

    assert origin.fs.downloaded == ["bucket/folder/data.txt"]
    assert (destination / "data.txt").read_bytes() == b"data"
