import pytest
from lamindb_setup import settings
from lamindb_setup.core._settings import SetupPaths
from lamindb_setup.core.upath import UPath


def test_lamin_cache_dir_must_be_absolute(monkeypatch):
    monkeypatch.setenv("LAMIN_CACHE_DIR", "relative/cache")
    with pytest.raises(ValueError) as error:
        _ = settings.cache_dir
    assert "LAMIN_CACHE_DIR must be a valid absolute path" in str(error.value)


def test_cloud_to_local_no_update():
    cache_dir = settings.cache_dir
    assert (
        SetupPaths.cloud_to_local_no_update("s3://bucket/uid/file.txt")
        == cache_dir / "bucket/uid/file.txt"
    )
    assert (
        SetupPaths.cloud_to_local_no_update(
            "s3://bucket/uid/file.txt", cache_key="instance/data/file.txt"
        )
        == cache_dir / "instance/data/file.txt"
    )


@pytest.mark.parametrize(
    "cache_key",
    ["../" * 4 + "tmp/escape.txt", "/tmp/escape.txt", "data/../../../tmp/escape.txt"],
)
def test_cloud_to_local_no_update_outside_cache(cache_key):
    with pytest.raises(ValueError) as error:
        SetupPaths.cloud_to_local_no_update("s3://bucket/uid/file.txt", cache_key)
    assert "resolves outside the cache directory" in str(error.value)


def test_cloud_to_local_no_update_local_filepath(tmp_path):
    # a local filepath is returned as is, the cache key is ignored
    filepath = tmp_path / "file.txt"
    assert (
        SetupPaths.cloud_to_local_no_update(filepath, "../escape.txt").as_posix()
        == filepath.as_posix()
    )


def test_upath_cache_cloud_path(monkeypatch):
    cached_paths: list[tuple[str, str]] = []

    def fake_sync(self, destination, **kwargs):
        cached_paths.append((self.as_posix(), destination.as_posix()))
        return True

    monkeypatch.setattr(UPath, "synchronize_to", fake_sync, raising=False)
    filepath = UPath("s3://bucket/uid/file.txt")

    local_path = filepath.cache()

    assert local_path == settings.cache_dir / "bucket/uid/file.txt"
    assert cached_paths == [
        (
            "s3://bucket/uid/file.txt",
            (settings.cache_dir / "bucket/uid/file.txt").as_posix(),
        )
    ]
