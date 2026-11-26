import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from lamindb_setup import settings
from lamindb_setup._cache import clear_cache_dir, set_cache_dir
from lamindb_setup.core import _settings as settings_module
from lamindb_setup.core import _settings_store as settings_store
from lamindb_setup.core._settings import SetupPaths, SetupSettings
from lamindb_setup.core._settings_load import load_cache_path_from_settings
from lamindb_setup.core._settings_storage import StorageSettings
from lamindb_setup.core.upath import LocalPathClasses, UPath


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


@pytest.mark.parametrize(
    ("cloud_path", "cache_key", "expected_rel"),
    [
        ("s3://bucket/uid/file.txt", None, "bucket/uid/file.txt"),
        (
            "gs://bucket/nested/nonexistent/file.txt",
            None,
            "bucket/nested/nonexistent/file.txt",
        ),
        ("https://example.com/path/to/file.txt", None, "example.com/path/to/file.txt"),
        ("http://example.com/a/b/file.txt", None, "example.com/a/b/file.txt"),
        (
            "s3://bucket/uid/file.txt",
            "instance/deep/sub/file.txt",
            "instance/deep/sub/file.txt",
        ),
        ("s3://bucket/uid/file.txt", "a/b/../c/file.txt", "a/c/file.txt"),
    ],
)
def test_cloud_to_local_no_update_symlinked_cache_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cloud_path: str,
    cache_key: str | None,
    expected_rel: str,
):
    real_cache = (tmp_path / "real_cache").resolve()
    real_cache.mkdir()
    link_cache = tmp_path / "link_cache"
    link_cache.symlink_to(real_cache)
    monkeypatch.setenv("LAMIN_CACHE_DIR", str(link_cache))

    result = SetupPaths.cloud_to_local_no_update(cloud_path, cache_key=cache_key)

    assert isinstance(result, UPath)
    assert isinstance(result, Path)
    assert isinstance(result, LocalPathClasses)
    assert result == UPath(real_cache) / expected_rel
    assert result.is_relative_to(UPath(real_cache))
    assert result.is_relative_to(settings.cache_dir)


def test_cloud_to_local_no_update_symlinked_ancestor_not_yet_created(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    real_parent = (tmp_path / "real_parent").resolve()
    real_parent.mkdir()
    link_parent = tmp_path / "link_parent"
    link_parent.symlink_to(real_parent)
    cache_dir_env = link_parent / "nested" / "cache"
    assert not cache_dir_env.exists()
    monkeypatch.setenv("LAMIN_CACHE_DIR", str(cache_dir_env))

    result = SetupPaths.cloud_to_local_no_update("s3://bucket/uid/sub/file.txt")
    expected_root = real_parent / "nested" / "cache"

    assert isinstance(result, UPath)
    assert expected_root.is_dir()
    assert result == UPath(expected_root) / "bucket/uid/sub/file.txt"
    assert result.is_relative_to(UPath(expected_root))


def test_cloud_to_local_no_update_symlinked_default_cache_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    real_cache = (tmp_path / "volume" / "lamindb").resolve()
    real_cache.mkdir(parents=True)
    link_cache_root = tmp_path / "dot_cache"
    link_cache_root.symlink_to(real_cache.parent)
    monkeypatch.delenv("LAMIN_CACHE_DIR", raising=False)
    monkeypatch.setattr(settings, "_cache_dir", None)
    monkeypatch.setattr(settings_module, "load_cache_path_from_settings", lambda: None)
    monkeypatch.setattr(
        settings_module,
        "_default_cache_dir",
        lambda: UPath(link_cache_root / "lamindb"),
    )

    result = SetupPaths.cloud_to_local_no_update("s3://bucket/uid/file.txt")

    assert isinstance(result, UPath)
    assert result == UPath(real_cache) / "bucket/uid/file.txt"
    assert result.is_relative_to(UPath(real_cache))
    assert settings.cache_dir == UPath(real_cache)


def test_cloud_to_local_no_update_unresolved_cache_dir_property(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    real_cache = (tmp_path / "real_cache").resolve()
    real_cache.mkdir()
    link_cache = tmp_path / "link_cache"
    link_cache.symlink_to(real_cache)
    monkeypatch.setattr(
        SetupSettings, "cache_dir", property(lambda _self: UPath(link_cache))
    )

    result = SetupPaths.cloud_to_local_no_update("s3://bucket/uid/file.txt")

    assert isinstance(result, UPath)
    assert result == UPath(real_cache) / "bucket/uid/file.txt"
    assert result.is_relative_to(UPath(real_cache))


def test_upath_cache_and_storage_settings_with_symlinked_cache_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    real_cache = (tmp_path / "real_cache").resolve()
    real_cache.mkdir()
    link_cache = tmp_path / "link_cache"
    link_cache.symlink_to(real_cache)
    monkeypatch.setenv("LAMIN_CACHE_DIR", str(link_cache))

    synced: list[tuple[str, str]] = []

    def fake_sync(self, destination, **kwargs):
        synced.append((self.as_posix(), destination.as_posix()))
        return True

    monkeypatch.setattr(UPath, "synchronize_to", fake_sync, raising=False)

    cloud_file = UPath("s3://bucket/uid/file.txt")
    local_path = cloud_file.cache()  # type: ignore[attr-defined]
    expected = UPath(real_cache) / "bucket/uid/file.txt"
    assert local_path == expected
    assert local_path.parent.is_dir()
    assert synced == [("s3://bucket/uid/file.txt", expected.as_posix())]

    ssettings = StorageSettings("s3://bucket")
    assert ssettings.cloud_to_local_no_update(cloud_file) == expected


def test_cloud_to_local_no_update_symlinked_ancestor_readonly_nonexistent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    real_parent = (tmp_path / "real_parent").resolve()
    real_parent.mkdir()
    link_parent = tmp_path / "link_parent"
    link_parent.symlink_to(real_parent)
    cache_dir_env = link_parent / "unwritable" / "cache"
    monkeypatch.setenv("LAMIN_CACHE_DIR", str(cache_dir_env))

    def fail_mkdir(self, *args, **kwargs):
        raise PermissionError("read-only filesystem")

    monkeypatch.setattr(Path, "mkdir", fail_mkdir)

    result = SetupPaths.cloud_to_local_no_update("s3://bucket/uid/file.txt")
    expected_root = real_parent / "unwritable" / "cache"

    assert isinstance(result, UPath)
    assert not expected_root.exists()
    assert result == UPath(expected_root) / "bucket/uid/file.txt"
    assert result.is_relative_to(UPath(expected_root))


def test_cloud_to_local_no_update_dangling_symlink_cache_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    target_cache = (tmp_path / "not_yet_created_cache").resolve()
    link_cache = tmp_path / "dangling_link_cache"
    link_cache.symlink_to(target_cache)
    assert not target_cache.exists()
    monkeypatch.setenv("LAMIN_CACHE_DIR", str(link_cache))

    result = SetupPaths.cloud_to_local_no_update("s3://bucket/uid/file.txt")

    assert isinstance(result, UPath)
    assert target_cache.is_dir()
    assert result == UPath(target_cache) / "bucket/uid/file.txt"
    assert result.is_relative_to(UPath(target_cache))


@pytest.mark.parametrize("use_symlink_root", [False, True])
@pytest.mark.parametrize(
    ("cloud_path", "cache_key"),
    [
        ("s3://bucket/uid/file.txt", "../secret.txt"),
        ("s3://bucket/uid/file.txt", "a/b/../../../secret.txt"),
        ("s3://bucket/uid/file.txt", "/etc/passwd"),
        ("s3://bucket/uid/file.txt", "/bucket/file.txt"),
        ("s3://bucket/uid/file.txt", "../real_cache_other/secret.txt"),
        ("s3://bucket/../../secret.txt", None),
        ("https://example.com/../../secret.txt", None),
    ],
)
def test_cloud_to_local_no_update_blocks_traversal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    use_symlink_root: bool,
    cloud_path: str,
    cache_key: str | None,
):
    real_cache = (tmp_path / "real_cache").resolve()
    real_cache.mkdir()
    sibling_cache = (tmp_path / "real_cache_other").resolve()
    sibling_cache.mkdir()
    if use_symlink_root:
        cache_root = tmp_path / "link_cache"
        cache_root.symlink_to(real_cache)
    else:
        cache_root = real_cache
    monkeypatch.setenv("LAMIN_CACHE_DIR", str(cache_root))

    with pytest.raises(ValueError, match="resolves outside the cache directory"):
        SetupPaths.cloud_to_local_no_update(cloud_path, cache_key=cache_key)


@pytest.mark.parametrize("use_symlink_root", [False, True])
def test_cloud_to_local_no_update_blocks_interior_escape_symlinks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, use_symlink_root: bool
):
    real_cache = (tmp_path / "real_cache").resolve()
    real_cache.mkdir()
    outside = (tmp_path / "outside").resolve()
    outside.mkdir()
    sibling = (tmp_path / "real_cache_other").resolve()
    sibling.mkdir()
    if use_symlink_root:
        cache_root = tmp_path / "link_cache"
        cache_root.symlink_to(real_cache)
    else:
        cache_root = real_cache
    monkeypatch.setenv("LAMIN_CACHE_DIR", str(cache_root))

    # Symlink inside the cache pointing outside the real cache root
    (real_cache / "evil_dir").symlink_to(outside)
    # Dangling symlink inside the cache pointing to a non-existent file outside
    (real_cache / "evil_file").symlink_to(outside / "future.txt")
    # Symlink inside the cache pointing to a sibling-prefix directory
    (real_cache / "evil_sibling").symlink_to(sibling)

    for cloud_path, cache_key in [
        ("s3://bucket/uid/file.txt", "evil_dir/secret.txt"),
        ("s3://evil_dir/secret.txt", None),
        ("s3://bucket/uid/file.txt", "evil_file"),
        ("s3://bucket/uid/file.txt", "evil_sibling/secret.txt"),
    ]:
        with pytest.raises(ValueError, match="resolves outside the cache directory"):
            SetupPaths.cloud_to_local_no_update(cloud_path, cache_key=cache_key)


@pytest.fixture
def cache_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.delenv("LAMIN_CACHE_DIR", raising=False)
    monkeypatch.setattr(settings, "_cache_dir", None)
    monkeypatch.setattr(settings_store, "settings_dir", tmp_path)
    monkeypatch.setattr(settings_store, "system_settings_dir", tmp_path)
    return tmp_path.resolve()


@pytest.fixture
def symlinked_default_cache(
    cache_settings: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Path]:
    target = cache_settings / "volume" / "lamindb"
    target.mkdir(parents=True)
    ancestor = cache_settings / "dot_cache"
    ancestor.symlink_to(target.parent, target_is_directory=True)
    alias = ancestor / target.name
    monkeypatch.setattr(settings_module, "_default_cache_dir", lambda: UPath(alias))
    return alias, target


@pytest.mark.parametrize(
    "configured_system_cache", [True, False], ids=["system", "platform"]
)
def test_set_cache_dir_preserves_symlinked_default(
    cache_settings: Path,
    symlinked_default_cache: tuple[Path, Path],
    configured_system_cache: bool,
):
    alias, target = symlinked_default_cache
    if configured_system_cache:
        settings_store.system_settings_file().write_text(f"lamindb_cache_path={alias}")
    original = target / "shared.txt"
    original.write_bytes(b"shared cache data")
    destination = cache_settings / "user_cache"
    assert settings.cache_dir == UPath(target)

    with patch.object(shutil, "rmtree", wraps=shutil.rmtree) as delete:
        set_cache_dir(destination)

    assert (destination / original.name).read_bytes() == b"shared cache data"
    assert target.is_dir(), delete.call_args_list
    assert original.read_bytes() == b"shared cache data"
    assert alias.parent.is_symlink()
    delete.assert_not_called()
    assert load_cache_path_from_settings() == destination
    assert settings.cache_dir == UPath(destination)


@pytest.mark.parametrize("populated", [True, False], ids=["populated", "empty"])
def test_set_cache_dir_reapplying_symlink_and_target(
    cache_settings: Path, populated: bool
):
    target = cache_settings / "real_cache"
    target.mkdir()
    alias = cache_settings / "link_cache"
    alias.symlink_to(target, target_is_directory=True)
    settings_store.platform_user_storage_settings_file().write_text(
        f"lamindb_cache_path={alias}"
    )
    if populated:
        (target / "data.txt").write_bytes(b"cached bytes")
    assert settings.cache_dir == UPath(target)

    with (
        patch.object(shutil, "copytree", wraps=shutil.copytree) as copy,
        patch.object(shutil, "rmtree", wraps=shutil.rmtree) as delete,
    ):
        set_cache_dir(alias)
        set_cache_dir(target)

    assert alias.is_symlink()
    assert alias.samefile(target)
    if populated:
        assert (target / "data.txt").read_bytes() == b"cached bytes"
    else:
        assert list(target.iterdir()) == []
    copy.assert_not_called()
    delete.assert_not_called()
    assert load_cache_path_from_settings() == target
    assert settings.cache_dir == UPath(target)


@pytest.mark.parametrize(
    "from_custom", [False, True], ids=["already-default", "custom"]
)
def test_set_cache_dir_reset_to_symlinked_default(
    cache_settings: Path, symlinked_default_cache: tuple[Path, Path], from_custom: bool
):
    alias, target = symlinked_default_cache
    settings_store.system_settings_file().write_text(f"lamindb_cache_path={alias}")
    (target / "shared.txt").write_bytes(b"shared cache data")
    source = target
    if from_custom:
        source = cache_settings / "custom_cache"
        source.mkdir()
        (source / "custom.txt").write_bytes(b"custom cache data")
    settings_store.platform_user_storage_settings_file().write_text(
        f"lamindb_cache_path={source}"
    )
    assert settings.cache_dir == UPath(source)

    with (
        patch.object(shutil, "copytree", wraps=shutil.copytree) as copy,
        patch.object(shutil, "rmtree", wraps=shutil.rmtree) as delete,
    ):
        set_cache_dir(None)

    assert alias.parent.is_symlink()
    assert (target / "shared.txt").read_bytes() == b"shared cache data"
    if from_custom:
        assert (target / "custom.txt").read_bytes() == b"custom cache data"
        assert not source.exists()
        delete.assert_called_once_with(source)
    else:
        copy.assert_not_called()
        delete.assert_not_called()
    assert settings_store.platform_user_storage_settings_file().read_text() == (
        "lamindb_cache_path=null"
    )
    assert settings.cache_dir == UPath(target)
    assert SetupSettings().cache_dir == UPath(target)


@pytest.mark.parametrize("target_exists", [True, False], ids=["existing", "dangling"])
def test_set_cache_dir_moves_between_symlinked_user_caches(
    cache_settings: Path,
    symlinked_default_cache: tuple[Path, Path],
    target_exists: bool,
):
    _, default_target = symlinked_default_cache
    source = cache_settings / "old_cache"
    source.mkdir()
    (source / "data.txt").write_bytes(b"cached bytes")
    source_alias = cache_settings / "old_link"
    source_alias.symlink_to(source, target_is_directory=True)
    settings_store.platform_user_storage_settings_file().write_text(
        f"lamindb_cache_path={source_alias}"
    )
    target = cache_settings / "new_cache"
    if target_exists:
        target.mkdir()
        (target / "existing.txt").write_bytes(b"existing cache data")
    target_alias = cache_settings / "new_link"
    target_alias.symlink_to(target, target_is_directory=True)
    assert settings.cache_dir == UPath(source)

    with patch.object(shutil, "rmtree", wraps=shutil.rmtree) as delete:
        set_cache_dir(target_alias)

    assert (target / "data.txt").read_bytes() == b"cached bytes"
    if target_exists:
        assert (target / "existing.txt").read_bytes() == b"existing cache data"
    assert target_alias.is_symlink()
    assert target_alias.samefile(target)
    assert source_alias.is_symlink()
    assert not source.exists()
    delete.assert_called_once_with(source)
    assert default_target.is_dir()
    assert load_cache_path_from_settings() == target
    assert settings.cache_dir == UPath(target)


@pytest.mark.parametrize("layout", ["direct", "ancestor", "file-uri"])
def test_clear_cache_dir_retains_symlink(
    cache_settings: Path, monkeypatch: pytest.MonkeyPatch, layout: str
):
    target = cache_settings / "volume" / "real_cache"
    (target / "nested").mkdir(parents=True)
    (target / "nested" / "data.txt").write_bytes(b"cached bytes")
    link = cache_settings / "link"
    if layout == "ancestor":
        link.symlink_to(target.parent, target_is_directory=True)
        alias = link / target.name
    else:
        link.symlink_to(target, target_is_directory=True)
        alias = link
    monkeypatch.setenv(
        "LAMIN_CACHE_DIR", alias.as_uri() if layout == "file-uri" else str(alias)
    )
    monkeypatch.setattr(
        SetupSettings, "instance", SimpleNamespace(_is_cloud_sqlite=False)
    )

    clear_cache_dir()

    assert link.is_symlink()
    assert alias.samefile(target)
    assert target.is_dir()
    assert list(target.iterdir()) == []


def test_lamin_cache_dir_rejects_symlink_loop(
    cache_settings: Path, monkeypatch: pytest.MonkeyPatch
):
    a = cache_settings / "a"
    b = cache_settings / "b"
    a.symlink_to(b, target_is_directory=True)
    b.symlink_to(a, target_is_directory=True)
    monkeypatch.setenv("LAMIN_CACHE_DIR", str(a))

    with pytest.raises(ValueError, match="Cache directory contains a symlink loop"):
        _ = settings.cache_dir

    assert a.readlink() == b
    assert b.readlink() == a
    assert set(cache_settings.iterdir()) == {a, b}
