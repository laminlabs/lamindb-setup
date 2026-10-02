---
execute_via: python
---

# Check synchronization of local and cloud files and directories

```python
import os

instance_name = "test-sqlite-sync"
!lamin connect {instance_name}
!yes | lamin delete {instance_name}
```

```python
from lamindb_setup import init, settings
from lamindb_setup.core.upath import UPath, LocalPathClasses
import shutil
import time
import pytest
```

```python
init(
    storage=f"s3://lamindb-ci/{instance_name}",
    name=instance_name,
)
```

Set everything up before starting the tests

```python
dir_sync = settings.storage.root / "dir_sync"
dir_sync.fs.invalidate_cache()
if dir_sync.is_dir():
    dir_sync.rmdir()
assert not dir_sync.exists()

(dir_sync / "file1").touch()
(dir_sync / "file2").touch()

assert dir_sync.is_dir()
```

```python
dir_sync_local = settings.paths.cloud_to_local_no_update(dir_sync)
if dir_sync_local.is_dir():
    for file in dir_sync_local.iterdir():
        file.unlink()
    dir_sync_local.rmdir()
assert not dir_sync_local.exists()
```

```python
num_files = lambda directory: len(
    [file for file in directory.rglob("*") if file.is_file()]
)
```

```python
assert num_files(dir_sync) == 2
```

Test `cloud_to_local_no_update` paths

```python
test_local_path = UPath("./some/local/path")
assert settings.paths.cloud_to_local_no_update(test_local_path) == test_local_path
assert (
    settings.paths.cloud_to_local_no_update(test_local_path.as_posix())
    == test_local_path
)
assert (
    settings.paths.cloud_to_local_no_update(test_local_path, cache_key="some/cache/key")
    == test_local_path
)
```

```python
assert (
    settings.paths.cloud_to_local_no_update(dir_sync)
    == settings.cache_dir / f"lamindb-ci/{instance_name}/dir_sync"
)
assert (
    settings.paths.cloud_to_local_no_update(dir_sync.as_posix())
    == settings.cache_dir / f"lamindb-ci/{instance_name}/dir_sync"
)

assert (
    settings.paths.cloud_to_local_no_update(dir_sync, cache_key="dir_cache/key")
    == settings.cache_dir / "dir_cache/key"
)
assert (
    settings.paths.cloud_to_local_no_update(
        dir_sync.as_posix(), cache_key="dir_cache/key"
    )
    == settings.cache_dir / "dir_cache/key"
)
```

```python
# for http urls
http_path = UPath(
    "https://raw.githubusercontent.com/laminlabs/lamindb-setup/refs/heads/main/README.md"
)
assert http_path.protocol == "https"

http_stat = http_path.stat()
assert http_stat.st_size != 0
assert http_stat.st_mtime == 0
assert http_stat.as_info()["type"] == "file"
```

```python
http_key = "raw.githubusercontent.com/laminlabs/lamindb-setup/refs/heads/main/README.md"

assert (
    settings.paths.cloud_to_local_no_update(http_path) == settings.cache_dir / http_key
)
assert (
    settings.paths.cloud_to_local_no_update(str(http_path))
    == settings.cache_dir / http_key
)
assert (
    settings.paths.cloud_to_local_no_update(http_path, cache_key="check/README.md")
    == settings.cache_dir / "check/README.md"
)
```

Test `cloud_to_local` with `cache_key`

```python
dir_sync_local = settings.paths.cloud_to_local_no_update(
    dir_sync.as_posix(), cache_key="dir_cache/key"
)
assert dir_sync_local == settings.cache_dir / "dir_cache/key"

assert dir_sync.synchronize_to(dir_sync_local, just_check=True)
assert not dir_sync_local.exists()
```

```python
assert dir_sync.synchronize_to(dir_sync_local, just_check=False)
assert dir_sync_local.is_dir()
assert num_files(dir_sync_local) == 2
```

```python
for file in dir_sync_local.iterdir():
    file.unlink()
dir_sync_local.rmdir()
```

Test `cloud_to_local` for http

```python
http_local = settings.paths.cloud_to_local(http_path)
```

```python
assert isinstance(http_local, LocalPathClasses)
assert http_local.stat().st_size == http_path.stat().st_size
```

```python
http_local_mtime = http_local.stat().st_mtime
# no changes here because the file exists already
assert settings.paths.cloud_to_local(http_path).stat().st_mtime == http_local_mtime
```

```python
http_local.unlink()
```

```python
# test download_to
http_path.download_to(http_local, print_progress=True)
assert not http_path.synchronize_to(http_local)
```

```python
http_local.unlink()
```

Test sync of general files and directories

```python
dir_sync_local = settings.paths.cloud_to_local(dir_sync)
assert not dir_sync.synchronize_to(dir_sync_local)
```

```python
assert dir_sync_local.is_dir()
assert num_files(dir_sync_local) == 2
```

```python
for obj in dir_sync_local.rglob("*"):
    if obj.is_file():
        obj.unlink()
assert num_files(dir_sync_local) == 0
```

```python
assert dir_sync.synchronize_to(dir_sync_local)
assert num_files(dir_sync_local) == 2
```

```python
for file in ("file1", "file2"):
    assert (dir_sync_local / file).stat().st_mtime == (
        dir_sync / file
    ).modified.timestamp()
```

```python
local_file = dir_sync_local / "file1"
local_file.unlink()
assert not local_file.exists()
assert num_files(dir_sync_local) == 1
```

```python
dir_sync_local = settings.paths.cloud_to_local(dir_sync)
assert local_file.exists()
assert num_files(dir_sync_local) == 2
```

```python
for file in ("file1", "file2"):
    cloud_file = dir_sync / file
    local_file = dir_sync_local / file

    cloud_mtime = cloud_file.modified.timestamp()
    os.utime(local_file, times=(cloud_mtime - 1, cloud_mtime - 1))

    assert local_file.stat().st_mtime < cloud_mtime
```

```python
dir_sync_local = settings.paths.cloud_to_local(dir_sync)

for file in ("file1", "file2"):
    assert (dir_sync_local / file).stat().st_mtime == (
        dir_sync / file
    ).modified.timestamp()
```

```python
(dir_sync_local / "file1").unlink()

local_file_new = dir_sync_local / "test/file3"
local_file_new_parent = local_file_new.parent
local_file_new_parent.mkdir()
local_file_new.touch()
assert num_files(dir_sync_local) == 2
```

```python
dir_sync_local = settings.paths.cloud_to_local(dir_sync)
assert num_files(dir_sync_local) == 2
assert local_file_new.exists()
```

```python
time.sleep(1)
cloud_file = dir_sync / "file1"
# update cloud timestamp
cloud_file.fs.touch(cloud_file.as_posix(), truncate=True)

assert cloud_file.modified.timestamp() > local_file_new.stat().st_mtime
```

```python
dir_sync_local = settings.paths.cloud_to_local(dir_sync)

assert num_files(dir_sync_local) == 2, list(dir_sync_local.rglob("*"))
assert not local_file_new.exists()
assert not local_file_new_parent.exists()

for file in ("file1", "file2"):
    assert (dir_sync_local / file).stat().st_mtime == (
        dir_sync / file
    ).modified.timestamp()
```

```python
for file in dir_sync_local.iterdir():
    file.unlink()
```

check with `boto3` download

```python
file_sync = dir_sync / "file1"
file_sync_local = dir_sync_local / "file1"

assert file_sync.name == file_sync_local.name
assert not file_sync_local.exists()
```

```python
file_sync.synchronize_to(file_sync_local, print_progress=True, use_boto3=True)

assert file_sync_local.exists()
assert file_sync_local.stat().st_mtime == file_sync.modified.timestamp()
```

```python
file_sync_local.unlink()
```

```python
dir_sync_local.rmdir()
dir_sync.rmdir()
```

Get the paths to the cloud and local sqlite databases.

```python
sqlite_file = settings.instance._sqlite_file
```

```python
sqlite_file
```

Remote SQLite file does exists upon instance init:

```python
assert settings.instance._sqlite_file.exists()
```

Now mimic a new user who loads the instance (this runs 4s):

```python
settings.instance._update_local_sqlite_file()
```

Get the mere filepath of the local file, without any update:

```python
cache_file = settings.paths.cloud_to_local_no_update(sqlite_file)
```

```python
cache_file
```

Delete the local sqlite file:

```python
cache_file.unlink()
```

```python
assert not cache_file.exists()
```

Update the local version of the sqlite file:

```python
settings.instance._update_local_sqlite_file()
```

```python
assert cache_file.exists()
```

If the local sqlite database is older than the cloud one, the cloud database replaces the local sqlite database file.

```python
cloud_mtime = sqlite_file.modified.timestamp()
```

```python
cloud_mtime
```

```python
os.utime(cache_file, times=(cloud_mtime - 1, cloud_mtime - 1))
```

```python
assert cache_file.stat().st_mtime < sqlite_file.modified.timestamp()
```

```python
settings.instance._update_local_sqlite_file()
```

```python
assert cache_file.stat().st_mtime == sqlite_file.modified.timestamp()
```

check sync of huggingface dataset

```python
hf_path = UPath("hf://datasets/Koncopd/lamindb-test")
hf_path_local = settings.paths.cloud_to_local(hf_path)
```

```python
assert isinstance(hf_path_local, LocalPathClasses)
assert hf_path_local.is_dir()
assert num_files(hf_path) == num_files(hf_path_local)
```

```python
shutil.rmtree(hf_path_local)
```

```python
hf_path = UPath("hf://datasets/Koncopd/lamindb-test@main/anndata/pbmc68k_test.h5ad")
hf_path_local = settings.paths.cloud_to_local_no_update(hf_path)
assert isinstance(hf_path_local, LocalPathClasses)
```

```python
assert hf_path.synchronize_to(hf_path_local, just_check=True)
assert not hf_path_local.exists()
```

```python
assert hf_path.synchronize_to(hf_path_local)
assert hf_path_local.is_file()
```

```python
hf_path_local.unlink()
```

```python
hf_path = UPath("hf://datasets/Koncopd/lamindb-test@main/does_not_exist.file")
with pytest.raises(FileNotFoundError):
    hf_path.synchronize_to(UPath("./does_not_exist.file"), error_no_origin=True)
```

```python tags=["hide-cell"]
!yes | lamin delete {instance_name}
```
