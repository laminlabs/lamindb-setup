---
execute_via: python
---

# Load hosted instance

```python
import os

is_prod = os.getenv("LAMIN_ENV") in {None, "prod"}

if is_prod:
    from platformdirs import user_config_dir

    # otherwise writing to system_settings_file() fails
    os.environ["XDG_CONFIG_DIRS"] = user_config_dir()

import pytest
import shutil
import lamindb_setup as ln_setup
from lamindb_setup._connect_instance import _connect_cli
from lamindb_setup.core.upath import UPath
from lamindb_setup.core._hub_core import delete_instance
from lamindb_setup.core._hub_client import connect_hub_with_auth
from lamindb_setup.core.upath import StorageNotEmpty
from lamindb_setup.core._aws_options import get_user_aws_options_manager
from lamindb_setup.core._settings_store import system_settings_file
from datetime import datetime, timezone
from aiobotocore.credentials import AioRefreshableCredentials

instance_name = "my-hosted"
assert ln_setup.settings.user.handle == "testuser1"
```

```python
# Set lamindb_s3_anon in system.env for prod test (deleted at end of notebook)
if is_prod:
    system_env = system_settings_file()
    system_env.parent.mkdir(parents=True, exist_ok=True)
    system_env.write_text("lamindb_s3_anon=true\n")
```

```python
_connect_cli(f"testuser1/{instance_name}")  # cover here for cloud sqlite
```

```python
root = ln_setup.settings.storage.root
```

```python
if is_prod:
    # check managed session is present and preserved between re-connects of fs
    session = root.storage_options["session"]
    assert session is not None

    fs = root.fs
    assert session is fs.session
    fs.connect()
    assert session is fs.session
    fs.connect()
    assert session is fs.session
```

```python
root.view_tree()
```

```python
target_path = root / "test_notebooks.py"
target_path.upload_from("test_notebooks.py")
assert target_path.exists()
```

```python
# lamin-hosted-test is not a managed bucket on staging
if is_prod:
    manager = get_user_aws_options_manager()
    # Test that lamindb_s3_anon from system.env enabled anon mode
    assert manager.anon, "lamindb_s3_anon from system.env should enable anon mode"

    credentials = manager._sessions_cache[root.as_posix() + "/"]._credentials

    assert isinstance(credentials, AioRefreshableCredentials)
    # force expiry and corrupt credentials to prove refresh is required
    credentials._expiry_time = datetime(2000, 1, 1, tzinfo=timezone.utc)
    assert credentials.refresh_needed()
    # corrupt to check that refresh is applied before any access
    credentials._access_key = "INVALID"
    credentials._secret_key = "INVALID"
    credentials._token = "INVALID"
    credentials._frozen_credentials = None
```

```python
local_path = UPath("./test_boto3_dl.py")
# fs.stat() inside refreshes the original session even though use_boto3 is True
target_path.synchronize_to(local_path, print_progress=True, use_boto3=True)
assert local_path.exists()
```

```python
if is_prod:
    assert credentials._expiry_time > datetime.now(timezone.utc)
```

```python
test_dir = UPath("./test-dir-upload")
test_dir.mkdir()
(test_dir / "file1").touch()
subdir = test_dir / "subdir"
subdir.mkdir()
(subdir / "file2").touch()
subsubdir = subdir / "subsubdir"
subsubdir.mkdir()
(subsubdir / "file3").touch()
```

```python
target_dir = root / "test-dir-upload"
target_dir.upload_from(test_dir, create_folder=True)  # default

assert target_dir.is_dir()
assert (target_dir / "test-dir-upload").exists()
assert (target_dir / "test-dir-upload/file1").exists()

target_dir.rmdir()
assert not target_dir.exists()
```

```python
target_dir.upload_from(test_dir, create_folder=False)

assert target_dir.is_dir()
assert (target_dir / "file1").exists()
assert not (target_dir / "test-dir-upload").exists()
```

Test that instance can not be deleted from hub:

```python
with pytest.raises(StorageNotEmpty):
    delete_instance(f"testuser1/{instance_name}")
```

Test storage record for the root exists:

```python
hub = connect_hub_with_auth()
```

```python
response = hub.table("storage").select("*").eq("root", root.as_posix()).execute().data
assert len(response) == 1
assert response[0]["is_default"]
```

Delete everything:

```python
local_path.unlink()

shutil.rmtree(test_dir)

target_path.unlink()
assert not target_path.exists()

target_dir.rmdir()
assert not target_dir.exists()

assert ln_setup.settings.storage._mark_storage_root.exists()

ln_setup.delete(instance_name, force=True)
delete_instance(f"testuser1/{instance_name}")

# Remove system.env created at start (prod only)
if is_prod:
    system_env = system_settings_file()
    if system_env.exists():
        system_env.unlink(missing_ok=True)
```

Check that the storage record has been deleted:

```python
response = hub.table("storage").select("*").eq("root", root.as_posix()).execute().data
```

```python
assert len(response) == 0
```
