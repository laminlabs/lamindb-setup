---
execute_via: python
---

# Test cache management

```python
import os
from dotenv import dotenv_values
from platformdirs import user_config_dir

# otherwise writing to system_settings_file() fails
os.environ["XDG_CONFIG_DIRS"] = user_config_dir()

import pytest
import lamindb_setup as ln_setup
from lamindb_setup._cache import set_cache_dir
from lamindb_setup.core._settings_store import (
    platform_user_storage_settings_file,
    system_settings_file,
)
from lamindb_setup.core._settings_load import load_cache_path_from_settings

ln_setup.login("testuser2")
```

```python
if platform_user_storage_settings_file().exists():
    platform_user_storage_settings_file().unlink()
```

```python
ln_setup.init(storage="s3://lamindb-ci/test-cache", name="test-cache")
```

```python
print(ln_setup.settings)
```

Check that setting the cache dir works.

```python
assert not platform_user_storage_settings_file().exists()
```

```python
initial_cache_dir = ln_setup.settings.cache_dir

system_cache_dir = initial_cache_dir.parent / "Cache_system"
system_cache_dir.mkdir(parents=True, exist_ok=True)
```

You can specify system-wide cache dir via system settings file, it has the lowest priority.

```python
system_settings = system_settings_file()
system_settings.parent.mkdir(parents=True, exist_ok=True)

with open(system_settings, "w") as f:
    f.write(f"lamindb_cache_path={system_cache_dir.as_posix()}")
```

```python
ln_setup.settings._cache_dir = None

assert ln_setup.settings.cache_dir == system_cache_dir
```

```python
new_cache_dir = initial_cache_dir.parent / "Cache1"
```

```python
with pytest.raises(ValueError) as error:
    set_cache_dir("./")
assert error.exconly() == "ValueError: A path to the cache dir should be absolute."
```

```python
set_cache_dir(new_cache_dir)

ln_setup.settings._cache_dir = (
    None  # just reset to check that the cache dir is loaded correctly
)
```

```python
platform_user_storage_settings = platform_user_storage_settings_file()

assert platform_user_storage_settings.exists()

lamindb_cache_path = dotenv_values(platform_user_storage_settings)["lamindb_cache_path"]

assert lamindb_cache_path == new_cache_dir.as_posix(), lamindb_cache_path
```

```python
loaded_cache_path = load_cache_path_from_settings()

assert loaded_cache_path == new_cache_dir, loaded_cache_path
```

```python
assert ln_setup.settings.cache_dir == new_cache_dir, ln_setup.settings.cache_dir
```

```python
assert new_cache_dir.exists()
```

```python
assert new_cache_dir in ln_setup.settings.instance._sqlite_file_local.parents
```

Check clearing the cache dir, also closes the cloud sqlite instance.

```python
exit_status = os.system("lamin settings cache-dir clear")
assert exit_status == 0
```

```python
cache_content = list(new_cache_dir.iterdir())
assert len(cache_content) == 0, cache_content
```

Setting to `"null"` or `None` restores the default directory.

```python
set_cache_dir("null")
```

```python
assert ln_setup.settings.cache_dir == system_cache_dir
```

```python
set_cache_dir(None)
```

```python
assert ln_setup.settings.cache_dir == system_cache_dir
```

```python
system_settings.unlink()
```

```python
ln_setup.settings._cache_dir = None
assert ln_setup.settings.cache_dir == initial_cache_dir
```

Setting the cache dir through CLI doesn't affect the currently loaded settings because it is done in another process and the settings only check the cache dir in `settings.env` on init.

Check CLI.

```python
exit_status = os.system("lamin settings cache-dir get")
assert exit_status == 0
```

```python
exit_status = os.system("lamin settings cache-dir set 'null'")
assert exit_status == 0
```

Clean up.

```python
platform_user_storage_settings_file().unlink()
ln_setup.delete("test-cache", force=True)
```
