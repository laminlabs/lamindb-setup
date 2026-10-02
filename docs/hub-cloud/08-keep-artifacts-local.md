---
execute_via: python
---

# Connect with `keep_artifacts_local=True`

```python
# this shouldn't run in a notebook, but in the unit tests
# currently still limited because of inability to load multiple instances

import os
import pytest
import lamindb_setup as ln_setup
from upath import UPath
from pathlib import Path

name = f"keep-artifacts-local-setup-{os.environ['LAMIN_ENV']}"
storage = UPath(f"s3://lamindb-ci/{name}").as_posix()

ln_setup.login("testuser1")
```

```python
!lamin delete --force {name}
```

```python
ln_setup.init(storage=storage)

assert ln_setup.settings.instance.name == name
assert ln_setup.settings.instance.storage.type_is_cloud
assert ln_setup.settings.instance.storage.root_as_str == storage
assert (
    ln_setup.settings.instance._sqlite_file.as_posix() == f"{storage}/.lamindb/lamin.db"
)
```

```python
with pytest.raises(ValueError) as error:
    ln_setup.settings.instance.local_storage
assert (
    error.exconly()
    == "ValueError: `keep_artifacts_local` is False, switch via: ln.setup.settings.instance.keep_artifacts_local = True"
)
```

```python
ln_setup.settings.instance.keep_artifacts_local = True
with pytest.raises(ValueError) as error:
    ln_setup.settings.instance.local_storage
assert error.exconly().startswith(
    "ValueError: No local storage location found in current environment:"
)
```

```python
# now set local storage location
ln_setup.settings.instance.local_storage = "./my_local_storage", "testuser1-laptop"

assert (
    ln_setup.settings.instance.local_storage.root.as_posix()
    == UPath("./my_local_storage").resolve().as_posix()
)
assert (
    ln_setup.settings.instance.local_storage.root / ".lamindb/storage_uid.txt"
).read_text().splitlines()[0] == ln_setup.settings.instance.local_storage.uid
assert ln_setup.settings.instance.local_storage is not None
assert ln_setup.settings.instance.local_storage.region == "testuser1-laptop"
# the remote storage location is still in the regular slot
assert ln_setup.settings.instance.storage.root.as_posix() == storage
```

Another one:

```python
ln_setup.settings.instance.local_storage = "./my_local_storage2", "testuser1-laptop"
```

```python
assert (
    ln_setup.settings.instance.local_storage.root.as_posix()
    == UPath("./my_local_storage2").resolve().as_posix()
)
assert (
    ln_setup.settings.instance.local_storage.root / ".lamindb/storage_uid.txt"
).read_text().splitlines()[0] == ln_setup.settings.instance.local_storage.uid
```

See whether we can repeat this:

```python
ln_setup.settings.instance.local_storage = "./my_local_storage2", "testuser1-laptop"
```

And back to the initial one:

```python
ln_setup.settings.instance.local_storage = "./my_local_storage", "testuser1-laptop"
```

```python
import lamindb as ln

ln.Storage.df()
```

Add a test file:

```python
test_file = ln_setup.settings.instance.local_storage.root / ".lamindb/test_file.txt"
test_file.write_text("test")
```

```python
ln_setup.settings.instance.local_storage.root.view_tree()
```

```python
with pytest.raises(ln_setup.errors.StorageNotEmpty):
    ln_setup.delete(name, force=True)
```

```python
test_file.unlink()
```

Expand user directory:

```python
# Test expanduser works
test_path = "~/test/file.txt"
result = ln_setup.core.upath.create_path(test_path)
expected = UPath(Path(test_path).expanduser())
assert result.as_posix() == expected.as_posix()

# Test it doesn't affect absolute paths
test_path = "/absolute/path/file.txt"
result = ln_setup.core.upath.create_path(test_path)
assert str(result) == test_path
```

```python
ln_setup.delete(name, force=True)
```
