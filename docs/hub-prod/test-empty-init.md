---
execute_via: python
---

# Test initialization in empty s3 bucket

```python
import lamindb_setup as ln_setup
from lamindb_setup.core.upath import UPath
```

```python
root_str = "s3://lamindb-setup-ci-empty-bucket"
root_path = UPath(root_str, cache_regions=True)
```

```python
for s in root_path.iterdir():
    if s.is_file():
        s.unlink()
    elif s.is_dir():
        s.rmdir()
```

```python
assert list(root_path.iterdir()) == []
```

```python
ln_setup.init(storage=root_str)
```

```python
assert ln_setup.settings.storage.type_is_cloud
assert ln_setup.settings.storage.root_as_str == root_str
assert ln_setup.settings.storage.region == "us-east-1"
assert str(ln_setup.settings.instance._sqlite_file) == f"{root_str}/.lamindb/lamin.db"
```

```python
ln_setup.delete("lamindb-setup-ci-empty-bucket", force=True)
```

```python
for s in root_path.iterdir():
    if s.is_file():
        s.unlink()
    elif s.is_dir():
        s.rmdir()
```
