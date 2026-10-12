---
execute_via: python
---

# Test SQLite file locking

```python
import pytest
import lamindb_setup as ln_setup
from lamindb_setup.core.cloud_sqlite_locker import Locker, InstanceLockedException
from lamindb_setup.core.upath import UPath
```

```python
ln_setup.login("testuser2")
```

```python
root = UPath("s3://lamindb-ci/test-load-lock", cache_regions=True)
```

```python
exclusion = root / ".lamindb/_exclusion/"
# if cache is present, then .exists() for intermediate keys return False
# bug in s3fs
exclusion.fs.invalidate_cache()
if exclusion.exists():
    exclusion.rmdir()
```

```python
assert ln_setup.settings.user.handle == "testuser2"
```

```python
ln_setup.init(storage="s3://lamindb-ci/test-load-lock", name="test-load-lock")
instance_id = ln_setup.settings.instance._id

assert (
    ln_setup.settings.instance._cloud_sqlite_locker
    is ln_setup.settings.instance._cloud_sqlite_locker
)

ln_setup.close()
```

```python
# lock with some random id
locker = Locker("randuseridtt", storage_root=root, instance_id=instance_id)
```

```python
locker.lock()
```

```python
# with pytest.raises(InstanceLockedException):
#    ln_setup.connect("test-load-lock"), would like to test this, but need another python session
```

```python
locker.unlock()
```

```python
# ln_setup.connect("test-load-lock")
# ln_setup.close()
```

```python
# lock through testuser1, who has user id "DzTjkKse"
locker = Locker("DzTjkKse", storage_root=root, instance_id=instance_id)
```

```python
locker.lock()
```

```python
# with pytest.raises(InstanceLockedException) as error:
#     ln_setup.connect("test-load-lock")

# assert (
#     "InstanceLockedException: Cannot load the instance, it is locked by 'testuser1'"
#     " (uid: 'DzTjkKse', name: 'Test User1')."
#     in error.exconly()
# )
```

```python
locker.unlock()
```

```python
# ln_setup.connect("test-load-lock")
```

```python
# test ignore_prev_locker=True in unlock_cloud_sqlite_upon_exception
# i.e. test that the locker doesn't unlock if the locker hasn't changed during the function execution
# with pytest.raises(RuntimeError):
#     ln_setup.connect("test-load-lock")

# assert ln_setup.settings.instance._cloud_sqlite_locker._has_lock
```

```python
# ln_setup.close()
```

```python
# purely technical varibale to test failed load after locking
# ln_setup._connect_instance._TEST_FAILED_LOAD = True

# with pytest.raises(RuntimeError):
#     ln_setup.connect("test-load-lock")

# assert ln_setup.core.cloud_sqlite_locker._locker._has_lock is None

# ln_setup._connect_instance._TEST_FAILED_LOAD = False
```

```python
# ln_setup.connect("test-load-lock", _test=True)
```

```python
ln_setup.delete("test-load-lock", force=True)
```
