---
execute_via: python
---

# Test multi instance and multi user session

```bash
lamin login testuser1
lamin init --storage "./testsetup-prepare"
lamin disconnect --here
```

```python
import lamindb_setup as ln_setup
import lamindb as ln
import pytest
```

If you try to use lamindb, it will raise an `CurrentInstanceNotConfigured` and ask you to `init` or `connect` an instance via the python API.

```python
with pytest.raises(ln.setup.errors.CurrentInstanceNotConfigured):
    ln.track()
```

```python
assert ln_setup.settings.user.handle == "testuser1"
```

```python
ln_setup.init(storage="./testsetup")
```

```python
assert ln_setup.settings.instance.slug == "testuser1/testsetup"
```

```python
from lamindb.models import User
```

```python
User.objects.get(handle="testuser1")
```

```python
with pytest.raises(Exception):  # does not exist
    User.objects.get(handle="testuser2")
```

```python
ln.track()
```

Let us try connecting to another instance:

```python
with pytest.raises(ln.setup.errors.CannotSwitchDefaultInstance) as error:
    ln.connect("testsetup3")
assert error.exconly().endswith(
    "Cannot switch default instance while `ln.track()` is live: call `ln.finish()`"
)
```

Let us try to init another instance in the same Python session.

```python
with pytest.raises(ln.setup.errors.CannotSwitchDefaultInstance) as error:
    ln.setup.init(storage="./testsetup2")
assert error.exconly().endswith(
    "Cannot switch default instance while `ln.track()` is live: call `ln.finish()`"
)
```

Switch off `ln.track()`.

```python
ln.context._transform = None
```

Let us login with another user:

```python
ln_setup.login("testuser2")
```

```python
User.objects.get(handle="testuser2")
```

Connect to another instance in the same process:

```python
ln_setup.connect("testuser1/testsetup-prepare")
```

```python
assert ln_setup.settings.instance.slug == "testuser1/testsetup-prepare"
```

```bash
lamin login testuser1
lamin delete --force testsetup-prepare
lamin delete --force testsetup
```
