---
execute_via: python
---

# Connect to local instance

```bash
lamin disconnect --here
lamin delete --force mydata
# make a fresh mydata instance without any modules
lamin init --storage mydata
lamin disconnect --here
```

```python
import lamindb_setup as ln_setup
from lamindb_setup._check_setup import ModuleWasntConfigured
import pytest
```

## Load your own instance by name

If the user is the instance owner, load the instance by name:

```python
# no instance connected, prompts to connect
import bionty
```

```python
ln_setup.connect(
    "mydata", _reload_lamindb=False
)  # also test passing _reload_lamindb explicitly
```

```python
# this is a local intstance, no spaces needed
assert ln_setup.settings.instance.available_spaces is None
```

```python
# pertdb is not in the (schema) modules of mydata
with pytest.raises(ModuleWasntConfigured):
    # _check_instance_setup is called inside
    import pertdb
```

```python
# bionty was loaded as a default module
import bionty  # noqa
```

```python tags=["hide-cell"]
from pathlib import Path

assert ln_setup.settings.instance.storage.type_is_cloud is False
assert ln_setup.settings.instance.name == "mydata"

root_path = Path("./mydata").resolve()
assert ln_setup.settings.instance.storage.root == root_path
assert (
    ln_setup.settings.instance.db
    == f"sqlite:///{root_path.as_posix()}/.lamindb/lamin.db"
)
```

Delete:

```python
ln_setup.delete("mydata", force=True)
```
