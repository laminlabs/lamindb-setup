---
execute_via: python
---

# Init a local instance

```bash
lamin login testuser1
```

```python
import lamindb_setup as ln_setup
import os
```

```python
assert not ln_setup.settings.is_connected
```

```python
ln_setup.init(storage="./mydata", modules="bionty")
```

```python
assert ln_setup.settings.is_connected
```

```python tags=["hide-cell"]
from pathlib import Path
from lamindb.models import Storage

assert ln_setup.settings.instance.storage.type_is_cloud is False
assert ln_setup.settings.instance.owner == ln_setup.settings.user.handle
assert ln_setup.settings.instance.name == "mydata"
assert ln_setup.settings.instance.modules == {"bionty"}
assert ln_setup.settings.storage.root.as_posix() == Path("mydata").resolve().as_posix()
storage_root = ln_setup.settings.storage.root
assert storage_root.exists()
assert ln_setup.settings.storage._id is not None
assert (
    ln_setup.settings.instance.db
    == f"sqlite:///{Path('./mydata').resolve().as_posix()}/.lamindb/lamin.db"
)
assert ln_setup.settings.storage._instance_id == ln_setup.settings.instance._id
assert (
    Storage.objects.get(instance_uid=ln_setup.settings.instance.uid).root
    == ln_setup.settings.storage.root_as_str
)
```

```python
exit_status = os.system("lamin migrate deploy")
assert exit_status == 0
```

```python
ln_setup.delete("mydata", force=True)
```

```python
from lamindb_setup.core._settings_store import instance_settings_file

settings_file = instance_settings_file("mydata", "testuser1")
assert not storage_root.exists()
assert settings_file.exists() is False
```
