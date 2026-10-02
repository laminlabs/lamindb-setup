---
execute_via: python
---

# Test init cloud with db

```bash
lamin disconnect --here
lamin login testuser1
lamin delete --force test-init-cloud-with-db
docker stop pgtest && docker rm pgtest || true
```

```python
import os
import laminci
import lamindb_setup as ln_setup
from lamindb_setup.core._hub_core import delete_instance, get_instance_slug_by_uid
from lamindb_setup.core._hub_crud import select_instance_by_id
from lamindb_setup.core._hub_client import call_with_fallback_auth
```

```python
storage = f"s3://lamindb-ci/cloud_with_db_{os.getenv('LAMIN_ENV', 'prod')}"
```

```python
pgurl = laminci.db.setup_local_test_postgres()
```

Test initializing an instance with a cloud storage and a postgres db url.

```python
ln_setup.init(storage=storage, name="test-init-cloud-with-db", db=pgurl)
```

```python
from lamindb import __version__ as lamindb_version

instance_record = call_with_fallback_auth(
    select_instance_by_id, instance_id=ln_setup.settings.instance._id.hex
)

assert instance_record["lamindb_version"] == lamindb_version
```

```python
assert ln_setup.settings.instance.slug == get_instance_slug_by_uid(
    ln_setup.settings.instance.uid
)
```

```python
assert ln_setup.settings.storage.hub_record is not None
```

```python
assert ln_setup.settings.instance.name == "test-init-cloud-with-db"
```

```python
root = ln_setup.settings.storage.root
mark_file = ln_setup.settings.storage._mark_storage_root
```

```python
assert mark_file.exists()
```

```python
# this is a postgres instance, it doesn't have sqlite file
# we test here that delete_instance properly cleans up mark files if delete_mark_files=True
delete_instance(
    "testuser1/test-init-cloud-with-db", require_empty=True, delete_mark_files=True
)
```

```python
assert not mark_file.exists()
assert not root.exists()
```

```bash
docker stop pgtest && docker rm pgtest || true
```
