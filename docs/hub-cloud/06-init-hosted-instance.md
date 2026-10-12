---
execute_via: python
---

# Init hosted instance

```bash
lamin login testuser1
```

```python
import os

is_prod = os.getenv("LAMIN_ENV") in {None, "prod"}
if is_prod:
    os.environ["LAMIN_S3_ANON"] = "true"

import lamindb_setup as ln_setup
from lamindb_setup.core.upath import create_path, StorageNotEmpty
from lamindb_setup.core._hub_core import (
    delete_instance,
    call_with_fallback_auth,
    select_instance_by_owner_name,
)
from lamindb_setup.core._hub_crud import select_collaborator
from lamindb_setup.core._hub_client import connect_hub_with_auth
from lamindb_setup.core._aws_options import HOSTED_BUCKETS, get_user_aws_options_manager
import pytest

instance_name = "my-hosted"
assert ln_setup.settings.user.handle == "testuser1"
```

```python
try:
    delete_instance(f"testuser1/{instance_name}", require_empty=True)
except StorageNotEmpty:
    instance_with_storage = call_with_fallback_auth(
        select_instance_by_owner_name,
        owner="testuser1",
        name=instance_name,
    )
    root = create_path(instance_with_storage["storage"]["root"])
    for obj in root.rglob(""):
        if obj.is_file():
            obj.unlink()
    delete_instance(f"testuser1/{instance_name}", require_empty=True)
```

```python
with pytest.raises(ValueError):
    ln_setup.init(storage="create-s3")
```

```python
ln_setup.init(name="my-hosted", storage="create-s3")
```

```python
if is_prod:
    assert get_user_aws_options_manager().anon
```

```python
assert ln_setup.settings.instance.storage.type_is_cloud is True
assert ln_setup.settings.instance.owner == ln_setup.settings.user.handle
assert ln_setup.settings.instance.name == "my-hosted"
assert ln_setup.settings.storage.root.as_posix().startswith(HOSTED_BUCKETS)
assert ln_setup.settings.storage._id is not None

assert ln_setup.settings.storage._mark_storage_root.exists()
```

Test collaborator and storage record for the root exist:

```python
hub = connect_hub_with_auth()
```

```python
assert (
    select_collaborator(
        instance_id=ln_setup.settings.instance._id.hex,
        account_id=ln_setup.settings.user._uuid.hex,
        client=hub,
    )["role"]
    == "admin"
)
```

```python
response = (
    hub.table("storage")
    .select("*")
    .eq("root", ln_setup.settings.storage.root.as_posix())
    .execute()
    .data
)
assert len(response) == 1
assert response[0]["is_default"]
```

```python
ln_setup.close()
```
