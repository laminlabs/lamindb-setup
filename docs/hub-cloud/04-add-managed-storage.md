---
execute_via: python
---

# Add writeable and read-only storage locations to an instance

```python tags=["hide-cell"]
!lamin disconnect --here
!lamin login testuser1
!lamin delete --force test-add-managed-storage
!docker stop pgtest && docker rm pgtest
```

Create a local instance that's not registered on the hub, so that we can test interplay.

```python
!lamin init --storage "./storage-of-another-instance"
```

```python
import os

os.environ["LAMINHUB_ID"] = "00000000-0000-0000-0000-000000000000"
```

```python
import laminci
import pytest
from pathlib import Path
import lamindb_setup as ln_setup
from lamindb_setup._set_managed_storage import set_managed_storage
```

```python tags=["hide-cell"]
pgurl = laminci.db.setup_local_test_postgres()
```

```python
ln_setup.init(storage="./storage1", name="test-add-managed-storage", db=pgurl)
```

Test adding a referenced read-only storage location.

```python
import lamindb as ln

mark_lines = (
    Path("./storage-of-another-instance/.lamindb/storage_uid.txt")
    .read_text()
    .splitlines()
)
marked_instance_uid = next(
    line.split("=", 1)[1] for line in mark_lines if line.startswith("instance_uid=")
)
storage = ln.Storage(root="./storage-of-another-instance").save()
assert storage.instance_uid == marked_instance_uid
```

Now continue with writeable storage locations.

```python
instance_id = ln_setup.settings.instance._id
storage1_uid = ln_setup.settings.storage.uid
```

This errors at first.

```python
with pytest.raises(ValueError) as error:
    set_managed_storage("./storage2")
assert (
    error.exconly()
    == "ValueError: Can't add additional managed storage locations for instances that aren't managed through the hub."
)
```

Register the instance on the hub.

```python
ln_setup.register()
```

```python
assert ln_setup.settings.instance.is_on_hub
assert not ln_setup.settings.instance.is_managed_by_hub
```

After registering the instance on the hub, things work out:

```python
set_managed_storage("./storage2", host="testuser1-laptop")
```

```python tags=["hide-cell"]
storage2_uid = ln_setup.settings.storage.uid
assert ln_setup.settings.storage.root_as_str == f"{Path.cwd()}/storage2"
assert (
    ln_setup.settings.storage.root / ".lamindb/storage_uid.txt"
).read_text().splitlines()[0] == ln_setup.settings.storage.uid
assert ln_setup.settings.storage.is_on_hub
```

Let's confirm things are idempotent and we can switch between storage locations.

```python
set_managed_storage("./storage1", host="testuser1-laptop")
```

```python
assert ln_setup.settings.storage.root_as_str == f"{Path.cwd()}/storage1"
assert (
    ln_setup.settings.storage.root / ".lamindb/storage_uid.txt"
).read_text().splitlines()[0] == ln_setup.settings.storage.uid
assert ln_setup.settings.storage.is_on_hub
assert ln_setup.settings.storage.uid == storage1_uid
```

Repeat:

```python
set_managed_storage("./storage1", host="testuser1-laptop")
```

```python
assert ln_setup.settings.storage.root_as_str == f"{Path.cwd()}/storage1"
assert (
    ln_setup.settings.storage.root / ".lamindb/storage_uid.txt"
).read_text().splitlines()[0] == ln_setup.settings.storage.uid
assert ln_setup.settings.storage.is_on_hub
assert ln_setup.settings.storage.uid == storage1_uid
```

```python
set_managed_storage("./storage2", host="testuser1-laptop")
```

```python
storage2_uid = ln_setup.settings.storage.uid
assert ln_setup.settings.storage.root_as_str == f"{Path.cwd()}/storage2"
assert (
    ln_setup.settings.storage.root / ".lamindb/storage_uid.txt"
).read_text().splitlines()[0] == ln_setup.settings.storage.uid
assert ln_setup.settings.storage.is_on_hub
assert ln_setup.settings.storage.uid == storage2_uid
```

Cloud storage:

```python
cloud_storage = f"s3://lamindb-ci/storage3_{os.getenv('LAMIN_ENV', 'prod')}"
```

```python
set_managed_storage(cloud_storage)
```

```python tags=["hide-cell"]
assert ln_setup.settings.storage.type_is_cloud
assert ln_setup.settings.storage.root_as_str == cloud_storage
assert ln_setup.settings.storage.region == "us-west-1"
assert (
    ln_setup.settings.storage.root / ".lamindb/storage_uid.txt"
).read_text().splitlines()[0] == ln_setup.settings.storage.uid
# root.fs contains the underlying fsspec filesystem
assert (
    ln_setup.settings.storage.root.fs.cache_regions  # set by lamindb to True for s3 by default
)
assert ln_setup.settings.storage._instance_id is not None
```

You can set any additional `fsspec` filesystem arguments for cloud storage, such as `profile` or `cache_regions` (for s3 only), for example:

```python
set_managed_storage(cloud_storage, cache_regions=False)
```

```python
# test cache_regions
assert not ln_setup.settings.storage.root.fs.cache_regions
```

Cloud storage with mere read access:

```python
with pytest.raises(ValueError) as error:
    set_managed_storage("gs://rxrx1-europe-west4/images/test/HEPG2-08")
assert error.exconly().startswith(
    "ValueError: Cannot manage storage without write access"
)
```

Add testuser2 as a collaborator to the instance, sign them in and let them add another storage location:

```python
from lamindb_setup.core._hub_client import connect_hub_with_auth
from lamincentral.client import SupabaseClientWrapper
from laminhub_rest.core.instance_collaborator import InstanceCollaboratorHandler
from laminhub_rest.core.organization import OrganizationMemberHandler

admin_hub = SupabaseClientWrapper(connect_hub_with_auth())
organization_member_handler = OrganizationMemberHandler(admin_hub)

assert ln_setup.settings.user.handle == "testuser1"
organization_id = ln_setup.settings.user._uuid  # testuser1

ln_setup.login("testuser2")
assert ln_setup.settings.user.handle == "testuser2"
account_id = ln_setup.settings.user._uuid

try:
    try:
        organization_member_handler.add(
            organization_id=organization_id,
            account_id=account_id,
            role="member",
        )
    except KeyError:
        # we don't set LAMIN_API_KEY, so broadcasting cache invalidation fails
        # it should still be fine with adding to the organization
        pass
    try:
        InstanceCollaboratorHandler(admin_hub).add(
            account_id=account_id,
            instance_id=instance_id,
            role="write",
        )
    except TypeError:
        # the above failes in the mid due to no resource_db_server associated with the instance
        # but still adds the collaborator
        pass

    # although technically a user doesn't have to be a collaborator to insert into storage table
    set_managed_storage("./storage4", host="testuser2-laptop")
    assert ln_setup.settings.storage.root_as_str == f"{Path.cwd()}/storage4"
    # attempt to delete the instance with testuser2
    with pytest.raises(PermissionError) as error:
        ln_setup.delete("testuser1/test-add-managed-storage", force=True)

finally:
    try:
        organization_member_handler.remove(
            organization_id=organization_id,
            account_id=account_id,
        )
    except KeyError:
        # we don't set LAMIN_API_KEY, so broadcasting cache invalidation fails
        # it should still be fine with deletion from the organization
        pass
    admin_hub.auth.sign_out(options={"scope": "local"})
```

Delete test instance through testuser1:

```python
ln_setup.login("testuser1")
ln_setup.delete("test-add-managed-storage", force=True)
!docker stop pgtest && docker rm pgtest
```

Assert everything is deleted:

```python
from lamindb_setup.core._hub_client import call_with_fallback_auth
from lamindb_setup.core._hub_crud import select_instance_by_id
from lamindb_setup.core._hub_core import get_storage_records_for_instance

assert (
    call_with_fallback_auth(select_instance_by_id, instance_id=instance_id.hex) is None
)
assert not get_storage_records_for_instance(instance_id)
```
