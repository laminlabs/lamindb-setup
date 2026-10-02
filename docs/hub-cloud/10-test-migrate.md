---
execute_via: python
---

# Migrate a hosted instance

```python
import os
import lamindb_setup as ln_setup

instance = (
    "laminlabs/lamindata"
    if os.getenv("LAMIN_ENV") in {None, "prod"}
    else "laminlabs/load-test"
)
```

```python
ln_setup.login("testuser1")
```

```python
ln_setup.connect(instance, use_root_db_user=True)
```

```python
# double connect to check migration with django reset
ln_setup.connect(instance, use_root_db_user=True)
```

```python
assert "root" in ln_setup.settings.instance.db
```

```python
ln_setup.migrate.deploy()
```
