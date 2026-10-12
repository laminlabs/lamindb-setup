---
execute_via: python
---

# Connect to a public instance anonymously

```python
import pytest
import lamindb_setup as ln_setup
```

```python
ln_setup.logout()
```

```python
ln_setup.connect("laminlabs/lamin-site-assets")
```

```python
# for postgres instances just connecting should not initialize storage.root
assert ln_setup.settings.storage._root is None
```

```python
ln_setup.close()
```

```python
ln_setup.login("testuser2")
```
