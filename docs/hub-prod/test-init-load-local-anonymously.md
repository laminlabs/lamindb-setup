---
execute_via: python
---

# Init load local anonymously

```python
import lamindb_setup as ln_setup
```

Check current user.

```python
ln_setup.login("testuser2")
assert ln_setup.settings.user.handle == "testuser2"
```

logout and check that the user info was updated.

```python
ln_setup.logout()
assert ln_setup.settings.user.handle == "anonymous"
```

Init a local instance anonymously.

```python
!lamin disconnect --here
!lamin init --storage ./test-anonymous-init --modules bionty
```

Load the instance and check.

```python
ln_setup.connect("test-anonymous-init")
assert ln_setup.settings.instance.name == "test-anonymous-init"
assert ln_setup.settings.instance.owner == "anonymous"
```

Clean up.

```python
ln_setup.delete("test-anonymous-init", force=True)
```

```python
ln_setup.login("testuser2")
```
