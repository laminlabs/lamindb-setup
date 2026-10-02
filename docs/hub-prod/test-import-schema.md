---
execute_via: python
---

# Test load through schema module

Also see the corresponding FAQ notebook in lamindb: `import-modules`.

You'll load the instance in the same way as calling `import lamindb` when you import a schema module.

```python
!lamin init --storage test-implicit-load --modules pertdb,bionty
```

```python
import lamindb_setup

assert not lamindb_setup.core.django.IS_SETUP
```

```python
import pertdb
```

```python
assert lamindb_setup.core.django.IS_SETUP
```

```python
pertdb.Compound
```

```python
assert lamindb_setup.core.django.IS_SETUP
```

```python
!lamin delete --force test-implicit-load
```
