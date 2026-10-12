---
execute_via: python
---

# Test invalid schema module name

```python
import lamindb_setup as ln_setup
import pytest
```

```python
with pytest.raises(ImportError):
    ln_setup.init(
        storage="./test-invalid-modules", modules="bionty,invalid_module_name"
    )
```
