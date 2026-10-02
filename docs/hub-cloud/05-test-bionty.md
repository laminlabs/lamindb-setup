---
execute_via: python
---

# Test bionty

```python
import lamindb_setup as ln_setup
from subprocess import getoutput
import pandas as pd
```

Pass bionty to init:

```python
ln_setup.init(storage="mydata2", modules="bionty")
```

Check whether sources are written:

```python
from bionty import Source
```

```python
sources_df = pd.DataFrame(Source.objects.all().values())
sources_df.head()
```

```python
assert sources_df.shape[0] > 0
```

Test what happens if we accidentally re-init the instance:

```python
output = getoutput("lamin init --storage mydata2 --modules bionty")
print(output)
```

Now, let's remove/corrupt the instance settings file so that the init is actually triggered:

```python
ln_setup.settings.instance._get_settings_file().unlink()
output = getoutput("lamin init --storage mydata2 --modules bionty")
print(output)
```

Check that everything is still in place:

```python
sources_df = pd.DataFrame(Source.objects.all().values())
sources_df.head()
```

```python
!lamin delete --force mydata2
```
