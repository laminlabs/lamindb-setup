[![codecov](https://codecov.io/gh/laminlabs/lamindb-setup/branch/main/graph/badge.svg)](https://codecov.io/gh/laminlabs/lamindb-setup)

# `lamindb_setup`: Setup & configure LaminDB

The `lamindb` library offers joint database and storage access through an ORM that's based on the Django ORM and `universal_pathlib`. Django requires an environment with settings to be inplace to import models.

`lamindb_setup` takes care of setting up this environment so that a user importing `lamindb` can focus on high-level data workloads.

The `lamindb_setup` API can be used standalone but is also re-exported as [`lamindb.setup`](https://docs.lamin.ai/lamindb.setup).
