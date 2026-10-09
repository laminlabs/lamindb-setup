[![codecov](https://codecov.io/gh/laminlabs/lamindb-setup/branch/main/graph/badge.svg)](https://codecov.io/gh/laminlabs/lamindb-setup)

# `lamindb_setup`: Setup & configure LaminDB

The `lamindb` library offers joint database and storage access through registries that are based on the Django ORM and `universal_pathlib`. Django requires an environment with settings to be in place at import time. `lamindb_setup` takes care of setting up an environment so that a user importing `lamindb` can focus on high-level data workloads.

The `lamindb_setup` API can be used standalone or through `lamindb.setup`.

Read the [docs](https://docs.lamin.ai/lamindb.setup).
