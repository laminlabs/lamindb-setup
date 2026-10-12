from lamindb_setup._connect_instance import _connect_cli

# _connect_cli persists settings without connecting Django, so this must be the
# same instance laminprofiler already connected (laminlabs/lamindb-benchmarks).
_connect_cli("laminlabs/lamindb-benchmarks")
