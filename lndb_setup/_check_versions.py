from lamindb_schema import __version__ as lamindb_schema_v
from lndb_schema_core import __version__ as lndb_schema_core_v
from packaging import version

if version.parse(lamindb_schema_v) < version.parse("0.3.0"):
    raise RuntimeError("lamindb needs lamindb_schema_v >= 0.3.0")
if version.parse(lndb_schema_core_v) < version.parse("0.3.0"):
    raise RuntimeError("lamindb needs lndb_schema_core_v >= 0.3.0")
