from lamindb_schema import __version__ as lamindb_schema_v
from lnschema_core import __version__ as lnschema_core_v
from packaging import version

if version.parse(lamindb_schema_v) < version.parse("0.3.0"):
    raise RuntimeError("lamindb needs lamindb_schema_v >= 0.3.0")
if version.parse(lnschema_core_v) < version.parse("0.3.0"):
    raise RuntimeError("lamindb needs lnschema_core_v >= 0.3.0")
