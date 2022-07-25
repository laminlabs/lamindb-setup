from lamindb_schema import __version__ as lamindb_schema_version
from packaging import version

if version.parse(lamindb_schema_version) < version.parse("0.3.0"):
    raise RuntimeError("lamindb needs nbproject >= 0.3.0")
