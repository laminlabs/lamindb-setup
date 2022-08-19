from lnschema_core import __version__ as lnschema_core_v
from packaging import version

if version.parse(lnschema_core_v) < version.parse("0.3.2"):
    raise RuntimeError("lamindb needs lnschema_core_v >= 0.3.2")
