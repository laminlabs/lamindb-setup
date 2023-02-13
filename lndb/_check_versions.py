from lnhub_rest import __version__ as lnhub_rest_v
from packaging import version

if version.parse(lnhub_rest_v) != version.parse("0.3.2"):
    raise RuntimeError("Upgrade lnhub_rest: pip install lnhub_rest==0.3.2")
