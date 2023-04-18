from lnhub_rest import __version__ as lnhub_rest_v
from packaging import version

if version.parse(lnhub_rest_v) < version.parse("0.8.1"):
    raise SystemExit("Please upgrade lnhub_rest: pip install lnhub_rest -U")
