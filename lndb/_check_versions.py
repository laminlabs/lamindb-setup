from lnhub_rest import __version__ as lnhub_rest_v
from lnhub_rest import check_breaks_lndb
from packaging import version

if check_breaks_lndb():
    raise SystemExit(
        "Your lamindb installation is out-of-date.\n"
        "Please upgrade: pip install lamindb -U"
    )


if version.parse(lnhub_rest_v) >= version.parse("0.4.2"):
    raise SystemExit("Please upgrade lnhub_rest: pip install lnhub_rest -U")
