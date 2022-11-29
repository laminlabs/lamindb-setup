# monitored instances
import pandas as pd

# sqlite
lamin_site_assets = "s3://lamin-site-assets/lamin-site-assets.lndb"
bionty_assets = "s3://bionty-assets/bionty-assets.lndb"
harmonic_test = "s3://ln-harmonic-docking/ln-harmonic-docking.lndb"

# postgres
lamindata = "postgresql://postgres:lamin-data-admin-0@lamindata.ciwirckhwtkd.eu-central-1.rds.amazonaws.com:5432/lamindata"  # noqa
retro_test = "postgresql://batman:robin@35.222.187.204:5432/retro"


_instances = [
    (lamin_site_assets, "lnschema_core"),
    (harmonic_test, "lnschema_core"),
    (bionty_assets, "lnschema_core"),
    (retro_test, "lnschema_core"),
    (retro_test, "lnschema_bionty"),
    (retro_test, "lnschema_wetlab"),
    (lamindata, "lnschema_core"),
    (lamindata, "lnschema_bionty"),
    (lamindata, "lnschema_wetlab"),
]

instances = pd.DataFrame(_instances)
