from pathlib import Path
from typing import Dict

import pandas as pd
import sqlmodel as sqm

from ._db import insert
from ._settings_instance import InstanceSettings


def write_bionty_versions(isettings: InstanceSettings):
    """Write bionty _current.yaml to the bionty_versions table."""
    if "bionty" in isettings.schema:
        import bionty as bt
        from bionty.dev._io import load_yaml

        basedir = Path(bt.__file__).parent / "versions"
        _current = load_yaml(basedir / "_current.yaml")
        _local = load_yaml(basedir / "_local.yaml")

        rows = []
        for i, (entity, db) in enumerate(_current.items()):
            db_name = next(iter(db))
            db_version = db[db_name]
            row = {
                "id": i,
                "entity": entity,
                "database": db_name,
                "database_v": str(db_version),
                "database_url": _local.get(entity)
                .get(db_name)
                .get("versions")
                .get(db_version),
            }
            rows.append(row)

        insert.bionty_versions(rows)


def load_bionty_versions(isettings: InstanceSettings):
    """Write bionty_versions to _lndb.yaml in bionty."""
    if "bionty" in isettings.schema:
        import bionty as bt
        from bionty.dev._io import write_yaml
        from lnschema_bionty import dev

        basedir = Path(bt.__file__).parent / "versions"

        df = pd.read_sql(
            sqm.select(dev.bionty_versions), isettings.db_engine(future=False)
        )
        # avoid breaking change
        if df.shape[0] == 0:
            write_bionty_versions(isettings)
        df_lndb = df.set_index("entity")[["database", "database_v"]]
        lndb_dict: Dict = {}
        for entity, db in df_lndb.iterrows():
            lndb_dict[entity] = {}
            lndb_dict[entity][db["database"]] = db["database_v"]
        write_yaml(lndb_dict, basedir / "_lndb.yaml")
