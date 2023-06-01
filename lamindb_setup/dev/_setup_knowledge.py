from pathlib import Path
from typing import Dict

import pandas as pd
import sqlmodel as sqm
from IPython.display import display as ipython_display

from ._db import insert
from ._settings_instance import InstanceSettings


def write_bionty_versions(isettings: InstanceSettings):
    """Write bionty ._current.yaml to the CurrentBiontyVersions table."""
    if "bionty" in isettings.schema:
        import bionty as bt
        from bionty.dev._io import load_yaml
        from lnschema_bionty import dev

        basedir = Path(bt.__file__).parent / "versions"
        _current = load_yaml(basedir / "._current.yaml")
        local = load_yaml(Path.home() / ".lamin/bionty/versions/local.yaml")

        # here we set integer ids from 0
        records = []
        for i, (entity, db) in enumerate(_current.items()):
            db_name = next(iter(db))
            db_version = db[db_name]
            record = dev.BiontyVersions(
                id=i,
                entity=entity,
                database=db_name,
                database_v=db_version,
                database_url=local.get(entity)
                .get(db_name)
                .get("versions")
                .get(db_version)[0],
            )
            records.append(record)

        insert.bionty_versions(records)


def load_bionty_versions(isettings: InstanceSettings, display: bool = False):
    """Write CurrentBiontyVersions to ._lamindb_setup.yaml in bionty."""
    if "bionty" in isettings.schema:
        import bionty as bt
        from bionty.dev._io import write_yaml
        from lnschema_bionty import dev

        basedir = Path(bt.__file__).parent / "versions"

        # these two lines help over the incomplete migration
        # of the core schema module v0.34.0 and related in lnschema_bionty
        # v0.18.0
        dev.BiontyVersions.__table__.schema = None
        dev.CurrentBiontyVersions.__table__.schema = None

        stmt = sqm.select(dev.BiontyVersions).join(dev.CurrentBiontyVersions)
        with isettings.session() as ss:
            results = ss.exec(stmt).all()
        # avoid breaking change
        # if no versions were written in the db, write versions from bionty
        if len(results) == 0:
            write_bionty_versions(isettings)
        records = [row.dict() for row in results]
        df = pd.DataFrame.from_records(records)
        df_lndb = df.set_index("entity")[["database", "database_v"]]
        if display:
            ipython_display(df_lndb)
        lndb_dict: Dict = {}
        for entity, db in df_lndb.iterrows():
            lndb_dict[entity] = {}
            lndb_dict[entity][db["database"]] = db["database_v"]
        write_yaml(lndb_dict, basedir / "._lamindb_setup.yaml")
