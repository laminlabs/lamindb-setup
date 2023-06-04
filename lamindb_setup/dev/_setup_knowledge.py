from pathlib import Path
from typing import Dict

from ._settings_instance import InstanceSettings


def write_bionty_versions(isettings: InstanceSettings):
    """Write bionty ._current.yaml to the CurrentBiontyVersions table."""
    if "bionty" in isettings.schema:
        import bionty as bt
        from lnschema_bionty import BiontyVersions, CurrentBiontyVersions

        columns_mapper = {
            "bionty class": "entity",
            "source key": "source_key",
            "ontology": "source_name",
        }

        all_versions = bt.display_available_versions(return_df=True).reset_index()
        all_versions.columns = all_versions.columns.str.lower()
        all_versions.rename(columns=columns_mapper, inplace=True)
        all_versions_dict = all_versions.to_dict(orient="records")

        current_versions = bt.display_active_versions(return_df=True).reset_index()
        current_versions.columns = current_versions.columns.str.lower()
        current_versions.rename(columns=columns_mapper, inplace=True)
        current_versions = current_versions.set_index(["entity", "species"])

        all_records = []
        current_records = []

        for kwargs in all_versions_dict:
            record = BiontyVersions(**kwargs)
            all_records.append(record)
            current = current_versions.loc[
                (kwargs["entity"], kwargs["species"])
            ].to_dict()
            if (current["source_key"] == kwargs["source_key"]) and (
                current["version"] == kwargs["version"]
            ):
                current_records.append(CurrentBiontyVersions(bionty_version=record))

        BiontyVersions.objects.bulk_create(all_records)
        CurrentBiontyVersions.objects.bulk_create(current_records)


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

        import sqlalchemy as sqm  # no module-level import, not a dependency!!!

        stmt = sqm.select(dev.BiontyVersions).join(dev.CurrentBiontyVersions)
        with isettings.session() as ss:
            results = ss.exec(stmt).all()
        # avoid breaking change
        # if no versions were written in the db, write versions from bionty
        if len(results) == 0:
            write_bionty_versions(isettings)
        records = [row.dict() for row in results]

        import pandas as pd  # no module-level import, is slow!!!
        from IPython.display import display as ipython_display  # is slow!

        df = pd.DataFrame.from_records(records)
        df_lndb = df.set_index("entity")[["database", "database_v"]]
        if display:
            ipython_display(df_lndb)
        lndb_dict: Dict = {}
        for entity, db in df_lndb.iterrows():
            lndb_dict[entity] = {}
            lndb_dict[entity][db["database"]] = db["database_v"]
        write_yaml(lndb_dict, basedir / "._lamindb_setup.yaml")
