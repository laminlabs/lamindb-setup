import lnschema_bionty
import sqlmodel as sqm

from lndb_setup import init, settings


def test_init_bionty():
    init(storage="test-bionty", schema="bionty")

    with sqm.Session(settings.instance.db_engine()) as session:
        session.exec(
            sqm.select(lnschema_bionty.dev.CurrentBiontyVersions).where(
                lnschema_bionty.dev.CurrentBiontyVersions.entity == "Gene"
            )
        ).one()
