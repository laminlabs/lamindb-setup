import lnschema_bionty
import sqlmodel as sqm

from lndb import init, login, settings


def test_init_bionty():
    login("testuser1@lamin.ai", password="cEvcwMJFX4OwbsYVaMt2Os6GxxGgDUlBGILs2RyS")
    init(storage="test-bionty", schema="bionty")

    with sqm.Session(settings.instance.engine) as session:
        session.exec(
            sqm.select(lnschema_bionty.dev.CurrentBiontyVersions).where(
                lnschema_bionty.dev.CurrentBiontyVersions.entity == "Gene"
            )
        ).one()
