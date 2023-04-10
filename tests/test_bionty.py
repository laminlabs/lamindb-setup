import sqlmodel as sqm

import lndb


def test_init_bionty():
    lndb.login(
        "testuser1@lamin.ai", password="cEvcwMJFX4OwbsYVaMt2Os6GxxGgDUlBGILs2RyS"
    )
    lndb.init(storage="test-bionty", schema="bionty")

    import lnschema_bionty

    with sqm.Session(lndb.settings.instance.engine) as session:
        session.exec(
            sqm.select(lnschema_bionty.dev.CurrentBiontyVersions).where(
                lnschema_bionty.dev.CurrentBiontyVersions.entity == "Gene"
            )
        ).one()
