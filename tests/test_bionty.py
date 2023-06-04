import sqlmodel as sqm

import lamindb_setup as ln_setup


def test_init_bionty():
    _USE_DJANGO = True
    if not _USE_DJANGO:
        ln_setup.login(
            "testuser1@lamin.ai", password="cEvcwMJFX4OwbsYVaMt2Os6GxxGgDUlBGILs2RyS"
        )
        ln_setup.init(storage="test-bionty", schema="bionty")

        import lnschema_bionty

        with sqm.Session(ln_setup.settings.instance.engine) as session:
            session.exec(
                sqm.select(lnschema_bionty.dev.CurrentBiontyVersions).where(
                    lnschema_bionty.dev.CurrentBiontyVersions.entity == "Gene"
                )
            ).one()
    else:
        print("to come")
