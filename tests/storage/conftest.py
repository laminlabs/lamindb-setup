import pytest


@pytest.fixture(scope="session")
def simple_instance():
    import lamindb_setup as ln_setup

    ln_setup.init(storage="./testdb", modules="bionty,wetlab")
    yield
    ln_setup.delete("testdb", force=True)
