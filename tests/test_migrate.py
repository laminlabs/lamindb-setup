import pytest

import lamindb_setup as ln_setup


@pytest.fixture
def setup_instance():
    ln_setup.init(storage="./testdb")
    yield
    ln_setup.delete("testdb")


def test_migrate_check(setup_instance):
    assert ln_setup.migrate.check()
