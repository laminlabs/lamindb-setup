from __future__ import annotations

import lamindb_setup as ln_setup
import pytest


@pytest.fixture
def setup_instance():
    ln_setup.init(storage="./testdb")
    yield
    ln_setup.delete("testdb", force=True)


# tested in notebook right now
# def test_migrate_create(setup_instance):
#     assert ln_setup.migrate.create() is None
# def test_migrate_deploy(setup_instance):
#     assert ln_setup.migrate.deploy() is None


def test_migrate_check(setup_instance):
    assert ln_setup.migrate.check()
