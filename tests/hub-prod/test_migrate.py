import lamindb_setup as ln_setup


def test_migrate_create():
    assert ln_setup.migrate.create() is None


def test_migrate_deploy():
    assert ln_setup.migrate.deploy() is None


def test_migrate_check():
    assert ln_setup.migrate.check()
