from lamindb_setup.dev._django import get_migrations_to_sync  # backward compat version


def test_get_migrations_to_sync():
    get_migrations_to_sync()


# test make migrations covered in test_migrate_create
