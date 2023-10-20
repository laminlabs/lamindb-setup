from lamindb_setup.dev.django import get_migrations_to_sync
from lamindb_setup import django


def test_get_migrations_to_sync():
    get_migrations_to_sync()


# test make migrations covered in test_migrate_create


def test_django():
    django("sqlsequencereset", "lnschema_core")
