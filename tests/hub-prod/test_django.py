from __future__ import annotations

from lamindb_setup import django

# test make migrations covered in test_migrate_create


def test_django():
    django("sqlsequencereset", "lnschema_core")
