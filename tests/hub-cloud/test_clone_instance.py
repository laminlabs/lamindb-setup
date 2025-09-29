import sqlite3
from urllib.parse import urlparse

import lamindb_setup as setup
import pandas as pd
from django.db import connection


def test_init_clone_successful():
    setup.connect("laminlabs/lamindata")

    original_tables = pd.read_sql(
        "SELECT tablename as name FROM pg_tables WHERE schemaname='public'", connection
    )

    setup.core.init_clone("laminlabs/lamindata")

    db_uri = setup.settings.instance.db
    db_path = (
        urlparse(db_uri).path
        if db_uri.startswith("sqlite:///")
        else db_uri.replace("sqlite:///", "")
    )

    clone_conn = sqlite3.connect(db_path)
    clone_tables = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table'", clone_conn
    )
    clone_conn.close()

    excluded_prefixes = ("clinicore_", "ourprojects_", "hubmodule_")
    excluded_tables = {
        "django_content_type",
        "lamindb_writelogmigrationstate",
        "lamindb_writelogtablestate",
    }

    clone_missing_tables = {
        "lamindb_person",
        "lamindb_personproject",
        "lamindb_recordperson",
        "lamindb_reference_authors",
    }

    clone_only_tables = {
        "sqlite_sequence",
        "lamindb_page",
        "lamindb_referencerecord",
    }

    original_filtered = original_tables[
        ~original_tables["name"].str.startswith(excluded_prefixes)
        & ~original_tables["name"].isin(excluded_tables)
    ]

    expected_tables = (
        set(original_filtered["name"]) - clone_missing_tables
    ) | clone_only_tables
    actual_tables = set(clone_tables["name"])

    assert actual_tables == expected_tables
    setup.disconnect()
