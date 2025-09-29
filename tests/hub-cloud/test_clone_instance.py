import sqlite3

import lamindb_setup as setup
import pandas as pd
from django.db import connection


def test_init_clone_successful():
    setup.connect("laminlabs/lamin-dev")

    original_tables = pd.read_sql(
        "SELECT tablename as name FROM pg_tables WHERE schemaname='public'", connection
    )

    setup.core.init_clone("laminlabs/lamin-dev")

    clone_conn = sqlite3.connect(setup.settings.instance.db.replace("sqlite:///", ""))
    clone_tables = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table'", clone_conn
    )
    clone_conn.close()

    # Filter out tables that are expected to be missing in clone
    excluded_prefixes = ["clinicore_", "ourprojects_", "hubmodule_"]
    excluded_tables = {
        "django_content_type",
        "lamindb_page",
        "lamindb_referencerecord",
        "lamindb_writelogmigrationstate",
        "lamindb_writelogtablestate",
    }

    original_filtered = original_tables[
        ~original_tables["name"].str.startswith(tuple(excluded_prefixes))
    ]
    original_filtered = original_filtered[
        ~original_filtered["name"].isin(excluded_tables)
    ]

    # Add sqlite_sequence and person tables that appear in clone but not original
    # TODO the lamindb ones should not be necessary here
    clone_only_tables = {
        "sqlite_sequence",
        "lamindb_person",
        "lamindb_personproject",
        "lamindb_recordperson",
        "lamindb_reference_authors",
    }
    expected_tables = set(original_filtered["name"]) | clone_only_tables

    assert set(clone_tables["name"]) == expected_tables

    setup.disconnect()
