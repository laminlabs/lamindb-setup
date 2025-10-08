import sqlite3
from urllib.parse import urlparse

import lamindb_setup as setup
import pandas as pd
import pytest
from django.db import connection

# To run these tests locally, you need to have the local backend set up: https://github.com/laminlabs/laminhub?tab=readme-ov-file#local-backend
# Ensure that rest-hub is installed in editable mode
# It might be required to use a specific supabase version. Currently 2.31.8 works and the latest version 2.48.3 does not.


def test_init_clone_successful(create_instance_fine_grained_access):
    instance = create_instance_fine_grained_access
    print("CLONE TESTING")

    setup.connect(f"testadmin1/{instance.name}")

    original_tables = pd.read_sql(
        "SELECT tablename as name FROM pg_tables WHERE schemaname='public'", connection
    )
    print(original_tables)

    setup.core.init_clone(f"testadmin1/{instance.name}")

    setup.disconnect()
    """

    # needs to be adapted when we don't connect anymore
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
        "awsdms_ddl_audit",
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


def test_init_clone_account_does_not_exist():
    with pytest.raises(ValueError) as e:
        setup.core.init_clone("thisuserreallydoesntexist/lamindata")
    assert (
        "Cloning failed because the account thisuserreallydoesntexist does not exist."
        == str(e.value)
    )


def test_init_clone_instance_not_found():
    with pytest.raises(ValueError) as e:
        setup.core.init_clone("laminlabs/thisinstancereallydoesntexist")
    assert (
        "Cloning failed because the instance thisinstancereallydoesntexist was not found."
        == str(e.value)
    )
"""


# not yet covering the case default-storage-does-not-exist-on-hub
