import shutil
import sqlite3
from subprocess import DEVNULL, run
from urllib.parse import urlparse

import lamindb_setup as ln_setup
import pandas as pd
import pytest
from django.db import connection
from laminci.db import setup_local_test_postgres


@pytest.fixture
def local_postgres_instance():
    pgurl = setup_local_test_postgres()
    ln_setup.init(
        storage="./test-postgres-local",
        modules="bionty",
        name="test-postgres-local",
        db=pgurl,
    )

    yield

    try:
        shutil.rmtree("./test-postgres-local")
        ln_setup.delete("test-postgres-local", force=True)
    except Exception as e:
        print(e)
    run("docker stop pgtest && docker rm pgtest", shell=True, stdout=DEVNULL)


def test_init_clone_successful(local_postgres_instance):
    print("CLONE TESTING")

    ln_setup.connect("test-local-postgres")

    original_tables = pd.read_sql(
        "SELECT tablename as name FROM pg_tables WHERE schemaname='public'",
        ln_setup.settings.instance.db,
    )
    print(original_tables)
    """

    ln_setup.core.init_clone(f"testadmin1/{local_postgres_instance.name}")

    # needs to be adapted when we don't connect anymore
    db_uri = ln_setup.settings.instance.db
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

    print(clone_tables)

    ln_setup.disconnect()




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
