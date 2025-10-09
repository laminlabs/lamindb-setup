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
        shutil.rmtree("./test-postgres-local-test")
        ln_setup.delete("test-postgres-local", force=True)
    except Exception as e:
        print(e)
    run("docker stop pgtest && docker rm pgtest", shell=True, stdout=DEVNULL)


def test_init_clone_successful(local_postgres_instance):
    ln_setup.connect("test-local-postgres")

    postgres_tables = pd.read_sql(
        "SELECT tablename as name FROM pg_tables WHERE schemaname='public'",
        ln_setup.settings.instance.db,
    )

    ln_setup.core.init_clone("test-local-postgres")

    clone_tables = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table'",
        ln_setup.settings.instance.db,
    )

    clone_only_tables = {
        "sqlite_sequence",
    }

    expected_tables = (set(postgres_tables["name"])) | clone_only_tables
    actual_tables = set(clone_tables["name"])

    assert actual_tables == expected_tables

    # TODO: check some attributes like owner etc
