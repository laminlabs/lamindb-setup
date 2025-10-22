import shutil
from subprocess import DEVNULL, run

import lamindb_setup as ln_setup
import pandas as pd
import pytest
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


def test_init_copy_successful(local_postgres_instance):
    """Test `init_local_sqlite()` creates SQLite clone with matching schema and metadata."""
    original_owner = ln_setup.settings.instance.owner
    original_name = ln_setup.settings.instance.name
    original_storage = str(ln_setup.settings.storage.root)
    original_postgres_tables = pd.read_sql(
        "SELECT tablename as name FROM pg_tables WHERE schemaname='public'",
        ln_setup.settings.instance.db,
    )

    ln_setup.core.init_local_sqlite("test-postgres-local")

    clone_tables = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table'",
        ln_setup.settings.instance.db,
    )

    # tables between original postgres instance and local SQLite instance must match
    clone_only_tables = {"sqlite_sequence"}
    expected_tables = set(original_postgres_tables["name"]) | clone_only_tables
    actual_tables = set(clone_tables["name"])
    assert actual_tables == expected_tables

    # metadata like owner, name, and storage must match & instance dialect must be sqlite
    assert ln_setup.settings.instance.owner == original_owner
    assert ln_setup.settings.instance.name == original_name
    assert str(ln_setup.settings.storage.root) == original_storage
    assert ln_setup.settings.instance.dialect == "sqlite"
    assert ln_setup.settings.instance.is_on_hub is False


def test_connect_local_sqlite(local_postgres_instance):
    """Test connect_local_sqlite() loads SQLite clone from local settings.

    Verifies that `connect_local_sqlite()` can reconnect to a SQLite clone after
    disconnection by reading from local settings files, bypassing hub lookup.

    Limitation: This test uses a local Postgres instance rather than a remote hub instance.
    In production, `connect_local_sqlite()` would bypass fetching the original Postgres config from the hub and load the SQLite clone instead.
    """
    original_owner = ln_setup.settings.instance.owner
    original_name = ln_setup.settings.instance.name

    ln_setup.core.init_local_sqlite("test-postgres-local")

    assert ln_setup.settings.instance.dialect == "sqlite"
    assert ln_setup.settings.instance.is_on_hub is False

    ln_setup.close()
    ln_setup.core.connect_local_sqlite(f"{original_owner}/{original_name}")

    assert ln_setup.settings.instance.dialect == "sqlite"
    assert ln_setup.settings.instance.owner == original_owner
    assert ln_setup.settings.instance.name == original_name
    assert ln_setup.settings.instance.is_on_hub is False

    with pytest.raises(ValueError, match="SQLite clone not found"):
        ln_setup.core.connect_local_sqlite(f"{original_owner}/nonexistent")
