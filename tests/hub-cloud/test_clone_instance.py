import shutil
from pathlib import Path
from subprocess import DEVNULL, run
from unittest.mock import MagicMock, Mock, patch
from uuid import uuid4

import lamindb_setup as ln_setup
import pandas as pd
import pytest
from laminci.db import setup_local_test_postgres


@pytest.fixture(scope="module")
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
    assert ln_setup.settings.instance.is_managed_by_hub is False


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


def test_connect_remote_sqlite(tmp_path):
    mock_instance_id = uuid4()
    mock_storage_root = "s3://my-bucket/data"

    with (
        patch(
            "lamindb_setup._connect_instance._connect_instance"
        ) as mock_connect_instance,
        patch("lamindb_setup.settings") as mock_settings,
        patch("lamindb_setup.core._settings_instance.InstanceSettings"),
        patch("lamindb_setup.core.upath.create_path") as mock_create_path,
        patch("lamindb_setup.core._clone.connect_local_sqlite"),
    ):
        mock_instance = Mock()
        mock_instance._id = mock_instance_id
        mock_instance.owner = "testowner"
        mock_instance.name = "testname"
        mock_instance.modules = ["module1", "module2"]
        mock_instance.storage.root = mock_storage_root
        mock_instance.storage.root_as_str = mock_storage_root

        mock_settings.instance = mock_instance
        mock_settings.storage = Mock()
        mock_settings.cache_dir = Path(tmp_path)

        mock_connect_instance.return_value = mock_instance

        def fake_download(target_path):
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(b"fake db content")

        def mock_create_path_fn(path):
            mock_file = MagicMock()
            if path.endswith(".gz"):
                mock_file.exists.return_value = False
            else:
                mock_file.exists.return_value = True
                mock_file.download_to.side_effect = fake_download
            return mock_file

        mock_create_path.side_effect = mock_create_path_fn

        from lamindb_setup.core._clone import connect_remote_sqlite

        connect_remote_sqlite("testowner/testname", copy_suffix="-copy")
