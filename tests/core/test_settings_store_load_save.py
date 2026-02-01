"""Test settings store/load/save: _settings_store, _settings_load, _settings_save.

Covers reading and serializing .env files: full round-trip, missing required
vs optional fields, and additional (unknown) fields in the .env.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from lamindb_setup.core._settings_load import (
    load_instance_settings,
    setup_instance_from_store,
)
from lamindb_setup.core._settings_save import save_instance_settings
from lamindb_setup.core._settings_store import InstanceSettingsStore

if TYPE_CHECKING:
    from pathlib import Path

PREFIX = "lamindb_instance_"

# Sample .env content matching InstanceSettingsStore (current format).
FULL_ENV_CONTENT = """lamindb_instance_owner=laminlabs
lamindb_instance_name=lamindata
lamindb_instance_storage_root=s3://lamindata
lamindb_instance_storage_region=us-east-1
lamindb_instance_db=postgresql://037ba1e08d804f91a90275a47735076a_public:awcgcU06egZ0FSooIRalMi7j8Dw5r6GA7zB60An0@database2.cmyfs24wugc3.us-east-1.rds.amazonaws.com:5432/db1
lamindb_instance_schema_str=bionty,pertdb
lamindb_instance_id=037ba1e08d804f91a90275a47735076a
lamindb_instance_git_repo=null
lamindb_instance_keep_artifacts_local=False
lamindb_instance_api_url=https://aws.us-east-1.lamin.ai/api
lamindb_instance_schema_id=24117545d7f8e48a7df35ff8e17fd25a
lamindb_instance_fine_grained_access=True
lamindb_instance_db_permissions=public
lamindb_instance_is_clone=False
"""


def test_settings_env_full_roundtrip(tmp_path: Path) -> None:
    """Read full .env, load to store, serialize via InstanceSettings save, load again."""
    env_file = tmp_path / "current_instance.env"
    env_file.write_text(FULL_ENV_CONTENT)

    store1 = InstanceSettingsStore.from_env_file(env_file, PREFIX)
    settings = setup_instance_from_store(store1)
    saved_file = tmp_path / "saved.env"
    save_instance_settings(settings, saved_file)

    store2 = InstanceSettingsStore.from_env_file(saved_file, PREFIX)

    assert store2.owner == store1.owner == "laminlabs"
    assert store2.name == store1.name == "lamindata"
    assert store2.storage_root == store1.storage_root == "s3://lamindata"
    assert store2.id == store1.id == "037ba1e08d804f91a90275a47735076a"
    assert store2.fine_grained_access is store1.fine_grained_access is True
    assert store2.is_clone is store1.is_clone is False
    assert store2.api_url == store1.api_url
    assert store2.schema_id == store1.schema_id
    assert store2.db_permissions == store1.db_permissions == "public"


def test_settings_env_missing_required_field_raises(tmp_path: Path) -> None:
    """Missing required key (no default / without Optional in dataclass) raises ValueError."""
    # Required fields in InstanceSettingsStore (no default): owner, name, storage_root,
    # storage_region, db, schema_str, id, git_repo, keep_artifacts_local.
    content_without_owner = "\n".join(
        line
        for line in FULL_ENV_CONTENT.strip().split("\n")
        if not line.startswith("lamindb_instance_owner=")
    )
    env_file = tmp_path / "missing_required.env"
    env_file.write_text(content_without_owner)

    with pytest.raises(
        ValueError, match="Missing required key.*lamindb_instance_owner"
    ):
        InstanceSettingsStore.from_env_file(env_file, PREFIX)


def test_settings_env_missing_optional_field_succeeds(tmp_path: Path) -> None:
    """Missing optional key (with default / Optional type) loads; field is None or default."""
    # Optional fields: api_url, schema_id, fine_grained_access, db_permissions, is_clone.
    content_without_api_url = "\n".join(
        line
        for line in FULL_ENV_CONTENT.strip().split("\n")
        if not line.startswith("lamindb_instance_api_url=")
    )
    env_file = tmp_path / "missing_optional.env"
    env_file.write_text(content_without_api_url)

    store = InstanceSettingsStore.from_env_file(env_file, PREFIX)
    assert store.api_url is None
    assert store.owner == "laminlabs"
    assert store.schema_id == "24117545d7f8e48a7df35ff8e17fd25a"
    assert store.fine_grained_access is True
    assert store.is_clone is False


def test_settings_env_optional_field_explicit_null_succeeds(tmp_path: Path) -> None:
    """Optional key present with value 'null' loads as None."""
    content = FULL_ENV_CONTENT.replace(
        "lamindb_instance_api_url=https://aws.us-east-1.lamin.ai/api",
        "lamindb_instance_api_url=null",
    )
    env_file = tmp_path / "optional_null.env"
    env_file.write_text(content)

    store = InstanceSettingsStore.from_env_file(env_file, PREFIX)
    assert store.api_url is None


def test_settings_env_additional_fields_ignored(tmp_path: Path) -> None:
    """Additional (unknown) keys in .env are ignored on load; load succeeds."""
    extra_content = (
        FULL_ENV_CONTENT.strip()
        + "\nlamindb_instance_extra_field=foo\nlamindb_instance_another=bar\n"
    )
    env_file = tmp_path / "extra_fields.env"
    env_file.write_text(extra_content)

    store = InstanceSettingsStore.from_env_file(env_file, PREFIX)
    assert store.owner == "laminlabs"
    assert store.name == "lamindata"
    # Extra keys are not on the store; no error.
    assert not hasattr(store, "extra_field")
    assert not hasattr(store, "another")


def test_settings_env_load_via_load_instance_settings(tmp_path: Path) -> None:
    """Full .env loads through load_instance_settings (public API) and round-trip works."""
    env_file = tmp_path / "current_instance.env"
    env_file.write_text(FULL_ENV_CONTENT)

    settings = load_instance_settings(instance_settings_file=env_file)
    assert settings.owner == "laminlabs"
    assert settings.name == "lamindata"
    assert str(settings.storage.root).rstrip("/") == "s3://lamindata"

    saved_file = tmp_path / "saved.env"
    save_instance_settings(settings, saved_file)
    settings2 = load_instance_settings(instance_settings_file=saved_file)
    assert settings2.owner == settings.owner
    assert settings2.name == settings.name
    assert str(settings2.storage.root).rstrip("/") == str(settings.storage.root).rstrip(
        "/"
    )
