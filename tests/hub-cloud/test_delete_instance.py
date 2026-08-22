from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import lamindb_setup as ln_setup
import pytest
from lamindb_setup._connect_instance import InstanceNotFoundError
from lamindb_setup._delete import _delete_dev_dir_and_local_marker_if_exists

if TYPE_CHECKING:
    from lamindb_setup.core._settings_instance import InstanceSettings

    pass


def test_delete_invalid_name():
    with pytest.raises(InstanceNotFoundError):
        ln_setup.delete("invalid/name")


def test_delete_removes_dev_dir_mapping_and_local_marker(tmp_path, monkeypatch) -> None:
    from lamindb_setup.core import _settings_store as settings_store

    monkeypatch.setattr(settings_store, "settings_dir", tmp_path)

    dev_dir = tmp_path / "project"
    marker = dev_dir / ".lamin" / "current_instance"
    marker.parent.mkdir(parents=True)
    marker.write_text("owner/name")

    dev_dir_settings_file = tmp_path / "dev-dir--owner--name.txt"
    dev_dir_settings_file.write_text(dev_dir.as_posix())

    isettings = cast(
        "InstanceSettings",
        SimpleNamespace(owner="owner", name="name", slug="owner/name"),
    )
    _delete_dev_dir_and_local_marker_if_exists(isettings)

    assert not dev_dir_settings_file.exists()
    assert not marker.exists()
    assert not marker.parent.exists()


def test_delete_keeps_marker_for_other_instance_slug(tmp_path, monkeypatch) -> None:
    from lamindb_setup.core import _settings_store as settings_store

    monkeypatch.setattr(settings_store, "settings_dir", tmp_path)

    dev_dir = tmp_path / "project"
    marker = dev_dir / ".lamin" / "current_instance"
    marker.parent.mkdir(parents=True)
    marker.write_text("owner/other-instance")

    dev_dir_settings_file = tmp_path / "dev-dir--owner--name.txt"
    dev_dir_settings_file.write_text(dev_dir.as_posix())

    isettings = cast(
        "InstanceSettings",
        SimpleNamespace(owner="owner", name="name", slug="owner/name"),
    )
    _delete_dev_dir_and_local_marker_if_exists(isettings)

    assert not dev_dir_settings_file.exists()
    assert marker.exists()
