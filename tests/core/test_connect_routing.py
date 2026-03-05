from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import lamindb_setup._connect_instance as connect_instance
import lamindb_setup.core.django as django_core


class _DummyInstanceSettings:
    def __init__(self, owner: str, name: str) -> None:
        self.owner = owner
        self.name = name

    @property
    def slug(self) -> str:
        return f"{self.owner}/{self.name}"


class _FakeConnectedInstance:
    def __init__(self) -> None:
        self.slug = "owner/name"
        self._is_cloud_sqlite = False
        self._sqlite_file_local = Path("unused")
        self._locker_user = None

    def _persist(self, write_to_disk: bool = False) -> None:
        return None

    def _load_db(self) -> tuple[bool, str]:
        return True, ""


def test_validate_connection_state_none_none_skips_reset(monkeypatch):
    connect_instance.settings._instance_settings = _DummyInstanceSettings(
        "none", "none"
    )
    called = {"reset": 0}
    monkeypatch.setattr(
        connect_instance, "reset_django", lambda: called.__setitem__("reset", 1)
    )

    did_reset = connect_instance.validate_connection_state("owner", "name")

    assert did_reset is False
    assert called["reset"] == 0


def test_validate_connection_state_connected_instance_resets(monkeypatch):
    connect_instance.settings._instance_settings = _DummyInstanceSettings(
        "owner", "old"
    )
    monkeypatch.setitem(
        sys.modules, "lamindb", SimpleNamespace(context=SimpleNamespace(transform=None))
    )
    called = {"reset": 0}
    monkeypatch.setattr(
        connect_instance, "reset_django", lambda: called.__setitem__("reset", 1)
    )

    warning_calls: list[str] = []
    monkeypatch.setattr(
        connect_instance.logger,
        "warning",
        lambda message: warning_calls.append(message),
    )
    did_reset = connect_instance.validate_connection_state("owner", "new")

    assert did_reset is True
    assert called["reset"] == 1
    assert warning_calls == ["re-setting django module variables, is experimental"]


def test_connect_rewrites_module_vars_only_after_reset(monkeypatch):
    fake_isettings = _FakeConnectedInstance()
    monkeypatch.setattr(connect_instance, "_check_instance_setup", lambda: True)
    monkeypatch.setattr(
        connect_instance,
        "_connect_instance",
        lambda *args, **kwargs: fake_isettings,
    )
    monkeypatch.setattr(connect_instance, "silence_loggers", lambda: None)
    monkeypatch.setattr(
        connect_instance, "load_from_isettings", lambda *args, **kwargs: None
    )

    rewrite_calls = {"n": 0}
    monkeypatch.setattr(
        connect_instance,
        "reset_django_module_variables",
        lambda: rewrite_calls.__setitem__("n", rewrite_calls["n"] + 1),
    )

    monkeypatch.setattr(
        connect_instance,
        "validate_connection_state",
        lambda owner, name, use_root_db_user=False: False,
    )
    connect_instance.connect("owner/name", _reload_lamindb=True)
    assert rewrite_calls["n"] == 0

    monkeypatch.setattr(
        connect_instance,
        "validate_connection_state",
        lambda owner, name, use_root_db_user=False: True,
    )
    connect_instance.connect("owner/name", _reload_lamindb=True)
    assert rewrite_calls["n"] == 1


def test_module_mismatch_warning_includes_modules_command(monkeypatch):
    warning_calls: list[str] = []
    monkeypatch.setattr(
        django_core.logger,
        "warning",
        lambda message: warning_calls.append(message),
    )
    django_core._warn_module_mismatch(
        target_apps={"lamindb", "bionty"},
        current_apps={"lamindb"},
    )
    assert len(warning_calls) == 1
    assert "instance schema modules differ" in warning_calls[0]
    assert "lamin settings modules set bionty" in warning_calls[0]
