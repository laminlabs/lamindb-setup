from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import lamindb_setup._connect_instance as connect_instance
import lamindb_setup.core.django as django_core
import pytest
from lamindb_setup._check_setup import _check_instance_setup
from lamindb_setup.errors import ModuleWasntConfigured


class _DummyInstanceSettings:
    def __init__(self, owner: str, name: str, modules: set[str] | None = None) -> None:
        self.owner = owner
        self.name = name
        self._modules = set() if modules is None else modules

    @property
    def slug(self) -> str:
        return f"{self.owner}/{self.name}"

    @property
    def modules(self) -> set[str]:
        return self._modules


class _FakeConnectedInstance:
    def __init__(self) -> None:
        self.slug = "owner/name"
        self.dialect = "postgresql"
        self.db = "private"
        self._is_cloud_sqlite = False
        self._sqlite_file_local = Path("unused")
        self._locker_user = None

    def _persist(self, write_to_disk: bool = False) -> None:
        return None

    def _load_db(self) -> tuple[bool, str]:
        return True, ""


def test_validate_connection_state_none_none_skips_reset(monkeypatch):
    monkeypatch.setattr(
        connect_instance.settings,
        "_instance_settings",
        _DummyInstanceSettings("none", "none"),
        raising=False,
    )
    called = {"reset": 0}
    monkeypatch.setattr(
        connect_instance, "reset_django", lambda: called.__setitem__("reset", 1)
    )

    did_reset, already_connected = connect_instance.validate_connection_state(
        "owner", "name"
    )

    assert did_reset is False
    assert already_connected is False
    assert called["reset"] == 0


def test_validate_connection_state_connected_instance_resets(monkeypatch):
    monkeypatch.setattr(
        connect_instance.settings,
        "_instance_settings",
        _DummyInstanceSettings("owner", "old"),
        raising=False,
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
    did_reset, already_connected = connect_instance.validate_connection_state(
        "owner", "new"
    )

    assert did_reset is True
    assert already_connected is False
    assert called["reset"] == 1
    assert len(warning_calls) == 1
    assert warning_calls[0].startswith("re-setting django")


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
        lambda owner, name, use_root_db_user=False: (False, False),
    )
    connect_instance.connect("owner/name", _reload_lamindb=True)
    assert rewrite_calls["n"] == 0

    monkeypatch.setattr(
        connect_instance,
        "validate_connection_state",
        lambda owner, name, use_root_db_user=False: (True, False),
    )
    connect_instance.connect("owner/name", _reload_lamindb=True)
    assert rewrite_calls["n"] == 1


def test_connect_prints_modules_warning(monkeypatch):
    fake_isettings = _FakeConnectedInstance()
    monkeypatch.setattr(connect_instance, "_check_instance_setup", lambda: False)
    monkeypatch.setattr(
        connect_instance,
        "_connect_instance",
        lambda *args, **kwargs: fake_isettings,
    )
    monkeypatch.setattr(connect_instance, "silence_loggers", lambda: None)
    monkeypatch.setattr(
        connect_instance, "load_from_isettings", lambda *args, **kwargs: None
    )

    def _load_db_with_modules_warning() -> tuple[bool, str]:
        connect_instance.settings.modules_warning = "module mismatch warning"
        return True, ""

    monkeypatch.setattr(fake_isettings, "_load_db", _load_db_with_modules_warning)

    warning_calls: list[str] = []
    monkeypatch.setattr(
        connect_instance.logger,
        "warning",
        lambda message: warning_calls.append(message),
    )

    connect_instance.connect("owner/name", _reload_lamindb=False)

    assert "module mismatch warning" in warning_calls


def test_module_mismatch_warning_includes_modules_command():
    message = django_core._warn_module_mismatch(
        target_apps={"lamindb", "bionty"},
        current_apps={"lamindb"},
    )
    assert message is not None
    assert "instance" in message
    assert "has non-configured modules: bionty" in message
    assert "lamin settings modules set bionty" in message


def test_module_mismatch_warning_uses_empty_schema_str_for_core_only():
    message = django_core._warn_module_mismatch(
        target_apps={"lamindb"},
        current_apps={"lamindb", "bionty"},
    )
    assert message is not None
    assert 'lamin settings modules set ""' in message


def test_check_setup_uses_instance_modules_when_django_is_setup(monkeypatch):
    import lamindb_setup._check_setup as check_setup

    monkeypatch.setattr(django_core, "IS_SETUP", True)
    monkeypatch.setattr(
        check_setup.settings,
        "_instance_settings",
        _DummyInstanceSettings("owner", "name", modules={"bionty"}),
        raising=False,
    )
    with pytest.raises(ModuleWasntConfigured):
        _check_instance_setup(from_module="pertdb")


def test_update_db_using_local_prefers_sqlite_clone(monkeypatch, tmp_path):
    hub_instance_result = {
        "db_scheme": "postgresql",
        "db": "postgresql://none:none@fakeserver.xyz:5432/mydb",
    }
    settings_file = tmp_path / "instance.env"
    sqlite_db_url = "sqlite:////tmp/clone.db"
    monkeypatch.setattr(
        connect_instance,
        "try_synchronize_sqlite_clone",
        lambda storage_root: sqlite_db_url,
    )

    db = connect_instance.update_db_using_local(
        hub_instance_result=hub_instance_result,
        settings_file=settings_file,
        storage_root="s3://bucket/instance",
    )

    assert db == sqlite_db_url


def test_update_db_using_local_raises_without_sqlite_clone(monkeypatch, tmp_path):
    hub_instance_result = {
        "db_scheme": "postgresql",
        "db": "postgresql://none:none@fakeserver.xyz:5432/mydb",
    }
    settings_file = tmp_path / "instance.env"
    monkeypatch.setattr(
        connect_instance,
        "try_synchronize_sqlite_clone",
        lambda storage_root: None,
    )

    with pytest.raises(PermissionError, match="No database access"):
        connect_instance.update_db_using_local(
            hub_instance_result=hub_instance_result,
            settings_file=settings_file,
            storage_root="s3://bucket/instance",
        )


def test_connect_instance_uses_sqlite_clone_before_permission_error(
    monkeypatch, tmp_path
):
    settings_file = tmp_path / "instance.env"
    instance_result = {
        "id": "11111111-1111-1111-1111-111111111111",
        "name": "myinstance",
        "schema_str": "lamindb",
        "git_repo": None,
        "keep_artifacts_local": False,
        "api_url": None,
        "schema_id": None,
        "fine_grained_access": False,
        "db_permissions": "read",
        "db_scheme": "postgresql",
        "db": "postgresql://none:none@fakeserver.xyz:5432/mydb",
    }
    storage_result = {
        "root": "s3://bucket/myinstance",
        "region": None,
        "lnid": "abc123",
        "id": "22222222-2222-2222-2222-222222222222",
    }

    monkeypatch.setattr(
        connect_instance,
        "instance_settings_file",
        lambda name, owner: settings_file,
    )
    monkeypatch.setattr(
        "lamindb_setup.core._hub_core.connect_instance_hub",
        lambda **kwargs: (instance_result, storage_result),
    )
    monkeypatch.setattr(
        connect_instance,
        "try_synchronize_sqlite_clone",
        lambda storage_root: "sqlite:////tmp/clone.db",
    )
    monkeypatch.setattr(
        connect_instance,
        "StorageSettings",
        lambda **kwargs: SimpleNamespace(root=kwargs["root"]),
    )
    monkeypatch.setattr(
        connect_instance,
        "InstanceSettings",
        lambda **kwargs: SimpleNamespace(
            modules=set(kwargs["modules"].split(",")),
            db=kwargs["db"],
            is_remote=True,
            dialect="sqlite",
        ),
    )

    isettings = connect_instance._connect_instance("owner", "myinstance")

    assert isettings.db == "postgresql://none:none@fakeserver.xyz:5432/mydb"
