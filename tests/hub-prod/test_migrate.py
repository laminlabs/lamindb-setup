from types import SimpleNamespace

import lamindb_setup as ln_setup
from lamindb_setup import _migrate


def test_migrate_create():
    assert ln_setup.migrate.create() is None


def test_migrate_deploy():
    assert ln_setup.migrate.deploy() is None


def test_migrate_check():
    assert ln_setup.migrate.check()


def test_migrate_on_hub_default_infers_from_managed_instance(monkeypatch):
    monkeypatch.delenv("LAMIN_MIGRATE_ON_HUB", raising=False)

    assert _migrate._infer_migrate_on_hub_default(is_managed_by_hub=True) is True
    assert _migrate._infer_migrate_on_hub_default(is_managed_by_hub=False) is False


def test_migrate_on_hub_env_var_overrides_inferred_default(monkeypatch):
    monkeypatch.setenv("LAMIN_MIGRATE_ON_HUB", "false")
    assert _migrate._infer_migrate_on_hub_default(is_managed_by_hub=True) is False

    monkeypatch.setenv("LAMIN_MIGRATE_ON_HUB", "true")
    assert _migrate._infer_migrate_on_hub_default(is_managed_by_hub=False) is True


def test_migrate_deploy_uses_local_path_when_env_var_overrides(monkeypatch):
    fake_instance = SimpleNamespace(
        is_managed_by_hub=True,
        is_on_hub=True,
        slug="owner/name",
    )
    fake_user = SimpleNamespace(access_token="token")
    monkeypatch.setattr(
        _migrate.settings,
        "_instance_settings",
        fake_instance,
        raising=False,
    )
    monkeypatch.setattr(_migrate.settings, "_user_settings", fake_user, raising=False)
    monkeypatch.setenv("LAMIN_MIGRATE_ON_HUB", "false")

    called = {"deploy": 0}
    monkeypatch.setattr(
        _migrate.migrate,
        "_deploy",
        lambda package_name=None, number=None: called.__setitem__("deploy", 1),
    )

    _migrate.migrate.deploy()

    assert called["deploy"] == 1
