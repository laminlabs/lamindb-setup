from __future__ import annotations

from types import SimpleNamespace

from lamindb_setup import _migrate


def test_migrate_on_lambda_default_infers_from_managed_instance(monkeypatch):
    monkeypatch.delenv("LAMIN_MIGRATE_ON_LAMBDA", raising=False)

    assert _migrate._infer_migrate_on_lambda_default(is_managed_by_hub=True) is True
    assert _migrate._infer_migrate_on_lambda_default(is_managed_by_hub=False) is False


def test_migrate_on_lambda_env_var_overrides_inferred_default(monkeypatch):
    monkeypatch.setenv("LAMIN_MIGRATE_ON_LAMBDA", "false")
    assert _migrate._infer_migrate_on_lambda_default(is_managed_by_hub=True) is False

    monkeypatch.setenv("LAMIN_MIGRATE_ON_LAMBDA", "true")
    assert _migrate._infer_migrate_on_lambda_default(is_managed_by_hub=False) is True


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
    monkeypatch.setenv("LAMIN_MIGRATE_ON_LAMBDA", "false")

    called = {"deploy": 0}
    monkeypatch.setattr(
        _migrate.migrate,
        "_deploy",
        lambda package_name=None, number=None: called.__setitem__("deploy", 1),
    )

    _migrate.migrate.deploy()

    assert called["deploy"] == 1
