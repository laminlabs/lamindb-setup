from unittest.mock import MagicMock, call
from uuid import UUID

import lamindb_setup as ln_setup
import lamindb_setup._init_instance as init_instance
import pytest
from django.conf import empty
from django.conf import settings as django_settings
from lamindb_setup.core import django as django_lamin
from lamindb_setup.core._settings import settings
from lamindb_setup.core._settings_instance import InstanceSettings


@pytest.fixture(autouse=True)
def unconfigured_django():
    previous_settings = object.__getattribute__(django_settings, "_wrapped")
    object.__setattr__(django_settings, "_wrapped", empty)
    yield
    object.__setattr__(django_settings, "_wrapped", previous_settings)


def test_init_template_database(monkeypatch):
    previous_isettings = settings._instance_settings
    calls = []

    monkeypatch.setattr(django_lamin, "IS_SETUP", False)
    monkeypatch.setattr(
        django_lamin,
        "reset_django",
        lambda: pytest.fail("init_template_database() must not reset Django"),
    )
    monkeypatch.setattr(
        init_instance,
        "_mark_postgres_db_as_template",
        lambda: calls.append("mark_postgres_db_as_template"),
    )

    def init_db(isettings):
        calls.append("init_db")
        assert isettings._id == UUID(int=0)
        assert isettings.slug == "none/none"
        assert isettings.db == "postgresql://postgres:pwd@localhost:5432/template"
        assert isettings.modules == {"bionty", "pertdb"}
        assert settings.instance is isettings

    monkeypatch.setattr(InstanceSettings, "_init_db", init_db)

    init_instance.init_template_database(
        uri="postgresql://postgres:pwd@localhost:5432/template",
        modules="bionty, pertdb",
    )

    assert calls == ["init_db", "mark_postgres_db_as_template"]
    assert settings._instance_settings is previous_isettings
    assert not hasattr(ln_setup, "init_template_database")
    assert not hasattr(ln_setup, "init_db_template")


def test_mark_postgres_db_as_template(monkeypatch):
    connection = MagicMock()
    connection.settings_dict = {"NAME": "lamin_template"}
    connection.ops.quote_name.return_value = '"lamin_template"'
    monkeypatch.setattr("django.db.connection", connection)
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (False,)

    init_instance._mark_postgres_db_as_template()

    assert cursor.execute.call_args_list == [
        call(
            'ALTER DATABASE "lamin_template" '
            "WITH IS_TEMPLATE TRUE ALLOW_CONNECTIONS FALSE"
        ),
        call(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid <> pg_backend_pid())"
        ),
    ]


def test_mark_postgres_db_as_template_rejects_other_connections(monkeypatch):
    connection = MagicMock()
    connection.settings_dict = {"NAME": "lamin_template"}
    connection.ops.quote_name.return_value = '"lamin_template"'
    monkeypatch.setattr("django.db.connection", connection)
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (True,)

    with pytest.raises(RuntimeError, match="other open connections"):
        init_instance._mark_postgres_db_as_template()

    cursor.execute.assert_called_with(
        'ALTER DATABASE "lamin_template" WITH IS_TEMPLATE FALSE ALLOW_CONNECTIONS TRUE'
    )


def test_init_template_database_cleans_up_on_error(monkeypatch):
    previous_isettings = settings._instance_settings

    monkeypatch.setattr(django_lamin, "IS_SETUP", False)
    monkeypatch.setattr(
        django_lamin,
        "reset_django",
        lambda: pytest.fail("init_template_database() must not reset Django"),
    )

    def raise_error(isettings):
        django_lamin.IS_MIGRATING = True
        raise RuntimeError("migration failed")

    monkeypatch.setattr(InstanceSettings, "_init_db", raise_error)

    with pytest.raises(RuntimeError, match="migration failed"):
        init_instance.init_template_database(
            uri="postgresql://postgres:pwd@localhost:5432/template"
        )

    assert django_lamin.IS_MIGRATING is False
    assert settings._instance_settings is previous_isettings


def test_init_template_database_rejects_connected_instance(monkeypatch):
    monkeypatch.setattr(django_lamin, "IS_SETUP", True)

    with pytest.raises(RuntimeError, match="without configured Django settings"):
        init_instance.init_template_database(
            uri="postgresql://postgres:pwd@localhost:5432/template"
        )


def test_init_template_database_rejects_configured_django(monkeypatch):
    monkeypatch.setattr(django_lamin, "IS_SETUP", False)
    object.__setattr__(django_settings, "_wrapped", object())

    with pytest.raises(RuntimeError, match="without configured Django settings"):
        init_instance.init_template_database(
            uri="postgresql://postgres:pwd@localhost:5432/template"
        )


def test_init_template_database_rejects_non_postgres_uri(monkeypatch):
    monkeypatch.setattr(django_lamin, "IS_SETUP", False)

    with pytest.raises(ValueError, match="must be a PostgreSQL connection URI"):
        init_instance.init_template_database(uri="sqlite:///template.db")


@pytest.mark.parametrize("database", ["postgres", "template0", "template1"])
def test_init_template_database_rejects_system_database(monkeypatch, database):
    monkeypatch.setattr(django_lamin, "IS_SETUP", False)

    with pytest.raises(ValueError, match="must name a dedicated PostgreSQL database"):
        init_instance.init_template_database(
            uri=f"postgresql://postgres:pwd@localhost:5432/{database}"
        )
