from __future__ import annotations

import lamindb_setup as ln_setup


def test_auto_connect():
    current_state = ln_setup.settings.auto_connect
    ln_setup.settings.auto_connect = True
    assert ln_setup.settings._auto_connect_path.exists()
    ln_setup.settings.auto_connect = False
    assert not ln_setup.settings._auto_connect_path.exists()
    ln_setup.settings.auto_connect = current_state


def test_prune_django_api():
    current_state = ln_setup.settings.prune_django_api
    ln_setup.settings.prune_django_api = True
    assert ln_setup.settings._prune_django_api_path.exists()
    ln_setup.settings.prune_django_api = False
    assert not ln_setup.settings._prune_django_api_path.exists()
    ln_setup.settings.prune_django_api = current_state
