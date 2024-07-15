from __future__ import annotations

import subprocess
from pathlib import Path

import lamindb_setup as ln_setup


def test_auto_connect():
    current_state = ln_setup.settings.auto_connect
    ln_setup.settings.auto_connect = True
    assert ln_setup.settings._auto_connect_path.exists()
    ln_setup.settings.auto_connect = False
    assert not ln_setup.settings._auto_connect_path.exists()
    ln_setup.settings.auto_connect = current_state


def is_repo_clean() -> bool:
    from django import db

    django_dir = Path(db.__file__).parent.parent
    result = subprocess.run(
        ["git", "diff"],
        capture_output=True,
        text=True,
        check=True,
        cwd=django_dir,
    )
    print(result.stdout)
    print(result.stderr)
    return result.stdout.strip() == "" and result.stderr.strip() == ""


def test_private_django_api():
    current_state = ln_setup.settings.private_django_api
    ln_setup.settings.private_django_api = True
    assert not is_repo_clean()
    assert ln_setup.settings._private_django_api_path.exists()
    ln_setup.settings.private_django_api = False
    assert is_repo_clean()
    assert not ln_setup.settings._private_django_api_path.exists()
    ln_setup.settings.private_django_api = current_state
