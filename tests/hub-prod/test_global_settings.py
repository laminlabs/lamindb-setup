from __future__ import annotations

import subprocess
from pathlib import Path

import lamindb_setup as ln_setup
from lamindb_setup.core.hashing import hash_dir


def test_auto_connect():
    current_state = ln_setup.settings.auto_connect
    ln_setup.settings.auto_connect = True
    assert ln_setup.settings._auto_connect_path.exists()
    ln_setup.settings.auto_connect = False
    assert not ln_setup.settings._auto_connect_path.exists()
    ln_setup.settings.auto_connect = current_state


def test_private_django_api():
    from django import db

    django_dir = Path(db.__file__).parent.parent

    # below, we're checking whether a repo is clean via the internal hashing
    # function
    # def is_repo_clean() -> bool:
    #     from django import db

    #     django_dir = Path(db.__file__).parent.parent
    #     print(django_dir)
    #     result = subprocess.run(
    #         ["git", "diff"],
    #         capture_output=True,
    #         text=True,
    #         cwd=django_dir,
    #     )
    #     print(result.stdout)
    #     print(result.stderr)
    #     return result.stdout.strip() == "" and result.stderr.strip() == ""

    _, orig_hash, _, _ = hash_dir(django_dir)
    current_state = ln_setup.settings.private_django_api
    ln_setup.settings.private_django_api = True
    # do not run below on CI, but only locally
    # installing django via git didn't succeed
    # assert not is_repo_clean()
    _, hash, _, _ = hash_dir(django_dir)
    assert hash != orig_hash
    assert ln_setup.settings._private_django_api_path.exists()
    ln_setup.settings.private_django_api = False
    # assert is_repo_clean()
    _, hash, _, _ = hash_dir(django_dir)
    assert hash == orig_hash
    assert not ln_setup.settings._private_django_api_path.exists()
    ln_setup.settings.private_django_api = current_state
