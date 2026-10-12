"""Tests for lamindb_setup.switch."""

import lamindb as ln
import lamindb_setup as ln_setup
import pytest


def test_switch_create_existing_branch_raises():
    """Switch with create=True and existing branch raises BranchAlreadyExists with hint."""
    with pytest.raises(ln.errors.BranchAlreadyExists) as exc_info:
        ln_setup.switch("main", create=True)
    msg = str(exc_info.value)
    assert "already exists" in msg
    assert "-c/--create" in msg or "Omit" in msg


def test_switch_nonexistent_branch_suggests_create():
    missing = "nonexistent_branch_xyz_for_create_hint"
    with pytest.raises(ln.errors.DoesNotExist) as exc_info:
        ln_setup.switch(missing)
    msg = str(exc_info.value)
    assert "does not exist" in msg
    assert f"lamin switch -c {missing}" in msg


def test_switch_space():
    ln_setup.switch("all", space=True)
    assert ln_setup.settings.space.name == "all"
