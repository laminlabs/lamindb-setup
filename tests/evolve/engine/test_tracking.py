import os
import subprocess
from pathlib import Path

import pytest
from lamindb_setup.evolve.engine import _track as tracking


def test_git_gate_checks_the_plan_its_lock_and_the_upstream(tmp_path: Path) -> None:
    def git(*args: str) -> None:
        subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True)

    plan = tmp_path / "plan.py"
    plan.write_text("# plan\n")
    workspace = tracking.GitWorkspace()
    assert workspace.ensure_all_committed_and_pushed([]) == {}
    with pytest.raises(tracking.TrackError, match="not inside a git repository"):
        workspace.repo_for(plan)
    git("init", "-b", "main")
    git("config", "user.email", "test@example.org")
    git("config", "user.name", "Test")
    repo = workspace.repo_for(plan)
    assert repo.remote_url() is None
    assert tracking.repo_relative_path(plan, workspace=workspace) == Path("plan.py")
    with pytest.raises(tracking.TrackError, match="not tracked"):
        workspace.ensure_all_committed_and_pushed([plan])
    git("add", plan.name)
    git("commit", "--no-gpg-sign", "-m", "Add plan")
    with pytest.raises(tracking.TrackError, match="no upstream"):
        workspace.ensure_all_committed_and_pushed([plan])
    git("clone", "--bare", ".", "origin.git")
    git("remote", "add", "origin", str(tmp_path / "origin.git"))
    git("fetch", "origin")
    git("branch", "--set-upstream-to=origin/main", "main")
    lock = tmp_path / "plan.lock"
    tracking.ensure_lock_files_committed([lock])
    lock.write_text("{}")
    with pytest.raises(tracking.TrackError, match="lock file.*uncommitted"):
        tracking.ensure_lock_files_committed([lock])
    plan.write_text("# changed\n")
    with pytest.raises(tracking.TrackError, match="plan file.*uncommitted"):
        workspace.ensure_all_committed_and_pushed([plan])
    git("add", lock.name, plan.name)
    git("commit", "--no-gpg-sign", "-m", "Edit plan")
    tracking.ensure_lock_files_committed([lock])
    with pytest.raises(tracking.TrackError, match="not been pushed"):
        repo.ensure_head_pushed()
    git("checkout", "--detach")
    with pytest.raises(tracking.TrackError, match="not been pushed"):
        repo.ensure_head_pushed()
    git("checkout", "--detach", "origin/main")
    assert tracking.ensure_committed_and_pushed(plan) == Path(plan.name)
    with (
        pytest.raises(tracking.TrackError, match="invalid transform stem uid"),
        tracking.track_plan(plan, stem_uid="bad", workspace=workspace),
    ):
        pytest.fail("invalid uid should be rejected before tracking starts")


@pytest.mark.parametrize(
    ("origin", "expected"),
    [
        ("git@example.org:owner/plans.git", "https://example.org/owner/plans"),
        (
            "ssh://git@example.org:2222/owner/plans.git",
            "https://example.org/owner/plans",
        ),
        ("https://example.org/owner/plans.git", "https://example.org/owner/plans"),
        ("ssh://example.org", None),
    ],
)
def test_git_sync_restores_origin_after_failure(
    tmp_path: Path, origin: str, expected: str | None
) -> None:
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    repo = tracking.GitWorkspace().repo_at(tmp_path)
    repo.git("remote", "add", "origin", origin)
    with (
        pytest.raises(RuntimeError, match="apply failed"),
        repo.https_origin_with_ssh_insteadof() as remote,
    ):
        assert remote == expected
        if expected and not origin.startswith("https://"):
            assert repo.remote_url() == expected
            assert (
                repo.git("remote", "get-url", "origin")
                .stdout.strip()
                .startswith(origin.split("owner")[0])
            )
        raise RuntimeError("apply failed")
    assert repo.remote_url() == origin
    assert repo.git("config", "--local", "--get-regexp", "^url\\.").returncode == 1


def test_operation_tracks_same_transform_and_records_interrupt(ln, monkeypatch):
    monkeypatch.setenv(tracking.TRACK_PREFIX_ENV_VAR, "maintenance")
    assert tracking.track_prefix() == "maintenance"
    with tracking.track_operation("repair", params={"attempt": 1}) as completed:
        pass
    with (
        pytest.raises(KeyboardInterrupt),
        tracking.track_operation("repair", params={"attempt": 2}) as aborted,
    ):
        raise KeyboardInterrupt
    completed.refresh_from_db()
    aborted.refresh_from_db()
    assert completed.status == "completed"
    assert aborted.status == "aborted"
    assert (
        aborted.transform.stem_uid
        == completed.transform.stem_uid
        == tracking.plan_transform_uid("maintenance/repair")
    )
    visited = []
    generated = tracking.generate_stem_uids(
        2,
        instances=["first", "first", "second"],
        connect=lambda slug: visited.append(slug) or slug,
        ln=ln,
    )
    assert visited == ["first", "second"]
    assert len(set(generated)) == 2
    assert completed.transform.stem_uid not in generated


def test_failed_connect_restores_modules_and_announces_the_user_once(
    monkeypatch, capsys
):
    from unittest.mock import create_autospec

    from lamindb_setup.evolve.engine import _connect

    setup = create_autospec(_connect.LaminSetup, instance=True)
    setup.user.side_effect = LookupError("no current user")
    setup.get_modules.return_value = "bionty"
    setup.connect.side_effect = ConnectionError("instance unavailable")
    monkeypatch.setattr(_connect, "_user_announced", False)
    for slug in ("org/first", "org/second"):
        with pytest.raises(ConnectionError, match="instance unavailable"):
            _connect.safe_connect(slug, setup=setup, modules="pertdb")
        assert setup.set_modules.call_args.args == ("bionty",)
    assert capsys.readouterr().err.count("unknown (not logged in") == 1
    _connect.reset_user_announcement()
    _connect.echo_current_user(setup=setup)
    assert "unknown (not logged in" in capsys.readouterr().err


def test_default_instance_mask_is_restored_unless_already_connected(
    monkeypatch,
) -> None:
    from unittest.mock import create_autospec

    from lamindb_setup import settings
    from lamindb_setup.evolve.engine import _connect

    setup = create_autospec(_connect.LaminSetup, instance=True)
    original = settings._instance_settings
    setup.is_connected.return_value = True
    with _connect.no_default_instance(setup=setup):
        assert settings._instance_settings is original
    setup.is_connected.return_value = False
    monkeypatch.setenv(_connect.CURRENT_INSTANCE_ENV_VAR, "org/current")
    with pytest.raises(RuntimeError), _connect.no_default_instance(setup=setup):
        assert settings._instance_settings is not original
        raise RuntimeError("stop")
    assert settings._instance_settings is original
    assert os.environ[_connect.CURRENT_INSTANCE_ENV_VAR] == "org/current"


def test_tracking_continues_when_lamindb_declines_the_git_origin(
    tmp_path: Path, monkeypatch
) -> None:
    from contextlib import nullcontext
    from types import SimpleNamespace
    from unittest.mock import Mock, PropertyMock

    path = tmp_path / "plan.py"
    workspace = Mock(spec=tracking.GitWorkspace)
    workspace.verified_rel.return_value = Path("plan.py")
    workspace.root_for.return_value = tmp_path
    workspace.session_remote.return_value = nullcontext("https://example.test/org/repo")
    settings = Mock()
    setter = PropertyMock(side_effect=ValueError("origin unavailable"))
    monkeypatch.setattr(type(settings), "sync_git_repo", setter, raising=False)
    run = SimpleNamespace(report=None)
    client = SimpleNamespace(
        settings=settings, context=SimpleNamespace(run=run), track=Mock(), finish=Mock()
    )
    with tracking.track_plan(path, ln=client, workspace=workspace) as (_, current):
        assert current is run
    setter.assert_called_once_with("https://example.test/org/repo")
    client.track.assert_called_once()
    client.finish.assert_called_once_with()


@pytest.mark.parametrize("origin", [None, "ssh://example.test", "/tmp/local-repo"])
def test_unusable_remotes_produce_no_sync_url(origin) -> None:
    assert tracking._https_remote_url(origin) is None
    if origin is not None:
        assert tracking._git_url_bases(origin) is None
