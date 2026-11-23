# Adapted source tests use deliberately invalid inputs and lightweight stand-ins.
# mypy: disable-error-code="arg-type, attr-defined, call-overload, func-returns-value, return-value"
"""Tests for lamindb_setup.evolve.ci (config, PR check, Markdown rendering)."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from lamindb_setup.evolve.ci._config import ConfigError, load_config
from lamindb_setup.evolve.ci._markdown import render_report
from lamindb_setup.evolve.ci._pr import (
    ChangeRecord,
    DeploymentDiff,
    PlanValidation,
    PRReport,
    run_pr_check,
)
from lamindb_setup.evolve.console import OutputFormat

if TYPE_CHECKING:
    from collections.abc import Iterator

# --- config -----------------------------------------------------------------


def _write(tmp_path: Path, name: str, text: str) -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def _make_plan(tmp_path: Path, name: str) -> Path:
    return _write(tmp_path, name, "# a plan file\n")


def test_load_config_parses_instances_and_plans(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    _make_plan(tmp_path, "plan_b.py")
    config = _write(
        tmp_path,
        "lamin_schemas.yml",
        """
        instances:
          - slug: acme/dev
            plans:
              - plan_a.py:a
              - plan_b.py
            spaces: [sp1]
          - slug: acme/prod
            plans:
              - plan_a.py:a
            exclude_spaces: [sp2]
        """,
    )
    cfg = load_config(config)
    assert [i.slug for i in cfg.instances] == ["acme/dev", "acme/prod"]
    dev = cfg.instances[0]
    assert dev.plans[0].var == "a"
    assert dev.plans[0].path == (tmp_path / "plan_a.py").resolve()
    assert dev.plans[1].var is None
    assert dev.spaces == ["sp1"]
    assert cfg.instances[1].exclude_spaces == ["sp2"]
    assert cfg.plan_targets() == ["plan_a.py:a", "plan_b.py"]


def test_load_config_parses_drift_output_dir(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        "drift_output_dir: drift\ninstances:\n  - slug: a/b\n    plans: [plan_a.py]\n",
    )
    cfg = load_config(config)
    assert cfg.drift_output_dir == (tmp_path / "drift").resolve()


def test_load_config_drift_output_dir_defaults_to_none(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path, "c.yml", "instances:\n  - slug: a/b\n    plans: [plan_a.py]\n"
    )
    assert load_config(config).drift_output_dir is None


def test_load_config_rejects_non_string_drift_output_dir(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        "drift_output_dir: []\ninstances:\n  - slug: a/b\n    plans: [plan_a.py]\n",
    )
    with pytest.raises(ConfigError, match="'drift_output_dir' must be a non-empty"):
        load_config(config)


def test_load_config_import_root_defaults_to_the_config_dir(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path, "c.yml", "instances:\n  - slug: a/b\n    plans: [plan_a.py]\n"
    )
    cfg = load_config(config)
    assert cfg.project_root is None
    assert cfg.import_root == tmp_path.resolve()


def test_load_config_parses_project_root(tmp_path: Path) -> None:
    (tmp_path / "conf").mkdir()
    _make_plan(tmp_path / "conf", "plan_a.py")
    config = _write(
        tmp_path / "conf",
        "c.yml",
        "project_root: ..\ninstances:\n  - slug: a/b\n    plans: [plan_a.py]\n",
    )
    cfg = load_config(config)
    assert cfg.project_root == tmp_path.resolve()
    assert cfg.import_root == tmp_path.resolve()


def test_load_config_rejects_missing_project_root(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        "project_root: nope\ninstances:\n  - slug: a/b\n    plans: [plan_a.py]\n",
    )
    with pytest.raises(ConfigError, match="'project_root' is not a directory"):
        load_config(config)


def test_load_config_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.yml")


def test_load_config_rejects_unknown_top_level_key(tmp_path: Path) -> None:
    config = _write(tmp_path, "c.yml", "instances: []\nextra: 1\n")
    with pytest.raises(ConfigError, match="unknown top-level key"):
        load_config(config)


def test_load_config_rejects_unknown_instance_key(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        "instances:\n  - slug: a/b\n    plans: [plan_a.py]\n    bogus: 1\n",
    )
    with pytest.raises(ConfigError, match="unknown key"):
        load_config(config)


def test_load_config_requires_plans(tmp_path: Path) -> None:
    config = _write(tmp_path, "c.yml", "instances:\n  - slug: a/b\n    plans: []\n")
    with pytest.raises(ConfigError, match="non-empty 'plans'"):
        load_config(config)


def test_load_config_requires_slug(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(tmp_path, "c.yml", "instances:\n  - plans: [plan_a.py]\n")
    with pytest.raises(ConfigError, match="'slug'"):
        load_config(config)


def test_load_config_missing_plan_file(tmp_path: Path) -> None:
    config = _write(
        tmp_path, "c.yml", "instances:\n  - slug: a/b\n    plans: [missing.py]\n"
    )
    with pytest.raises(ConfigError, match="plan file not found"):
        load_config(config)


def test_load_config_rejects_duplicate_slug(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        "instances:\n"
        "  - slug: a/b\n    plans: [plan_a.py]\n"
        "  - slug: a/b\n    plans: [plan_a.py]\n",
    )
    with pytest.raises(ConfigError, match="duplicate instance slug"):
        load_config(config)


def test_load_config_rejects_duplicate_plan_path_aliases(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        "instances:\n  - slug: a/b\n    plans: [plan_a.py:a, ./plan_a.py:a]\n",
    )

    with pytest.raises(ConfigError, match="lists plan .* more than once"):
        load_config(config)


def test_load_config_rejects_implicit_and_explicit_aliases(
    tmp_path: Path,
) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        "instances:\n  - slug: a/b\n    plans: [plan_a.py, plan_a.py:a]\n",
    )

    with pytest.raises(ConfigError, match="lists plan .* more than once"):
        load_config(config)


def test_plan_targets_deduplicate_path_aliases_across_instances(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        "instances:\n"
        "  - slug: a/dev\n    plans: [plan_a.py:a]\n"
        "  - slug: a/prod\n    plans: [./plan_a.py:a]\n",
    )

    assert load_config(config).plan_targets() == ["plan_a.py:a"]


def test_load_config_rejects_inconsistent_implicit_alias_across_instances(
    tmp_path: Path,
) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        "instances:\n"
        "  - slug: a/dev\n    plans: [plan_a.py]\n"
        "  - slug: a/prod\n    plans: [plan_a.py:a]\n",
    )

    with pytest.raises(ConfigError, match="explicit variables consistently"):
        load_config(config)


def test_load_config_rejects_bad_target_suffix(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path, "c.yml", "instances:\n  - slug: a/b\n    plans: ['plan_a.py:']\n"
    )
    with pytest.raises(ConfigError, match="path/to/plan.py:plan_variable"):
        load_config(config)


def test_load_config_parses_ignored_plans(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        "instances:\n  - slug: a/b\n    plans: [plan_a.py]\n"
        "ignored_plans:\n  - plans/plan_drift_*.py\n",
    )
    cfg = load_config(config)
    assert cfg.ignored_plans == ["plans/plan_drift_*.py"]


def test_load_config_ignored_plans_defaults_empty(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path, "c.yml", "instances:\n  - slug: a/b\n    plans: [plan_a.py]\n"
    )
    assert load_config(config).ignored_plans == []


def test_load_config_parses_track_space(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    _make_plan(tmp_path, "plan_b.py")
    config = _write(
        tmp_path,
        "c.yml",
        "track_space: Default\ninstances:\n"
        "  - slug: acme/dev\n    plans: [plan_a.py]\n    track_space: DevMaintenance\n"
        "  - slug: acme/prod\n    plans: [plan_b.py]\n",
    )
    cfg = load_config(config)
    assert cfg.track_space == "Default"
    assert cfg.instances[0].track_space == "DevMaintenance"
    assert cfg.instances[1].track_space is None
    # Per-instance wins; top-level is the fallback for instances without one.
    assert cfg.track_space_for("acme/dev") == "DevMaintenance"
    assert cfg.track_space_for("acme/prod") == "Default"
    # Unknown instance falls back to the top-level default.
    assert cfg.track_space_for("acme/unknown") == "Default"


def test_load_config_track_space_defaults_to_none(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path, "c.yml", "instances:\n  - slug: a/b\n    plans: [plan_a.py]\n"
    )
    cfg = load_config(config)
    assert cfg.track_space is None
    assert cfg.track_space_for("a/b") is None


def test_load_config_rejects_blank_track_space(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        'track_space: ""\ninstances:\n  - slug: a/b\n    plans: [plan_a.py]\n',
    )
    with pytest.raises(ConfigError, match="'track_space' must be a non-empty"):
        load_config(config)


def test_load_config_parses_track_prefix(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    _make_plan(tmp_path, "plan_b.py")
    config = _write(
        tmp_path,
        "c.yml",
        "track_prefix: org/schemas\ninstances:\n"
        "  - slug: acme/dev\n    plans: [plan_a.py]\n    track_prefix: org/dev\n"
        "  - slug: acme/prod\n    plans: [plan_b.py]\n",
    )
    cfg = load_config(config)
    assert cfg.track_prefix == "org/schemas"
    assert cfg.instances[0].track_prefix == "org/dev"
    assert cfg.instances[1].track_prefix is None
    # Per-instance wins; top-level is the fallback for instances without one.
    assert cfg.track_prefix_for("acme/dev") == "org/dev"
    assert cfg.track_prefix_for("acme/prod") == "org/schemas"


def test_load_config_track_prefix_defaults_to_none(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path, "c.yml", "instances:\n  - slug: a/b\n    plans: [plan_a.py]\n"
    )
    cfg = load_config(config)
    assert cfg.track_prefix is None
    assert cfg.track_prefix_for("a/b") is None


def test_load_config_rejects_blank_track_prefix(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        'track_prefix: ""\ninstances:\n  - slug: a/b\n    plans: [plan_a.py]\n',
    )
    with pytest.raises(ConfigError, match="'track_prefix' must be a non-empty"):
        load_config(config)


def test_track_config_for_folds_both_settings_over_the_environment(
    tmp_path: Path,
) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        "instances:\n  - slug: acme/dev\n    plans: [plan_a.py]\n"
        "    track_prefix: org/dev\n    track_space: DevMaintenance\n",
    )
    cfg = load_config(config)

    resolved = cfg.track_config_for("acme/dev")

    assert (resolved.prefix, resolved.space) == ("org/dev", "DevMaintenance")


def test_load_config_parses_allow_data_validated_changes(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        "allow_data_validated_changes: true\n"
        "instances:\n  - slug: a/b\n    plans: [plan_a.py]\n",
    )
    assert load_config(config).allow_data_validated_changes is True


def test_load_config_allow_data_validated_changes_defaults_to_false(
    tmp_path: Path,
) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path, "c.yml", "instances:\n  - slug: a/b\n    plans: [plan_a.py]\n"
    )
    assert load_config(config).allow_data_validated_changes is False


def test_load_config_rejects_non_bool_allow_data_validated_changes(
    tmp_path: Path,
) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        "allow_data_validated_changes: yep\n"
        "instances:\n  - slug: a/b\n    plans: [plan_a.py]\n",
    )
    with pytest.raises(
        ConfigError, match="'allow_data_validated_changes' must be a boolean"
    ):
        load_config(config)


@pytest.mark.parametrize(
    ("line", "expected"),
    [("", True), ("strict_renames: true\n", True), ("strict_renames: false\n", False)],
)
def test_load_config_parses_strict_renames(
    tmp_path: Path, line: str, expected: bool
) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        f"{line}instances:\n  - slug: a/b\n    plans: [plan_a.py]\n",
    )
    assert load_config(config).strict_renames is expected


@pytest.mark.parametrize("value", ["nope", "1", "'false'", "[false]"])
def test_load_config_rejects_non_bool_strict_renames(
    tmp_path: Path, value: str
) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        f"strict_renames: {value}\ninstances:\n  - slug: a/b\n    plans: [plan_a.py]\n",
    )
    with pytest.raises(ConfigError, match="'strict_renames' must be a boolean"):
        load_config(config)


# --- markdown ---------------------------------------------------------------


def test_render_report_all_clear() -> None:
    report = PRReport(
        validations=[PlanValidation(target="plan_a.py:a", ok=True)],
        diffs=[
            DeploymentDiff(
                instance_slug="acme/dev", plan_target="plan_a.py:a", ok=True, changes=[]
            )
        ],
    )
    md = render_report(report)
    assert "no changes against any instance" in md
    assert "\u2705 `plan_a.py:a`" in md
    assert "up to date" in md


def test_render_report_with_changes() -> None:
    report = PRReport(
        validations=[PlanValidation(target="plan_a.py:a", ok=True)],
        diffs=[
            DeploymentDiff(
                instance_slug="acme/dev",
                plan_target="plan_a.py:a",
                ok=True,
                changes=[
                    ChangeRecord(
                        action="create", kind="Feature", name="col", uid="uid1"
                    )
                ],
            )
        ],
    )
    md = render_report(report)
    assert "pending changes" in md
    assert "1 to create, 0 to update, 0 to delete" in md
    assert "+ Feature 'col'  uid1" in md
    assert report.has_changes
    assert not report.has_errors


def test_render_report_validation_error() -> None:
    report = PRReport(
        validations=[PlanValidation(target="plan_a.py:a", ok=False, error="boom")],
    )
    md = render_report(report)
    assert "Validation failed" in md
    assert "boom" in md
    assert report.has_errors


def test_render_report_truncates_long_diffs() -> None:
    changes = [
        ChangeRecord(action="create", kind="Feature", name=f"c{i}", uid=f"u{i}")
        for i in range(120)
    ]
    report = PRReport(
        diffs=[
            DeploymentDiff(
                instance_slug="acme/dev",
                plan_target="plan_a.py:a",
                ok=True,
                changes=changes,
            )
        ]
    )
    md = render_report(report)
    assert "more change(s)" in md


def test_render_report_renders_transfer() -> None:
    report = PRReport(
        validations=[PlanValidation(target="plan_a.py:a", ok=True)],
        diffs=[
            DeploymentDiff(
                instance_slug="acme/dev",
                plan_target="plan_a.py:a",
                ok=True,
                changes=[
                    ChangeRecord(
                        action="transfer",
                        kind="Feature",
                        name="moved_col",
                        uid="uidm",
                        details=["now tracked by plan_b.py:b"],
                    )
                ],
            )
        ],
    )
    md = render_report(report)
    assert "1 to transfer" in md
    assert "> Feature 'moved_col'  uidm" in md
    assert "now tracked by plan_b.py:b" in md
    assert report.has_changes


def test_render_report_notes_missing_lock_file() -> None:
    report = PRReport(
        validations=[PlanValidation(target="plan_lab_workflow.py:wetlab", ok=True)],
        diffs=[
            DeploymentDiff(
                instance_slug="acme/dev",
                plan_target="plan_lab_workflow.py:wetlab",
                ok=True,
                changes=[],
                lock_missing=True,
            )
        ],
    )
    md = render_report(report)
    assert "No lock file for `plan_lab_workflow.py:wetlab`" in md
    assert "acme/dev" in md
    # an all-unchanged diff with a missing lock is not a pending change
    assert not report.has_changes


_CREATED_IN_RUN = "managed by prerequisite plan 'base' (created by it in this run)"


def _borrowed(name: str, *warnings: str) -> ChangeRecord:
    """A borrowed object the instance lacks, created by prerequisite `base`."""
    return ChangeRecord(
        action="create",
        kind="Feature",
        name=name,
        uid="(known after apply)",
        details=["managed by a prerequisite plan"] if warnings else [_CREATED_IN_RUN],
        warnings=list(warnings),
        borrowed=True,
    )


def _child_diff(*changes: ChangeRecord) -> DeploymentDiff:
    return DeploymentDiff(
        instance_slug="acme/dev",
        plan_target="plan_child.py",
        ok=True,
        changes=list(changes),
    )


def test_render_report_does_not_count_borrowed_objects_as_creates() -> None:
    own = ChangeRecord(action="create", kind="Feature", name="b", uid="u")
    diff = _child_diff(_borrowed("ft"), _borrowed("a"), own)
    report = PRReport(diffs=[diff])

    md = render_report(report)

    assert diff.actionable == [own]
    assert diff.borrowed == diff.changes[:2]
    assert diff.missing_prerequisites == []
    assert (
        "<summary>`plan_child.py` \u2192 `acme/dev` \u2014 1 to create, 0 to update, "
        "0 to delete, 2 to be created by prerequisite plans</summary>"
    ) in md
    # Listed as context lines, crediting the plan that creates them.
    assert f"  Feature 'ft'  (known after apply)\n    {_CREATED_IN_RUN}\n" in md
    assert "+ Feature 'b'  u" in md
    assert "WARNING" not in md


def test_a_borrowed_object_a_configured_prerequisite_creates_is_not_pending() -> None:
    diff = _child_diff(_borrowed("ft"))
    report = PRReport(diffs=[diff])

    md = render_report(report)

    assert not report.has_changes
    assert "no changes against any instance" in md
    # Still listed -- the instance lacks it -- but collapsed: nothing waits on it.
    assert "<details>\n<summary>`plan_child.py`" in md
    assert f"  Feature 'ft'  (known after apply)\n    {_CREATED_IN_RUN}" in md


def test_a_missing_prerequisite_is_pending_but_not_a_create() -> None:
    from lamindb_setup.evolve.core import ChangeWarning, WarningCategory

    warning = ChangeWarning(
        "owned by a prerequisite plan that has not been applied to this instance",
        WarningCategory.PREREQUISITE,
    )
    diff = _child_diff(_borrowed("ft", warning))
    report = PRReport(diffs=[diff])

    md = render_report(report)

    assert diff.actionable == []
    assert diff.missing_prerequisites == diff.changes
    # `ci apply` refuses it until the prerequisite is applied or configured.
    assert report.has_changes
    assert "<details open>" in md
    assert "0 to create, 0 to update, 0 to delete, 1 to be created by" in md
    assert f"  ! WARNING: {warning}" in md


def _rename_changes() -> list[ChangeRecord]:
    """A delete + create that looks like an undeclared rename of `old`."""
    return [
        ChangeRecord(action="delete", kind="Feature", name="old", uid="uid_old"),
        ChangeRecord(
            action="create",
            kind="Feature",
            name="new",
            uid="(known after apply)",
            warnings=["looks like a rename of deleted Feature 'old'"],
            suspected_rename_of="old",
        ),
    ]


def test_render_report_shows_a_refused_diff_above_its_refusal() -> None:
    report = PRReport(
        diffs=[
            DeploymentDiff(
                instance_slug="acme/dev",
                plan_target="plan_a.py:a",
                ok=False,
                error="refusing to apply 'a': 1 change(s) look like a rename",
                changes=_rename_changes(),
            )
        ]
    )

    md = render_report(report)

    assert report.has_errors
    assert "Validation failed" in md
    heading = md.index("#### \u274c `plan_a.py:a` \u2192 `acme/dev`")
    diff = md.index("<summary>Diff \u2014 1 to create, 0 to update, 1 to delete")
    create = md.index("+ Feature 'new'")
    refusal = md.index("```\nrefusing to apply 'a': 1 change(s) look like a rename")
    assert heading < diff < create < refusal
    assert "- Feature 'old'  uid_old" in md
    assert "! WARNING: looks like a rename of deleted Feature 'old'" in md


def test_render_report_shows_only_the_error_of_a_diff_that_did_not_run() -> None:
    report = PRReport(
        diffs=[
            DeploymentDiff(
                instance_slug="acme/dev",
                plan_target="plan_a.py:a",
                ok=False,
                error="connection failed: boom",
            )
        ]
    )

    md = render_report(report)

    assert "#### \u274c `plan_a.py:a` \u2192 `acme/dev`\n\n```\nconnection" in md
    assert "<details" not in md


# --- run_pr_check orchestration ---------------------------------------------


class _FakePlan:
    def __init__(self) -> None:
        self.finalized = False
        self.name = "fake"
        self._exports = None
        self.dependencies: list[object] = []
        self.depends_on: tuple[object, ...] = ()
        self.finalize_count = 0
        self.reset_count = 0

    def finalize(self) -> None:
        self.finalized = True
        self.finalize_count += 1

    def reset(self) -> None:
        self.finalized = False
        self.reset_count += 1


def _config_two_instances(tmp_path: Path) -> Path:
    _make_plan(tmp_path, "plan_a.py")
    _make_plan(tmp_path, "plan_b.py")
    return _write(
        tmp_path,
        "lamin_schemas.yml",
        "instances:\n"
        "  - slug: acme/dev\n    plans: [plan_a.py:a, plan_b.py:b]\n"
        "  - slug: acme/prod\n    plans: [plan_a.py:a]\n",
    )


def test_run_pr_check_validates_and_diffs_all(tmp_path: Path) -> None:
    cfg = load_config(_config_two_instances(tmp_path))

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        return _FakePlan(), tmp_path / "plan.py", object()

    def fake_connect(slug: str) -> str:
        return slug.replace("/", "-")

    def fake_compute(
        plan: object,
        plan_file: Path,
        slug: str,
        claimed_keys: set[str],
        claimed_by: dict[str, str] | None = None,
        allow_data_validated_changes: bool = False,
        co_applied_keys: set[str] | None = None,
    ) -> list[ChangeRecord]:
        return [ChangeRecord(action="create", kind="Feature", name="x", uid="u")]

    report = run_pr_check(
        cfg, load_plan=fake_load, connect=fake_connect, compute=fake_compute
    )
    assert {v.target for v in report.validations} == {"plan_a.py:a", "plan_b.py:b"}
    assert all(v.ok for v in report.validations)
    # plan_a on dev + prod, plan_b on dev = 3 diffs
    assert len(report.diffs) == 3
    assert report.has_changes


def test_run_pr_check_reports_its_stages(tmp_path: Path) -> None:
    # Without progress the check is silent for as long as it takes to load,
    # validate and diff every plan, which reads as a hang.
    cfg = load_config(_config_two_instances(tmp_path))
    messages: list[str] = []
    tracked: list[str] = []

    def fake_track(items: list[object], description: str) -> list[object]:
        tracked.append(description)
        return items

    run_pr_check(
        cfg,
        report=messages.append,
        track=fake_track,
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan.py", object()),
        connect=lambda slug: slug.replace("/", "-"),
        compute=lambda *a, **k: [],
    )
    assert "Connecting to acme/dev" in messages
    assert "Connecting to acme/prod" in messages
    assert any(d.startswith("Loading ") for d in tracked)
    assert any(d.startswith("Validating ") for d in tracked)
    assert any(d.startswith("Diffing plans against ") for d in tracked)


def test_run_pr_check_is_silent_without_progress_callbacks(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = load_config(_config_two_instances(tmp_path))

    run_pr_check(
        cfg,
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan.py", object()),
        connect=lambda slug: slug.replace("/", "-"),
        compute=lambda *a, **k: [],
    )
    assert capsys.readouterr().err == ""


def test_run_pr_check_constructs_all_plans_before_finalizing(tmp_path: Path) -> None:
    # Finalizing a plan imports lamindb, after which the construction guard
    # rejects further plans; so every plan must be constructed before any is
    # finalized.  This fake mimics that guard: constructing while a previous
    # plan has been finalized raises, exactly as the real guard would.
    cfg = load_config(_config_two_instances(tmp_path))
    any_finalized = [False]

    class _GuardedPlan:
        def __init__(self) -> None:
            if any_finalized[0]:
                raise RuntimeError(
                    "lamindb has already been imported; plans must be defined "
                    "before importing lamindb."
                )
            self.name = "guarded"
            self._exports = None
            self.dependencies: list[object] = []
            self.depends_on: tuple[object, ...] = ()

        def finalize(self) -> None:
            any_finalized[0] = True

    def fake_load(target: str) -> tuple[_GuardedPlan, Path, object]:
        return _GuardedPlan(), tmp_path / "plan.py", object()

    report = run_pr_check(
        cfg,
        load_plan=fake_load,
        connect=lambda s: s,
        compute=lambda *a: [],
    )
    assert {v.target for v in report.validations} == {"plan_a.py:a", "plan_b.py:b"}
    assert all(v.ok for v in report.validations), [v.error for v in report.validations]


def test_run_pr_check_filters_changed_files(tmp_path: Path) -> None:
    cfg = load_config(_config_two_instances(tmp_path))

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        return _FakePlan(), tmp_path / "plan.py", object()

    def fake_compute(
        plan: object,
        plan_file: Path,
        slug: str,
        claimed_keys: set[str],
        claimed_by: dict[str, str] | None = None,
        allow_data_validated_changes: bool = False,
        co_applied_keys: set[str] | None = None,
    ) -> list[ChangeRecord]:
        return []

    report = run_pr_check(
        cfg,
        changed_files=[str(tmp_path / "plan_b.py")],
        load_plan=fake_load,
        connect=lambda s: s,
        compute=fake_compute,
    )
    assert {v.target for v in report.validations} == {"plan_b.py:b"}
    # plan_b is only on dev
    assert {d.instance_slug for d in report.diffs} == {"acme/dev"}


def test_run_pr_check_blocks_diff_when_an_unchanged_sibling_is_invalid(
    tmp_path: Path,
) -> None:
    cfg = load_config(_config_two_instances(tmp_path))
    plan_a = cfg.instances[0].plans[0]
    plan_b = cfg.instances[0].plans[1]
    computed: list[str] = []

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        if target == plan_b.load_target:
            raise RuntimeError("cannot import sibling")
        plan = _FakePlan()
        plan.name = target
        return plan, tmp_path / "plan.py", object()

    def fake_compute(
        plan: _FakePlan,
        plan_file: Path,
        slug: str,
        claimed_keys: set[str],
        claimed_by: dict[str, str] | None = None,
        allow_data_validated_changes: bool = False,
        co_applied_keys: set[str] | None = None,
    ) -> list[ChangeRecord]:
        computed.append(slug)
        return []

    report = run_pr_check(
        cfg,
        changed_files=[str(tmp_path / "plan_a.py")],
        load_plan=fake_load,
        connect=lambda s: s,
        compute=fake_compute,
    )

    assert report.validations == [PlanValidation(target=plan_a.target, ok=True)]
    assert len(report.diffs) == 2
    dev = next(diff for diff in report.diffs if diff.instance_slug == "acme/dev")
    assert not dev.ok
    assert plan_b.target in (dev.error or "")
    assert "cannot import sibling" in (dev.error or "")
    assert computed == ["acme/prod"]
    assert report.has_errors


def test_run_pr_check_records_validation_failure(tmp_path: Path) -> None:
    cfg = load_config(_config_two_instances(tmp_path))

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        raise RuntimeError("cannot import plan")

    connected: list[str] = []

    report = run_pr_check(
        cfg,
        load_plan=fake_load,
        connect=lambda s: connected.append(s) or s,
        compute=lambda *a, **k: [],
    )
    assert all(not v.ok for v in report.validations)
    assert report.has_errors
    # no plan validated, so no instance connection is attempted
    assert connected == []


def test_run_pr_check_records_diff_failure(tmp_path: Path) -> None:
    cfg = load_config(_config_two_instances(tmp_path))

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        return _FakePlan(), tmp_path / "plan.py", object()

    def fake_compute(
        plan: object,
        plan_file: Path,
        slug: str,
        claimed_keys: set[str],
        claimed_by: dict[str, str] | None = None,
        allow_data_validated_changes: bool = False,
        co_applied_keys: set[str] | None = None,
    ) -> list[ChangeRecord]:
        raise RuntimeError("connection lost")

    report = run_pr_check(
        cfg, load_plan=fake_load, connect=lambda s: s, compute=fake_compute
    )
    assert all(not d.ok for d in report.diffs)
    assert report.has_errors


def test_run_pr_check_fails_a_diff_ci_apply_would_refuse_as_a_rename(
    tmp_path: Path,
) -> None:
    cfg = load_config(_config_two_instances(tmp_path))

    report = run_pr_check(
        cfg,
        changed_files=[str(tmp_path / "plan_b.py")],
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan.py", object()),
        connect=lambda slug: slug.replace("/", "-"),
        compute=lambda *a, **k: _rename_changes(),
    )

    (diff,) = report.diffs
    assert not diff.ok
    assert diff.suspected_renames == [("Feature", "old", "new")]
    assert (diff.error or "").startswith(
        "refusing to apply 'fake': 1 change(s) look like a rename but were not "
        "declared with ctx.moved"
    )
    assert "Feature 'old' -> 'new'" in (diff.error or "")
    # The diff refused is kept, for the report to show above the refusal.
    assert diff.changes == _rename_changes()
    assert report.has_errors


def test_run_pr_check_only_warns_of_a_rename_without_strict_renames(
    tmp_path: Path,
) -> None:
    cfg = load_config(_config_two_instances(tmp_path))

    report = run_pr_check(
        cfg,
        changed_files=[str(tmp_path / "plan_b.py")],
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan.py", object()),
        connect=lambda slug: slug.replace("/", "-"),
        compute=lambda *a, **k: _rename_changes(),
        strict_renames=False,
    )

    (diff,) = report.diffs
    assert diff.ok
    assert diff.error is None
    assert not report.has_errors
    assert report.has_changes
    assert "Plan shows pending changes" in render_report(report)


def _missing_prerequisite_changes(*, owner: bool = True) -> list[ChangeRecord]:
    """A borrowed object no configured plan creates, as the diff records it."""
    from lamindb_setup.evolve.core import (
        ChangeWarning,
        MissingPrerequisite,
        PrerequisitePlan,
        WarningCategory,
    )

    warning = ChangeWarning(
        "owned by a prerequisite plan that has not been applied to this instance",
        WarningCategory.PREREQUISITE,
    )
    borrowed = _borrowed("ft", warning)
    if owner:
        borrowed.missing_prerequisite = MissingPrerequisite(
            kind="Feature",
            name="ft",
            key="Feature:ft",
            owner=PrerequisitePlan("base", "plans/plan_base.py"),
        )
    own = ChangeRecord(action="create", kind="Feature", name="b", uid="u")
    return [borrowed, own]


def test_run_pr_check_fails_a_diff_borrowing_from_an_unapplied_prerequisite(
    tmp_path: Path,
) -> None:
    cfg = load_config(_config_two_instances(tmp_path))

    report = run_pr_check(
        cfg,
        changed_files=[str(tmp_path / "plan_b.py")],
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan.py", object()),
        connect=lambda slug: slug.replace("/", "-"),
        compute=lambda *a, **k: _missing_prerequisite_changes(),
    )

    (diff,) = report.diffs
    assert not diff.ok
    # The refusal `apply` gives, naming the configured instance.
    assert diff.error == (
        "refusing to apply 'fake': 1 object(s) it borrows from prerequisite plan "
        "'base' (plans/plan_base.py) do not exist on acme/dev:\n  Feature 'ft'\n"
        "Apply that plan first (lamin evolve apply plans/plan_base.py -i "
        "acme/dev), or list both plans in one apply -- they are applied "
        "dependencies-first."
    )
    # The diff refused is kept, for the report to show above the refusal.
    assert diff.changes == _missing_prerequisite_changes()
    assert report.has_errors
    md = render_report(report)
    assert md.index("```diff") < md.index("```\nrefusing to apply 'fake'")


def test_run_pr_check_names_an_unknown_prerequisite_of_an_injected_diff(
    tmp_path: Path,
) -> None:
    cfg = load_config(_config_two_instances(tmp_path))

    report = run_pr_check(
        cfg,
        changed_files=[str(tmp_path / "plan_b.py")],
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan.py", object()),
        connect=lambda slug: slug.replace("/", "-"),
        compute=lambda *a, **k: _missing_prerequisite_changes(owner=False),
    )

    (diff,) = report.diffs
    assert not diff.ok
    assert (diff.error or "").startswith(
        "refusing to apply 'fake': 1 object(s) it borrows from a prerequisite "
        "plan do not exist on acme/dev:\n  Feature 'ft'\n"
    )


def test_run_pr_check_keeps_both_refusals_of_a_diff(tmp_path: Path) -> None:
    cfg = load_config(_config_two_instances(tmp_path))

    report = run_pr_check(
        cfg,
        changed_files=[str(tmp_path / "plan_b.py")],
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan.py", object()),
        connect=lambda slug: slug.replace("/", "-"),
        compute=lambda *a, **k: [
            *_rename_changes(),
            *_missing_prerequisite_changes(),
        ],
    )

    (diff,) = report.diffs
    rename, prerequisite = (diff.error or "").split("\n\n")
    assert rename.startswith("refusing to apply 'fake': 1 change(s) look like")
    assert prerequisite.startswith("refusing to apply 'fake': 1 object(s) it borrows")


def test_run_pr_check_passes_a_borrowed_object_a_configured_plan_creates(
    tmp_path: Path,
) -> None:
    cfg = load_config(_config_two_instances(tmp_path))

    report = run_pr_check(
        cfg,
        changed_files=[str(tmp_path / "plan_b.py")],
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan.py", object()),
        connect=lambda slug: slug.replace("/", "-"),
        compute=lambda *a, **k: [_borrowed("ft")],
    )

    (diff,) = report.diffs
    assert diff.ok
    assert not report.has_errors


@pytest.mark.parametrize(
    ("line", "flag", "refused"),
    [
        ("", True, True),
        ("strict_renames: false\n", True, False),
        ("strict_renames: true\n", False, False),
    ],
)
def test_run_pr_check_ands_strict_renames_with_the_config(
    tmp_path: Path, line: str, flag: bool, refused: bool
) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "lamin_schemas.yml",
        f"{line}instances:\n  - slug: acme/dev\n    plans: [plan_a.py:a]\n",
    )

    report = run_pr_check(
        load_config(config),
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan.py", object()),
        connect=lambda slug: slug,
        compute=lambda *a, **k: _rename_changes(),
        lock_exists=lambda plan_file, slug: True,
        strict_renames=flag,
    )

    (diff,) = report.diffs
    assert diff.ok is not refused
    assert report.has_errors is refused


def test_run_pr_check_passes_sibling_claimed_keys(tmp_path: Path) -> None:
    cfg = load_config(_config_two_instances(tmp_path))
    dev_a = cfg.instances[0].plans[0]
    dev_b = cfg.instances[0].plans[1]

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        plan = _FakePlan()
        plan.name = target
        return plan, tmp_path / "plan.py", object()

    captured: dict[tuple[str, str], set[str]] = {}

    def fake_compute(
        plan: _FakePlan,
        plan_file: Path,
        slug: str,
        claimed_keys: set[str],
        claimed_by: dict[str, str] | None = None,
        allow_data_validated_changes: bool = False,
        co_applied_keys: set[str] | None = None,
    ) -> list[ChangeRecord]:
        captured[(plan.name, slug)] = claimed_keys
        return []

    report = run_pr_check(
        cfg,
        load_plan=fake_load,
        connect=lambda s: s,
        compute=fake_compute,
        keys_of=lambda plan: {plan.name},
        lock_exists=lambda plan_file, slug: True,
    )
    assert all(d.ok for d in report.diffs)
    # On dev each plan claims its sibling's key, protecting moved objects.
    assert captured[(dev_b.load_target, "acme/dev")] == {dev_a.load_target}
    assert captured[(dev_a.load_target, "acme/dev")] == {dev_b.load_target}
    # On prod plan_a is the only plan, so nothing is claimed.
    assert captured[(dev_a.load_target, "acme/prod")] == set()


def test_run_pr_check_passes_claimed_by_destination(tmp_path: Path) -> None:
    cfg = load_config(_config_two_instances(tmp_path))
    dev_a = cfg.instances[0].plans[0]
    dev_b = cfg.instances[0].plans[1]

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        plan = _FakePlan()
        plan.name = target
        return plan, tmp_path / "plan.py", object()

    captured: dict[tuple[str, str], dict[str, str]] = {}

    def fake_compute(
        plan: _FakePlan,
        plan_file: Path,
        slug: str,
        claimed_keys: set[str],
        claimed_by: dict[str, str] | None = None,
        allow_data_validated_changes: bool = False,
        co_applied_keys: set[str] | None = None,
    ) -> list[ChangeRecord]:
        captured[(plan.name, slug)] = claimed_by or {}
        return []

    run_pr_check(
        cfg,
        load_plan=fake_load,
        connect=lambda s: s,
        compute=fake_compute,
        keys_of=lambda plan: {plan.name},
        lock_exists=lambda plan_file, slug: True,
    )
    # Diffing plan_a, plan_b's key maps back to plan_b as the new owner.
    assert captured[(dev_a.load_target, "acme/dev")] == {
        dev_b.load_target: dev_b.target
    }
    assert captured[(dev_b.load_target, "acme/dev")] == {
        dev_a.load_target: dev_a.target
    }


class _FakeObj:
    def __init__(self, name: str) -> None:
        self.name = name


def test_run_pr_check_passes_what_configured_prerequisites_create(
    tmp_path: Path,
) -> None:
    # `child` borrows from `base`: both are configured on dev, dependent
    # first, and only `child` on prod.
    _make_plan(tmp_path, "plan_base.py")
    _make_plan(tmp_path, "plan_child.py")
    cfg = load_config(
        _write(
            tmp_path,
            "lamin_schemas.yml",
            "instances:\n"
            "  - slug: acme/dev\n    plans: [plan_child.py, plan_base.py]\n"
            "  - slug: acme/prod\n    plans: [plan_child.py]\n",
        )
    )
    base, child = _FakePlan(), _FakePlan()
    base.name, child.name = "base", "child"
    base.own_dependencies = [SimpleNamespace(obj=_FakeObj("b_ft"))]
    child.depends_on = (base,)
    plans = {"plan_base.py": base, "plan_child.py": child}

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        path = Path(target)
        return plans[path.name], path, object()

    captured: dict[tuple[str, str], set[str] | None] = {}

    def fake_compute(
        plan: _FakePlan,
        plan_file: Path,
        slug: str,
        claimed_keys: set[str],
        claimed_by: dict[str, str] | None = None,
        allow_data_validated_changes: bool = False,
        co_applied_keys: set[str] | None = None,
    ) -> list[ChangeRecord]:
        captured[(plan.name, slug)] = co_applied_keys
        return []

    report = run_pr_check(
        cfg,
        load_plan=fake_load,
        connect=lambda s: s,
        compute=fake_compute,
        keys_of=lambda plan: {plan.name},
        lock_exists=lambda plan_file, slug: True,
    )

    assert all(d.ok for d in report.diffs)
    # `ci apply` applies base first on dev, so what it declares is created.
    assert captured[("child", "acme/dev")] == {"_FakeObj:b_ft"}
    assert captured[("base", "acme/dev")] == set()
    # Nothing configured on prod creates it: still a missing prerequisite there.
    assert captured[("child", "acme/prod")] == set()


def test_run_pr_check_flags_missing_lock_file(tmp_path: Path) -> None:
    cfg = load_config(_config_two_instances(tmp_path))

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        return _FakePlan(), tmp_path / "plan.py", object()

    def fake_compute(
        plan: object,
        plan_file: Path,
        slug: str,
        claimed_keys: set[str],
        claimed_by: dict[str, str] | None = None,
        allow_data_validated_changes: bool = False,
        co_applied_keys: set[str] | None = None,
    ) -> list[ChangeRecord]:
        return []

    # plan_b has a lock for dev; plan_a has none anywhere.
    def fake_lock_exists(plan_file: Path, slug: str) -> bool:
        return False

    report = run_pr_check(
        cfg,
        load_plan=fake_load,
        connect=lambda s: s,
        compute=fake_compute,
        lock_exists=fake_lock_exists,
    )
    assert report.diffs
    assert all(d.lock_missing for d in report.diffs)


def test_run_pr_check_forwards_allow_data_validated_changes(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "lamin_schemas.yml",
        "allow_data_validated_changes: true\n"
        "instances:\n  - slug: acme/dev\n    plans: [plan_a.py:a]\n",
    )
    cfg = load_config(config)
    captured: list[bool] = []

    def fake_compute(
        plan: object,
        plan_file: Path,
        slug: str,
        claimed_keys: set[str],
        claimed_by: dict[str, str] | None = None,
        allow_data_validated_changes: bool = False,
        co_applied_keys: set[str] | None = None,
    ) -> list[ChangeRecord]:
        captured.append(allow_data_validated_changes)
        return []

    run_pr_check(
        cfg,
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan.py", object()),
        connect=lambda s: s,
        compute=fake_compute,
    )
    # The config setting is threaded into the per-instance diff computation.
    assert captured == [True]


@pytest.mark.parametrize(
    ("line", "flag", "expected"),
    [
        ("", False, False),
        ("", True, True),
        ("allow_data_validated_changes: false\n", True, True),
        ("allow_data_validated_changes: true\n", False, True),
    ],
)
def test_run_pr_check_ors_allow_data_validated_changes_with_the_config(
    tmp_path: Path, line: str, flag: bool, expected: bool
) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "lamin_schemas.yml",
        f"{line}instances:\n  - slug: acme/dev\n    plans: [plan_a.py:a]\n",
    )
    captured: list[bool] = []

    def fake_compute(
        *args: object, allow_data_validated_changes: bool = False, **kwargs: object
    ) -> list[ChangeRecord]:
        captured.append(allow_data_validated_changes)
        return []

    run_pr_check(
        load_config(config),
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan.py", object()),
        connect=lambda s: s,
        compute=fake_compute,
        allow_data_validated_changes=flag,
    )
    # As `ci apply` does: either the flag or the config opts the diff in.
    assert captured == [expected]


# --- drift detection --------------------------------------------------------


class _FakeLockEntry:
    def __init__(
        self, uid: str, kind: str | None = None, name: str | None = None
    ) -> None:
        self.uid = uid
        self.kind = kind
        self.name = name


class _FakeLock:
    def __init__(self, uids: list[str]) -> None:
        self.objects = {u: _FakeLockEntry(u) for u in uids}

    def uid_for(self, key: str) -> str | None:  # pragma: no cover - unused here
        entry = self.objects.get(key)
        return entry.uid if entry else None


class _FakeIntroResult:
    def __init__(
        self,
        output: Path,
        reconstructed: int,
        errors: list[str],
        out_of_scope_uids: dict[str, int] | None = None,
    ) -> None:
        self.output = output
        self.reconstructed = reconstructed
        self.errors = errors
        self.out_of_scope_uids = out_of_scope_uids or {}


def _drift_config(tmp_path: Path) -> Path:
    _make_plan(tmp_path, "plan_a.py")
    _make_plan(tmp_path, "plan_b.py")
    return _write(
        tmp_path,
        "lamin_schemas.yml",
        "instances:\n  - slug: acme/dev\n    plans: [plan_a.py:a, plan_b.py:b]\n",
    )


def _drift_config_two_instances(tmp_path: Path) -> Path:
    _make_plan(tmp_path, "plan_a.py")
    return _write(
        tmp_path,
        "lamin_schemas.yml",
        "instances:\n"
        "  - slug: acme/dev\n    plans: [plan_a.py:a]\n"
        "  - slug: acme/prod\n    plans: [plan_a.py:a]\n",
    )


def test_run_drift_detection_collects_tracked_and_writes_plan(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._drift import run_drift_detection

    cfg = load_config(_drift_config(tmp_path))
    seen_excludes: list[set[str]] = []

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        return _FakePlan(), tmp_path / f"{target}.py", object()

    def fake_read_lock(plan_file: Path, slug: str) -> _FakeLock:
        return _FakeLock(["uid_tracked"])

    def fake_introspect(instance: str, output: Path, **kwargs: object) -> object:
        seen_excludes.append(set(kwargs["exclude_uids"]))  # type: ignore[arg-type]
        return _FakeIntroResult(output, reconstructed=3, errors=[])

    def fake_compute(plan: object, plan_file: Path, slug: str) -> list[ChangeRecord]:
        return [
            ChangeRecord(action="update", kind="Feature", name="x", uid="uid_tracked")
        ]

    result = run_drift_detection(
        cfg,
        tmp_path,
        load_plan=fake_load,
        connect=lambda s: s.replace("/", "-"),
        introspect=fake_introspect,
        compute=fake_compute,
        read_lock=fake_read_lock,
    )
    inst = result.instances[0]
    assert inst.slug == "acme/dev"
    assert inst.untracked_objects == 3
    assert inst.modified_or_deleted == 2  # one update per plan (a + b)
    assert inst.has_drift
    assert result.has_drift
    # both plans contribute their tracked uid to the introspection exclude set
    assert seen_excludes == [{"uid_tracked"}]


def test_run_drift_detection_skips_plans_without_lock(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._drift import run_drift_detection

    cfg = load_config(_drift_config(tmp_path))

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        return _FakePlan(), tmp_path / "plan.py", object()

    result = run_drift_detection(
        cfg,
        tmp_path,
        load_plan=fake_load,
        connect=lambda s: s,
        introspect=lambda i, o, **k: _FakeIntroResult(o, 0, []),
        compute=lambda *a: [],
        read_lock=lambda pf, slug: None,
    )
    inst = result.instances[0]
    assert inst.skipped_plans == ["plan_a.py:a", "plan_b.py:b"]
    assert not inst.has_drift


def test_run_drift_detection_forwards_configured_spaces(tmp_path: Path) -> None:
    # The per-instance `spaces` / `exclude_spaces` keys configured in
    # lamin_schemas.yml must reach the introspection read so drift detection only
    # scans the intended spaces of an existing instance.
    from lamindb_setup.evolve.ci._drift import run_drift_detection

    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "lamin_schemas.yml",
        "instances:\n"
        "  - slug: acme/dev\n"
        "    plans: [plan_a.py:a]\n"
        "    spaces: [sp_keep1, sp_keep2]\n"
        "    exclude_spaces: [sp_drop]\n",
    )
    cfg = load_config(config)
    seen: dict[str, object] = {}

    def fake_introspect(instance: str, output: Path, **kwargs: object) -> object:
        seen["space_uids"] = kwargs["space_uids"]
        seen["exclude_space_uids"] = kwargs["exclude_space_uids"]
        return _FakeIntroResult(output, reconstructed=0, errors=[])

    run_drift_detection(
        cfg,
        tmp_path,
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan_a.py", object()),
        connect=lambda s: s.replace("/", "-"),
        introspect=fake_introspect,
        compute=lambda *a: [],
        read_lock=lambda pf, slug: _FakeLock(["uid_tracked"]),
    )
    assert seen["space_uids"] == ["sp_keep1", "sp_keep2"]
    assert seen["exclude_space_uids"] == ["sp_drop"]


def test_run_drift_detection_passes_no_space_filter_when_unconfigured(
    tmp_path: Path,
) -> None:
    # When no spaces are configured the introspection read must receive `None`
    # (scan everything), not an empty list.
    from lamindb_setup.evolve.ci._drift import run_drift_detection

    cfg = load_config(_drift_config(tmp_path))
    seen: dict[str, object] = {}

    def fake_introspect(instance: str, output: Path, **kwargs: object) -> object:
        seen["space_uids"] = kwargs["space_uids"]
        seen["exclude_space_uids"] = kwargs["exclude_space_uids"]
        return _FakeIntroResult(output, reconstructed=0, errors=[])

    run_drift_detection(
        cfg,
        tmp_path,
        load_plan=lambda target: (_FakePlan(), tmp_path / f"{target}.py", object()),
        connect=lambda s: s.replace("/", "-"),
        introspect=fake_introspect,
        compute=lambda *a: [],
        read_lock=lambda pf, slug: _FakeLock(["uid_tracked"]),
    )
    assert seen["space_uids"] is None
    assert seen["exclude_space_uids"] is None


def test_run_drift_detection_constructs_plans_before_connecting(
    tmp_path: Path,
) -> None:
    # Connecting to an instance imports lamindb, after which the construction
    # guard rejects further plans; so every configured plan must be constructed
    # before any connection.  This fake mimics that guard: constructing after a
    # connection has happened raises, exactly as the real guard would.
    from lamindb_setup.evolve.ci._drift import run_drift_detection

    cfg = load_config(_drift_config(tmp_path))
    connected = [False]

    class _GuardedPlan:
        def __init__(self) -> None:
            if connected[0]:
                raise RuntimeError(
                    "lamindb has already been imported; plans must be defined "
                    "before importing lamindb."
                )
            self.name = "guarded"
            self._exports = None
            self.dependencies: list[object] = []
            self.depends_on: tuple[object, ...] = ()

        def finalize(self) -> None:
            pass

    def fake_load(target: str) -> tuple[_GuardedPlan, Path, object]:
        return _GuardedPlan(), tmp_path / f"{target}.py", object()

    def fake_connect(slug: str) -> str:
        connected[0] = True
        return slug.replace("/", "-")

    result = run_drift_detection(
        cfg,
        tmp_path,
        load_plan=fake_load,
        connect=fake_connect,
        introspect=lambda i, o, **k: _FakeIntroResult(o, 0, []),
        compute=lambda *a: [],
        read_lock=lambda pf, slug: _FakeLock([]),
    )
    inst = result.instances[0]
    assert inst.errors == []


def test_run_drift_detection_suggests_exporting_tracked_objects(
    tmp_path: Path,
) -> None:
    # An untracked object references a tracked-but-unexported object; drift
    # detection should suggest exporting it from the owning plan rather than
    # silently dropping the reference.
    from lamindb_setup.evolve.ci._drift import run_drift_detection

    cfg = load_config(_drift_config(tmp_path))  # plan_a.py:a, plan_b.py:b on acme/dev

    # plan_a tracks a root FeatureType 'Root' (uid 'uid_root') but does not export
    # it; plan_b tracks nothing relevant.
    plan_a_lock = _FakeLock([])
    plan_a_lock.objects = {
        "FeatureType:Root": _FakeLockEntry("uid_root", "FeatureType", "Root")
    }

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        return _FakePlan(), tmp_path / f"{target}.py", object()

    def fake_read_lock(plan_file: Path, slug: str) -> _FakeLock:
        return plan_a_lock if "plan_a" in plan_file.name else _FakeLock([])

    def fake_introspect(instance: str, output: Path, **kwargs: object) -> object:
        # 'uid_root' is tracked (suggest export); 'uid_ghost' is tracked by no
        # plan (a genuine out-of-scope reference, no suggestion).
        return _FakeIntroResult(
            output,
            reconstructed=0,
            errors=["something references out-of-scope"],
            out_of_scope_uids={"uid_root": 4, "uid_ghost": 1},
        )

    result = run_drift_detection(
        cfg,
        tmp_path,
        load_plan=fake_load,
        connect=lambda s: s.replace("/", "-"),
        introspect=fake_introspect,
        compute=lambda *a: [],
        read_lock=fake_read_lock,
    )
    inst = result.instances[0]
    assert inst.has_export_suggestions
    assert result.has_export_suggestions
    assert len(inst.export_suggestions) == 1
    suggestion = inst.export_suggestions[0]
    assert suggestion.plan_target == "plan_a.py:a"
    # only the tracked uid is suggested; the untracked 'uid_ghost' is omitted
    assert [obj.uid for obj in suggestion.objects] == ["uid_root"]
    assert suggestion.objects[0].kind == "FeatureType"
    assert suggestion.objects[0].name == "Root"
    assert suggestion.objects[0].references == 4
    # suggestions alone do not count as drift
    assert not inst.has_drift


def test_run_drift_detection_no_suggestions_when_all_in_scope(
    tmp_path: Path,
) -> None:
    # When no out-of-scope reference points at a tracked object, there are no
    # export suggestions.
    from lamindb_setup.evolve.ci._drift import run_drift_detection

    cfg = load_config(_drift_config(tmp_path))

    result = run_drift_detection(
        cfg,
        tmp_path,
        load_plan=lambda target: (_FakePlan(), tmp_path / f"{target}.py", object()),
        connect=lambda s: s.replace("/", "-"),
        introspect=lambda i, o, **k: _FakeIntroResult(o, 3, []),
        compute=lambda *a: [],
        read_lock=lambda pf, slug: _FakeLock(["uid_tracked"]),
    )
    inst = result.instances[0]
    assert not inst.has_export_suggestions
    assert inst.export_suggestions == []


def test_render_drift_report_no_drift() -> None:
    from lamindb_setup.evolve.ci._drift import DriftResult, InstanceDriftResult
    from lamindb_setup.evolve.ci._markdown import render_drift_report

    result = DriftResult(instances=[InstanceDriftResult(slug="acme/dev")])
    md = render_drift_report(result)
    assert "No drift detected" in md
    assert "All objects are tracked and unchanged" in md


def test_render_drift_report_with_drift(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._drift import DriftResult, InstanceDriftResult
    from lamindb_setup.evolve.ci._markdown import render_drift_report

    inst = InstanceDriftResult(
        slug="acme/dev",
        output=tmp_path / "plan_drift_acme-dev.py",
        untracked_objects=2,
        skipped_plans=["plan_b.py:b"],
        diffs=[
            DeploymentDiff(
                instance_slug="acme/dev",
                plan_target="plan_a.py:a",
                ok=True,
                changes=[
                    ChangeRecord(action="delete", kind="Feature", name="gone", uid="u1")
                ],
            )
        ],
    )
    md = render_drift_report(DriftResult(instances=[inst]))
    assert "Drift detected" in md
    assert "Untracked objects: **2**" in md
    assert "Modified/deleted tracked objects: **1**" in md
    assert "No lock file for: `plan_b.py:b`" in md
    assert "- Feature 'gone'  u1" in md


def test_render_drift_report_renders_empty_object_warning() -> None:
    from lamindb_setup.evolve.ci._drift import DriftResult, InstanceDriftResult
    from lamindb_setup.evolve.ci._markdown import render_drift_report

    inst = InstanceDriftResult(
        slug="acme/dev",
        warnings=[
            "Schema 'AnnData of CELLxGENE version 5.2.0' (uid 'h6apWAUrUgGXxyVB'): "
            "empty schema (itype is 'Composite' but it has no slots and no "
            "members); it cannot be reconstructed as a valid Schema. Consider "
            "deleting it from the instance."
        ],
    )
    md = render_drift_report(DriftResult(instances=[inst]))
    assert "AnnData of CELLxGENE version 5.2.0" in md
    assert "Consider deleting it from the instance." in md


def test_render_drift_report_renders_export_suggestions() -> None:
    from lamindb_setup.evolve.ci._drift import (
        DriftResult,
        ExportSuggestion,
        ExportSuggestionObject,
        InstanceDriftResult,
    )
    from lamindb_setup.evolve.ci._markdown import render_drift_report

    inst = InstanceDriftResult(
        slug="acme/dev",
        errors=["X references out-of-scope feature type (uid 'uid_root')"],
        export_suggestions=[
            ExportSuggestion(
                plan_target="plan_a.py:a",
                plan_var="a",
                objects=[
                    ExportSuggestionObject(
                        uid="uid_root",
                        kind="FeatureType",
                        name="Root",
                        references=4,
                    )
                ],
            )
        ],
    )
    md = render_drift_report(DriftResult(instances=[inst]))
    assert "tracked but not exported" in md
    assert "Export suggestions" in md
    assert "Export **1** object(s) from `plan_a.py:a`" in md
    assert "**FeatureType** `Root` (uid `uid_root`)" in md
    assert "referenced by 4 untracked object(s)" in md


def _untracked_summary(objects: list[object]) -> object:
    from lamindb_setup.evolve.render import PlanRef, SummaryModel

    return SummaryModel(
        title="Untracked objects",
        plan=PlanRef(name="drift", default_space=None),
        objects=objects,  # type: ignore[arg-type]
    )


def test_render_drift_report_renders_untracked_hierarchy() -> None:
    from lamindb_setup.evolve.ci._drift import DriftResult, InstanceDriftResult
    from lamindb_setup.evolve.ci._markdown import render_drift_report
    from lamindb_setup.evolve.render import ObjectInfo

    objects = [
        ObjectInfo(obj_id=0, kind="FeatureType", name="grp"),
        ObjectInfo(
            obj_id=1, kind="Feature", name="col", details=["dtype=str"], parent_id=0
        ),
    ]
    inst = InstanceDriftResult(
        slug="acme/dev", untracked_objects=2, summary=_untracked_summary(objects)
    )
    md = render_drift_report(DriftResult(instances=[inst]))
    assert "<summary>Untracked objects by type</summary>" in md
    assert "#### Features (2)" in md
    assert "- **FeatureType** `grp`" in md
    # the feature nests one level under its type
    assert "  - **Feature** `col` (dtype=str)" in md


def test_render_drift_report_caps_untracked_hierarchy() -> None:
    from lamindb_setup.evolve.ci._drift import DriftResult, InstanceDriftResult
    from lamindb_setup.evolve.ci._markdown import render_drift_report
    from lamindb_setup.evolve.render import ObjectInfo

    objects = [ObjectInfo(obj_id=i, kind="Feature", name=f"f{i}") for i in range(30)]
    inst = InstanceDriftResult(
        slug="acme/dev", untracked_objects=30, summary=_untracked_summary(objects)
    )
    md = render_drift_report(DriftResult(instances=[inst]))
    assert "#### Features (30)" in md
    # 30 objects, capped at 25 per group -> 5 more
    assert "\u2026 5 more" in md


def test_render_drift_report_condenses_for_github_issue() -> None:
    from lamindb_setup.evolve.ci._drift import DriftResult, InstanceDriftResult
    from lamindb_setup.evolve.ci._markdown import render_drift_report
    from lamindb_setup.evolve.render import ObjectInfo

    objects = [ObjectInfo(obj_id=i, kind="Feature", name=f"f{i}") for i in range(30)]
    inst = InstanceDriftResult(
        slug="acme/dev", untracked_objects=30, summary=_untracked_summary(objects)
    )
    full = render_drift_report(DriftResult(instances=[inst]))
    condensed = render_drift_report(DriftResult(instances=[inst]), max_chars=200)
    # the headline counts survive, the verbose hierarchy is dropped
    assert "Untracked objects: **30**" in condensed
    assert "Untracked objects by type" not in condensed
    assert "Report condensed to fit GitHub" in condensed
    assert len(condensed) < len(full)
    # a report already within budget is returned unchanged
    assert render_drift_report(DriftResult(instances=[inst]), max_chars=10**6) == full


# --- ci apply ---------------------------------------------------------------


def _record_apply_one() -> tuple[list[tuple[Path, str, str]], object]:
    """Return a `(calls, apply_one)` pair recording each `apply_one` call."""
    calls: list[tuple[Path, str, str]] = []

    def apply_one(
        plan_obj: object,
        plan_file: Path,
        stem_uid: str,
        instance: str,
        instance_slug: str,
        *,
        claimed_keys: set[str],
        confirm: object,
        output_format: object,
        strict_renames: bool = True,
        allow_data_validated_changes: bool = False,
        strict_uids: bool = True,
        track_config: object = None,
    ) -> str | None:
        # ci apply runs unattended: it must never hand apply_one a prompt.
        assert confirm is None
        calls.append((plan_file, instance, instance_slug))
        return (
            f"applied {plan_file.name} to {instance}"
            if output_format is not OutputFormat.terminal
            else None
        )

    return calls, apply_one


def test_run_apply_applies_every_instance_plan(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._apply import run_apply

    cfg = load_config(_config_two_instances(tmp_path))
    calls, apply_one = _record_apply_one()

    sections = run_apply(
        cfg,
        load_plan=lambda target: (_FakePlan(), tmp_path / f"{target}.py", object()),
        connect=lambda slug: slug.replace("/", "-"),
        require_stem_uid=lambda module, plan_file: "stemuid01234",
        ensure_pushed=lambda plan_files: None,
        apply_one=apply_one,
    )
    # acme/dev applies plan_a + plan_b; acme/prod applies plan_a => 3 applies.
    applied = {(plan_file.name, instance) for plan_file, instance, _slug in calls}
    assert applied == {
        ("plan_a.py:a.py", "acme/dev"),
        ("plan_b.py:b.py", "acme/dev"),
        ("plan_a.py:a.py", "acme/prod"),
    }
    assert sections == []  # terminal mode returns no markdown sections


def test_run_apply_reports_its_stages(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._apply import run_apply

    cfg = load_config(_config_two_instances(tmp_path))
    _calls, apply_one = _record_apply_one()
    messages: list[str] = []

    run_apply(
        cfg,
        report=messages.append,
        load_plan=lambda target: (_FakePlan(), tmp_path / f"{target}.py", object()),
        connect=lambda slug: slug.replace("/", "-"),
        require_stem_uid=lambda module, plan_file: "stemuid01234",
        ensure_pushed=lambda plan_files: None,
        apply_one=apply_one,
    )
    assert "Loading plan_a.py:a" in messages
    assert "Connecting to acme/dev" in messages
    assert "Applying plan_a.py:a to acme-dev (1/2)" in messages
    assert "Applying plan_b.py:b to acme-dev (2/2)" in messages


def test_run_apply_reads_allow_data_validated_changes_from_config(
    tmp_path: Path,
) -> None:
    from lamindb_setup.evolve.ci._apply import run_apply

    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "lamin_schemas.yml",
        "allow_data_validated_changes: true\n"
        "instances:\n  - slug: acme/dev\n    plans: [plan_a.py:a]\n",
    )
    cfg = load_config(config)
    captured: list[bool] = []

    def apply_one(
        plan_obj: object,
        plan_file: Path,
        stem_uid: str,
        instance: str,
        instance_slug: str,
        *,
        claimed_keys: set[str],
        confirm: object,
        output_format: object,
        strict_renames: bool = True,
        allow_data_validated_changes: bool = False,
        strict_uids: bool = True,
        track_config: object = None,
    ) -> str | None:
        captured.append(allow_data_validated_changes)
        return None

    run_apply(
        cfg,
        load_plan=lambda target: (_FakePlan(), tmp_path / f"{target}.py", object()),
        connect=lambda slug: slug.replace("/", "-"),
        require_stem_uid=lambda module, plan_file: "stemuid01234",
        ensure_pushed=lambda plan_files: None,
        apply_one=apply_one,
    )
    # The config setting opts the apply in even without the CLI flag.
    assert captured == [True]


@pytest.mark.parametrize(
    ("line", "flag", "expected"),
    [
        ("", True, True),
        ("strict_renames: false\n", True, False),
        ("strict_renames: true\n", False, False),
    ],
)
def test_run_apply_ands_strict_renames_with_the_config(
    tmp_path: Path, line: str, flag: bool, expected: bool
) -> None:
    from lamindb_setup.evolve.ci._apply import run_apply

    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "lamin_schemas.yml",
        f"{line}instances:\n  - slug: acme/dev\n    plans: [plan_a.py:a]\n",
    )
    captured: list[bool] = []

    def apply_one(*args: object, strict_renames: bool = True, **kwargs: object) -> None:
        captured.append(strict_renames)

    run_apply(
        load_config(config),
        strict_renames=flag,
        load_plan=lambda target: (_FakePlan(), tmp_path / f"{target}.py", object()),
        connect=lambda slug: slug.replace("/", "-"),
        require_stem_uid=lambda module, plan_file: "stemuid01234",
        ensure_pushed=lambda plan_files: None,
        apply_one=apply_one,
    )
    # Either the flag or the config turns the check off.
    assert captured == [expected]


def test_run_apply_honours_the_configured_track_prefix_and_space(
    tmp_path: Path,
) -> None:
    """`ci apply` used to ignore the config's track settings and fall back to
    the environment, unlike `apply -c`."""
    from lamindb_setup.evolve.ci._apply import run_apply

    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "lamin_schemas.yml",
        "track_prefix: org/schemas\ntrack_space: Default\n"
        "instances:\n  - slug: acme/dev\n    plans: [plan_a.py:a]\n"
        "    track_space: DevMaintenance\n",
    )
    cfg = load_config(config)
    captured: list[object] = []

    def apply_one(
        plan_obj: object,
        plan_file: Path,
        stem_uid: str,
        instance: str,
        instance_slug: str,
        *,
        claimed_keys: set[str],
        confirm: object,
        output_format: object,
        strict_renames: bool = True,
        allow_data_validated_changes: bool = False,
        strict_uids: bool = True,
        track_config: object = None,
    ) -> str | None:
        captured.append(track_config)
        return None

    run_apply(
        cfg,
        load_plan=lambda target: (_FakePlan(), tmp_path / f"{target}.py", object()),
        connect=lambda slug: slug.replace("/", "-"),
        require_stem_uid=lambda module, plan_file: "stemuid01234",
        ensure_pushed=lambda plan_files: None,
        apply_one=apply_one,
    )

    # Top-level prefix, and the instance's own space winning over the default.
    assert len(captured) == 1
    resolved = captured[0]
    assert (resolved.prefix, resolved.space) == ("org/schemas", "DevMaintenance")


def test_run_apply_refinalizes_plans_after_connecting_each_instance(
    tmp_path: Path,
) -> None:
    # Switching instances rebuilds LaminDB's model classes, so a plan finalized
    # against a previous instance is stale for the next one.  run_apply must
    # reset the plans *after* connecting to each instance and finalize them
    # afresh, and every reset must happen after that instance's connection.
    from lamindb_setup.evolve.ci._apply import run_apply

    cfg = load_config(_config_two_instances(tmp_path))
    _calls, apply_one = _record_apply_one()

    created: list[_FakePlan] = []
    events: list[str] = []

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        plan = _FakePlan()
        _orig_reset = plan.reset

        def _reset() -> None:
            events.append("reset")
            _orig_reset()

        plan.reset = _reset  # type: ignore[method-assign]
        created.append(plan)
        return plan, tmp_path / f"{target}.py", object()

    def fake_connect(slug: str) -> str:
        events.append(f"connect:{slug}")
        return slug

    run_apply(
        cfg,
        load_plan=fake_load,
        connect=fake_connect,
        require_stem_uid=lambda module, plan_file: "stemuid01234",
        ensure_pushed=lambda plan_files: None,
        apply_one=apply_one,
    )

    # Two unique plans are loaded once and reused across two instances; each is
    # reset once per instance iteration (2 x 2 = 4 resets), and finalized once
    # per instance it is configured on (plan_a on dev + prod, plan_b on dev =>
    # 3 finalizes total).
    assert len(created) == 2
    assert sum(p.reset_count for p in created) == 4
    assert sum(p.finalize_count for p in created) == 3
    # A reset only ever follows a connect (never before the first connection).
    assert events[0].startswith("connect:")
    for i, event in enumerate(events):
        if event == "reset":
            assert any(e.startswith("connect:") for e in events[:i])


def test_run_apply_markdown_collects_sections(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._apply import run_apply

    cfg = load_config(_config_two_instances(tmp_path))
    _calls, apply_one = _record_apply_one()

    sections = run_apply(
        cfg,
        output_format=OutputFormat.markdown,
        load_plan=lambda target: (_FakePlan(), tmp_path / f"{target}.py", object()),
        connect=lambda slug: slug,
        require_stem_uid=lambda module, plan_file: "stemuid01234",
        ensure_pushed=lambda plan_files: None,
        apply_one=apply_one,
    )
    assert len(sections) == 3
    assert all(s.startswith("applied ") for s in sections)


def test_run_apply_filters_changed_files(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._apply import run_apply

    cfg = load_config(_config_two_instances(tmp_path))
    calls, apply_one = _record_apply_one()

    run_apply(
        cfg,
        changed_files=[str(tmp_path / "plan_b.py")],
        load_plan=lambda target: (_FakePlan(), tmp_path / f"{target}.py", object()),
        connect=lambda slug: slug,
        require_stem_uid=lambda module, plan_file: "stemuid01234",
        ensure_pushed=lambda plan_files: None,
        apply_one=apply_one,
    )
    # plan_b is only configured on acme/dev, whose plans (a + b) apply together.
    assert {instance for _file, instance, _slug in calls} == {"acme/dev"}
    assert {plan_file.name for plan_file, _i, _s in calls} == {
        "plan_a.py:a.py",
        "plan_b.py:b.py",
    }


def test_run_apply_rejects_unconfigured_changed_file(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._apply import run_apply

    cfg = load_config(_config_two_instances(tmp_path))
    _make_plan(tmp_path, "plan_other.py")

    with pytest.raises(ConfigError, match="not found in"):
        run_apply(
            cfg,
            changed_files=[str(tmp_path / "plan_other.py")],
            load_plan=lambda target: (_FakePlan(), tmp_path / "p.py", object()),
            connect=lambda slug: slug,
            require_stem_uid=lambda module, plan_file: "stemuid01234",
            ensure_pushed=lambda plan_files: None,
            apply_one=lambda *a, **k: None,
        )


# --- verify-config ----------------------------------------------------------


def _verify_layout(tmp_path: Path, *, ignored: list[str] | None = None) -> Path:
    """Lay out plans/plan_a.py (referenced) + plan_b.py and return the config."""
    plans = tmp_path / "plans"
    plans.mkdir()
    (plans / "plan_a.py").write_text("# a\n", encoding="utf-8")
    (plans / "plan_b.py").write_text("# b\n", encoding="utf-8")
    text = "instances:\n  - slug: a/b\n    plans: [plans/plan_a.py]\n"
    if ignored is not None:
        text += "ignored_plans:\n" + "".join(f"  - {g}\n" for g in ignored)
    return _write(tmp_path, "lamin_schemas.yml", text)


def test_verify_config_flags_orphan(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._verify import verify_config

    cfg = load_config(_verify_layout(tmp_path))
    result = verify_config(cfg)
    assert [p.name for p in result.orphans] == ["plan_b.py"]
    assert [p.name for p in result.referenced] == ["plan_a.py"]
    assert not result.ok


def test_verify_config_ignored_plan_passes(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._verify import verify_config

    cfg = load_config(_verify_layout(tmp_path, ignored=["plans/plan_b.py"]))
    result = verify_config(cfg)
    assert result.orphans == []
    assert [p.name for p in result.ignored] == ["plan_b.py"]
    assert result.ok


def test_verify_config_ignored_plan_with_lock_fails(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._verify import verify_config

    config = _verify_layout(tmp_path, ignored=["plans/plan_b.py"])
    (tmp_path / "plans" / "plan_b.a-b.lock").write_text("{}\n", encoding="utf-8")
    result = verify_config(load_config(config))
    assert not result.ok
    assert [p.name for p in result.ignored_with_locks] == ["plan_b.py"]


def test_run_apply_ignores_removed_changed_plan(tmp_path: Path) -> None:
    # A plan removed in the change (its file deleted, dropped from the config)
    # still shows up in the changed-file diff but no longer exists on disk; it
    # must be ignored rather than aborting the apply, while the instance's
    # surviving sibling plans (changed and still configured) still apply.
    from lamindb_setup.evolve.ci._apply import run_apply

    cfg = load_config(_config_two_instances(tmp_path))
    calls, apply_one = _record_apply_one()

    run_apply(
        cfg,
        changed_files=[
            str(tmp_path / "plan_b.py"),  # changed + configured on acme/dev
            str(tmp_path / "plan_removed.py"),  # deleted in this change, not on disk
        ],
        load_plan=lambda target: (_FakePlan(), tmp_path / f"{target}.py", object()),
        connect=lambda slug: slug,
        require_stem_uid=lambda module, plan_file: "stemuid01234",
        ensure_pushed=lambda plan_files: None,
        apply_one=apply_one,
    )
    # The removed plan is ignored; acme/dev is still selected via plan_b and
    # applies its configured plans.
    assert {instance for _file, instance, _slug in calls} == {"acme/dev"}


def test_run_apply_constructs_all_plans_before_connecting(tmp_path: Path) -> None:
    # Connecting to an instance imports lamindb, after which the construction
    # guard rejects further plans; so every configured plan must be constructed
    # before any connection.  This fake mimics that guard: constructing after a
    # connection has happened raises, exactly as the real guard would.
    from lamindb_setup.evolve.ci._apply import run_apply

    cfg = load_config(_config_two_instances(tmp_path))
    connected = [False]

    class _GuardedPlan:
        def __init__(self) -> None:
            if connected[0]:
                raise RuntimeError(
                    "lamindb has already been imported; plans must be defined "
                    "before importing lamindb."
                )
            self.name = "guarded"
            self._exports = None
            self.dependencies: list[object] = []
            self.depends_on: tuple[object, ...] = ()

        def finalize(self) -> None:
            pass

        def reset(self) -> None:
            pass

    def fake_connect(slug: str) -> str:
        connected[0] = True
        return slug

    run_apply(
        cfg,
        load_plan=lambda target: (_GuardedPlan(), tmp_path / f"{target}.py", object()),
        connect=fake_connect,
        require_stem_uid=lambda module, plan_file: "stemuid01234",
        ensure_pushed=lambda plan_files: None,
        apply_one=lambda *a, **k: None,
    )


# --- ci missing-exports -----------------------------------------------------


class _KeyedLock:
    """A fake lock mapping natural keys to entries (uid/kind/name)."""

    def __init__(self, entries: dict[str, tuple[str, str, str]]) -> None:
        self.objects = {
            key: _FakeLockEntry(uid, kind, name)
            for key, (uid, kind, name) in entries.items()
        }

    def uid_for(self, key: str) -> str | None:  # pragma: no cover - unused here
        entry = self.objects.get(key)
        return entry.uid if entry else None


class _ReconSpec:
    def __init__(self, deps: set[str]) -> None:
        self.deps = deps


class _ReconPlan:
    def __init__(self, specs: list[_ReconSpec], errors: list[str]) -> None:
        self.specs = specs
        self.errors = errors


def _group_for(inst: object, target: str) -> object:
    return next(g for g in inst.groups if g.plan_target == target)  # type: ignore[attr-defined]


def test_run_missing_exports_full_closure_in_one_pass(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._missing_exports import run_missing_exports

    cfg = load_config(_drift_config(tmp_path))  # acme/dev: plan_a.py:a, plan_b.py:b

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        return _FakePlan(), tmp_path / target.split(":", 1)[0], object()

    locks = {
        "plan_a.py": _KeyedLock(
            {
                "FeatureType:AType": ("uid_a_ft", "FeatureType", "AType"),
                "FeatureType:AType/Feature:af": ("uid_a_f", "Feature", "af"),
            }
        ),
        "plan_b.py": _KeyedLock(
            {"SchemaType:BType/Schema:BSchema": ("uid_b_s", "Schema", "BSchema")}
        ),
    }

    def fake_read_lock(plan_file: Path, slug: str) -> _KeyedLock:
        return locks[plan_file.name]

    seen: dict[str, object] = {}

    def fake_reconstruct(
        instance: object, exclude_uids: set[str], external_refs: dict[str, object]
    ) -> _ReconPlan:
        seen["exclude_uids"] = set(exclude_uids)
        seen["external_keys"] = set(external_refs)
        return _ReconPlan(
            specs=[
                _ReconSpec({"uid_a_ft", "uid_b_s"}),
                _ReconSpec({"uid_a_f", "uid_a_ft"}),
                _ReconSpec({"uid_a_f", "untracked_dep"}),
            ],
            errors=[
                "Feature 'signal_link' (uid 'x'): unsupported dtype: 'url'",
                "Sheet 'S' (uid 'y'): depends on Z which could not be reconstructed",
            ],
        )

    result = run_missing_exports(
        cfg,
        load_plan=fake_load,
        connect=lambda s: s.replace("/", "-"),
        read_lock=fake_read_lock,
        reconstruct=fake_reconstruct,
    )

    # The whole closure is offered in one pass: every tracked object is fed to
    # reconstruction as an external ref (so nothing fails for an out-of-scope
    # tracked reference), and the same set is excluded from the read.
    all_tracked = {"uid_a_ft", "uid_a_f", "uid_b_s"}
    assert seen["external_keys"] == all_tracked
    assert seen["exclude_uids"] == all_tracked

    inst = result.instances[0]
    assert inst.slug == "acme/dev"
    assert inst.reconstructable == 3
    assert inst.to_export == 3
    assert result.has_missing

    group_a = _group_for(inst, "plan_a.py:a")
    # sorted FeatureType before Feature; reference counts from the specs' deps
    assert [(o.kind, o.name, o.uid, o.references) for o in group_a.objects] == [
        ("FeatureType", "AType", "uid_a_ft", 2),
        ("Feature", "af", "uid_a_f", 2),
    ]
    assert group_a.objects[0].natural_key == "FeatureType:AType"
    group_b = _group_for(inst, "plan_b.py:b")
    assert [(o.name, o.uid, o.references) for o in group_b.objects] == [
        ("BSchema", "uid_b_s", 1)
    ]

    # blocked splits genuine root causes from cascades
    roots = [b for b in inst.blocked if b.root_cause]
    cascades = [b for b in inst.blocked if not b.root_cause]
    assert len(roots) == 1 and "unsupported dtype" in roots[0].message
    assert len(cascades) == 1


def test_run_missing_exports_reports_nothing_when_all_reachable(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._markdown import render_missing_exports_report
    from lamindb_setup.evolve.ci._missing_exports import run_missing_exports

    cfg = load_config(_drift_config(tmp_path))

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        return _FakePlan(), tmp_path / target.split(":", 1)[0], object()

    def fake_reconstruct(
        instance: object, exclude_uids: set[str], external_refs: dict[str, object]
    ) -> _ReconPlan:
        return _ReconPlan(specs=[_ReconSpec(set())], errors=[])

    result = run_missing_exports(
        cfg,
        load_plan=fake_load,
        connect=lambda s: s,
        read_lock=lambda pf, slug: _KeyedLock(
            {"FeatureType:T": ("u", "FeatureType", "T")}
        ),
        reconstruct=fake_reconstruct,
    )
    assert not result.has_missing
    md = render_missing_exports_report(result)
    assert "<!-- lamin-schemas-missing-exports -->" in md
    assert "Nothing to export" in md


def test_run_missing_exports_filters_changed_instances(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._missing_exports import run_missing_exports

    cfg = load_config(_config_two_instances(tmp_path))  # dev: a,b  prod: a

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        return _FakePlan(), tmp_path / target.split(":", 1)[0], object()

    result = run_missing_exports(
        cfg,
        changed_files=[str(tmp_path / "plan_b.py")],
        load_plan=fake_load,
        connect=lambda s: s,
        read_lock=lambda pf, slug: _KeyedLock({}),
        reconstruct=lambda inst, ex, refs: _ReconPlan(specs=[], errors=[]),
    )
    assert [i.slug for i in result.instances] == ["acme/dev"]


def test_run_missing_exports_skips_plans_without_lock(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._missing_exports import run_missing_exports

    cfg = load_config(_drift_config(tmp_path))

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        return _FakePlan(), tmp_path / target.split(":", 1)[0], object()

    result = run_missing_exports(
        cfg,
        load_plan=fake_load,
        connect=lambda s: s,
        read_lock=lambda pf, slug: None,
        reconstruct=lambda inst, ex, refs: _ReconPlan(specs=[], errors=[]),
    )
    inst = result.instances[0]
    assert inst.skipped_plans == ["plan_a.py:a", "plan_b.py:b"]
    assert not inst.has_missing


def test_render_missing_exports_report_lists_table_and_blocked() -> None:
    import dataclasses
    import json

    from lamindb_setup.evolve.ci._markdown import render_missing_exports_report
    from lamindb_setup.evolve.ci._missing_exports import (
        BlockedObject,
        InstanceMissingExports,
        MissingExport,
        MissingExportsResult,
        PlanExportGroup,
    )

    inst = InstanceMissingExports(
        slug="acme/dev",
        reconstructable=7,
        groups=[
            PlanExportGroup(
                plan_target="plans/plan_x.py:x",
                plan_var="x",
                objects=[
                    MissingExport(
                        uid="U1",
                        kind="Feature",
                        name="amplicon_name",
                        natural_key="FeatureType:Gene/Feature:amplicon_name",
                        references=4,
                    )
                ],
            )
        ],
        blocked=[
            BlockedObject("Feature 'x' (uid 'z'): unsupported dtype: 'url'", True),
            BlockedObject(
                "Sheet 'q': depends on X which could not be reconstructed", False
            ),
        ],
    )
    result = MissingExportsResult(instances=[inst])
    md = render_missing_exports_report(result)
    assert "## lamin evolve missing-exports" in md
    assert "natural key" in md  # how-to preamble mentions it
    assert "| kind | name | natural key | uid | refs |" in md
    assert "`FeatureType:Gene/Feature:amplicon_name`" in md
    assert "`U1`" in md
    assert "Blocked" in md and "unsupported dtype" in md
    assert "Export 1 object(s)" in md

    # the --json payload is a plain dict tree
    payload = json.loads(json.dumps(dataclasses.asdict(result)))
    assert payload["instances"][0]["groups"][0]["objects"][0]["uid"] == "U1"


def test_a_failed_introspection_is_not_no_drift() -> None:
    """The zero defaults of an unchecked instance read exactly like a clean one."""
    from lamindb_setup.evolve.ci._drift import InstanceDriftResult

    broken = InstanceDriftResult(slug="acc/inst", errors=["introspection failed: 401"])

    assert not broken.has_drift
    assert broken.has_errors


def test_a_failed_diff_counts_as_an_error() -> None:
    from lamindb_setup.evolve.ci._drift import InstanceDriftResult
    from lamindb_setup.evolve.ci._pr import DeploymentDiff

    broken = InstanceDriftResult(
        slug="acc/inst",
        diffs=[
            DeploymentDiff(
                instance_slug="acc/inst",
                plan_target="p.py:p",
                ok=False,
                error="boom",
            )
        ],
    )

    assert not broken.has_drift
    assert broken.has_errors


def test_a_failed_reconstruction_is_not_nothing_to_export() -> None:
    from lamindb_setup.evolve.ci._missing_exports import InstanceMissingExports

    broken = InstanceMissingExports(slug="acc/inst", errors=["reconstruct failed"])

    assert not broken.has_missing
    assert broken.has_errors


def test_run_apply_isolates_a_failing_instance_and_still_applies_the_rest(
    tmp_path: Path,
) -> None:
    """Stopping would leave the remaining instances neither applied nor reported."""
    from lamindb_setup.evolve.ci._apply import CIApplyError, run_apply

    cfg = load_config(_config_two_instances(tmp_path))
    seen: list[str] = []

    def apply_one(
        plan_obj: object, plan_file: Path, stem_uid: str, instance: str, slug: str, **kw
    ) -> str | None:
        seen.append(instance)
        if instance == "acme/dev":
            raise RuntimeError("instance is down")
        return f"section for {instance}"

    with pytest.raises(CIApplyError) as excinfo:
        run_apply(
            cfg,
            output_format=OutputFormat.markdown,
            load_plan=lambda t: (_FakePlan(), tmp_path / f"{t}.py", object()),
            connect=lambda slug: slug.replace("/", "-"),
            require_stem_uid=lambda module, plan_file: "stemuid01234",
            ensure_pushed=lambda plan_files: None,
            apply_one=apply_one,
        )

    # acme/prod was still attempted after acme/dev failed...
    assert "acme/prod" in seen
    # ...and the sections it produced ride along on the error, so the command
    # can write its report before exiting non-zero.
    assert excinfo.value.sections == ["section for acme/prod"]
    assert [slug for slug, _error in excinfo.value.failures] == ["acme/dev"]
    assert "instance is down" in str(excinfo.value)


def test_run_apply_failure_is_a_clean_cli_error(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._apply import CIApplyError
    from lamindb_setup.evolve.core import LaminSchemasError

    assert issubclass(CIApplyError, LaminSchemasError)


def test_a_failed_connect_does_not_abort_the_whole_drift_run(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._drift import run_drift_detection

    cfg = load_config(_drift_config_two_instances(tmp_path))
    reached: list[str] = []

    def connect(slug: str) -> str:
        reached.append(slug)
        if slug == "acme/dev":
            raise RuntimeError("401 unauthorized")
        return slug.replace("/", "-")

    result = run_drift_detection(
        cfg,
        tmp_path / "out",
        load_plan=lambda t: (_FakePlan(), tmp_path / f"{t}.py", object()),
        connect=connect,
        introspect=lambda instance, output, **kw: _FakeIntroResult(
            output, reconstructed=0, errors=[]
        ),
        compute=lambda plan_obj, plan_file, slug: [],
        read_lock=lambda plan_file, slug: _FakeLock([]),
    )

    assert reached == ["acme/dev", "acme/prod"]
    dev = next(i for i in result.instances if i.slug == "acme/dev")
    assert dev.has_errors
    assert "connection failed: 401 unauthorized" in dev.errors[0]
    prod = next(i for i in result.instances if i.slug == "acme/prod")
    assert not prod.has_errors


def test_refs_dependencies_first_orders_by_plan_dependency() -> None:
    """lamin_schemas.yml lists which plans are deployed, not in what order."""
    from lamindb_setup.evolve.ci._apply import _refs_dependencies_first
    from lamindb_setup.evolve.ci._config import PlanRef

    upstream = _FakePlan()
    downstream = _FakePlan()
    downstream.depends_on = (upstream,)
    up_ref = PlanRef(target="plan_up.py:up", path=Path("plan_up.py"), var="up")
    down_ref = PlanRef(
        target="plan_down.py:down", path=Path("plan_down.py"), var="down"
    )
    loaded = {
        up_ref.target: (upstream, Path("plan_up.py"), object()),
        down_ref.target: (downstream, Path("plan_down.py"), object()),
    }

    # configured dependent-first; applied prerequisite-first
    ordered = _refs_dependencies_first([down_ref, up_ref], loaded)

    assert [ref.target for ref in ordered] == [up_ref.target, down_ref.target]


def test_refs_dependencies_first_keeps_independent_plans_in_config_order() -> None:
    from lamindb_setup.evolve.ci._apply import _refs_dependencies_first
    from lamindb_setup.evolve.ci._config import PlanRef

    a_ref = PlanRef(target="plan_a.py:a", path=Path("plan_a.py"), var="a")
    b_ref = PlanRef(target="plan_b.py:b", path=Path("plan_b.py"), var="b")
    loaded = {
        a_ref.target: (_FakePlan(), Path("plan_a.py"), object()),
        b_ref.target: (_FakePlan(), Path("plan_b.py"), object()),
    }

    ordered = _refs_dependencies_first([b_ref, a_ref], loaded)

    assert [ref.target for ref in ordered] == [b_ref.target, a_ref.target]


# --- restricting a run to some instances (--instance/-i) ---------------------


def test_select_keeps_configuration_order_and_ignores_repeats(tmp_path: Path) -> None:
    cfg = load_config(_config_two_instances(tmp_path))

    def slugs(selection: list[str] | None) -> list[str]:
        return [instance.slug for instance in cfg.select(selection)]

    # ci apply relies on the configuration's order, not the arguments'.
    assert slugs(["acme/prod", "acme/dev", "acme/prod"]) == ["acme/dev", "acme/prod"]
    assert slugs(["acme/prod"]) == ["acme/prod"]
    assert slugs(None) == ["acme/dev", "acme/prod"]
    assert slugs([]) == ["acme/dev", "acme/prod"]


def test_select_refuses_an_unknown_slug_naming_the_configured_ones(
    tmp_path: Path,
) -> None:
    from lamindb_setup.evolve.core import InvalidArgumentError

    cfg = load_config(_config_two_instances(tmp_path))

    with pytest.raises(InvalidArgumentError) as excinfo:
        cfg.select(["acme/dev", "acme/staging", "acme/qa", "acme/staging"])

    assert str(excinfo.value) == (
        "unknown instance(s) 'acme/staging', 'acme/qa': "
        f"{cfg.config_path} configures acme/dev, acme/prod"
    )


def test_restricted_to_keeps_every_other_setting(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "lamin_schemas.yml",
        "strict_uids: false\ntrack_prefix: team/schemas\ninstances:\n"
        "  - slug: acme/dev\n    plans: [plan_a.py:a]\n"
        "  - slug: acme/prod\n    plans: [plan_a.py:a]\n",
    )
    cfg = load_config(config)

    narrowed = cfg.restricted_to(["acme/prod"])

    assert [instance.slug for instance in narrowed.instances] == ["acme/prod"]
    assert (narrowed.strict_uids, narrowed.track_prefix) == (False, "team/schemas")
    assert narrowed.config_path == cfg.config_path
    assert [instance.slug for instance in cfg.instances] == ["acme/dev", "acme/prod"]
    assert cfg.restricted_to(None) is cfg


@pytest.mark.parametrize(
    ("changed", "instances", "applied"),
    [
        (None, ["acme/prod"], ["acme/prod"]),
        # Given in reverse, applied in configuration order.
        (None, ["acme/prod", "acme/dev"], ["acme/dev", "acme/dev", "acme/prod"]),
        (["plan_a.py"], ["acme/prod"], ["acme/prod"]),
        # plan_b is configured on acme/dev only: the selections do not meet, and
        # plan_b is no unconfigured plan just because acme/dev is not selected.
        (["plan_b.py"], ["acme/prod"], []),
        (["plan_b.py"], ["acme/dev", "acme/prod"], ["acme/dev", "acme/dev"]),
    ],
)
def test_run_apply_intersects_changed_files_with_instances(
    tmp_path: Path,
    changed: list[str] | None,
    instances: list[str],
    applied: list[str],
) -> None:
    from lamindb_setup.evolve.ci._apply import run_apply

    cfg = load_config(_config_two_instances(tmp_path))
    calls, apply_one = _record_apply_one()

    run_apply(
        cfg,
        changed_files=None if changed is None else [str(tmp_path / f) for f in changed],
        instances=instances,
        load_plan=lambda target: (_FakePlan(), tmp_path / f"{target}.py", object()),
        connect=lambda slug: slug,
        require_stem_uid=lambda module, plan_file: "stemuid01234",
        ensure_pushed=lambda plan_files: None,
        apply_one=apply_one,
    )

    assert [instance for _file, instance, _slug in calls] == applied


def test_run_apply_refuses_an_unknown_instance_before_loading_a_plan(
    tmp_path: Path,
) -> None:
    from lamindb_setup.evolve.ci._apply import run_apply
    from lamindb_setup.evolve.core import InvalidArgumentError

    cfg = load_config(_config_two_instances(tmp_path))

    def load_plan(target: str) -> tuple[object, Path, object]:  # pragma: no cover
        raise AssertionError("no plan is loaded for a run that cannot start")

    with pytest.raises(InvalidArgumentError, match="'acme/staging'"):
        run_apply(cfg, instances=["acme/staging"], load_plan=load_plan)


def test_run_pr_check_diffs_only_the_selected_instances(tmp_path: Path) -> None:
    cfg = load_config(_config_two_instances(tmp_path))
    loaded: list[str] = []
    connected: list[str] = []

    def fake_load(target: str) -> tuple[_FakePlan, Path, object]:
        loaded.append(target)
        return _FakePlan(), tmp_path / "plan.py", object()

    def fake_connect(slug: str) -> str:
        connected.append(slug)
        return slug

    report = run_pr_check(
        cfg,
        instances=["acme/prod"],
        load_plan=fake_load,
        connect=fake_connect,
        compute=lambda *a, **k: [],
    )

    # plan_b is deployed to acme/dev only, so it is not this run's to validate.
    assert loaded == [str(tmp_path / "plan_a.py") + ":a"]
    assert [v.target for v in report.validations] == ["plan_a.py:a"]
    assert [(d.instance_slug, d.plan_target) for d in report.diffs] == [
        ("acme/prod", "plan_a.py:a")
    ]
    assert connected == ["acme/prod"]


@pytest.mark.parametrize(
    ("changed", "instances", "diffs"),
    [
        ("plan_a.py", ["acme/prod"], [("acme/prod", "plan_a.py:a")]),
        ("plan_b.py", ["acme/prod"], []),
        ("plan_b.py", ["acme/dev", "acme/prod"], [("acme/dev", "plan_b.py:b")]),
    ],
)
def test_run_pr_check_intersects_changed_files_with_instances(
    tmp_path: Path, changed: str, instances: list[str], diffs: list[tuple[str, str]]
) -> None:
    cfg = load_config(_config_two_instances(tmp_path))

    report = run_pr_check(
        cfg,
        changed_files=[str(tmp_path / changed)],
        instances=instances,
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan.py", object()),
        connect=lambda slug: slug,
        compute=lambda *a, **k: [],
    )

    assert [(d.instance_slug, d.plan_target) for d in report.diffs] == diffs
    assert [v.target for v in report.validations] == sorted({t for _s, t in diffs})


def test_run_drift_detection_checks_only_the_selected_instances(
    tmp_path: Path,
) -> None:
    from lamindb_setup.evolve.ci._drift import run_drift_detection

    cfg = load_config(_drift_config_two_instances(tmp_path))
    connected: list[str] = []

    def fake_connect(slug: str) -> str:
        connected.append(slug)
        return slug.replace("/", "-")

    result = run_drift_detection(
        cfg,
        tmp_path / "drift",
        instances=["acme/prod"],
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan.py", object()),
        connect=fake_connect,
        introspect=lambda i, o, **k: _FakeIntroResult(o, 0, []),
        compute=lambda *a: [],
        read_lock=lambda pf, slug: None,
    )

    assert [inst.slug for inst in result.instances] == ["acme/prod"]
    assert connected == ["acme/prod"]


def test_run_drift_detection_refuses_an_unknown_instance_before_writing(
    tmp_path: Path,
) -> None:
    from lamindb_setup.evolve.ci._drift import run_drift_detection
    from lamindb_setup.evolve.core import InvalidArgumentError

    cfg = load_config(_drift_config_two_instances(tmp_path))

    with pytest.raises(InvalidArgumentError, match="'acme/staging'"):
        run_drift_detection(cfg, tmp_path / "drift", instances=["acme/staging"])

    assert not (tmp_path / "drift").exists()


@pytest.mark.parametrize(
    ("changed", "instances", "checked"),
    [
        (None, ["acme/prod"], ["acme/prod"]),
        ("plan_a.py", ["acme/prod"], ["acme/prod"]),
        ("plan_b.py", ["acme/prod"], []),
        ("plan_b.py", ["acme/prod", "acme/dev"], ["acme/dev"]),
    ],
)
def test_run_missing_exports_intersects_changed_files_with_instances(
    tmp_path: Path, changed: str | None, instances: list[str], checked: list[str]
) -> None:
    from lamindb_setup.evolve.ci._missing_exports import run_missing_exports

    cfg = load_config(_config_two_instances(tmp_path))

    result = run_missing_exports(
        cfg,
        changed_files=None if changed is None else [str(tmp_path / changed)],
        instances=instances,
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan.py", object()),
        connect=lambda slug: slug,
        read_lock=lambda plan_file, slug: None,
        reconstruct=lambda instance, exclude, refs: _ReconPlan([], []),
    )

    assert [inst.slug for inst in result.instances] == checked


# --- ci plan --preview-migrations -------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [("", False), ("preview_migrations: true\n", True)],
)
def test_load_config_parses_preview_migrations(
    tmp_path: Path, text: str, expected: bool
) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path, "c.yml", f"{text}instances:\n  - slug: a/b\n    plans: [plan_a.py]\n"
    )
    assert load_config(config).preview_migrations is expected


def test_load_config_rejects_non_bool_preview_migrations(tmp_path: Path) -> None:
    _make_plan(tmp_path, "plan_a.py")
    config = _write(
        tmp_path,
        "c.yml",
        "preview_migrations: always\n"
        "instances:\n  - slug: a/b\n    plans: [plan_a.py]\n",
    )
    with pytest.raises(ConfigError, match="'preview_migrations' must be a boolean"):
        load_config(config)


def test_render_report_notes_a_rehearsal() -> None:
    note = "> [!NOTE]\n> Migrations rehearsed:"

    assert note in render_report(PRReport(preview_migrations=True))
    assert note not in render_report(PRReport())


class _Rehearsals:
    """`run_pr_check` hooks recording what is diffed, and in which rehearsal.

    `down` depends on `up`; `failing` names the plans whose diff raises,
    `renaming` those whose diff holds a rename not declared with `ctx.moved`.
    """

    def __init__(self, *failing: str, renaming: tuple[str, ...] = ()) -> None:
        self.events: list[str] = []
        self.failing = set(failing)
        self.renaming = set(renaming)
        self.up = _FakePlan()
        self.up.name = "up"
        self.up.own_dependencies = []  # type: ignore[attr-defined]
        self.down = _FakePlan()
        self.down.name = "down"
        self.down.depends_on = (self.up,)
        self._slug = ""

    def load(self, target: str) -> tuple[_FakePlan, Path, object]:
        plan = self.down if target.endswith(":down") else self.up
        return plan, Path(target.partition(":")[0]), object()

    def connect(self, slug: str) -> str:
        self._slug = slug
        return slug

    @contextmanager
    def rehearsal(self) -> Iterator[None]:
        self.events.append(f"open {self._slug}")
        yield
        self.events.append(f"close {self._slug}")

    def compute(
        self,
        plan: _FakePlan,
        plan_file: Path,
        slug: str,
        claimed_keys: set[str],
        claimed_by: dict[str, str] | None = None,
        allow_data_validated_changes: bool = False,
        co_applied_keys: set[str] | None = None,
    ) -> list[ChangeRecord]:
        self.events.append(f"{plan.name} on {slug}")
        if plan.name in self.failing:
            raise RuntimeError(f"{plan.name}: map_to_records cannot map 'gamma'")
        if plan.name in self.renaming:
            return _rename_changes()
        return [
            ChangeRecord(
                action="update",
                kind="Feature",
                name=plan.name,
                uid="u",
                details=["map_to_records: 2 matched"],
            )
        ]

    def run(self, config: Path, **kwargs: object) -> PRReport:
        return run_pr_check(
            load_config(config),
            load_plan=self.load,
            connect=self.connect,
            compute=self.compute,
            rehearsal=self.rehearsal,
            keys_of=lambda plan: {plan.name},
            lock_exists=lambda plan_file, slug: True,
            **kwargs,  # type: ignore[arg-type]
        )


def _rehearsal_config(tmp_path: Path, head: str = "") -> Path:
    """`down` (configured first) and its prerequisite `up` on dev; `up` on prod."""
    _make_plan(tmp_path, "plan_up.py")
    _make_plan(tmp_path, "plan_down.py")
    return _write(
        tmp_path,
        "lamin_schemas.yml",
        f"{head}instances:\n"
        "  - slug: acme/dev\n    plans: [plan_down.py:down, plan_up.py:up]\n"
        "  - slug: acme/prod\n    plans: [plan_up.py:up]\n",
    )


def _outcomes(report: PRReport) -> list[tuple[str, str, str | None]]:
    return [(d.plan_target, d.instance_slug, d.error) for d in report.diffs]


def test_run_pr_check_rehearses_each_instance_dependencies_first(
    tmp_path: Path,
) -> None:
    hooks = _Rehearsals()

    report = hooks.run(_rehearsal_config(tmp_path), preview_migrations=True)

    # One rehearsal per instance, prerequisites first, as ci apply runs them.
    assert hooks.events == [
        "open acme/dev",
        "up on acme/dev",
        "down on acme/dev",
        "close acme/dev",
        "open acme/prod",
        "up on acme/prod",
        "close acme/prod",
    ]
    # Reported in configuration order all the same.
    assert _outcomes(report) == [
        ("plan_down.py:down", "acme/dev", None),
        ("plan_up.py:up", "acme/dev", None),
        ("plan_up.py:up", "acme/prod", None),
    ]
    assert report.preview_migrations
    assert "    map_to_records: 2 matched" in render_report(report)
    # Re-finalized against each instance, as ci apply does.
    assert hooks.up.reset_count >= 2


@pytest.mark.parametrize(
    ("head", "kwargs", "rehearsed"),
    [
        ("", {}, False),
        ("", {"preview_migrations": True}, True),
        ("preview_migrations: true\n", {}, True),
        ("preview_migrations: false\n", {"preview_migrations": True}, True),
    ],
)
def test_run_pr_check_ors_preview_migrations_with_the_config(
    tmp_path: Path, head: str, kwargs: dict[str, bool], rehearsed: bool
) -> None:
    hooks = _Rehearsals()

    report = hooks.run(_rehearsal_config(tmp_path, head), **kwargs)

    assert ("open acme/dev" in hooks.events) is rehearsed
    assert report.preview_migrations is rehearsed
    if not rehearsed:
        # Without a rehearsal, ci plan diffs in configuration order, untouched.
        assert hooks.events == ["down on acme/dev", "up on acme/dev", "up on acme/prod"]
        assert hooks.up.reset_count == 0


def test_run_pr_check_stops_an_instances_rehearsal_at_its_first_failure(
    tmp_path: Path,
) -> None:
    hooks = _Rehearsals("up")

    report = hooks.run(_rehearsal_config(tmp_path), preview_migrations=True)

    # ci apply stops an instance at its first failure; the next one still runs.
    assert "down on acme/dev" not in hooks.events
    assert hooks.events.count("close acme/dev") == 1
    assert "up on acme/prod" in hooks.events
    failure = "up: map_to_records cannot map 'gamma'"
    assert _outcomes(report) == [
        ("plan_down.py:down", "acme/dev", _NOT_REHEARSED),
        ("plan_up.py:up", "acme/dev", failure),
        ("plan_up.py:up", "acme/prod", failure),
    ]
    assert report.has_errors


def test_run_pr_check_stops_an_instances_rehearsal_where_ci_apply_refuses(
    tmp_path: Path,
) -> None:
    hooks = _Rehearsals(renaming=("up",))

    report = hooks.run(_rehearsal_config(tmp_path), preview_migrations=True)

    assert "down on acme/dev" not in hooks.events
    down, up, _prod = report.diffs
    assert down.error == _NOT_REHEARSED
    assert up.error is not None and "ctx.moved" in up.error
    assert up.changes


def test_run_pr_check_reports_no_unchanged_plan_after_a_failure(
    tmp_path: Path,
) -> None:
    hooks = _Rehearsals("up")

    report = hooks.run(
        _rehearsal_config(tmp_path),
        changed_files=[str(tmp_path / "plan_up.py")],
        preview_migrations=True,
    )

    # `down` is not rehearsed after `up` failed, and nothing of it changed.
    assert "down on acme/dev" not in hooks.events
    assert [(target, slug) for target, slug, _error in _outcomes(report)] == [
        ("plan_up.py:up", "acme/dev"),
        ("plan_up.py:up", "acme/prod"),
    ]


_NOT_REHEARSED = (
    "not rehearsed: plan_up.py:up failed before it, and ci apply stops the "
    "instance there too"
)


@pytest.mark.parametrize("failing", [(), ("up",)])
def test_run_pr_check_rehearses_the_unchanged_plans_ci_apply_applies(
    tmp_path: Path, failing: tuple[str, ...]
) -> None:
    hooks = _Rehearsals(*failing)

    report = hooks.run(
        _rehearsal_config(tmp_path),
        changed_files=[str(tmp_path / "plan_down.py")],
        preview_migrations=True,
    )

    # prod deploys no changed plan, so it is not checked at all.
    assert hooks.events[:2] == ["open acme/dev", "up on acme/dev"]
    assert "open acme/prod" not in hooks.events
    if not failing:
        # The unchanged prerequisite is rehearsed, not reported.
        assert _outcomes(report) == [("plan_down.py:down", "acme/dev", None)]
        assert not report.has_errors
        return
    # ci apply would fail on it, so the check fails on it too.
    assert [(target, error is not None) for target, _, error in _outcomes(report)] == [
        ("plan_down.py:down", True),
        ("plan_up.py:up", True),
    ]
    assert report.diffs[1].error == (
        "rehearsing this unchanged plan, which ci apply applies along with the "
        "changed ones, failed: up: map_to_records cannot map 'gamma'"
    )
    assert report.has_errors
