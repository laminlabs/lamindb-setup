# Adapted source tests use deliberately invalid inputs and lightweight stand-ins.
# mypy: disable-error-code="arg-type, return-value"
"""Tests for keeping plan-managed uids aligned across instances.

Two halves: `apply` pinning an object to the uid the plan's lock files for its
other instances record (:mod:`lamindb_setup.evolve.engine._alignment`), and
`repair align-uids` rewriting an instance's uids to a source's
(:mod:`lamindb_setup.evolve.maintenance._align_uids`).

A single test instance plays every role: "other instances" are lock files and
:class:`SiblingUids` built by hand, and a source is a :class:`SourceIdentity`
built from uids the instance does not have yet.
"""

from __future__ import annotations

import json
import secrets
import string
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from lamindb_setup.evolve import (
    Feature,
    FeatureType,
    LaminDType,
    Plan,
    PlanContext,
    PlanExports,
    Record,
    RecordType,
    Schema,
    SchemaType,
    Sheet,
    create_plan,
)
from lamindb_setup.evolve.ci._config import ConfigError, load_config
from lamindb_setup.evolve.ci._markdown import render_report
from lamindb_setup.evolve.ci._pr import ChangeRecord, DeploymentDiff, PRReport
from lamindb_setup.evolve.core import (
    Action,
    ApplyMode,
    ChangeItem,
    UidMisalignment,
    WarningCategory,
)
from lamindb_setup.evolve.engine._alignment import (
    SiblingUids,
    UidAlignmentError,
    read_sibling_uids,
    sibling_lock_paths,
)
from lamindb_setup.evolve.engine._apply import compute_changes, write_lock
from lamindb_setup.evolve.engine._lock import (
    LOCK_VERSION,
    LockEntry,
    LockFile,
    lock_path,
    natural_key,
)
from lamindb_setup.evolve.maintenance._align_uids import (
    HOLDER_CONFLICT,
    HOLDER_FREE,
    HOLDER_MERGE,
    HOLDER_REASSIGN,
    AlignError,
    SourceIdentity,
    compute_alignment,
    execute_alignment,
    read_source_identity,
    render_alignment,
    rewrite_target_locks,
    with_prerequisites,
)
from lamindb_setup.evolve.render import (
    render_changes,
    render_changes_json,
    render_changes_markdown,
    render_uid_misalignments,
    uid_misalignment_message,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType


_BASE62 = string.digits + string.ascii_letters
_SOURCE = "acme-source"
_TARGET = "acme-target"


def _uid(kind: str) -> str:
    length = 12 if kind in ("Feature", "FeatureType") else 16
    return "".join(secrets.choice(_BASE62) for _ in range(length))


@create_plan
def _align_plan(ctx: PlanContext, exports: None) -> None:
    import lamindb as ln

    ft = FeatureType("al_ft")
    f_name = Feature("al_name", dtype=LaminDType(str), type=ft)
    f_note = Feature("al_note", dtype=LaminDType(str), type=ft)
    rt = RecordType("al_rt")
    f_link = Feature(
        "al_link", dtype=LaminDType(ln.models.Record, type__uid=rt.uid), type=ft
    )
    st = SchemaType("al_st")
    schema = Schema(
        name="AlSchema",
        features=[f_name, f_note, f_link],
        index=f_name,
        optionals=[f_note],
        type=st,
    )
    Sheet("al_sheet", schema=schema, type=rt)
    Record("al_value", type=rt)


class _CoreExports(PlanExports):
    feature_type: FeatureType


@create_plan
def _core_plan(ctx: PlanContext, exports: _CoreExports) -> None:
    exports.feature_type = FeatureType("core_ft")


@create_plan
def _downstream_plan(
    ctx: PlanContext, exports: None, core: Plan[_CoreExports] = _core_plan
) -> None:
    Feature("down_col", dtype=LaminDType(str), type=core.exports.feature_type)


@create_plan
def _moved_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("mv_ft")
    ctx.moved(Feature("mv_new", dtype=LaminDType(str), type=ft), "mv_old")


@create_plan
def _optionals_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("op_ft")
    first = Feature("op_a", dtype=LaminDType(str), type=ft)
    second = Feature("op_b", dtype=LaminDType(str), type=ft)
    Schema(name="OpSchema", features=[first, second], optionals=[first, second])


def _twin_plan(*, optional: bool) -> Plan[None]:
    """A typed plan schema with `tw_b` optional, or made required."""

    @create_plan
    def _twin(ctx: PlanContext, exports: None) -> None:
        ft = FeatureType("tw_ft")
        a = Feature("tw_a", dtype=LaminDType(str), type=ft)
        b = Feature("tw_b", dtype=LaminDType(str), type=ft)
        Schema(
            name="TwSchema",
            features=[a, b],
            optionals=[b] if optional else None,
            type=SchemaType("tw_st"),
        )

    return _twin


_TWIN_OPTIONAL = _twin_plan(optional=True)
_TWIN_REQUIRED = _twin_plan(optional=False)

_PLANS = (
    _align_plan,
    _core_plan,
    _downstream_plan,
    _moved_plan,
    _optionals_plan,
    _TWIN_OPTIONAL,
    _TWIN_REQUIRED,
)


@pytest.fixture(autouse=True)
def _fresh_plans() -> Iterator[None]:
    """Module-level plans keep their finalized objects (and resolved promises)."""
    for plan in _PLANS:
        plan.reset()
    yield
    for plan in _PLANS:
        plan.reset()


def _own_keys(plan: Plan[Any]) -> dict[str, str]:
    """Natural key -> kind for every object `plan` declares itself."""
    plan.finalize()
    return {natural_key(d.obj): type(d.obj).__name__ for d in plan.own_dependencies}


def _siblings(
    tracked: dict[str, str], seeded: dict[str, str] | None = None
) -> SiblingUids:
    return SiblingUids(
        tracked={key: {_SOURCE: uid} for key, uid in tracked.items()},
        seeded={key: {_SOURCE: uid} for key, uid in (seeded or {}).items()},
    )


def _pinned_uids(plan: Plan[Any]) -> tuple[dict[str, str], dict[str, str]]:
    tracked: dict[str, str] = {}
    seeded: dict[str, str] = {}
    for key, kind in _own_keys(plan).items():
        (seeded if kind == "Record" else tracked)[key] = _uid(kind)
    return tracked, seeded


def _apply(
    plan: Plan[Any], lock_file: Path, **kwargs: Any
) -> Any:  # pragma: no cover - thin helper
    ctx = compute_changes(
        plan, mode=ApplyMode.APPLY, lock=LockFile.read(lock_file), **kwargs
    )
    write_lock(ctx, plan, lock_file)
    return ctx


def _registry(ln: LamindbModuleType, kind: str) -> Any:
    return {
        "FeatureType": ln.Feature,
        "Feature": ln.Feature,
        "SchemaType": ln.Schema,
        "Schema": ln.Schema,
        "RecordType": ln.Record,
        "Sheet": ln.Record,
        "Record": ln.Record,
    }[kind]


# === Lock file v6 ===============================================================


def test_lock_round_trips_the_seeded_section(tmp_path: Path) -> None:
    lock = LockFile(plan="p", seeded={"RecordType:rt/Record:v": "a" * 16})
    lock.add(LockEntry(key="RecordType:rt", kind="RecordType", name="rt", uid="b" * 16))
    path = tmp_path / "p.acme.lock"
    lock.write(path)

    read = LockFile.read(path)
    assert read is not None
    assert read.version == LOCK_VERSION == 6
    assert read.seeded == {"RecordType:rt/Record:v": "a" * 16}
    assert read.remove("RecordType:rt/Record:v")
    assert not read.is_empty
    assert read.remove("RecordType:rt")
    assert read.is_empty


def test_lock_without_seeded_section_is_read_as_empty(tmp_path: Path) -> None:
    path = tmp_path / "p.acme.lock"
    path.write_text(json.dumps({"version": 5, "plan": "p", "objects": {}}))
    lock = LockFile.read(path)
    assert lock is not None
    assert lock.seeded == {}
    assert lock.migrated_from == 5
    assert "seeded" not in lock.to_json()


def test_malformed_seeded_section_is_refused(tmp_path: Path) -> None:
    from lamindb_setup.evolve.engine._lock import LockFileError

    path = tmp_path / "p.acme.lock"
    path.write_text(json.dumps({"version": 6, "plan": "p", "seeded": ["x"]}))
    with pytest.raises(LockFileError):
        LockFile.read(path)


# === Sibling locks ==============================================================


def _write_lock(path: Path, plan: str, uids: dict[str, str]) -> None:
    lock = LockFile(plan=plan)
    for key, uid in uids.items():
        lock.add(LockEntry(key=key, kind="FeatureType", name=key, uid=uid))
    lock.write(path)


def test_sibling_lock_paths_lists_the_other_instances(tmp_path: Path) -> None:
    plan_file = tmp_path / "plan_x.py"
    for slug in ("acme-dev", "acme-prod", "acme-target"):
        _write_lock(lock_path(plan_file, slug), "x", {})
    (tmp_path / "plan_x.lock").write_text("{}")  # legacy, instance-less
    (tmp_path / "plan_xy.acme-dev.lock").write_text("{}")  # another plan

    assert set(sibling_lock_paths(plan_file, exclude="acme-target")) == {
        "acme-dev",
        "acme-prod",
    }


def test_read_sibling_uids_agrees_disagrees_and_ignores_other_plans(
    tmp_path: Path,
) -> None:
    plan_file = tmp_path / "plan_x.py"
    name = _core_plan.name
    _write_lock(lock_path(plan_file, "acme-dev"), name, {"A": "a1", "B": "b1"})
    _write_lock(lock_path(plan_file, "acme-prod"), name, {"A": "a1", "B": "b2"})
    _write_lock(lock_path(plan_file, "acme-target"), name, {"A": "zz"})
    # same stem prefix, but recording another plan
    _write_lock(lock_path(plan_file, "v2.acme-dev"), "other", {"A": "nope"})

    siblings = read_sibling_uids(plan_file, _core_plan, "acme-target")

    assert siblings.recorded("A") == {"acme-dev": "a1", "acme-prod": "a1"}
    assert siblings.canonical("A") == "a1"
    assert siblings.canonical("B") is None
    assert siblings.disagree("B")
    assert siblings.canonical("C") is None
    assert not siblings.disagree("C")


def test_read_sibling_uids_follows_the_plans_declared_moves(tmp_path: Path) -> None:
    plan_file = tmp_path / "plan_moved.py"
    old_key = "FeatureType:mv_ft/Feature:mv_old"
    _write_lock(lock_path(plan_file, "acme-dev"), _moved_plan.name, {old_key: "u1"})

    siblings = read_sibling_uids(plan_file, _moved_plan, "acme-target")

    assert siblings.canonical("FeatureType:mv_ft/Feature:mv_new") == "u1"
    assert siblings.canonical(old_key) is None


# === Pinning at apply ===========================================================


def test_apply_creates_every_object_with_the_pinned_uid(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    tracked, seeded = _pinned_uids(_align_plan)
    siblings = _siblings(tracked, seeded)

    preview = compute_changes(
        _align_plan, mode=ApplyMode.PLAN, lock=None, sibling_uids=siblings
    )
    rt_key = "RecordType:al_rt"
    rt_change = next(c for c in preview.changes if c.key == rt_key)
    assert rt_change.action is Action.CREATE
    assert f"uid {tracked[rt_key]} as on {_SOURCE}" in rt_change.details

    lock_file = lock_path(tmp_path / "plan_align.py", _TARGET)
    ctx = _apply(_align_plan, lock_file, sibling_uids=siblings)

    assert not ctx.uid_misalignments
    by_key = {c.key: c.uid for c in ctx.changes}
    assert by_key == {**tracked, **seeded}
    link = ln.Feature.get(name="al_link")
    assert tracked[rt_key] in link._dtype_str
    lock = LockFile.read(lock_file)
    assert lock is not None
    assert lock.seeded == seeded
    assert {key: entry.uid for key, entry in lock.objects.items()} == tracked


@pytest.mark.parametrize("prefetch", [True, False])
def test_object_renamed_here_is_adopted_by_its_pinned_uid(
    instance: TestInstanceInfo, ln: LamindbModuleType, prefetch: bool
) -> None:
    pinned = _uid("FeatureType")
    stray = ln.Feature(name="renamed_ft", is_type=True, dtype=None)  # type: ignore[arg-type]
    stray.uid = pinned
    stray.save()

    ctx = compute_changes(
        _align_plan,
        mode=ApplyMode.APPLY,
        lock=None,
        prefetch=prefetch,
        sibling_uids=_siblings({"FeatureType:al_ft": pinned}),
    )

    change = next(c for c in ctx.changes if c.key == "FeatureType:al_ft")
    assert change.action is Action.MOVE
    assert change.uid == pinned
    assert any("adopted by uid" in detail for detail in change.details)
    assert ln.Feature.get(uid=pinned).name == "al_ft"


def test_existing_object_under_another_uid_is_misaligned(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_align_plan, mode=ApplyMode.APPLY, lock=None)
    _align_plan.reset()
    tracked, seeded = _pinned_uids(_align_plan)

    ctx = compute_changes(
        _align_plan,
        mode=ApplyMode.PLAN,
        lock=None,
        sibling_uids=_siblings(tracked, seeded),
    )

    misaligned = ctx.uid_misalignments
    assert {c.key for c in misaligned} == set(tracked) | set(seeded)
    assert {c.action for c in misaligned} == {Action.NO_CHANGE}
    ft = next(c for c in misaligned if c.key == "FeatureType:al_ft")
    assert ft.uid_misalignment is not None
    assert ft.uid_misalignment.others == {_SOURCE: tracked["FeatureType:al_ft"]}
    assert "on the other instances" in ft.uid_misalignment.reason


def test_siblings_that_disagree_refuse_to_create(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    key = "FeatureType:al_ft"
    siblings = SiblingUids(
        tracked={key: {"acme-dev": _uid("FeatureType"), "acme-prod": _uid("Feature")}}
    )

    preview = compute_changes(
        _align_plan, mode=ApplyMode.PLAN, lock=None, sibling_uids=siblings
    )
    change = next(c for c in preview.changes if c.key == key)
    assert change.action is Action.CREATE
    assert change.uid_misalignment is not None
    assert "different uids" in change.uid_misalignment.reason

    with pytest.raises(UidAlignmentError, match="no single uid"):
        compute_changes(
            _align_plan, mode=ApplyMode.APPLY, lock=None, sibling_uids=siblings
        )

    _align_plan.reset()
    lenient = compute_changes(
        _align_plan,
        mode=ApplyMode.APPLY,
        lock=None,
        sibling_uids=siblings,
        strict_uids=False,
    )
    created = next(c for c in lenient.changes if c.key == key)
    assert created.uid not in siblings.recorded(key).values()
    assert any("fresh uid" in warning for warning in created.warnings)


def test_a_fresh_uid_fallback_is_a_caveat_not_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    key = "FeatureType:al_ft"
    siblings = SiblingUids(
        tracked={key: {"acme-dev": _uid("FeatureType"), "acme-prod": _uid("Feature")}}
    )

    _align_plan.reset()
    preview = compute_changes(
        _align_plan,
        mode=ApplyMode.PLAN,
        lock=None,
        sibling_uids=siblings,
        strict_uids=False,
    )

    change = next(c for c in preview.changes if c.key == key)
    [warning] = change.warnings
    assert "fresh uid" in warning
    assert warning.category is WarningCategory.CAVEAT
    terminal = render_changes(preview, title="t")
    markdown = render_changes_markdown(preview, title="t")
    assert "changed outside the plan" not in terminal + markdown
    # The misalignment has a callout of its own.
    assert "[!WARNING]" not in markdown
    assert "[!CAUTION]" in markdown


def test_trashed_holder_of_the_pinned_uid_is_moved_aside(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    pinned = _uid("FeatureType")
    old = ln.Feature(name="al_ft", is_type=True, dtype=None)  # type: ignore[arg-type]
    old.uid = pinned
    old.save()
    old.delete(permanent=False)

    ctx = compute_changes(
        _align_plan,
        mode=ApplyMode.APPLY,
        lock=None,
        sibling_uids=_siblings({"FeatureType:al_ft": pinned}),
    )

    change = next(c for c in ctx.changes if c.key == "FeatureType:al_ft")
    assert change.action is Action.CREATE
    assert change.uid == pinned
    assert any("trashed copy" in detail for detail in change.details)
    trashed = ln.Feature.objects.get(pk=old.pk)
    assert trashed.branch_id == -1
    assert trashed.uid != pinned
    assert ln.Feature.objects.get(uid=pinned).pk != old.pk


def test_live_holder_of_the_pinned_uid_refuses(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    pinned = _uid("FeatureType")
    holder = ln.Feature(name="unrelated", dtype="str")
    holder.uid = pinned
    holder.save()
    siblings = _siblings({"FeatureType:al_ft": pinned})

    preview = compute_changes(
        _align_plan, mode=ApplyMode.PLAN, lock=None, sibling_uids=siblings
    )
    change = next(c for c in preview.changes if c.key == "FeatureType:al_ft")
    assert change.uid_misalignment is not None
    assert "already held here by Feature 'unrelated'" in change.uid_misalignment.reason

    with pytest.raises(UidAlignmentError):
        compute_changes(
            _align_plan, mode=ApplyMode.APPLY, lock=None, sibling_uids=siblings
        )


# === Rendering ==================================================================


def test_shared_prefetch_looks_objects_up_by_their_pinned_uid(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    from lamindb_setup.evolve.engine._apply import ExistenceCache, prefetch_plans

    pinned = _uid("FeatureType")
    stray = ln.Feature(name="renamed_ft", is_type=True, dtype=None)  # type: ignore[arg-type]
    stray.uid = pinned
    stray.save()
    siblings = _siblings({"FeatureType:al_ft": pinned})
    cache = ExistenceCache()

    prefetch_plans(
        [(_align_plan, None)],
        existing_cache=cache,
        members_cache={},
        sibling_uids=[siblings],
    )
    ctx = compute_changes(
        _align_plan,
        mode=ApplyMode.PLAN,
        lock=None,
        existing_cache=cache,
        prefetch=False,
        sibling_uids=siblings,
    )

    change = next(c for c in ctx.changes if c.key == "FeatureType:al_ft")
    assert change.action is Action.MOVE
    assert cache.queries == 0


def test_apply_one_refuses_misaligned_uids_from_the_lock_files_on_disk(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
) -> None:
    from lamindb_setup.evolve.console import OutputFormat
    from lamindb_setup.evolve.engine import ApplyRefusedError, apply_one

    plan_file = tmp_path / "plan_align.py"
    compute_changes(_align_plan, mode=ApplyMode.APPLY, lock=None)
    _align_plan.reset()
    other = LockFile(plan=_align_plan.name)
    other.add(
        LockEntry(
            key="FeatureType:al_ft",
            kind="FeatureType",
            name="al_ft",
            uid=_uid("FeatureType"),
        )
    )
    other.write(lock_path(plan_file, "acme-general"))

    def _apply_one(**kwargs: Any) -> str | None:
        return apply_one(
            _align_plan,
            plan_file,
            "stemuid01234",
            "acme/target",
            _TARGET,
            claimed_keys=set(),
            confirm=None,
            output_format=OutputFormat.terminal,
            **kwargs,
        )

    with pytest.raises(ApplyRefusedError, match="--no-strict-uids"):
        _apply_one()
    assert not lock_path(plan_file, _TARGET).exists()

    _align_plan.reset()
    _apply_one(strict_uids=False)
    assert lock_path(plan_file, _TARGET).exists()


# === Rendering ==================================================================


def _misaligned_change() -> ChangeItem:
    return ChangeItem(
        obj_id=1,
        key="FeatureType:ft",
        kind="FeatureType",
        name="ft",
        action=Action.NO_CHANGE,
        uid="S1",
        uid_misalignment=UidMisalignment(
            uid="S1", others={"acme-general": "G1"}, reason="uid S1 here, G1 there"
        ),
    )


def test_misalignment_is_rendered_in_every_view() -> None:
    class _Changes:
        changes = [_misaligned_change()]

    ctx = _Changes()
    assert "UID: uid S1 here, G1 there (acme-general: G1)" in render_changes(
        ctx, title="t"
    )
    markdown = render_changes_markdown(ctx, title="t")
    assert "1 uid(s) misaligned" in markdown
    assert "! UID:" in markdown
    payload = json.loads(render_changes_json(ctx, title="t"))
    assert payload["counts"]["uid_misaligned"] == 1
    assert payload["changes"][0]["uid_misalignment"]["others"] == {"acme-general": "G1"}
    notes = render_uid_misalignments(ctx, instance_slug="acme-sandbox")
    assert "repair align-uids" in notes[0]
    assert "FeatureType 'ft'" in notes[1]
    assert "--no-strict-uids" in uid_misalignment_message(ctx.changes)


def test_ci_plan_reports_misalignment_as_a_pending_change() -> None:
    record = ChangeRecord(
        action="no-change",
        kind="FeatureType",
        name="ft",
        uid="S1",
        uid_misalignment="uid S1 here, G1 there (acme-general: G1)",
    )
    diff = DeploymentDiff(
        instance_slug="acme/sandbox", plan_target="plan.py:p", ok=True, changes=[record]
    )
    report = PRReport(diffs=[diff])

    assert diff.misaligned == [record]
    assert not diff.actionable
    assert report.has_changes
    rendered = render_report(report)
    assert "1 uid(s) misaligned" in rendered
    assert "repair align-uids" in rendered
    assert "| FeatureType | ft |" in rendered


def _misaligned_diff(*extra: ChangeRecord) -> DeploymentDiff:
    record = ChangeRecord(
        action="no-change",
        kind="FeatureType",
        name="ft",
        uid="S1",
        uid_misalignment="uid S1 here, G1 there (acme-general: G1)",
    )
    return DeploymentDiff(
        instance_slug="acme/sandbox",
        plan_target="plan.py:p",
        ok=True,
        changes=[record, *extra],
    )


def test_ci_plan_only_reports_misalignment_when_strict_uids_is_off() -> None:
    # apply goes ahead with strict_uids off, so the check previewing it must not
    # hold the pull request back on a misalignment alone.
    report = PRReport(diffs=[_misaligned_diff()], strict_uids=False)

    assert not report.has_changes
    rendered = render_report(report)
    assert "no changes against any instance" in rendered
    assert "1 uid(s) misaligned" in rendered
    assert "[!NOTE]" in rendered
    assert "`strict_uids` is off" in rendered
    assert "refuses" not in rendered
    assert "repair align-uids" in rendered
    assert "<details open>" not in rendered


def test_ci_plan_still_reports_real_changes_when_strict_uids_is_off() -> None:
    create = ChangeRecord(action="create", kind="Feature", name="f", uid="F1")
    report = PRReport(diffs=[_misaligned_diff(create)], strict_uids=False)

    assert report.has_changes
    assert "<details open>" in render_report(report)


class _FakeCIPlan:
    name = "a"
    depends_on: tuple[()] = ()
    dependencies: list[object] = []

    def finalize(self) -> None:
        return None

    def reset(self) -> None:
        return None


@pytest.mark.parametrize(
    ("extra", "flag", "expected"),
    [
        ("", True, True),
        ("strict_uids: false\n", True, False),
        ("", False, False),
    ],
)
def test_ci_plan_turns_strict_uids_off_from_the_config_or_the_flag(
    tmp_path: Path, extra: str, flag: bool, expected: bool
) -> None:
    from lamindb_setup.evolve.ci._pr import run_pr_check

    cfg = load_config(_config(tmp_path, extra))
    report = run_pr_check(
        cfg,
        load_plan=lambda target: (_FakeCIPlan(), tmp_path / "plan_a.py", object()),
        connect=lambda slug: slug.replace("/", "-"),
        compute=lambda *args, **kwargs: [_misaligned_diff().changes[0]],
        strict_uids=flag,
    )

    assert report.strict_uids is expected
    assert report.has_changes is expected


# === Configuration ==============================================================


def _config(tmp_path: Path, extra: str = "") -> Path:
    (tmp_path / "plan_a.py").write_text("")
    path = tmp_path / "lamin_schemas.yml"
    path.write_text(
        f"{extra}instances:\n  - slug: acme/dev\n    plans:\n      - plan_a.py:a\n"
    )
    return path


def test_strict_uids_config_key(tmp_path: Path) -> None:
    assert load_config(_config(tmp_path)).strict_uids is True
    assert load_config(_config(tmp_path, "strict_uids: false\n")).strict_uids is False
    invalid = _config(tmp_path, "strict_uids: nope\n")
    with pytest.raises(ConfigError, match="strict_uids"):
        load_config(invalid)


def test_ci_apply_turns_strict_uids_off_from_the_config(tmp_path: Path) -> None:
    from lamindb_setup.evolve.ci._apply import run_apply

    class _FakePlan:
        name = "a"
        depends_on: tuple[()] = ()
        dependencies: list[object] = []

        def finalize(self) -> None:
            return None

        def reset(self) -> None:
            return None

    seen: list[bool] = []

    def apply_one(*args: Any, strict_uids: bool = True, **kwargs: Any) -> None:
        seen.append(strict_uids)

    cfg = load_config(_config(tmp_path, "strict_uids: false\n"))
    run_apply(
        cfg,
        load_plan=lambda target: (_FakePlan(), tmp_path / "plan_a.py", object()),
        connect=lambda slug: slug.replace("/", "-"),
        require_stem_uid=lambda module, plan_file: "stemuid01234",
        ensure_pushed=lambda plan_files: None,
        apply_one=apply_one,
    )
    assert seen == [False]


# === align-uids =================================================================


def _identity(
    plan: Plan[Any], tracked: dict[str, str], seeded: dict[str, str]
) -> SourceIdentity:
    return SourceIdentity(
        slug=_SOURCE, tracked={plan.name: tracked}, seeded={plan.name: seeded}
    )


def _applied_target(tmp_path: Path) -> tuple[Path, Path, Any]:
    plan_file = tmp_path / "plan_align.py"
    target_lock = lock_path(plan_file, _TARGET)
    ctx = _apply(_align_plan, target_lock)
    return plan_file, target_lock, ctx


def test_align_rewrites_uids_references_hashes_and_the_lock(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    plan_file, target_lock, applied = _applied_target(tmp_path)
    before = {c.key: c.uid for c in applied.changes}
    schema_hash = ln.Schema.get(name="AlSchema").hash
    tracked, seeded = _pinned_uids(_align_plan)

    alignment = compute_alignment(
        [(_align_plan, plan_file)],
        _identity(_align_plan, tracked, seeded),
        instance="acme/target",
        slug=_TARGET,
    )
    assert {r.key for r in alignment.renames} == set(tracked) | set(seeded)
    assert not alignment.holders

    execute_alignment(alignment, dry_run=True)
    assert alignment.ok
    assert not alignment.executed
    assert alignment.rewritten_dtypes >= 1
    assert ln.Feature.get(name="al_ft").uid == before["FeatureType:al_ft"]

    execute_alignment(alignment)
    assert alignment.executed
    for key, uid in {**tracked, **seeded}.items():
        kind = _own_keys(_align_plan)[key]
        assert _registry(ln, kind).objects.filter(uid=uid).exists(), key

    link = ln.Feature.get(name="al_link")
    assert tracked["RecordType:al_rt"] in link._dtype_str
    assert before["RecordType:al_rt"] not in link._dtype_str
    schema = ln.Schema.get(name="AlSchema")
    assert schema.index.name == "al_name"
    assert [f.name for f in schema.optionals.get()] == ["al_note"]
    assert schema.hash != schema_hash
    rehashed = schema.hash
    schema.save()
    assert schema.hash == rehashed  # the alignment left it current
    assert "AlSchema" in alignment.rehashed

    assert rewrite_target_locks(alignment) == [target_lock]
    lock = LockFile.read(target_lock)
    assert lock is not None
    assert {k: e.uid for k, e in lock.objects.items()} == tracked
    assert lock.seeded == seeded

    _align_plan.reset()
    replanned = compute_changes(
        _align_plan,
        mode=ApplyMode.PLAN,
        lock=lock,
        sibling_uids=_siblings(tracked, seeded),
    )
    assert {c.action for c in replanned.changes} == {Action.NO_CHANGE}
    assert not [c for c in replanned.changes if c.warnings]
    assert not replanned.uid_misalignments


def test_align_frees_a_uid_held_in_the_trash(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    plan_file, _lock, _ctx = _applied_target(tmp_path)
    tracked, seeded = _pinned_uids(_align_plan)
    trashed = ln.Feature(name="old_ft", is_type=True, dtype=None)  # type: ignore[arg-type]
    trashed.uid = tracked["FeatureType:al_ft"]
    trashed.save()
    trashed.delete(permanent=False)

    alignment = compute_alignment(
        [(_align_plan, plan_file)],
        _identity(_align_plan, tracked, seeded),
        instance="acme/target",
        slug=_TARGET,
    )
    [holder] = alignment.holders
    assert holder.action == HOLDER_FREE
    execute_alignment(alignment)

    assert ln.Feature.get(name="al_ft").uid == tracked["FeatureType:al_ft"]
    moved = ln.Feature.objects.get(pk=trashed.pk)
    assert moved.branch_id == -1
    assert moved.uid == holder.new_uid != tracked["FeatureType:al_ft"]


def _live_stub_record_type(ln: LamindbModuleType, name: str, uid: str) -> Any:
    previous = ln.settings.creation.search_names
    ln.settings.creation.search_names = False
    try:
        stub = ln.Record(name=name, is_type=True)
        stub.uid = uid
        stub.save()
    finally:
        ln.settings.creation.search_names = previous
    return stub


def test_align_refuses_a_live_holder_by_default(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    plan_file, _lock, applied = _applied_target(tmp_path)
    tracked, seeded = _pinned_uids(_align_plan)
    _live_stub_record_type(ln, "someone_else", tracked["RecordType:al_rt"])

    alignment = compute_alignment(
        [(_align_plan, plan_file)],
        _identity(_align_plan, tracked, seeded),
        instance="acme/target",
        slug=_TARGET,
    )
    [holder] = alignment.conflicts
    assert holder.action == HOLDER_CONFLICT
    assert "--reassign-colliding" in holder.reason
    assert not alignment.ok

    execute_alignment(alignment)
    assert not alignment.executed
    rt = ln.Record.get(name="al_rt", is_type=True)
    assert rt.uid == {c.key: c.uid for c in applied.changes}["RecordType:al_rt"]
    assert "refused" in render_alignment([alignment], title="t")


def test_align_reassigns_a_live_holder_with_its_references(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    plan_file, _lock, _ctx = _applied_target(tmp_path)
    tracked, seeded = _pinned_uids(_align_plan)
    wanted = tracked["RecordType:al_rt"]
    stub = _live_stub_record_type(ln, "someone_else", wanted)
    ln.Feature(name="stub_link", dtype=stub).save()
    assert wanted in ln.Feature.get(name="stub_link")._dtype_str

    alignment = compute_alignment(
        [(_align_plan, plan_file)],
        _identity(_align_plan, tracked, seeded),
        instance="acme/target",
        slug=_TARGET,
        reassign_colliding=True,
    )
    [holder] = alignment.holders
    assert holder.action == HOLDER_REASSIGN
    execute_alignment(alignment)

    assert ln.Record.get(name="al_rt", is_type=True).uid == wanted
    stub = ln.Record.objects.get(pk=stub.pk)
    assert stub.uid == holder.new_uid != wanted
    # the unmanaged feature follows the record it pointed at
    assert stub.uid in ln.Feature.get(name="stub_link")._dtype_str
    assert wanted in ln.Feature.get(name="al_link")._dtype_str


def test_align_merges_an_equivalent_duplicate(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    plan_file, _lock, _ctx = _applied_target(tmp_path)
    tracked, seeded = _pinned_uids(_align_plan)
    wanted = tracked["RecordType:al_rt"]
    stub = _live_stub_record_type(ln, "al_rt", wanted)
    child = ln.Record(name="synced_row", type=stub)
    child.save()

    alignment = compute_alignment(
        [(_align_plan, plan_file)],
        _identity(_align_plan, tracked, seeded),
        instance="acme/target",
        slug=_TARGET,
        merge_duplicates=True,
    )
    [holder] = alignment.holders
    assert holder.action == HOLDER_MERGE
    assert any("Record.type" in ref for ref in holder.references)
    execute_alignment(alignment)
    assert alignment.executed, alignment.error

    assert not ln.Record.objects.filter(pk=stub.pk).exists()
    rt = ln.Record.get(name="al_rt", is_type=True)
    assert rt.uid == wanted
    assert ln.Record.objects.get(pk=child.pk).type_id == rt.pk
    assert alignment.moved_references >= 1


def test_align_merge_drops_links_the_stub_duplicates(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from lamindb.models import SchemaFeature

    plan_file, _lock, _ctx = _applied_target(tmp_path)
    tracked, seeded = _pinned_uids(_align_plan)
    key = "FeatureType:al_ft/Feature:al_note"
    wanted = tracked[key]
    feature = ln.Feature.get(name="al_note")
    schema = ln.Schema.get(name="AlSchema")
    previous = ln.settings.creation.search_names
    ln.settings.creation.search_names = False
    try:
        stub = ln.Feature(name="al_note", dtype="str", type=feature.type)
    finally:
        ln.settings.creation.search_names = previous
    stub.uid = wanted
    stub.save()
    # the stub is linked to the same schema the plan's feature is a member of
    SchemaFeature(schema=schema, feature=stub).save()

    alignment = compute_alignment(
        [(_align_plan, plan_file)],
        _identity(_align_plan, tracked, seeded),
        instance="acme/target",
        slug=_TARGET,
        merge_duplicates=True,
    )
    [holder] = alignment.holders
    assert holder.action == HOLDER_MERGE, holder.reason
    execute_alignment(alignment)
    assert alignment.executed, alignment.error

    assert alignment.dropped_duplicates == 1
    assert not ln.Feature.objects.filter(pk=stub.pk).exists()
    assert ln.Feature.get(name="al_note").uid == wanted
    members = SchemaFeature.objects.filter(schema_id=schema.pk)
    assert sorted(members.values_list("feature_id", flat=True)) == sorted(
        f.pk for f in ln.Schema.get(name="AlSchema").members
    )
    assert members.filter(feature_id=feature.pk).count() == 1


def test_align_rewrites_the_locks_of_plans_built_on_the_aligned_ones(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from lamindb_setup.evolve.maintenance._align_uids import collect_instance_locks

    core_file = tmp_path / "plan_core.py"
    down_file = tmp_path / "plan_down.py"
    _apply(_core_plan, lock_path(core_file, _TARGET))
    down_lock = lock_path(down_file, _TARGET)
    _apply(_downstream_plan, down_lock)
    (tmp_path / "plan_core.other-instance.lock").write_text("{}")
    new_uid = _uid("FeatureType")

    alignment = compute_alignment(
        [(_core_plan, core_file)],
        SourceIdentity(
            slug=_SOURCE, tracked={_core_plan.name: {"FeatureType:core_ft": new_uid}}
        ),
        instance="acme/target",
        slug=_TARGET,
    )
    assert collect_instance_locks(alignment, [tmp_path]) == [down_lock.resolve()]
    execute_alignment(alignment)
    written = rewrite_target_locks(alignment)
    assert down_lock.resolve() in [path.resolve() for path in written]

    _downstream_plan.reset()
    replanned = compute_changes(
        _downstream_plan, mode=ApplyMode.PLAN, lock=LockFile.read(down_lock)
    )
    assert {c.action for c in replanned.changes} == {Action.NO_CHANGE}
    assert not [c for c in replanned.changes if c.warnings]


def test_align_keeps_a_schemas_optionals_sorted_in_the_lock(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    plan_file = tmp_path / "plan_op.py"
    lock_file = lock_path(plan_file, _TARGET)
    applied = _apply(_optionals_plan, lock_file)
    uids = {c.key: c.uid for c in applied.changes}
    a, b = "FeatureType:op_ft/Feature:op_a", "FeatureType:op_ft/Feature:op_b"
    low, high = sorted([a, b], key=lambda key: uids[key])
    # The renames flip the order the two optionals sort in.
    tracked = {low: "z" * 12, high: "0" * 12}

    alignment = compute_alignment(
        [(_optionals_plan, plan_file)],
        SourceIdentity(slug=_SOURCE, tracked={_optionals_plan.name: tracked}),
        instance="acme/target",
        slug=_TARGET,
    )
    execute_alignment(alignment)
    rewrite_target_locks(alignment)

    _optionals_plan.reset()
    replanned = compute_changes(
        _optionals_plan, mode=ApplyMode.PLAN, lock=LockFile.read(lock_file)
    )
    assert {c.action for c in replanned.changes} == {Action.NO_CHANGE}
    assert not [c for c in replanned.changes if c.warnings]


def test_align_rehashes_schemas_of_a_reassigned_feature(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    plan_file, _lock, _ctx = _applied_target(tmp_path)
    tracked, seeded = _pinned_uids(_align_plan)
    holder = ln.Feature(name="elsewhere", dtype="str")
    holder.uid = tracked["FeatureType:al_ft/Feature:al_note"]
    holder.save()
    other = ln.Schema(name="OtherSchema", features=[holder])
    other.save()

    alignment = compute_alignment(
        [(_align_plan, plan_file)],
        _identity(_align_plan, tracked, seeded),
        instance="acme/target",
        slug=_TARGET,
        reassign_colliding=True,
    )
    execute_alignment(alignment)
    assert alignment.executed, alignment.error

    other = ln.Schema.objects.get(pk=other.pk)
    rehashed = other.hash
    other.save()
    assert other.hash == rehashed
    assert "OtherSchema" in alignment.rehashed


def test_align_rehashes_every_schema_it_reaches_as_lamindb_would(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    plan_file, _lock, _ctx = _applied_target(tmp_path)
    tracked, seeded = _pinned_uids(_align_plan)
    name = ln.Feature.get(name="al_name")
    extra = ln.Feature(name="al_extra", dtype="str").save()
    # Schemas no plan manages that hold a renamed feature: unnamed, as lamindb
    # infers one per curated artifact, and composites slotting the plan's own
    # (keyed, with an optional) -- one inside another, so order matters.
    inferred = ln.Schema(features=[name, extra.with_config(optional=True)]).save()
    composite = ln.Schema(
        name="AlComposite",
        otype="AnnData",
        slots={"obs": ln.Schema.get(name="AlSchema"), "var": inferred},
    ).save()
    outer = ln.Schema(name="AlOuter", otype="AnnData", slots={"obs": composite}).save()
    untouched = ln.Schema(name="AlUntouched", features=[extra]).save()
    reached = [inferred, composite, outer]
    hashes = {s.pk: s.hash for s in [*reached, untouched]}

    alignment = compute_alignment(
        [(_align_plan, plan_file)],
        _identity(_align_plan, tracked, seeded),
        instance="acme/target",
        slug=_TARGET,
    )
    execute_alignment(alignment)
    assert alignment.executed, alignment.error

    for pk in hashes:
        schema = ln.Schema.objects.get(pk=pk)
        aligned = schema.hash
        schema.save(print_hash_mutation_warning=False)
        assert schema.hash == aligned, f"{schema} is not hashed as lamindb would"
    for schema in reached:
        assert ln.Schema.objects.get(pk=schema.pk).hash != hashes[schema.pk]
    assert ln.Schema.objects.get(pk=untouched.pk).hash == hashes[untouched.pk]
    assert {str(inferred.uid), "AlComposite", "AlOuter"} <= set(alignment.rehashed)


def test_align_issues_as_many_statements_for_many_schemas_as_for_few(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from django.db import connection
    from django.test.utils import CaptureQueriesContext

    plan_file, _lock, _ctx = _applied_target(tmp_path)
    tracked, seeded = _pinned_uids(_align_plan)
    name = ln.Feature.get(name="al_name")

    def add_inferred_schemas(count: int) -> None:
        for _ in range(count):
            extra = ln.Feature(name=f"al_{secrets.token_hex(4)}", dtype="str").save()
            ln.Schema(features=[name, extra]).save()

    def preview() -> tuple[int, int]:
        alignment = compute_alignment(
            [(_align_plan, plan_file)],
            _identity(_align_plan, tracked, seeded),
            instance="acme/target",
            slug=_TARGET,
        )
        with CaptureQueriesContext(connection) as queries:
            execute_alignment(alignment, dry_run=True)
        assert alignment.ok, alignment.error
        return len(queries), len(alignment.rehashed)

    add_inferred_schemas(2)
    few, rehashed_few = preview()
    add_inferred_schemas(20)
    many, rehashed_many = preview()

    # A remote instance charges a round trip per statement, so rehashing the
    # schemas a renamed feature reaches must not cost one per schema.
    assert rehashed_many == rehashed_few + 20
    assert many == few


def test_align_refuses_a_rehash_onto_another_schemas_content(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    from lamindb_setup.evolve.maintenance._align_uids import _write_hashes

    first = ln.Schema(
        name="AlClashA", features=[ln.Feature(name="al_a", dtype="str").save()]
    ).save()
    second = ln.Schema(
        name="AlClashB", features=[ln.Feature(name="al_b", dtype="str").save()]
    ).save()
    taken = second.hash
    assert taken is not None
    first.hash = taken

    with pytest.raises(AlignError, match="same content as schema 'AlClashB'") as exc:
        _write_hashes({first.pk: taken}, {first.pk: first})
    assert str(exc.value).startswith("once aligned, schema 'AlClashA'")
    assert "'lamin evolve repair duplicates' lists" in str(exc.value)

    # lamindb only keeps the content of a schema outside the trash to itself.
    second.delete(permanent=False)
    _write_hashes({first.pk: taken}, {first.pk: first})
    assert ln.Schema.objects.get(pk=first.pk).hash == taken


def test_align_judges_a_rehash_only_against_hashes_already_known(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    from lamindb_setup.evolve.maintenance._align_uids import _write_hashes

    first = ln.Schema(
        name="AlKnownA", features=[ln.Feature(name="al_ka", dtype="str").save()]
    ).save()
    second = ln.Schema(
        name="AlKnownB", features=[ln.Feature(name="al_kb", dtype="str").save()]
    ).save()
    taken = second.hash
    assert taken is not None
    first.hash = taken

    # `second` is rehashed later in the same run: the hash it holds now is
    # not the one it ends up with, so it does not count against `first`.
    _write_hashes(
        {first.pk: taken}, {first.pk: first, second.pk: second}, unsettled={second.pk}
    )
    assert ln.Schema.objects.get(pk=first.pk).hash == taken


def test_align_rehash_falls_back_to_lamindbs_save(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    from lamindb_setup.evolve.maintenance._align_uids import _rehash

    member = ln.Feature(name="al_member", dtype="str").save()
    elsewhere = ln.Feature(name="al_elsewhere", dtype="str").save()
    schema = ln.Schema(name="AlFallback", features=[member]).save()
    before = schema.hash
    # An index that is not a member, as a copy without its members leaves it:
    # only lamindb's own save knows where to find it.
    ln.Schema.objects.filter(pk=schema.pk).update(
        _aux={"af": {"3": str(elsewhere.uid)}}
    )

    assert _rehash(set(), {schema.pk}) == {str(schema.uid): "AlFallback"}

    schema = ln.Schema.objects.get(pk=schema.pk)
    assert schema.hash != before
    rehashed = schema.hash
    schema.save(print_hash_mutation_warning=False)
    assert schema.hash == rehashed


# --- a content two live schemas already share ------------------------------


@pytest.fixture
def writable_storage(ln: LamindbModuleType) -> None:
    """Point the cached instance's `Storage` row at where the run actually is."""
    import lamindb_setup as ln_setup

    storage = ln_setup.settings.storage
    root = storage.root_as_str
    ln.Storage.objects.filter(uid=Path(root).name).update(root=root)
    storage._record = None


def _shared_content(ln: LamindbModuleType, tmp_path: Path) -> tuple[Path, Any, Any]:
    """The plan schema and an inferred schema already sharing their content.

    An artifact is curated while the plan's `tw_b` is
    still optional somewhere, so lamindb records its columns -- `tw_a` and
    `tw_b`, both required -- as a new unnamed schema.  A later apply makes
    `tw_b` required, giving the plan schema exactly that content, which
    lamindb before 2.12 let through; here the inferred schema waits in the
    trash meanwhile.  Returns the plan file, the inferred schema and the
    artifact recording it.
    """
    import pandas as pd
    from lamindb.models.schema import ArtifactSchema

    plan_file = tmp_path / "plan_twin.py"
    lock_file = lock_path(plan_file, _TARGET)
    _apply(_TWIN_OPTIONAL, lock_file)
    a, b = (ln.Feature.get(name=name) for name in ("tw_a", "tw_b"))
    c = ln.Feature(name="tw_c", dtype=str).save()
    validator = ln.Schema(
        name="TwValidator",
        features=[a, b.with_config(optional=True), c.with_config(optional=True)],
    ).save()
    frame = pd.DataFrame({"tw_a": ["x"], "tw_b": ["y"]})
    artifact = ln.Artifact.from_dataframe(
        frame, key="tw/frame.parquet", schema=validator
    ).save()
    inferred = ArtifactSchema.objects.get(artifact=artifact, slot="columns").schema
    assert inferred.name is None

    inferred.delete(permanent=False)
    _apply(_TWIN_REQUIRED, lock_file, allow_data_validated_changes=True)
    ln.Schema.objects.filter(pk=inferred.pk).update(branch_id=1)
    inferred = ln.Schema.objects.get(pk=inferred.pk)
    assert inferred.hash == ln.Schema.get(name="TwSchema").hash
    return plan_file, inferred, artifact


def _align_twins(
    plan_file: Path, **kwargs: Any
) -> tuple[Any, dict[str, str], dict[str, str]]:
    tracked, seeded = _pinned_uids(_TWIN_REQUIRED)
    alignment = compute_alignment(
        [(_TWIN_REQUIRED, plan_file)],
        _identity(_TWIN_REQUIRED, tracked, seeded),
        instance="acme/target",
        slug=_TARGET,
        **kwargs,
    )
    return alignment, tracked, seeded


def _plan_schema_uid(tracked: dict[str, str]) -> str:
    return next(uid for key, uid in tracked.items() if key.endswith("Schema:TwSchema"))


def test_align_absorbs_an_inferred_schema_already_holding_the_content(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    writable_storage: None,
) -> None:
    from lamindb.models.schema import ArtifactSchema

    plan_file, inferred, artifact = _shared_content(ln, tmp_path)
    alignment, tracked, seeded = _align_twins(plan_file)
    absorbs = f"absorbs inferred schema {str(inferred.uid)!r} with the same content"

    execute_alignment(alignment, dry_run=True)
    assert alignment.ok, alignment.error
    [settled] = alignment.settled
    assert settled.startswith(
        f"schema 'TwSchema' (uid {_plan_schema_uid(tracked)}) {absorbs}"
    )
    assert settled.endswith("(recorded columns of 1 artifact(s))")
    assert f"same content: {settled}" in render_alignment([alignment], title="t")
    assert ln.Schema.objects.filter(pk=inferred.pk).exists()

    execute_alignment(alignment)
    assert alignment.executed, alignment.error
    assert alignment.settled == [settled]
    assert not ln.Schema.objects.filter(pk=inferred.pk).exists()
    schema = ln.Schema.get(name="TwSchema")
    assert schema.uid == _plan_schema_uid(tracked)
    link = ArtifactSchema.objects.get(artifact=artifact, slot="columns")
    assert link.schema_id == schema.pk
    assert ln.Artifact.get(pk=artifact.pk).schema.name == "TwValidator"
    rehashed = schema.hash
    schema.save(print_hash_mutation_warning=False)
    assert schema.hash == rehashed
    assert str(inferred.uid) not in alignment.rehashed_uids

    [lock_file] = rewrite_target_locks(alignment)
    _TWIN_REQUIRED.reset()
    replanned = compute_changes(
        _TWIN_REQUIRED,
        mode=ApplyMode.PLAN,
        lock=LockFile.read(lock_file),
        sibling_uids=_siblings(tracked, seeded),
    )
    assert {c.action for c in replanned.changes} == {Action.NO_CHANGE}


def test_align_settles_a_shared_content_once_across_hash_writes(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    writable_storage: None,
) -> None:
    from lamindb_setup.evolve.maintenance._align_uids import (
        _SharedContent,
        _write_hashes,
    )

    _plan_file, inferred, _artifact = _shared_content(ln, tmp_path)
    schema = ln.Schema.get(name="TwSchema")
    schemas = {schema.pk: schema, inferred.pk: inferred}
    shared = _SharedContent()

    # A rehash that falls back to lamindb's save writes the hashes in hand
    # first, so one run can write hashes more than once.
    _write_hashes({schema.pk: schema.hash}, schemas, shared)
    _write_hashes({schema.pk: schema.hash}, schemas, shared)

    assert len(shared.settled) == 1
    assert not ln.Schema.objects.filter(pk=shared.absorbed.pop()).exists()


def test_align_refuses_a_schema_already_holding_the_content_it_cannot_absorb(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    writable_storage: None,
) -> None:
    plan_file, inferred, _artifact = _shared_content(ln, tmp_path)
    inferred.projects.add(ln.Project(name="tw_project").save())
    alignment, tracked, _seeded = _align_twins(plan_file)

    execute_alignment(alignment, dry_run=True)

    assert alignment.error is not None
    assert alignment.error.startswith(
        f"schema 'TwSchema' (uid {_plan_schema_uid(tracked)}) already has the "
        f"same content as unnamed schema {str(inferred.uid)!r}, which lamindb no "
        "longer allows"
    )
    assert (
        f"unnamed schema {str(inferred.uid)!r} cannot be absorbed: it is annotated "
        "with 1 project(s)"
    ) in alignment.error
    assert "Pass --merge-duplicates to merge it into schema 'TwSchema'" in (
        alignment.error
    )
    assert "repair duplicates" not in alignment.error
    assert ln.Schema.objects.filter(pk=inferred.pk).exists()


def _named_twin(ln: LamindbModuleType, tmp_path: Path) -> tuple[Path, Any, Any]:
    """A hand-made, named schema with the plan schema's content, validating data."""
    plan_file, twin, artifact = _shared_content(ln, tmp_path)
    ln.Schema.objects.filter(pk=twin.pk).update(name="TwStrict")
    ln.Artifact.objects.filter(pk=artifact.pk).update(schema_id=twin.pk)
    return plan_file, ln.Schema.objects.get(pk=twin.pk), artifact


def test_align_merges_a_named_schema_holding_the_content_when_asked(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    writable_storage: None,
) -> None:
    from lamindb.models.schema import ArtifactSchema

    plan_file, twin, artifact = _named_twin(ln, tmp_path)
    members = sorted(twin.members.values_list("name", flat=True))

    refused, _tracked, _seeded = _align_twins(plan_file)
    execute_alignment(refused, dry_run=True)
    assert refused.error is not None
    assert (
        f"schema 'TwStrict' (uid {twin.uid}) cannot be absorbed: it is named "
        "'TwStrict'; 1 artifact(s) validate against it"
    ) in refused.error
    assert "Pass --merge-duplicates" in refused.error

    alignment, tracked, _seeded = _align_twins(plan_file, merge_duplicates=True)
    execute_alignment(alignment)
    assert alignment.executed, alignment.error

    [settled] = alignment.settled
    assert f"merges schema 'TwStrict' (uid {twin.uid})" in settled
    schema = ln.Schema.get(name="TwSchema")
    assert schema.uid == _plan_schema_uid(tracked)
    assert ln.Artifact.objects.get(pk=artifact.pk).schema_id == schema.pk
    link = ArtifactSchema.objects.get(artifact=artifact, slot="columns")
    assert link.schema_id == schema.pk
    assert alignment.moved_references >= 2
    trashed = ln.Schema.objects.get(pk=twin.pk)
    assert trashed.branch_id == -1
    assert sorted(trashed.members.values_list("name", flat=True)) == members


def test_align_does_not_merge_a_schema_another_plan_manages(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    writable_storage: None,
) -> None:
    plan_file, twin, _artifact = _named_twin(ln, tmp_path)
    alignment, _tracked, _seeded = _align_twins(plan_file, merge_duplicates=True)
    other = LockFile(plan="other")
    other.add(
        LockEntry(key="Schema:TwStrict", kind="Schema", name="TwStrict", uid=twin.uid)
    )
    other_lock = lock_path(tmp_path / "plan_other.py", _TARGET)
    other.write(other_lock)
    alignment.lock_files.append(other_lock)

    execute_alignment(alignment)

    assert not alignment.executed
    assert alignment.error is not None
    assert "it is managed by a plan" in alignment.error
    assert "Plans manage more than one of them" in alignment.error
    assert ln.Schema.objects.get(pk=twin.pk).branch_id == 1


def test_fresh_uids_are_distinct_and_unused(
    instance: TestInstanceInfo, ln: LamindbModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    from lamindb_setup.evolve.engine import fresh_uid, fresh_uids

    uids = fresh_uids(ln.Feature, 3)
    assert len(set(uids)) == 3
    assert not ln.Feature.objects.filter(uid__in=uids).exists()
    assert fresh_uids(ln.Feature, 0) == []

    taken = str(ln.Feature(name="al_taken", dtype="str").save().uid)
    drawn = iter([taken, "A" * 12])
    monkeypatch.setattr("lamindb.base.uids.base62", lambda _n: next(drawn))
    assert fresh_uid(ln.Feature) == "A" * 12


def test_align_does_not_merge_a_holder_that_differs(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    plan_file, _lock, _ctx = _applied_target(tmp_path)
    tracked, seeded = _pinned_uids(_align_plan)
    _live_stub_record_type(ln, "not_al_rt", tracked["RecordType:al_rt"])

    alignment = compute_alignment(
        [(_align_plan, plan_file)],
        _identity(_align_plan, tracked, seeded),
        instance="acme/target",
        slug=_TARGET,
        merge_duplicates=True,
    )
    [holder] = alignment.holders
    assert holder.action == HOLDER_CONFLICT
    assert "cannot merge" in holder.reason


def test_two_objects_wanting_one_uid_is_refused(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    plan_file, _lock, _ctx = _applied_target(tmp_path)
    tracked, seeded = _pinned_uids(_align_plan)
    tracked["FeatureType:al_ft/Feature:al_note"] = tracked[
        "FeatureType:al_ft/Feature:al_name"
    ]
    identity = _identity(_align_plan, tracked, seeded)
    with pytest.raises(AlignError, match="would both take uid"):
        compute_alignment(
            [(_align_plan, plan_file)],
            identity,
            instance="acme/target",
            slug=_TARGET,
        )


def test_read_source_identity_verifies_the_lock_against_the_source(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    plan_file = tmp_path / "plan_align.py"
    source_lock = lock_path(plan_file, _SOURCE)
    applied = _apply(_align_plan, source_lock)
    uids = {c.key: c.uid for c in applied.changes}

    identity = read_source_identity([(_align_plan, plan_file)], _SOURCE)
    assert identity.tracked[_align_plan.name] == {
        k: v for k, v in uids.items() if k != "RecordType:al_rt/Record:al_value"
    }
    assert identity.seeded[_align_plan.name] == {
        "RecordType:al_rt/Record:al_value": uids["RecordType:al_rt/Record:al_value"]
    }
    assert identity.lock_files == [source_lock]

    # A lock predating the seeded section: the value is looked up live.
    lock = LockFile.read(source_lock)
    assert lock is not None
    lock.seeded = {}
    lock.write(source_lock)
    identity = read_source_identity([(_align_plan, plan_file)], _SOURCE)
    assert identity.looked_up == 1

    # A stale lock refuses the whole run.
    lock.objects["FeatureType:al_ft"].uid = _uid("FeatureType")
    lock.write(source_lock)
    with pytest.raises(AlignError, match="does not exist"):
        read_source_identity([(_align_plan, plan_file)], _SOURCE)


def test_read_source_identity_notes_plans_never_applied_there(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    identity = read_source_identity([(_align_plan, tmp_path / "plan_x.py")], _SOURCE)
    assert identity.missing_locks == [_align_plan.name]


def test_with_prerequisites_puts_prerequisites_first(tmp_path: Path) -> None:
    ordered = with_prerequisites([(_downstream_plan, tmp_path / "down.py")])
    assert [plan.name for plan, _path in ordered] == [
        _core_plan.name,
        _downstream_plan.name,
    ]
    assert ordered[0][1] == Path(__file__).resolve()
    assert ordered[1][1] == tmp_path / "down.py"
