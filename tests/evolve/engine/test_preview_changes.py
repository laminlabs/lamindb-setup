# Source test semantics retained.
# ruff: noqa: TC003
"""Tests for rehearsing an apply in a rolled-back transaction.

`plan --preview-migrations` used to dry-run each migration on its own, which
cannot reach a target the same plan creates.  :func:`preview_changes` applies
the whole plan instead -- creating what it creates, running every migration and
its post-check -- and rolls all of it back.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from lamindb_setup.evolve import (
    Feature,
    FeatureType,
    LaminDType,
    Record,
    RecordType,
    map_to_records,
)
from lamindb_setup.evolve.authoring._migrations import MigrationError
from lamindb_setup.evolve.authoring._plan import PlanContext, create_plan
from lamindb_setup.evolve.core import PLACEHOLDER_UID, Promise, provisional_promises
from lamindb_setup.evolve.engine import preview_changes, rehearsal
from lamindb_setup.evolve.engine._apply import (
    Action,
    ApplyMode,
    compute_changes,
    write_lock,
)
from lamindb_setup.evolve.engine._lock import LockFile

if TYPE_CHECKING:
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType


@create_plan
def _as_str(ctx: PlanContext, exports: None) -> None:
    Feature("pv_x", dtype=LaminDType(str), type=FeatureType("pv_ft"))


@create_plan
def _as_vocabulary(ctx: PlanContext, exports: None) -> None:
    import lamindb as ln

    vocabulary = RecordType("PvVocab")
    Record("alpha", type=vocabulary)
    Record("beta", type=vocabulary)
    feature = Feature(
        "pv_x",
        dtype=LaminDType(ln.Record, type__uid=vocabulary.uid),
        type=FeatureType("pv_ft"),
    )
    ctx.migrate(feature, map_to_records())


def _seed(ln: LamindbModuleType, tmp_path: Path, *values: str) -> LockFile:
    _as_str.reset()
    path = tmp_path / "pv.lock"
    write_lock(compute_changes(_as_str, mode=ApplyMode.APPLY, lock=None), _as_str, path)
    lock = LockFile.read(path)
    assert lock is not None
    sheet = ln.Record(name="PvSheet", is_type=True).save()
    for i, value in enumerate(values):
        ln.Record(name=f"row{i}", type=sheet, features={"pv_x": value}).save()
    return lock


def _untouched(ln: LamindbModuleType) -> bool:
    feature = ln.Feature.get(name="pv_x", is_type=False)
    vocabulary = ln.Record.filter(name="PvVocab", is_type=True).one_or_none()
    return feature.dtype_as_str == "str" and vocabulary is None


def test_a_plan_mode_dry_run_cannot_reach_a_target_the_plan_creates(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed(ln, tmp_path, "alpha")

    _as_vocabulary.reset()
    ctx = compute_changes(
        _as_vocabulary, mode=ApplyMode.PLAN, lock=lock, preview_migrations=True
    )

    feature = next(c for c in ctx.changes if c.name == "pv_x")
    assert any("not previewed" in d for d in feature.details), feature.details


def test_a_rehearsal_runs_the_migration_and_rolls_it_back(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed(ln, tmp_path, "alpha", "beta")

    _as_vocabulary.reset()
    ctx = preview_changes(_as_vocabulary, lock=lock)

    feature = next(c for c in ctx.changes if c.name == "pv_x")
    assert any("map_to_records: 2 matched" in d for d in feature.details), (
        feature.details
    )
    created = [c for c in ctx.changes if c.action is Action.CREATE]
    assert created and {c.uid for c in created} == {PLACEHOLDER_UID}
    # The dtype names the record type by the uid the rehearsal drew for it,
    # which the apply will not create it with.
    assert any(f"cat[Record[{PLACEHOLDER_UID}]]" in d for d in feature.details), (
        feature.details
    )
    assert _untouched(ln)


def test_a_rehearsal_fails_as_the_apply_would(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed(ln, tmp_path, "alpha", "gamma")

    _as_vocabulary.reset()
    with pytest.raises(MigrationError, match="'gamma'"):
        preview_changes(_as_vocabulary, lock=lock)

    assert _untouched(ln)


def test_a_rehearsal_writes_no_lock(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed(ln, tmp_path, "alpha")
    before = (tmp_path / "pv.lock").read_text()

    _as_vocabulary.reset()
    preview_changes(_as_vocabulary, lock=lock)

    assert (tmp_path / "pv.lock").read_text() == before


def test_a_plan_applies_for_real_after_a_rehearsal(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The rehearsal resolved the record type's uid on the plan's objects; the
    # apply that follows must not inherit it from the rolled-back record.
    lock = _seed(ln, tmp_path, "alpha", "beta")
    _as_vocabulary.reset()
    preview_changes(_as_vocabulary, lock=lock)

    ctx = compute_changes(_as_vocabulary, mode=ApplyMode.APPLY, lock=lock)

    vocabulary = ln.Record.get(name="PvVocab", is_type=True)
    feature = ln.Feature.get(name="pv_x", is_type=False)
    assert feature.dtype_as_str == f"cat[Record[{vocabulary.uid}]]"
    change = next(c for c in ctx.changes if c.name == "pv_x")
    assert any("map_to_records: 2 matched" in d for d in change.details)


def test_plans_rehearsed_together_see_each_other(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _seed(ln, tmp_path, "alpha")

    with rehearsal():
        _as_vocabulary.reset()
        preview_changes(_as_vocabulary, lock=lock)
        # Joined, not rolled back on its own: a later plan would see this.
        assert ln.Record.filter(name="PvVocab", is_type=True).one_or_none()

    assert _untouched(ln)


def test_provisional_promises_forget_only_what_they_resolved() -> None:
    resolutions: list[str] = []

    def resolve(arg: str, ctx: object) -> str:
        resolutions.append(arg)
        return f"{arg}-{len(resolutions)}"

    kept = Promise("kept", resolve)
    provisional = Promise("provisional", resolve)
    assert kept.resolve(None) == "kept-1"  # type: ignore[arg-type]

    with provisional_promises():
        assert provisional.resolve(None) == "provisional-2"  # type: ignore[arg-type]
        assert provisional.resolve(None) == "provisional-2"  # type: ignore[arg-type]
        assert kept.resolve(None) == "kept-1"  # type: ignore[arg-type]

    assert kept.resolve(None) == "kept-1"  # type: ignore[arg-type]
    assert provisional.resolve(None) == "provisional-3"  # type: ignore[arg-type]
