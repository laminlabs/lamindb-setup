"""Tests for lamindb_setup.evolve.maintenance._stranded_values."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from lamindb_setup.evolve import (
    Feature,
    FeatureType,
    LaminDType,
    drop_values,
    keep_values,
)
from lamindb_setup.evolve.authoring._plan import PlanContext, create_plan
from lamindb_setup.evolve.engine._apply import (
    ApplyMode,
    compute_changes,
    write_lock,
)
from lamindb_setup.evolve.engine._lock import LockFile
from lamindb_setup.evolve.maintenance._stranded_values import (
    compute_stranded_values,
    execute_stranded_values,
    render_stranded_values,
)

if TYPE_CHECKING:
    from pathlib import Path
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType

_KEY = "FeatureType:sv_ft/Feature:sv_drop"


@create_plan
def _sv_v1(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("sv_ft")
    Feature("sv_keep", dtype=LaminDType(str), type=ft)
    Feature("sv_drop", dtype=LaminDType(str), type=ft)


@create_plan
def _sv_v2(ctx: PlanContext, exports: None) -> None:
    """`sv_drop` gone, with its values deliberately kept."""
    ft = FeatureType("sv_ft")
    Feature("sv_keep", dtype=LaminDType(str), type=ft)
    ctx.migrate(_KEY, keep_values())


@create_plan
def _sv_v2_dropped(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("sv_ft")
    Feature("sv_keep", dtype=LaminDType(str), type=ft)
    ctx.migrate(_KEY, drop_values())


def _apply_v1(ln: LamindbModuleType, tmp_path: Path) -> LockFile:
    from lamindb.models import JsonValue

    _sv_v1.reset()
    applied = compute_changes(_sv_v1, mode=ApplyMode.APPLY, lock=None)
    write_lock(applied, _sv_v1, tmp_path / "sv.lock")  # type: ignore[arg-type]
    lock = LockFile.read(tmp_path / "sv.lock")
    assert lock is not None
    feat = ln.Feature.get(name="sv_drop", is_type=False)
    JsonValue(feature=feat, value="a").save()
    JsonValue(feature=feat, value="b").save()
    return lock


def test_a_clean_instance_reports_nothing(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    _apply_v1(ln, tmp_path)

    plan = compute_stranded_values()
    assert plan.is_empty
    assert plan.retained == []
    assert "No stranded values found." in render_stranded_values(plan, title="t")


def test_values_left_by_a_hand_deleted_feature_are_found(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The state the engine now refuses to create, but that older applies and
    # out-of-band deletions already left behind.
    _apply_v1(ln, tmp_path)
    ln.Feature.get(name="sv_drop", is_type=False).delete()

    plan = compute_stranded_values()
    assert [f.name for f in plan.features] == ["sv_drop"]
    entry = plan.features[0]
    assert entry.scalar_values == 2
    assert entry.retained is False
    assert entry.type_name == "sv_ft"

    text = render_stranded_values(plan, title="Stranded")
    assert "sv_drop" in text
    assert "2 scalar values" in text


def test_an_accepted_retention_is_listed_but_not_repaired(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # keep_values() records the acceptance on the feature, which rides into the
    # trash with it -- otherwise every deliberate retention would read as damage.
    lock = _apply_v1(ln, tmp_path)
    _sv_v2.reset()
    compute_changes(_sv_v2, mode=ApplyMode.APPLY, lock=lock)

    plan = compute_stranded_values()
    assert plan.is_empty
    assert [f.name for f in plan.retained] == ["sv_drop"]

    text = render_stranded_values(plan, title="Stranded")
    assert "Accepted retentions" in text

    # and nothing is deleted for it
    assert execute_stranded_values(plan) == 0
    trashed = ln.Feature.filter(name="sv_drop", branch_id=-1).one()
    assert trashed.values.count() == 2


def test_a_declared_drop_leaves_nothing_to_find(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _apply_v1(ln, tmp_path)
    _sv_v2_dropped.reset()
    compute_changes(_sv_v2_dropped, mode=ApplyMode.APPLY, lock=lock)

    plan = compute_stranded_values()
    assert plan.is_empty
    assert plan.retained == []


def test_execute_deletes_the_stranded_rows(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    _apply_v1(ln, tmp_path)
    ln.Feature.get(name="sv_drop", is_type=False).delete()

    plan = compute_stranded_values()
    assert execute_stranded_values(plan) == 2

    trashed = ln.Feature.filter(name="sv_drop", branch_id=-1).one()
    assert trashed.values.count() == 0
    # the feature itself is left in the trash, and the scan is now clean
    assert compute_stranded_values().is_empty


def test_a_deleted_feature_without_values_is_not_reported(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Trashed features are the norm; only the ones still owning values matter.
    _apply_v1(ln, tmp_path)
    ln.Feature.get(name="sv_keep", is_type=False).delete()

    plan = compute_stranded_values()
    assert [f.name for f in plan.features] == []


def test_the_scan_can_be_scoped_to_a_space(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    _apply_v1(ln, tmp_path)
    ln.Feature.get(name="sv_drop", is_type=False).delete()
    other = str(ln.Feature.filter(name="sv_drop", branch_id=-1).one().space.uid)

    assert compute_stranded_values(space_uids=[other]).features
    assert not compute_stranded_values(exclude_space_uids=[other]).features


def test_a_failure_part_way_leaves_every_row_in_place(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The rows go permanently, so a partial repair would be unrecoverable."""
    from lamindb.models import JsonValue
    from lamindb_setup.evolve.engine import _migrations as migrations_mod

    _apply_v1(ln, tmp_path)
    # Strand both features, so the repair has a second one to fail on after the
    # first has already been deleted -- the state the transaction has to undo.
    keep = ln.Feature.get(name="sv_keep", is_type=False)
    JsonValue(feature=keep, value="c").save()
    ln.Feature.get(name="sv_keep", is_type=False).delete()
    ln.Feature.get(name="sv_drop", is_type=False).delete()

    plan = compute_stranded_values()
    assert len(plan.features) == 2

    real_delete_all = migrations_mod._delete_all
    calls = {"n": 0}

    def _fail_on_the_second(stored: object) -> None:
        calls["n"] += 1
        if calls["n"] > 1:
            raise RuntimeError("connection lost mid-delete")
        real_delete_all(stored)  # type: ignore[arg-type]

    monkeypatch.setattr(migrations_mod, "_delete_all", _fail_on_the_second)
    with pytest.raises(RuntimeError, match="connection lost"):
        execute_stranded_values(plan)

    assert calls["n"] == 2
    # Neither feature lost a row: the first delete was rolled back with the second.
    assert ln.Feature.filter(name="sv_drop", branch_id=-1).one().values.count() == 2
    assert ln.Feature.filter(name="sv_keep", branch_id=-1).one().values.count() == 1
