"""Tests for lamindb_setup.evolve.engine._destroy."""

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
    Schema,
)
from lamindb_setup.evolve.authoring._plan import (
    Plan,
    PlanContext,
    PlanExports,
    create_plan,
)
from lamindb_setup.evolve.engine._apply import (
    Action,
    ApplyMode,
    DriftError,
    compute_changes,
    write_lock,
)
from lamindb_setup.evolve.engine._destroy import (
    ManagingPlan,
    compute_destroy,
    compute_destroy_group,
    delete_references,
    execute_destroy,
    managing_plans,
    merge_destroy_plans,
    plan_held_references,
    render_destroy,
    trash_references,
)
from lamindb_setup.evolve.engine._lock import LockEntry, LockFile

if TYPE_CHECKING:
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType


@create_plan
def _destroy_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("destroy_ft")
    f = Feature("destroy_col", dtype=LaminDType(str), type=ft)
    Schema(name="DestroySchema", features=[f])


@create_plan
def _destroy_plan_int(ctx: PlanContext, exports: None) -> None:
    # Same objects as `_destroy_plan` but with the feature's dtype changed
    # from `str` to `int` — the structural edit that cannot be applied in
    # place and must go through a destroy + recreate.
    ft = FeatureType("destroy_ft")
    f = Feature("destroy_col", dtype=LaminDType(int), type=ft)
    Schema(name="DestroySchema", features=[f])


class _UpstreamExports(PlanExports):
    ft: FeatureType


@create_plan
def _upstream_destroy_plan(ctx: PlanContext, exports: _UpstreamExports) -> None:
    exports.ft = FeatureType("upstream_ft")
    Feature("upstream_col", dtype=LaminDType(str), type=exports.ft)


@create_plan
def _child_destroy_plan(
    ctx: PlanContext,
    exports: None,
    upstream: Plan[_UpstreamExports] = _upstream_destroy_plan,
) -> None:
    child_ft = FeatureType("child_ft", type=upstream.exports.ft)
    f = Feature("child_col", dtype=LaminDType(str), type=child_ft)
    Schema(name="ChildSchema", features=[f])


@create_plan
def _destroy_records_plan(ctx: PlanContext, exports: None) -> None:
    rt = RecordType("destroy_rt")
    Record("destroy_record", type=rt)


@create_plan
def _bulk_destroy_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("bulk_destroy_ft")
    features = [
        Feature(f"bulk_destroy_col_{i:03d}", dtype=LaminDType(str), type=ft)
        for i in range(25)
    ]
    Schema(name="BulkDestroySchema", features=features)


@create_plan
def _small_scan_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("small_scan_ft")
    features = [
        Feature(f"small_scan_col_{i:03d}", dtype=LaminDType(str), type=ft)
        for i in range(5)
    ]
    Schema(name="SmallScanSchema", features=features)


@create_plan
def _large_scan_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("large_scan_ft")
    features = [
        Feature(f"large_scan_col_{i:03d}", dtype=LaminDType(str), type=ft)
        for i in range(40)
    ]
    Schema(name="LargeScanSchema", features=features)


def test_compute_destroy_reports_missing_when_nothing_applied(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    destroy_plan = compute_destroy(_destroy_plan, lock=None)

    rendered = render_destroy(destroy_plan, title="Before apply")
    assert "'destroy_col'  (not in instance)" in rendered
    assert "3 not present" in rendered
    assert destroy_plan.targets == []
    assert {m.kind for m in destroy_plan.missing} == {
        "FeatureType",
        "Feature",
        "Schema",
    }
    assert destroy_plan.is_safe


def test_compute_destroy_finds_targets_after_apply(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    destroy_plan = compute_destroy(_destroy_plan, lock=None)

    assert {t.kind for t in destroy_plan.targets} == {
        "FeatureType",
        "Feature",
        "Schema",
    }
    assert destroy_plan.missing == []
    # The plan's own internal references (schema -> feature, feature -> type)
    # are excluded, so the plan is safe to destroy.
    assert destroy_plan.is_safe
    assert destroy_plan.total_references == 0


def test_execute_destroy_soft_deletes_by_default(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)
    assert ln.Feature.filter(name="destroy_col").one_or_none() is not None
    assert ln.Schema.filter(name="DestroySchema").one_or_none() is not None

    destroy_plan = compute_destroy(_destroy_plan, lock=None)
    deleted = execute_destroy(destroy_plan)

    assert len(deleted) == 3
    # The default queryset hides trashed objects.
    assert ln.Feature.filter(name="destroy_col").one_or_none() is None
    assert ln.Feature.filter(name="destroy_ft", is_type=True).one_or_none() is None
    assert ln.Schema.filter(name="DestroySchema").one_or_none() is None

    # Soft delete moves objects to the trash (branch_id=-1) rather than removing
    # them, so they can still be recovered.
    assert ln.Feature.filter(name="destroy_col", branch_id=-1).one_or_none() is not None


def test_execute_destroy_permanent_removes_objects(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    destroy_plan = compute_destroy(_destroy_plan, lock=None)
    execute_destroy(destroy_plan, permanent=True)

    # Permanent deletion removes the rows entirely, including from the trash.
    assert ln.Feature.filter(name="destroy_col", branch_id=-1).one_or_none() is None
    assert ln.Schema.filter(name="DestroySchema", branch_id=-1).one_or_none() is None


def test_apply_after_soft_destroy_without_pruning_reports_the_trashed_object(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    ctx = compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)
    path = tmp_path / "destroy_plan.lock"
    write_lock(ctx, _destroy_plan, path)  # type: ignore[arg-type]
    lock = LockFile.read(path)
    assert lock is not None

    destroy_plan = compute_destroy(_destroy_plan, lock=lock)
    execute_destroy(destroy_plan)  # soft delete (the default) moves them to trash
    assert ln.Feature.filter(name="destroy_col").one_or_none() is None

    # Soft-deleted objects are hidden from name lookups but are still resolvable
    # by the uid the lock records.  Re-applying the plan used to silently
    # re-adopt the trashed feature and report NO_CHANGE, leaving the plan
    # "managing" a record that validates nothing; it now says so instead, and
    # names the two ways out.
    with pytest.raises(DriftError, match="soft-deleted") as excinfo:
        compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=lock)
    assert "branch_id=-1" in str(excinfo.value)
    assert ln.Feature.filter(name="destroy_col").one_or_none() is None

    # A plan that also changes the dtype (str -> int) stops at the same place:
    # the trashed record is what blocks it, not the configuration drift the
    # comparison against it would otherwise report.
    with pytest.raises(DriftError, match="soft-deleted"):
        compute_changes(_destroy_plan_int, mode=ApplyMode.APPLY, lock=lock)


def test_apply_after_soft_destroy_with_no_lock_entries_recreates_with_new_dtype(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    ctx = compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)
    path = tmp_path / "destroy_plan.lock"
    write_lock(ctx, _destroy_plan, path)  # type: ignore[arg-type]
    lock = LockFile.read(path)
    assert lock is not None

    destroy_plan = compute_destroy(_destroy_plan, lock=lock)
    execute_destroy(destroy_plan)  # soft delete (the default) moves them to trash

    for target in destroy_plan.targets:
        del lock.objects[target.key]
    assert not lock.objects

    recreated = compute_changes(_destroy_plan_int, mode=ApplyMode.APPLY, lock=lock)
    assert {c.action for c in recreated.changes} == {Action.CREATE}
    live = ln.Feature.filter(name="destroy_col").one_or_none()
    assert live is not None
    assert live._dtype_str == "int"
    assert ln.Schema.filter(name="DestroySchema").one_or_none() is not None


def test_compute_destroy_excludes_upstream_plan_objects(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_upstream_destroy_plan, mode=ApplyMode.APPLY, lock=None)
    compute_changes(_child_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    destroy_plan = compute_destroy(_child_destroy_plan, lock=None)

    target_names = {t.name for t in destroy_plan.targets}
    # Only the child plan's own objects are targeted; the objects created by the
    # upstream plan it depends on are left untouched.
    assert target_names == {"child_ft", "child_col", "ChildSchema"}
    assert "upstream_ft" not in target_names
    assert "upstream_col" not in target_names
    assert destroy_plan.is_safe


def test_execute_destroy_leaves_upstream_plan_objects_intact(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_upstream_destroy_plan, mode=ApplyMode.APPLY, lock=None)
    compute_changes(_child_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    destroy_plan = compute_destroy(_child_destroy_plan, lock=None)
    execute_destroy(destroy_plan, permanent=True)

    # The child plan's objects are gone.
    assert ln.Feature.filter(name="child_col").one_or_none() is None
    assert ln.Feature.filter(name="child_ft", is_type=True).one_or_none() is None
    assert ln.Schema.filter(name="ChildSchema").one_or_none() is None

    # The upstream plan's objects survive.
    assert ln.Feature.filter(name="upstream_col").one_or_none() is not None
    assert ln.Feature.filter(name="upstream_ft", is_type=True).one_or_none() is not None


def test_compute_destroy_group_excludes_sibling_references(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # The child borrows `upstream_ft`, so its owner is applied first.
    compute_changes(_upstream_destroy_plan, mode=ApplyMode.APPLY, lock=None)
    compute_changes(_child_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    # Destroying the upstream plan alone is blocked: the child's `child_ft`
    # references `upstream_ft` and is external to the upstream plan.
    upstream_only = compute_destroy(_upstream_destroy_plan, lock=None)
    assert not upstream_only.is_safe

    # Destroying both together scopes the reference away: `child_ft` is managed
    # by a listed plan, so it no longer blocks `upstream_ft`.
    upstream_plan, child_plan = compute_destroy_group(
        [_upstream_destroy_plan, _child_destroy_plan]
    )
    assert {t.name for t in upstream_plan.targets} == {"upstream_ft", "upstream_col"}
    assert {t.name for t in child_plan.targets} == {
        "child_ft",
        "child_col",
        "ChildSchema",
    }
    assert upstream_plan.is_safe
    assert child_plan.is_safe


def test_compute_destroy_group_single_plan_matches_compute_destroy(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    (group_plan,) = compute_destroy_group([_destroy_plan])
    solo_plan = compute_destroy(_destroy_plan, lock=None)

    assert {t.key for t in group_plan.targets} == {t.key for t in solo_plan.targets}
    assert group_plan.is_safe == solo_plan.is_safe


def test_execute_destroy_group_tears_down_all_plans(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_upstream_destroy_plan, mode=ApplyMode.APPLY, lock=None)
    compute_changes(_child_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    destroy_plans = compute_destroy_group([_upstream_destroy_plan, _child_destroy_plan])
    # Merge dependencies-first (upstream before child) so the reversed deletion
    # removes the child's `child_ft` before the `upstream_ft` it references.
    merged = merge_destroy_plans(destroy_plans, plan_name="group")
    execute_destroy(merged, permanent=True)

    # Both plans' objects are gone.
    assert ln.Feature.filter(name="child_col").one_or_none() is None
    assert ln.Feature.filter(name="child_ft", is_type=True).one_or_none() is None
    assert ln.Schema.filter(name="ChildSchema").one_or_none() is None
    assert ln.Feature.filter(name="upstream_col").one_or_none() is None
    assert ln.Feature.filter(name="upstream_ft", is_type=True).one_or_none() is None


def test_merge_destroy_plans_dedupes_and_preserves_order(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    one = compute_destroy(_destroy_plan, lock=None)
    two = compute_destroy(_destroy_plan, lock=None)

    merged = merge_destroy_plans([one, two], plan_name="dup")

    # The same plan listed twice contributes each object only once.
    keys = [t.key for t in merged.targets]
    assert len(keys) == len(set(keys))
    assert set(keys) == {t.key for t in one.targets}


def test_compute_destroy_refuses_when_externally_referenced(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    # An external schema, not managed by the plan, references the feature. It
    # carries an extra feature so it does not dedup to the plan's own schema.
    feature = ln.Feature.get(name="destroy_col", is_type=False)
    extra = ln.Feature(name="external_extra", dtype="str").save()
    ln.Schema(features=[feature, extra], otype="DataFrame").save()

    destroy_plan = compute_destroy(_destroy_plan, lock=None)

    assert not destroy_plan.is_safe
    assert destroy_plan.total_references >= 1
    blocked = {t.kind: t for t in destroy_plan.targets}["Feature"]
    assert blocked.reference_count >= 1

    with pytest.raises(RuntimeError, match="external reference"):
        execute_destroy(destroy_plan)

    # Nothing was deleted.
    assert ln.Feature.filter(name="destroy_col").one_or_none() is not None


def test_scan_counts_references_without_materializing_records(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    feature = ln.Feature.get(name="destroy_col", is_type=False)
    extra = ln.Feature(name="external_extra", dtype="str").save()
    ln.Schema(features=[feature, extra], otype="DataFrame").save()

    destroy_plan = compute_destroy(_destroy_plan, lock=None)

    blocked = {t.kind: t for t in destroy_plan.targets}["Feature"]
    detail = next(d for d in blocked.references if d.referencing_kind == "Schema")

    # The safety scan only fetched the blocking count; the referencing records
    # are not materialized until they are actually accessed.
    assert detail.count >= 1
    assert detail._records is None

    # Accessing `records` loads them lazily and matches the reported count.
    assert len(detail.records) == detail.count
    assert detail._records is not None


def test_destroy_plan_lists_individual_references(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    feature = ln.Feature.get(name="destroy_col", is_type=False)
    extra = ln.Feature(name="external_extra", dtype="str").save()
    ln.Schema(
        name="ExternalSchema", features=[feature, extra], otype="DataFrame"
    ).save()

    destroy_plan = compute_destroy(_destroy_plan, lock=None)
    references = destroy_plan.references()

    assert len(references) >= 1
    schema_ref = next(r for r in references if r.referencing_kind == "Schema")
    assert schema_ref.target_kind == "Feature"
    assert schema_ref.target_name == "destroy_col"
    assert schema_ref.relation == "schemas"
    assert schema_ref.attributes["name"] == "ExternalSchema"

    line = schema_ref.describe()
    assert "Schema(" in line
    assert "ExternalSchema" in line
    assert "schemas" in line
    assert "Feature 'destroy_col'" in line


def test_destroy_plan_references_limit_bounds_the_listing(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    feature = ln.Feature.get(name="destroy_col", is_type=False)
    # Several distinct external schemas each reference the plan-managed feature.
    for i in range(5):
        unique = ln.Feature(name=f"external_unique_{i}", dtype="str").save()
        ln.Schema(
            name=f"ExternalSchema{i}",
            features=[feature, unique],
            otype="DataFrame",
        ).save()

    destroy_plan = compute_destroy(_destroy_plan, lock=None)
    detail = next(
        d
        for t in destroy_plan.targets
        for d in t.references
        if d.referencing_kind == "Schema"
    )
    assert detail.count >= 5

    # The bounded listing never materializes the full relation: it returns at
    # most `limit` references and leaves the detail's record cache empty.
    limited = destroy_plan.references(limit=2)
    assert len(limited) == 2
    assert detail._records is None

    # `head` slices the query rather than loading every referencing row.
    assert len(detail.head(2)) == 2
    assert detail.head(0) == [] and destroy_plan.references(limit=0) == []
    assert detail._records is None
    records = detail.records
    assert detail.head(2) == records[:2]
    assert "referenced by 5 Schema" in render_destroy(
        destroy_plan, title="Before destroy"
    )


def test_destroy_plan_describes_link_records_by_record_and_value(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    # A record stores a value for the plan-managed feature. This creates a
    # RecordJson link row, which has no uid/name of its own.
    sheet = ln.Record(name="LinkSheet", is_type=True).save()
    ln.Record(name="LinkRecord", type=sheet, features={"destroy_col": "hello"}).save()

    destroy_plan = compute_destroy(_destroy_plan, lock=None)
    link_refs = [r for r in destroy_plan.references() if "links_record" in r.relation]

    assert link_refs, "expected a feature-value link reference"
    ref = link_refs[0]
    # The link row has no identifying attrs of its own, so it is described by
    # the record it attaches to and the value it stores.
    assert "record" in ref.attributes
    assert ref.attributes["record"] == "LinkRecord"
    assert ref.attributes["value"] == "hello"

    line = ref.describe()
    assert "record='LinkRecord'" in line
    assert "value='hello'" in line
    assert "()" not in line  # no empty parens for link rows


def _managing(name: str) -> ManagingPlan:
    return ManagingPlan(
        name=name, target=f"plan_{name}.py", lock_file=Path(f"plan_{name}.x.lock")
    )


def test_plan_held_references_attribute_rows_to_the_plans_managing_them(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)
    feature = ln.Feature.get(name="destroy_col", is_type=False)
    # A feature another plan manages, typed by this plan's feature type ...
    typed = ln.Feature(
        name="held_col", dtype="str", type=ln.Feature.get(name="destroy_ft")
    ).save()
    # ... a record it seeds, storing a value for this plan's feature (a link
    # row, which belongs to the record) ...
    sheet = ln.Record(name="HeldSheet", is_type=True).save()
    record = ln.Record(
        name="HeldRecord", type=sheet, features={"destroy_col": "hello"}
    ).save()
    # ... and a schema no plan manages.
    extra = ln.Feature(name="held_extra", dtype="str").save()
    ln.Schema(name="Unheld", features=[feature, extra], otype="DataFrame").save()

    destroy_plan = compute_destroy(_destroy_plan, lock=None)
    child, other = _managing("child"), _managing("other")
    managing = {
        typed.uid: (child,),
        record.uid: (child, other),
        # A legacy lock recording a target it borrows does not claim it.
        feature.uid: (other,),
    }

    held = plan_held_references(destroy_plan, managing)

    by_kind = {item.reference.referencing_kind: item for item in held}
    assert set(by_kind) == {"Feature", "RecordJson"}
    assert by_kind["Feature"].uids == {typed.uid}
    assert by_kind["Feature"].plans == (child,)
    assert by_kind["Feature"].reference.attributes["name"] == "held_col"
    assert by_kind["RecordJson"].uids == {record.uid}
    assert by_kind["RecordJson"].plans == (child, other)
    assert by_kind["RecordJson"].reference.attributes["record"] == "HeldRecord"
    assert destroy_plan.total_references == len(held) + 1
    assert plan_held_references(destroy_plan, {}) == []


def test_managing_plans_reads_the_instance_locks_of_the_project(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'x'\n")
    plans = tmp_path / "plans"
    for directory in (plans, tmp_path / ".hidden", tmp_path / "__pycache__"):
        directory.mkdir()

    def lock(path: Path, plan: str, **uids: str) -> None:
        path.with_name(f"{path.name.split('.')[0]}.py").write_text("")
        LockFile(
            plan=plan,
            objects={
                key: LockEntry(key=key, kind="Feature", name=key, uid=uid)
                for key, uid in uids.items()
            },
            seeded={"Record:seeded": f"seed{plan}"},
        ).write(path)

    lock(plans / "plan_base.inst.lock", "base", a="uidbase")
    lock(plans / "plan_child.inst.lock", "child", b="uidchild", c="uidshared")
    lock(plans / "nested_child.inst.lock", "nested", d="uidshared")
    lock(plans / "plan_child.other.lock", "child", b="uidelsewhere")
    lock(tmp_path / ".hidden" / "plan_h.inst.lock", "hidden", h="uidhidden")
    lock(tmp_path / "__pycache__" / "plan_p.inst.lock", "cache", p="uidcache")
    # A lock whose plan file is gone is no plan's any more.
    lock(plans / "plan_gone.inst.lock", "gone", g="uidgone")
    (plans / "plan_gone.py").unlink()
    monkeypatch.chdir(tmp_path)

    managing = managing_plans([plans / "plan_base.py"], "inst")

    child = managing["uidchild"][0]
    assert child.name == "child"
    assert child.describe() == "'child' (plans/plan_child.py)"
    assert child.lock_display == "plans/plan_child.inst.lock"
    assert [plan.name for plan in managing["uidshared"]] == ["nested", "child"]
    assert managing["seedchild"] == (child,)
    assert set(managing) == {"uidchild", "uidshared", "seedchild", "seednested"}


def test_execute_destroy_removes_record_types_and_records(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_records_plan, mode=ApplyMode.APPLY, lock=None)
    assert ln.Record.filter(name="destroy_record", is_type=False).one_or_none()

    destroy_plan = compute_destroy(_destroy_records_plan, lock=None)
    execute_destroy(destroy_plan)

    assert ln.Record.filter(name="destroy_record", is_type=False).one_or_none() is None
    assert ln.Record.filter(name="destroy_rt", is_type=True).one_or_none() is None


def test_execute_destroy_force_deletes_blocking_references(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    # An external schema and a record value both block deletion of the feature.
    feature = ln.Feature.get(name="destroy_col", is_type=False)
    extra = ln.Feature(name="external_extra", dtype="str").save()
    ln.Schema(features=[feature, extra], otype="DataFrame").save()
    sheet = ln.Record(name="LinkSheet", is_type=True).save()
    ln.Record(name="LinkRecord", type=sheet, features={"destroy_col": "hello"}).save()

    destroy_plan = compute_destroy(_destroy_plan, lock=None)
    assert not destroy_plan.is_safe

    deleted = execute_destroy(destroy_plan, permanent=True, force=True)

    assert len(deleted) == 3
    # Targets are gone, including from the trash.
    assert ln.Feature.filter(name="destroy_col", branch_id=-1).one_or_none() is None
    assert ln.Schema.filter(name="DestroySchema", branch_id=-1).one_or_none() is None


def test_execute_destroy_trash_references_keeps_everything_recoverable(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    # A full-object reference (external schema) and a link-row reference (a
    # record value) both block deletion of the feature.
    feature = ln.Feature.get(name="destroy_col", is_type=False)
    extra = ln.Feature(name="external_extra", dtype="str").save()
    ln.Schema(
        name="ExternalSchema", features=[feature, extra], otype="DataFrame"
    ).save()
    sheet = ln.Record(name="LinkSheet", is_type=True).save()
    ln.Record(name="LinkRecord", type=sheet, features={"destroy_col": "hello"}).save()

    destroy_plan = compute_destroy(_destroy_plan, lock=None)
    assert not destroy_plan.is_safe

    deleted = execute_destroy(destroy_plan, trash_references=True)
    assert len(deleted) == 3

    # The full-object reference is trashed (hidden) but recoverable.
    assert ln.Schema.filter(name="ExternalSchema").one_or_none() is None
    assert (
        ln.Schema.filter(name="ExternalSchema", branch_id=-1).one_or_none() is not None
    )

    # The targets are trashed (hidden) but recoverable, not permanently gone.
    assert ln.Feature.filter(name="destroy_col").one_or_none() is None
    assert ln.Feature.filter(name="destroy_col", branch_id=-1).one_or_none() is not None
    assert ln.Schema.filter(name="DestroySchema").one_or_none() is None
    assert (
        ln.Schema.filter(name="DestroySchema", branch_id=-1).one_or_none() is not None
    )

    # The un-trashable link row's owning data record is left untouched (live).
    assert ln.Record.filter(name="LinkRecord", is_type=False).one_or_none() is not None


def test_trash_references_trashes_full_records_and_leaves_link_rows(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    feature = ln.Feature.get(name="destroy_col", is_type=False)
    extra = ln.Feature(name="external_extra", dtype="str").save()
    ln.Schema(
        name="ExternalSchema", features=[feature, extra], otype="DataFrame"
    ).save()
    sheet = ln.Record(name="LinkSheet", is_type=True).save()
    ln.Record(name="LinkRecord", type=sheet, features={"destroy_col": "hello"}).save()

    destroy_plan = compute_destroy(_destroy_plan, lock=None)
    trashed, left = trash_references(destroy_plan)

    # At least the external schema is trashed; at least one link row is left.
    assert trashed >= 1
    assert left >= 1

    # The external schema is trashed (hidden from the default queryset) but
    # recoverable, and it no longer counts as a blocking reference.
    assert ln.Schema.filter(name="ExternalSchema").one_or_none() is None
    assert (
        ln.Schema.filter(name="ExternalSchema", branch_id=-1).one_or_none() is not None
    )

    # The un-trashable link row is left in place: its owning data record stays
    # live and untouched.
    assert ln.Record.filter(name="LinkRecord", is_type=False).one_or_none() is not None


def test_execute_destroy_trash_references_rejects_permanent_and_force(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)
    destroy_plan = compute_destroy(_destroy_plan, lock=None)

    with pytest.raises(ValueError, match="mutually exclusive"):
        execute_destroy(destroy_plan, permanent=True, trash_references=True)
    with pytest.raises(ValueError, match="mutually exclusive"):
        execute_destroy(destroy_plan, force=True, trash_references=True)


def test_delete_references_removes_blocking_records(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)

    feature = ln.Feature.get(name="destroy_col", is_type=False)
    extra = ln.Feature(name="external_extra", dtype="str").save()
    ln.Schema(
        name="ExternalSchema", features=[feature, extra], otype="DataFrame"
    ).save()

    destroy_plan = compute_destroy(_destroy_plan, lock=None)
    count = delete_references(destroy_plan)

    assert count >= 1
    # The external schema that referenced the feature is gone.
    assert ln.Schema.filter(name="ExternalSchema").one_or_none() is None

    # After removing the references the feature can be destroyed permanently.
    destroy_plan = compute_destroy(_destroy_plan, lock=None)
    assert destroy_plan.is_safe
    execute_destroy(destroy_plan, permanent=True)
    assert ln.Feature.filter(name="destroy_col", branch_id=-1).one_or_none() is None


def test_render_destroy_lists_targets(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from lamindb_setup.evolve.engine._destroy import print_destroy

    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)
    progress: list[str] = []
    destroy_plan = compute_destroy(_destroy_plan, lock=None, report=progress.append)
    assert progress == [
        "Finalizing plan",
        "Prefetching instance state",
        "Scanning for external references",
    ]
    print_destroy(destroy_plan, title="Destroy: test")
    assert "3 to delete" in capsys.readouterr().out

    text = render_destroy(destroy_plan, title="Destroy: test")
    assert "Destroy: test" in text
    assert "FeatureType" in text
    assert "'destroy_ft'" in text
    assert "3 to delete" in text


def test_compute_destroy_prefetches_existence_lookups(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
) -> None:
    from lamindb_setup.evolve.engine._apply import ExistenceCache

    compute_changes(_bulk_destroy_plan, mode=ApplyMode.APPLY, lock=None)
    _bulk_destroy_plan.finalize()
    objs = [dependency.obj for dependency in _bulk_destroy_plan.dependencies]
    assert len(objs) > 20  # a sizeable plan

    existing_cache = ExistenceCache()
    destroy_plan = compute_destroy(
        _bulk_destroy_plan, lock=None, existing_cache=existing_cache
    )

    # The bulk batched reads fill the cache up front, so no object resolves its
    # existence one row at a time (the N+1 that made destroy scale with object
    # count).
    assert existing_cache.queries == 0
    assert existing_cache.prefilled == len(objs)
    assert len(destroy_plan.targets) == len(objs)


def test_scan_query_count_is_independent_of_target_count(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    from django.db import connection
    from django.test.utils import CaptureQueriesContext

    compute_changes(_small_scan_plan, mode=ApplyMode.APPLY, lock=None)
    compute_changes(_large_scan_plan, mode=ApplyMode.APPLY, lock=None)

    def _query_count(plan: Plan[None]) -> int:
        with CaptureQueriesContext(connection) as ctx:
            compute_destroy(plan, lock=None)
        return len(ctx.captured_queries)

    small = _query_count(_small_scan_plan)
    large = _query_count(_large_scan_plan)

    # The large plan manages many more objects than the small one ...
    small_targets = len(compute_destroy(_small_scan_plan, lock=None).targets)
    large_targets = len(compute_destroy(_large_scan_plan, lock=None).targets)
    assert large_targets > small_targets * 3

    # ... yet every relation (bulk and many-to-many) is counted with a single
    # grouped query, so the safety check emits the same number of queries
    # regardless of how many objects the plan manages.
    assert small == large


def test_execute_destroy_rolls_back_on_failure(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
) -> None:
    from django.db.models import ProtectedError

    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)
    destroy_plan = compute_destroy(_destroy_plan, lock=None)

    # Deletions run in reverse creation order, so the feature type goes last.
    # Give it a reference the plan never saw: the instance gaining one between
    # planning and executing is exactly the race an atomic teardown has to
    # survive, and `Feature.type` is PROTECT, so the final delete genuinely
    # fails after the earlier ones have already run.
    feature_type = destroy_plan.targets[0].record
    assert feature_type.name == "destroy_ft"
    ln.Feature(name="destroy_latecomer", dtype="str", type=feature_type).save()

    with pytest.raises(ProtectedError):
        execute_destroy(destroy_plan, permanent=True)

    # The whole teardown runs in one transaction, so the earlier deletions are
    # rolled back: every object is still present (and not in the trash).
    assert ln.Feature.filter(name="destroy_col", is_type=False).one_or_none()
    assert ln.Feature.filter(name="destroy_ft", is_type=True).one_or_none()
    assert ln.Schema.filter(name="DestroySchema").one_or_none()


def test_delete_references_retries_a_protected_parent_after_its_child(ln) -> None:
    from django.db.models import ProtectedError
    from lamindb_setup.evolve.engine._destroy import (
        DestroyPlan,
        DestroyTarget,
        ReferenceDetail,
    )

    parent = ln.Record(name="external type", is_type=True).save()
    child = ln.Record(name="external member", type=parent).save()
    detail = ReferenceDetail("Record", "records", 1, _records=[parent])
    target = DestroyTarget("Feature:x", "Feature", "x", "target", None, [detail])
    plan = DestroyPlan("test", [target])
    with pytest.raises(ProtectedError):
        delete_references(plan)
    assert ln.Record.objects.filter(pk=parent.pk).exists()
    detail._records = [parent, child, parent]
    detail.count = 3
    assert delete_references(plan) == 2
    assert not ln.Record.objects.filter(
        name__in=["external type", "external member"]
    ).exists()


def test_hidden_many_to_many_destroy_scan_counts_only_external_records(ln) -> None:
    from django.db import connection, models
    from django.test.utils import isolate_apps
    from lamindb_setup.evolve.engine import _destroy as destroy

    with isolate_apps("lamindb"):

        class Item(models.Model):
            peers = models.ManyToManyField("self")

            class Meta:
                app_label = "lamindb"

        with connection.schema_editor() as editor:
            editor.create_model(Item)
        try:
            target, sibling, external = Item.objects.bulk_create(
                [Item(), Item(), Item()]
            )
            target.peers.add(sibling, external)
            relations = destroy._model_relations(Item)
            (relation,) = [rel for rel in relations if rel.relation == "peers"]
            assert relation.m2m_query_name is None
            item = destroy.DestroyTarget("Item:t", "Item", "t", "t", target)
            unit = destroy._ScanUnit(relation, [item], [target.pk])
            destroy._run_scan_unit(unit, {Item: {sibling.pk}})
            (blocked,) = item.references
            assert blocked.count == 1 and blocked.records == [external]
        finally:
            with connection.schema_editor() as editor:
                editor.delete_model(Item)


def test_destroy_reporting_handles_count_only_and_partial_reference_metadata(
    ln, tmp_path: Path
) -> None:
    from types import SimpleNamespace

    from lamindb_setup.evolve.engine import _destroy as destroy

    with pytest.raises(ValueError, match="same length"):
        compute_destroy_group([_destroy_plan], locks=[])
    detail = destroy.ReferenceDetail("Link", "incoming", count=2)
    item = destroy.DestroyTarget("Feature:x", "Feature", "x", "x", None, [detail])
    plan = destroy.DestroyPlan("test", [item])
    assert detail.head(2) == [] and plan.total_references == 2
    assert plan_held_references(plan, {"outside": ()}) == []
    assert destroy._record_attributes(SimpleNamespace(record=None)) == {}
    assert destroy._record_attributes(
        SimpleNamespace(value=SimpleNamespace(uid="guid"))
    ) == {"value": "guid"}
    assert destroy._owner_paths(ln.Artifact) == []
    assert destroy._display_path(tmp_path) == str(tmp_path)


def test_destroy_skips_cascaded_references_and_preserves_untrashable_ones() -> None:
    from unittest.mock import Mock

    from lamindb_setup.evolve.engine import _destroy as destroy

    child = Mock(pk=2, branch_id=1)
    parent = Mock(pk=1, branch_id=1)
    parent.delete.side_effect = lambda **kwargs: setattr(child, "pk", None)
    detail = destroy.ReferenceDetail(
        "Record", "references", 2, _records=[parent, child]
    )
    item = destroy.DestroyTarget(
        "Record:target", "Record", "target", "target", None, [detail]
    )
    plan = destroy.DestroyPlan("test", [item])
    assert delete_references(plan) == 1
    child.delete.assert_not_called()
    parent.delete.side_effect = ValueError("cannot trash")
    assert trash_references(plan) == (0, 1)
    parent.delete.assert_called_with(permanent=False)


def test_repoint_preserves_conflicting_links_and_reports_database_refusal(
    ln, monkeypatch
) -> None:
    from django.db import connection, models
    from django.test.utils import isolate_apps
    from lamindb_setup.evolve.core import LaminSchemasError
    from lamindb_setup.evolve.engine import _repoint

    with isolate_apps("lamindb"):

        class Annotation(models.Model):
            owner = models.ForeignKey(ln.Record, models.CASCADE)
            key = models.CharField(max_length=20)
            value = models.IntegerField()

            class Meta:
                app_label = "lamindb"
                constraints = [
                    models.UniqueConstraint(
                        fields=["owner", "key"], name="test_owner_key"
                    ),
                    models.UniqueConstraint(
                        fields=["owner", "value"],
                        condition=models.Q(value__gt=0),
                        name="test_positive_value",
                    ),
                ]

        fk = Annotation._meta.get_field("owner")
        # The test registry is isolated from the installed app registry.
        monkeypatch.setattr(
            _repoint, "referencing_fields", lambda _: [(Annotation, fk)]
        )
        old, new = [ln.Record(name=name).save() for name in ("old owner", "new owner")]
        with connection.schema_editor() as editor:
            editor.create_model(Annotation)
        try:
            left = Annotation.objects.create(owner=old, key="measurement", value=1)
            right = Annotation.objects.create(owner=new, key="measurement", value=2)
            with pytest.raises(LaminSchemasError, match="record different things"):
                _repoint.repoint_references(ln.Record, old.pk, new.pk)
            right.key, right.value = "other key", 1
            right.save()
            with pytest.raises(LaminSchemasError, match="refused by the database"):
                _repoint.repoint_references(ln.Record, old.pk, new.pk)
            left.refresh_from_db()
            assert left.owner_id == old.pk and Annotation.objects.count() == 2
        finally:
            with connection.schema_editor() as editor:
                editor.delete_model(Annotation)


def test_destroy_schedules_a_repeated_dependency_only_once(ln) -> None:
    from unittest.mock import Mock

    compute_changes(_destroy_plan, mode=ApplyMode.APPLY, lock=None)
    expected = compute_destroy(_destroy_plan)
    deps = list(_destroy_plan.dependencies)
    plan = Mock(spec=Plan, ctx=_destroy_plan.ctx, own_dependencies=deps)
    plan.name = _destroy_plan.name
    plan.dependencies = [*deps, deps[0]]

    actual = compute_destroy(plan)
    assert [t.uid for t in actual.targets] == [t.uid for t in expected.targets]
    assert actual.is_safe and not actual.missing
