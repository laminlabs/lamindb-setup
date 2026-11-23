# Source test semantics retained.
# ruff: noqa: TC003
"""Tests for changing a composite schema's slots.

Removing a slot only stops validating that part of a dataset, so it applies in
place.  Adding one -- or swapping a component for one that admits less -- would
judge datasets the composite already validated by something they were never
checked against: allowed when the composite validated nothing (with
`--allow-data-validated-changes`) or vouched for by `accept_slot_change`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from lamindb_setup.evolve import (
    Feature,
    FeatureType,
    LaminDType,
    Schema,
    accept_slot_change,
)
from lamindb_setup.evolve.authoring._plan import PlanContext, create_plan
from lamindb_setup.evolve.engine._apply import (
    Action,
    ApplyMode,
    DriftError,
    compute_changes,
    write_lock,
)
from lamindb_setup.evolve.engine._lock import LockFile

if TYPE_CHECKING:
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as LamindbModuleType


def _components() -> dict[str, Schema]:
    """Distinct components -- lamindb keeps one schema per content hash."""
    ft = FeatureType("cs_ft")
    f = {name: Feature(name, dtype=LaminDType(str), type=ft) for name in "abcd"}
    return {
        "obs1": Schema(name="CsObs1", features=[f["a"]]),
        "obs1+": Schema(
            name="CsObs1plus", features=[f["a"], f["d"]], optionals=[f["d"]]
        ),
        "obs2": Schema(name="CsObs2", features=[f["b"]]),
        "var": Schema(name="CsVar", features=[f["c"]]),
    }


def _plan(slots: dict[str, str], *, declared: bool = False, outer: bool = False):
    """Composite `CsC` slotting the named components (`obs1`, `var`, ...)."""

    @create_plan
    def _p(ctx: PlanContext, exports: None) -> None:
        parts = _components()
        composite = Schema(
            name="CsC",
            features=[],
            otype="AnnData",
            slots={slot: parts[name] for slot, name in slots.items()},
        )
        if outer:
            Schema(
                name="CsOuter",
                features=[],
                otype="MuData",
                slots={"rna:obs": composite},
            )
        if declared:
            ctx.migrate(composite, accept_slot_change(reason="every dataset has a var"))

    return _p


_OBS = _plan({"obs": "obs1"})
_OBS_VAR = _plan({"obs": "obs1", "var": "var"})
_OBS_VAR_DECLARED = _plan({"obs": "obs1", "var": "var"}, declared=True)
_OBS2 = _plan({"obs": "obs2"})
_OBS_WIDER = _plan({"obs": "obs1+"})
_OBS_OUTER = _plan({"obs": "obs1"}, outer=True)
_OBS_VAR_OUTER = _plan({"obs": "obs1", "var": "var"}, outer=True)


def _apply(plan: Any, lock: LockFile | None = None, **kwargs: Any) -> Any:
    plan.reset()
    return compute_changes(plan, mode=ApplyMode.APPLY, lock=lock, **kwargs)


def _locked(plan: Any, tmp_path: Path, lock: LockFile | None = None) -> LockFile:
    path = tmp_path / f"cs{len(list(tmp_path.iterdir()))}.lock"
    write_lock(_apply(plan, lock), plan, path)
    written = LockFile.read(path)
    assert written is not None
    return written


def _slots(ln: LamindbModuleType) -> dict[str, str]:
    composite = ln.Schema.get(name="CsC")
    return {slot: schema.name for slot, schema in composite.slots.items()}


def _details(ctx: Any) -> list[str]:
    return next(c for c in ctx.changes if c.key.endswith("Schema:CsC")).details


def _validate_with_composite(ln: LamindbModuleType) -> None:
    """Make `CsC` a schema that validated a dataset."""
    import pandas as pd

    artifact = ln.Artifact.from_dataframe(
        pd.DataFrame({"x": [1]}), key="cs.parquet"
    ).save()
    composite = ln.Schema.get(name="CsC")
    ln.Artifact.objects.filter(pk=artifact.pk).update(schema_id=composite.pk)


def test_removing_a_slot_applies_in_place(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _locked(_OBS_VAR, tmp_path)
    before = ln.Schema.get(name="CsC").hash

    ctx = _apply(_OBS, lock)

    assert "slot 'var' removed" in _details(ctx)
    assert _slots(ln) == {"obs": "CsObs1"}
    assert ln.Schema.get(name="CsC").hash != before


def test_adding_a_slot_to_a_composite_that_validated_nothing_takes_the_flag(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _locked(_OBS, tmp_path)

    with pytest.raises(DriftError) as excinfo:
        _apply(_OBS_VAR, lock)
    assert (
        "could be applied with --allow-data-validated-changes: the schema has "
        "validated no data" in str(excinfo.value)
    )

    ctx = _apply(_OBS_VAR, lock, allow_data_validated_changes=True)

    assert "slot 'var' \u2192 'Schema:CsVar'" in _details(ctx)
    assert _slots(ln) == {"obs": "CsObs1", "var": "CsVar"}


def test_accept_slot_change_adds_a_slot(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _locked(_OBS, tmp_path)

    ctx = _apply(_OBS_VAR_DECLARED, lock)

    assert any("every dataset has a var" in d for d in _details(ctx)), _details(ctx)
    assert _slots(ln) == {"obs": "CsObs1", "var": "CsVar"}


def test_a_slot_added_to_a_composite_that_validated_data_needs_a_vouch(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _locked(_OBS, tmp_path)
    _validate_with_composite(ln)

    with pytest.raises(DriftError, match="accept_slot_change"):
        _apply(_OBS_VAR, lock, allow_data_validated_changes=True)

    assert _slots(ln) == {"obs": "CsObs1"}


def test_swapping_a_component_is_refused_without_an_answer(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _locked(_OBS, tmp_path)

    with pytest.raises(DriftError, match="slot 'obs'"):
        _apply(_OBS2, lock)

    _apply(_OBS2, lock, allow_data_validated_changes=True)

    assert _slots(ln) == {"obs": "CsObs2"}


def test_a_component_admitting_more_is_swapped_in_place(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The same members plus an optional one: every dataset still validates.
    lock = _locked(_OBS, tmp_path)

    ctx = _apply(_OBS_WIDER, lock)

    assert "slot 'obs' 'CsObs1' \u2192 'Schema:CsObs1plus'" in _details(ctx)
    assert _slots(ln) == {"obs": "CsObs1plus"}


def test_a_slot_changed_outside_the_plan_is_remote_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _locked(_OBS, tmp_path)
    composite = ln.Schema.get(name="CsC")
    links = type(composite).components.through
    links.objects.create(
        composite=composite, component=ln.Schema.get(name="CsVar"), slot="var"
    )

    with pytest.raises(DriftError, match="remote drift.*\n.*slots"):
        _apply(_OBS, lock)


def test_a_slot_change_reapplies_as_no_change(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _locked(_OBS, tmp_path, _locked(_OBS_VAR, tmp_path))

    ctx = _apply(_OBS, lock)

    assert {c.action for c in ctx.changes} == {Action.NO_CHANGE}


def test_a_composite_slotting_the_changed_one_is_rehashed(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _locked(_OBS_VAR_OUTER, tmp_path)
    outer = ln.Schema.get(name="CsOuter").hash

    ctx = _apply(_OBS_OUTER, lock)

    assert any("rehashed composite 'CsOuter'" in d for d in _details(ctx)), _details(
        ctx
    )
    assert ln.Schema.get(name="CsOuter").hash != outer


_Build = Callable[[dict[str, Feature]], Schema]


def _part(
    name: str,
    members: str,
    *,
    optional: str = "",
    index: str | None = None,
    **flags: Any,
) -> _Build:
    """A component named `name` over the features `a`-`d` its letters name."""

    def build(f: dict[str, Feature]) -> Schema:
        return Schema(
            name=name,
            features=[f[m] for m in members],
            optionals=[f[m] for m in optional],
            index=f[index] if index is not None else None,
            **flags,
        )

    return build


def _obs_plan(slotted: str, **parts: _Build) -> Any:
    """Composite `CsC` whose `obs` slot holds `parts[slotted]`.

    Every part is declared, so a component swapped out stays in the plan.
    """

    @create_plan
    def _p(ctx: PlanContext, exports: None) -> None:
        ft = FeatureType("cs_ft")
        f = {name: Feature(name, dtype=LaminDType(str), type=ft) for name in "abcd"}
        built = {name: build(f) for name, build in parts.items()}
        Schema(name="CsC", features=[], otype="AnnData", slots={"obs": built[slotted]})

    return _p


def _preview(plan: Any, lock: LockFile | None = None, **kwargs: Any) -> Any:
    plan.reset()
    return compute_changes(plan, mode=ApplyMode.PLAN, lock=lock, **kwargs)


def _swap(old: _Build, new: _Build) -> tuple[Any, Any]:
    """Plans slotting `old`, then `new`, each declaring both."""
    return _obs_plan("old", old=old, new=new), _obs_plan("new", old=old, new=new)


# Swaps for a component that admits less: each old/new pair differs in exactly
# the property named, beside what keeps the two content hashes apart.
_NARROWING_SWAPS = {
    "flexible-off-under-maximal-set": _swap(
        _part("CsOld", "a", maximal_set=True, flexible=True),
        _part("CsNew", "a", maximal_set=True, flexible=False),
    ),
    "ordered-members-reordered": _swap(
        _part("CsOld", "ab", ordered_set=True),
        _part("CsNew", "ba", ordered_set=True),
    ),
    "index-set": _swap(_part("CsOld", "a"), _part("CsNew", "a", index="a")),
    "otype-changed": _swap(
        _part("CsOld", "a"),
        _part("CsNew", "ad", optional="d", otype="DataFrame"),
    ),
    "maximal-set-on": _swap(_part("CsOld", "a"), _part("CsNew", "a", maximal_set=True)),
    "minimal-set-on": _swap(
        _part("CsOld", "a", minimal_set=False), _part("CsNew", "a")
    ),
    "ordered-set-on": _swap(
        _part("CsOld", "ab"), _part("CsNew", "ab", ordered_set=True)
    ),
    "flexible-on": _swap(_part("CsOld", "a"), _part("CsNew", "a", flexible=True)),
    "optional-made-required": _swap(
        _part("CsOld", "ad", optional="d"), _part("CsNew", "ad")
    ),
    "required-member-added": _swap(_part("CsOld", "a"), _part("CsNew", "ad")),
}

_WIDENING_SWAPS = {
    "ordered-optional-appended": _swap(
        _part("CsOld", "ab", ordered_set=True),
        _part("CsNew", "abd", optional="d", ordered_set=True),
    ),
    "ordered-set-off-and-reordered": _swap(
        _part("CsOld", "ab", ordered_set=True), _part("CsNew", "ba")
    ),
    "flexible-off-without-maximal-set": _swap(
        _part("CsOld", "a", flexible=True), _part("CsNew", "a", flexible=False)
    ),
}

_CS_OLD = _part("CsOld", "a")
# Plans slotting CsOld, then CsNew: one the second plan creates, admitting more
# (`_CREATED_WIDER`) or less (`_CREATED_NARROWER`) than CsOld.
_ONLY_OLD = _obs_plan("old", old=_CS_OLD)
_CREATED_WIDER = _obs_plan("new", old=_CS_OLD, new=_part("CsNew", "ad", optional="d"))
_CREATED_NARROWER = _obs_plan("new", old=_CS_OLD, new=_part("CsNew", "ad"))
# CsNew requiring 'b', then the same run making it optional -- and CsNew with
# 'd' optional, then the same run making it required.
_EDITED_WIDER = (
    _obs_plan("old", old=_CS_OLD, new=_part("CsNew", "ab")),
    _obs_plan("new", old=_CS_OLD, new=_part("CsNew", "ab", optional="b")),
)
_EDITED_NARROWER = (
    _obs_plan("old", old=_CS_OLD, new=_part("CsNew", "ad", optional="d")),
    _obs_plan("new", old=_CS_OLD, new=_part("CsNew", "ad")),
)


def _twins(slotted: str) -> Any:
    """`CsC` slotting `slotted` beside `CsTwin`, which slots `obs2`."""

    @create_plan
    def _p(ctx: PlanContext, exports: None) -> None:
        parts = _components()
        Schema(name="CsC", features=[], otype="AnnData", slots={"obs": parts[slotted]})
        Schema(
            name="CsTwin", features=[], otype="AnnData", slots={"obs": parts["obs2"]}
        )

    return _p


_TWINS = _twins("obs1")
_TWINS_SWAPPED = _twins("obs2")


@create_plan
def _DATAFRAME(ctx: PlanContext, exports: None) -> None:
    """`CsDf`, a schema the plan gives no slots, beside a component for one."""
    parts = _components()
    features = parts["obs1"].features + parts["obs2"].features
    Schema(name="CsDf", features=features, otype="DataFrame")


@pytest.mark.parametrize("mode", [ApplyMode.PLAN, ApplyMode.APPLY])
@pytest.mark.parametrize(
    ("old", "new"), list(_NARROWING_SWAPS.values()), ids=list(_NARROWING_SWAPS)
)
def test_a_component_admitting_less_is_not_swapped_in_place(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    old: Any,
    new: Any,
    mode: ApplyMode,
) -> None:
    lock = _locked(old, tmp_path)
    new.reset()

    with pytest.raises(DriftError, match="slot 'obs' would judge validated data"):
        compute_changes(new, mode=mode, lock=lock)

    assert _slots(ln) == {"obs": "CsOld"}


@pytest.mark.parametrize("mode", [ApplyMode.PLAN, ApplyMode.APPLY])
@pytest.mark.parametrize(
    ("old", "new"), list(_WIDENING_SWAPS.values()), ids=list(_WIDENING_SWAPS)
)
def test_a_component_admitting_more_is_swapped_without_an_answer(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    old: Any,
    new: Any,
    mode: ApplyMode,
) -> None:
    lock = _locked(old, tmp_path)
    _validate_with_composite(ln)
    new.reset()

    ctx = compute_changes(new, mode=mode, lock=lock)

    assert "slot 'obs' 'CsOld' \u2192 'Schema:CsNew'" in _details(ctx)


def test_the_dry_run_swaps_in_a_component_the_plan_creates(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The wider component does not exist yet: the preview judges the plan's.
    lock = _locked(_ONLY_OLD, tmp_path)
    _validate_with_composite(ln)

    ctx = _preview(_CREATED_WIDER, lock)

    assert "slot 'obs' 'CsOld' \u2192 'Schema:CsNew'" in _details(ctx)
    _apply(_CREATED_WIDER, lock)
    assert _slots(ln) == {"obs": "CsNew"}


def test_the_dry_run_refuses_a_narrower_component_the_plan_creates(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _locked(_ONLY_OLD, tmp_path)

    with pytest.raises(DriftError, match="slot 'obs' would judge validated data"):
        _preview(_CREATED_NARROWER, lock)


def test_the_dry_run_judges_a_component_the_plan_widens_as_edited(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    before, after = _EDITED_WIDER
    lock = _locked(before, tmp_path)
    _validate_with_composite(ln)

    ctx = _preview(after, lock)

    assert "slot 'obs' 'CsOld' \u2192 'Schema:CsNew'" in _details(ctx)
    _apply(after, lock)
    assert _slots(ln) == {"obs": "CsNew"}


def test_the_dry_run_judges_a_component_the_plan_narrows_as_edited(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Making CsNew's 'd' required is allowed for CsNew, which validated nothing,
    # but not for the composite slotting it in, which did.
    before, after = _EDITED_NARROWER
    lock = _locked(before, tmp_path)
    _validate_with_composite(ln)

    with pytest.raises(DriftError, match="slot 'obs' would judge validated data"):
        _preview(after, lock, allow_data_validated_changes=True)


def test_the_dry_run_previews_a_swap_onto_taken_content(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _locked(_TWINS, tmp_path)

    with pytest.raises(DriftError, match="same content as another live schema"):
        _preview(_TWINS_SWAPPED, lock, allow_data_validated_changes=True)


def test_a_legacy_lock_without_slots_still_applies_a_slot_change(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _locked(_OBS_VAR, tmp_path)
    entry = lock.entry_for("Schema:CsC")
    assert entry is not None and entry.remote_snapshot is not None
    entry.remote_snapshot.pop("slots")
    entry.remote_hash = None

    ctx = _apply(_OBS, lock)

    assert "slot 'var' removed" in _details(ctx)
    assert _slots(ln) == {"obs": "CsObs1"}


def test_a_slot_removed_outside_the_plan_is_a_safe_remote_change(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _locked(_OBS_VAR, tmp_path)
    composite = ln.Schema.get(name="CsC")
    type(composite).components.through.objects.filter(
        composite=composite, slot="var"
    ).delete()

    ctx = _apply(_OBS, lock)

    change = next(c for c in ctx.changes if c.key.endswith("Schema:CsC"))
    assert any(
        "safe, non-invalidating way" in w and "slots" in w for w in change.warnings
    ), change.warnings
    assert _slots(ln) == {"obs": "CsObs1"}


def test_a_slot_added_outside_the_plan_to_a_schema_without_slots_is_removed(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Added through lamindb, which rehashes the schema -- that is what shows it.
    lock = _locked(_DATAFRAME, tmp_path)
    schema = ln.Schema.get(name="CsDf")
    schema.slots["var"] = ln.Schema.get(name="CsVar")
    schema.save()

    ctx = _apply(_DATAFRAME, lock)

    change = next(c for c in ctx.changes if c.key.endswith("Schema:CsDf"))
    assert "slot 'var' removed" in change.details
    assert ln.Schema.get(name="CsDf").slots == {}
