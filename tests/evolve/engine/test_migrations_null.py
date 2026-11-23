# Adapted source tests include deliberately invalid declarations and dynamic models.
# mypy: disable-error-code="arg-type,attr-defined,call-overload"
# Source test semantics retained.
# ruff: noqa: TC003
"""Backfilling with null, and with a feature's declared default."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from lamindb_setup.evolve import (
    DEFAULT,
    NULL,
    DataMigration,
    Feature,
    FeatureType,
    LaminDType,
    MigrationError,
    MigrationReport,
    Record,
    RecordType,
    Schema,
    Sheet,
    drop_rows,
    fill_missing,
    map_to_records,
    map_to_str,
    map_values,
)
from lamindb_setup.evolve.authoring._plan import PlanContext, create_plan
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


# === helpers =================================================================


def _persist_lock(ctx: object, plan: object, path: Path) -> LockFile:
    write_lock(ctx, plan, path)  # type: ignore[arg-type]
    lock = LockFile.read(path)
    assert lock is not None
    return lock


def _register_local_storage(ln: LamindbModuleType) -> None:
    """Point the test instance's storage record at its current temporary root."""
    import lamindb_setup

    root = lamindb_setup.settings.storage.root_as_str
    ln.Storage.objects.exclude(root=root).update(root=root)


# === schema-member backfill ==================================================
#
# One schema whose optional members span every storage class a dtype can use:
# the four scalar kinds, which keep their values in a JSON column, and a record
# dtype, which keeps its value as a foreign key and therefore has no null to
# store.  Narrowing them all at once is what proves a single `NULL`
# declaration covers the lot.

_NB_MEMBERS = ("nb_str", "nb_int", "nb_float", "nb_bool", "nb_cat")


def _every_dtype_plan(
    *,
    optional: bool,
    migrations: tuple[DataMigration, ...] = (),
    nullable: bool = True,
    defaults: dict[str, Any] | None = None,
    sheet: bool = True,
    name: str,
):
    defaults = defaults or {}

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        import lamindb as ln

        ft = FeatureType("nb_ft")
        vocab = RecordType("nb_vocab")
        Record("v1", type=vocab)
        Record("unknown", type=vocab)
        anchor = Feature("nb_anchor", dtype=LaminDType(str), type=ft)
        dtypes = {
            "nb_str": LaminDType(str),
            "nb_int": LaminDType(int),
            "nb_float": LaminDType(float),
            "nb_bool": LaminDType(bool),
            "nb_cat": LaminDType(ln.Record, type__uid=vocab.uid),
        }
        members: list[Feature] = [
            Feature(
                member_name,
                dtype=dtype,
                type=ft,
                nullable=nullable,
                default_value=defaults.get(member_name),
            )
            for member_name, dtype in dtypes.items()
        ]
        schema = Schema(
            name="NullBackfillSchema",
            features=[anchor, *members],
            optionals=members if optional else [],
        )
        if sheet:
            Sheet("nb_sheet", schema=schema, type=RecordType("nb_rt"))
        for migration in migrations:
            ctx.migrate(schema, migration)

    _plan.name = name
    return _plan


# One default per member, each of the member's own dtype -- which is the point
# of `DEFAULT`: it is resolved per feature, so a single declaration covers a
# schema whose members share no common value.
_DEFAULTS = {
    "nb_str": "unset",
    "nb_int": -1,
    "nb_float": -1.5,
    "nb_bool": False,
    "nb_cat": "unknown",
}

_all_optional_plan = _every_dtype_plan(optional=True, name="nb_opt")
_all_optional_strict_plan = _every_dtype_plan(
    optional=True, nullable=False, sheet=False, name="nb_opt_strict"
)
_all_null_plan = _every_dtype_plan(
    optional=False, migrations=(fill_missing(NULL),), name="nb_null"
)
_all_null_artifact_plan = _every_dtype_plan(
    optional=False,
    migrations=(fill_missing(NULL),),
    sheet=False,
    name="nb_null_artifact",
)
_all_default_plan = _every_dtype_plan(
    optional=False,
    migrations=(fill_missing(DEFAULT),),
    defaults=_DEFAULTS,
    name="nb_default",
)
_all_implicit_default_plan = _every_dtype_plan(
    optional=False, migrations=(fill_missing(),), name="nb_implicit"
)
_not_nullable_null_plan = _every_dtype_plan(
    optional=False,
    migrations=(fill_missing(NULL, feature="nb_str"),),
    nullable=False,
    sheet=False,
    name="nb_not_nullable",
)
_not_nullable_default_plan = _every_dtype_plan(
    optional=False,
    migrations=(fill_missing(DEFAULT, feature="nb_str"),),
    nullable=False,
    sheet=False,
    name="nb_no_default",
)


def _seed_rows(ln: LamindbModuleType, tmp_path: Path, plan: Any = None) -> LockFile:
    """Apply the all-optional plan and add two rows that omit every member."""
    plan = plan if plan is not None else _all_optional_plan
    plan.reset()
    applied = compute_changes(plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, plan, tmp_path / "nb.lock")
    sheet = ln.Record.get(name="nb_sheet", is_type=True)
    for row_name in ("r1", "r2"):
        ln.Record(name=row_name, type=sheet, features={"nb_anchor": "x"}).save()
    return lock


def _seed_artifacts(ln: LamindbModuleType, tmp_path: Path) -> LockFile:
    """Use optional non-nullable members as dataset columns, not row metadata."""
    import pandas as pd

    plan = _all_optional_strict_plan
    plan.reset()
    applied = compute_changes(plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, plan, tmp_path / "nb.lock")
    schema = ln.Schema.get(name="NullBackfillSchema", is_type=False)
    _register_local_storage(ln)
    for name in ("r1", "r2"):
        ln.Artifact.from_dataframe(
            pd.DataFrame({"nb_anchor": [name]}), key=f"{name}.parquet", schema=schema
        ).save()
    assert ln.Artifact.filter(schema=schema).count() == 2
    return lock


def test_null_backfills_every_dtype(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `NULL` has to answer for a newly-required member of *any* dtype.  lamindb
    # has no stored null, so what it writes is nothing at all -- an un-annotated
    # row -- and what makes that an answer rather than a skipped row is the
    # migration declaring it, which the post-check then honours.
    lock = _seed_rows(ln, tmp_path)
    _all_null_plan.reset()

    ctx = compute_changes(_all_null_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert change.action is Action.UPDATE
    # Two rows x five members.
    assert any("10 nulled" in detail for detail in change.details), change.details
    schema = ln.Schema.get(name="NullBackfillSchema", is_type=False)
    assert schema.optionals.get_uids() == []

    values = ln.Record.get(name="r1").features.get_values()
    # lamindb reports the now-required nullable members as null
    assert values == {"nb_anchor": "x", **dict.fromkeys(_NB_MEMBERS)}


def test_null_backfill_is_idempotent(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # With the instance already narrowed there is nothing left to migrate, so a
    # plan carrying the declaration forever still converges.
    lock = _seed_rows(ln, tmp_path)
    _all_null_plan.reset()
    ctx = compute_changes(_all_null_plan, mode=ApplyMode.APPLY, lock=lock)

    settled = _persist_lock(ctx, _all_null_plan, tmp_path / "nb2.lock")
    _all_null_plan.reset()
    again = compute_changes(_all_null_plan, mode=ApplyMode.APPLY, lock=settled)

    assert {c.action for c in again.changes} == {Action.NO_CHANGE}


def test_default_backfill_honours_the_feature_default(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # One `DEFAULT` declaration covers members of different dtypes, because it
    # is resolved per feature rather than per declaration.
    lock = _seed_rows(ln, tmp_path)
    _all_default_plan.reset()

    ctx = compute_changes(_all_default_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert change.action is Action.UPDATE
    assert any("10 transformed" in detail for detail in change.details), change.details
    values = ln.Record.get(name="r1").features.get_values()
    assert values["nb_str"] == "unset"
    assert values["nb_int"] == -1
    assert values["nb_float"] == -1.5
    assert values["nb_bool"] is False
    # The categorical default names a record, resolved against the member's own
    # dtype and written as a link row rather than as raw JSON.
    assert values["nb_cat"] == "unknown"


def test_default_backfill_falls_back_to_null(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # No `default_value` on any member, but all of them nullable: `DEFAULT`
    # then fills null, the order lamindb's own curator fills a missing column
    # in.  `fill_missing()` without a value means exactly this.
    lock = _seed_rows(ln, tmp_path)
    _all_implicit_default_plan.reset()

    ctx = compute_changes(_all_implicit_default_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert any("10 nulled" in detail for detail in change.details), change.details
    assert ln.Record.get(name="r2").features.get_values() == {
        "nb_anchor": "x",
        **dict.fromkeys(_NB_MEMBERS),
    }


def test_null_backfill_refused_for_a_non_nullable_member(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from django.db import transaction

    lock = _seed_artifacts(ln, tmp_path)
    _not_nullable_null_plan.reset()

    with pytest.raises(MigrationError, match="is not nullable"):
        with transaction.atomic():
            compute_changes(_not_nullable_null_plan, mode=ApplyMode.APPLY, lock=lock)

    # The failed apply rolled the narrowing back with it.
    schema = ln.Schema.get(name="NullBackfillSchema", is_type=False)
    assert len(schema.optionals.get_uids()) == 5


def test_default_backfill_refused_without_a_default_or_nullability(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from django.db import transaction

    lock = _seed_artifacts(ln, tmp_path)
    _not_nullable_default_plan.reset()

    with pytest.raises(MigrationError, match="declares no 'default_value'"):
        with transaction.atomic():
            compute_changes(_not_nullable_default_plan, mode=ApplyMode.APPLY, lock=lock)


# === per-row decisions =======================================================


def _per_row(feature: Any, row: Any) -> Any:
    """Null only `r1`; `r2` gets a real value for every member."""
    if row.name == "r1":
        return NULL
    return "v1" if feature.name == "nb_cat" else 7


_mixed_plan = _every_dtype_plan(
    optional=False,
    migrations=(fill_missing(_per_row, name="mixed"),),
    name="nb_mixed",
)


def test_a_callable_may_null_only_some_rows(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Only the rows the callable answers `NULL` for are exempted from the
    # post-check; the others still have to be filled, categorical included.
    lock = _seed_rows(ln, tmp_path)
    _mixed_plan.reset()

    ctx = compute_changes(_mixed_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert any("5 transformed, 5 nulled" in d for d in change.details), change.details
    assert ln.Record.get(name="r1").features.get_values()["nb_cat"] is None
    assert ln.Record.get(name="r2").features.get_values()["nb_cat"] == "v1"


_partial_plan = _every_dtype_plan(
    optional=False,
    migrations=(
        fill_missing(
            lambda feature, row: NULL if row.name == "r1" else None, name="partial"
        ),
    ),
    name="nb_partial",
)


def test_a_row_left_alone_still_fails_the_apply(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `None` keeps meaning "leave this row alone", which is not an answer: the
    # exemption is only ever granted to a row explicitly filled with null.
    from django.db import transaction

    lock = _seed_rows(ln, tmp_path)
    _partial_plan.reset()

    with pytest.raises(MigrationError, match="left 5 record"):
        with transaction.atomic():
            compute_changes(_partial_plan, mode=ApplyMode.APPLY, lock=lock)


_missing_label_plan = _every_dtype_plan(
    optional=False,
    migrations=(fill_missing("nowhere", feature="nb_cat"),),
    name="nb_missing_label",
)


def test_a_categorical_backfill_value_must_exist(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A categorical member is backfilled by *name*, resolved against the
    # member's own live dtype.  A name no record carries is refused rather than
    # written into the JSON store, which lamindb rejects on read.
    from django.db import transaction

    lock = _seed_rows(ln, tmp_path)
    _missing_label_plan.reset()

    with pytest.raises(MigrationError, match="no record of cat\\[Record\\["):
        with transaction.atomic():
            compute_changes(_missing_label_plan, mode=ApplyMode.APPLY, lock=lock)


def _ulabel_plan(
    *, optional: bool, migrations: tuple[DataMigration, ...] = (), name: str
):
    """The same narrowing for a categorical member with *no* type constraint.

    `cat[ULabel]` names a whole registry rather than one type within it, so the
    backfill's name lookup has no `type__uid` to narrow by -- the other half of
    resolving a value against the member's live dtype.
    """

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        import lamindb as ln

        ft = FeatureType("nbu_ft")
        anchor = Feature("nbu_anchor", dtype=LaminDType(str), type=ft)
        member = Feature(
            "nbu_label", dtype=LaminDType(ln.ULabel), type=ft, default_value="known"
        )
        schema = Schema(
            name="NullBackfillULabelSchema",
            features=[anchor, member],
            optionals=[member] if optional else [],
        )
        Sheet("nbu_sheet", schema=schema, type=RecordType("nbu_rt"))
        for migration in migrations:
            ctx.migrate(schema, migration)

    _plan.name = name
    return _plan


_ulabel_optional_plan = _ulabel_plan(optional=True, name="nbu_opt")
_ulabel_default_plan = _ulabel_plan(
    optional=False, migrations=(fill_missing(DEFAULT),), name="nbu_default"
)


def test_an_untyped_categorical_default_resolves_by_name(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `cat[ULabel]` constrains no type, so the name is looked up across the
    # whole registry rather than within one type.
    _ulabel_optional_plan.reset()
    applied = compute_changes(_ulabel_optional_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _ulabel_optional_plan, tmp_path / "nbu.lock")
    ln.ULabel(name="known").save()
    sheet = ln.Record.get(name="nbu_sheet", is_type=True)
    ln.Record(name="u1", type=sheet, features={"nbu_anchor": "x"}).save()

    _ulabel_default_plan.reset()
    ctx = compute_changes(_ulabel_default_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert change.action is Action.UPDATE
    assert ln.Record.get(name="u1").features.get_values()["nbu_label"] == "known"


def _list_cat_plan(
    *, optional: bool, migrations: tuple[DataMigration, ...] = (), name: str
):
    """The same narrowing for a *multi-valued* categorical member.

    `list[cat[ULabel]]` takes a sequence of names, so a backfill has to resolve
    each element and write one link row per label rather than treating the whole
    list as a single value.
    """

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        import lamindb as ln

        ft = FeatureType("nbl_ft")
        anchor = Feature("nbl_anchor", dtype=LaminDType(str), type=ft)
        member = Feature("nbl_labels", dtype=LaminDType(list[ln.ULabel]), type=ft)
        schema = Schema(
            name="NullBackfillListSchema",
            features=[anchor, member],
            optionals=[member] if optional else [],
        )
        Sheet("nbl_sheet", schema=schema, type=RecordType("nbl_rt"))
        for migration in migrations:
            ctx.migrate(schema, migration)

    _plan.name = name
    return _plan


_list_cat_optional_plan = _list_cat_plan(optional=True, name="nbl_opt")
_list_cat_filled_plan = _list_cat_plan(
    optional=False,
    migrations=(fill_missing(["alpha", "beta"], feature="nbl_labels"),),
    name="nbl_filled",
)
_list_cat_null_plan = _list_cat_plan(
    optional=False,
    migrations=(fill_missing(NULL, feature="nbl_labels"),),
    name="nbl_null",
)


def _seed_list_rows(ln: LamindbModuleType, tmp_path: Path) -> LockFile:
    _list_cat_optional_plan.reset()
    applied = compute_changes(_list_cat_optional_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _list_cat_optional_plan, tmp_path / "nbl.lock")
    ln.ULabel(name="alpha").save()
    ln.ULabel(name="beta").save()
    sheet = ln.Record.get(name="nbl_sheet", is_type=True)
    ln.Record(name="l1", type=sheet, features={"nbl_anchor": "x"}).save()
    return lock


def test_a_multivalued_categorical_backfill_writes_one_link_per_label(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A `list[cat[...]]` member is backfilled with a *sequence* of names; each
    # resolves separately and becomes its own link row, which is how lamindb
    # stores a multi-valued annotation.
    lock = _seed_list_rows(ln, tmp_path)
    _list_cat_filled_plan.reset()

    ctx = compute_changes(_list_cat_filled_plan, mode=ApplyMode.APPLY, lock=lock)

    assert {c.kind: c for c in ctx.changes}["Schema"].action is Action.UPDATE
    stored = ln.Record.get(name="l1").features.get_values()["nbl_labels"]
    assert sorted(stored) == ["alpha", "beta"]


def test_a_multivalued_categorical_member_can_be_nulled(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Null is an absent annotation here as everywhere else, so the list dtype
    # needs no special case of its own.
    lock = _seed_list_rows(ln, tmp_path)
    _list_cat_null_plan.reset()

    ctx = compute_changes(_list_cat_null_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert any("1 nulled" in detail for detail in change.details), change.details
    # stored as no link at all; lamindb reports the required member as empty
    assert ln.Record.get(name="l1").features.get_values()["nbl_labels"] == []


# === PLAN-mode preview =======================================================


def test_a_dry_run_resolves_fills_against_the_plan_not_the_instance(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `DEFAULT` is resolved against the member's `default_value`, which this
    # very plan is setting.  PLAN mode saves nothing, so a preview reading the
    # feature off the instance would resolve against the *old* config and report
    # an outcome the apply does not produce.
    lock = _seed_rows(ln, tmp_path)
    _all_default_plan.reset()

    preview = compute_changes(
        _all_default_plan, mode=ApplyMode.PLAN, lock=lock, preview_migrations=True
    )

    change = {c.kind: c for c in preview.changes}["Schema"]
    assert any(
        "backfills every record" in detail and "10 transformed" in detail
        for detail in change.details
    ), change.details
    # The dry-run rolled back: nothing was written and the defaults are unset.
    assert ln.Record.get(name="r1").features.get_values() == {"nb_anchor": "x"}
    assert ln.Feature.get(name="nb_str", is_type=False).default_value is None

    # The apply then does exactly what the preview said.
    _all_default_plan.reset()
    compute_changes(_all_default_plan, mode=ApplyMode.APPLY, lock=lock)
    assert ln.Record.get(name="r1").features.get_values()["nb_int"] == -1


def test_a_dry_run_honours_a_nullability_widening_the_plan_makes(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `NULL` is refused for a non-nullable member.  When the plan widens the
    # member to nullable in the same apply, the preview has to judge the fill
    # against the widened value or it reports a failure that cannot happen.
    lock = _seed_artifacts(ln, tmp_path)
    _all_null_artifact_plan.reset()

    preview = compute_changes(
        _all_null_artifact_plan, mode=ApplyMode.PLAN, lock=lock, preview_migrations=True
    )

    change = {c.kind: c for c in preview.changes}["Schema"]
    assert not any("would fail" in detail for detail in change.details), change.details
    assert any("10 nulled" in detail for detail in change.details), change.details
    # The report counts absent database annotations, not cells in dataset files.
    assert ln.Feature.get(name="nb_str", is_type=False).nullable is False
    assert len(ln.Schema.get(name="NullBackfillSchema").optionals.get_uids()) == 5
    for name in ("r1", "r2"):
        artifact = ln.Artifact.get(key=f"{name}.parquet")
        assert artifact.load().to_dict("list") == {"nb_anchor": [name]}


# === dtype changes ===========================================================


@create_plan
def _dtype_str_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("nbd_ft")
    Feature("nbd_value", dtype=LaminDType(str), type=ft, nullable=True)


@create_plan
def _dtype_str_strict_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("nbd_ft")
    Feature("nbd_value", dtype=LaminDType(str), type=ft, nullable=False)


def _dtype_plan(migration: DataMigration, name: str):
    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        import lamindb as ln

        ft = FeatureType("nbd_ft")
        rt = RecordType("nbd_vocab")
        Record("aspirin", type=rt)
        feature = Feature(
            "nbd_value",
            dtype=LaminDType(ln.Record, type__uid=rt.uid),
            type=ft,
            nullable=True,
        )
        ctx.migrate(feature, migration)

    _plan.name = name
    return _plan


_to_records_null_plan = _dtype_plan(
    map_to_records(mapping={"n/a": NULL}), "nbd_to_records"
)


@create_plan
def _to_int_null_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("nbd_ft")
    feature = Feature("nbd_value", dtype=LaminDType(int), type=ft, nullable=True)
    ctx.migrate(feature, map_values(transform=lambda value: NULL))


@create_plan
def _to_int_not_nullable_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("nbd_ft")
    feature = Feature("nbd_value", dtype=LaminDType(int), type=ft, nullable=False)
    ctx.migrate(feature, map_values(transform=lambda value: NULL))


@create_plan
def _to_int_default_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("nbd_ft")
    feature = Feature(
        "nbd_value", dtype=LaminDType(int), type=ft, nullable=True, default_value=-1
    )
    ctx.migrate(feature, map_values(transform=lambda value: DEFAULT))


def _seed_values(
    ln: LamindbModuleType, tmp_path: Path, plan: Any = None, **values: str
) -> LockFile:
    plan = plan if plan is not None else _dtype_str_plan
    plan.reset()
    applied = compute_changes(plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, plan, tmp_path / "nbd.lock")
    sheet = ln.Record(name="NbdSheet", is_type=True).save()
    for row_name, value in values.items():
        ln.Record(name=row_name, type=sheet, features={"nbd_value": value}).save()
    return lock


def test_a_mapping_may_send_a_value_to_null(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `str -> cat`: null for a categorical target is an un-annotated row, and
    # the stale scalar row still has to go, or the engine's stranded check fires.
    lock = _seed_values(ln, tmp_path, s1="aspirin", s2="n/a")
    _to_records_null_plan.reset()

    ctx = compute_changes(_to_records_null_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any("1 matched, 1 nulled" in d for d in change.details), change.details
    feat = ln.Feature.get(name="nbd_value", is_type=False)
    assert feat.links_recordjson.count() == 0
    assert ln.Record.get(name="s1").features.get_values()["nbd_value"] == "aspirin"
    assert "nbd_value" not in ln.Record.get(name="s2").features.get_values()


def test_a_transform_may_rewrite_scalar_values_to_null(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `str -> int`: neither value coerces, and null is what replaces them.
    # Nothing is stored for a null, so the stale scalar rows simply go -- which
    # is also what keeps the engine's stranded check satisfied.
    lock = _seed_values(ln, tmp_path, s1="n/a", s2="unknown")
    _to_int_null_plan.reset()

    ctx = compute_changes(_to_int_null_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any("2 nulled" in d for d in change.details), change.details
    feat = ln.Feature.get(name="nbd_value", is_type=False)
    assert feat.dtype_as_str == "int"
    assert feat.links_recordjson.count() == 0
    assert "nbd_value" not in ln.Record.get(name="s1").features.get_values()


def test_a_transform_may_rewrite_scalar_values_to_the_default(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `DEFAULT` resolves to a concrete value, which is then checked against the
    # target dtype like any other replacement.
    lock = _seed_values(ln, tmp_path, s1="n/a")
    _to_int_default_plan.reset()

    ctx = compute_changes(_to_int_default_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any("1 transformed" in d for d in change.details), change.details
    assert ln.Record.get(name="s1").features.get_values()["nbd_value"] == -1


def test_null_refused_for_a_non_nullable_dtype_change(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from django.db import transaction

    lock = _seed_values(ln, tmp_path, _dtype_str_strict_plan, s1="n/a")
    _to_int_not_nullable_plan.reset()

    with pytest.raises(MigrationError, match="is not nullable"):
        with transaction.atomic():
            compute_changes(_to_int_not_nullable_plan, mode=ApplyMode.APPLY, lock=lock)

    assert ln.Feature.get(name="nbd_value", is_type=False).dtype_as_str == "str"


@create_plan
def _back_to_str_null_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("nbd_ft")
    feature = Feature("nbd_value", dtype=LaminDType(str), type=ft, nullable=True)
    ctx.migrate(feature, map_to_str(mapping={"aspirin": NULL}))
    # The vocabulary is no longer declared: its values go with it, deliberately.
    ctx.migrate("RecordType:nbd_vocab", drop_rows())


def test_a_categorical_value_may_become_null(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `cat -> str`: the link row goes and nothing replaces it, so no
    # categorical row is left stranded on the now-scalar feature.
    lock = _seed_values(ln, tmp_path, s1="aspirin")
    _to_records_null_plan.reset()
    applied = compute_changes(_to_records_null_plan, mode=ApplyMode.APPLY, lock=lock)
    lock = _persist_lock(applied, _to_records_null_plan, tmp_path / "nbd2.lock")

    _back_to_str_null_plan.reset()
    ctx = compute_changes(_back_to_str_null_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any("1 nulled" in d for d in change.details), change.details
    assert ln.Feature.get(name="nbd_value", is_type=False).dtype_as_str == "str"
    assert "nbd_value" not in ln.Record.get(name="s1").features.get_values()


# === shared scalar store =====================================================


def test_a_null_drains_the_shared_scalar_store(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # An artifact's scalar values live in the shared `JsonValue`, not in a
    # per-row table; nulling them out has to drain that store too, or the
    # engine's stranded check fires.  The links back to the artifact are
    # `PROTECT`ed, so the row can only go once they are cleared.
    _register_local_storage(ln)
    lock = _seed_values(ln, tmp_path, s1="n/a")
    payload = tmp_path / "payload.txt"
    payload.write_text("payload")
    artifact = ln.Artifact(payload, key="nbd/payload.txt").save()
    artifact.features.add_values({"nbd_value": "unknown"})
    _to_int_null_plan.reset()

    compute_changes(_to_int_null_plan, mode=ApplyMode.APPLY, lock=lock)

    feat = ln.Feature.get(name="nbd_value", is_type=False)
    assert feat.values.count() == 0
    assert "nbd_value" not in artifact.features.get_values()


# === unit ====================================================================


def test_report_summary_counts_nulls() -> None:
    report = MigrationReport()
    report.matched = 2
    report.nulled = 1
    assert report.summary() == "2 matched, 1 nulled"


def test_sentinels_render_as_their_names() -> None:
    assert repr(NULL) == "NULL"
    assert repr(DEFAULT) == "DEFAULT"
