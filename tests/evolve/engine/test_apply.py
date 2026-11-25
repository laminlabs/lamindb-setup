# Adapted source tests include deliberately invalid declarations and dynamic models.
# mypy: disable-error-code="arg-type,assignment,call-overload,list-item,valid-type"
# Source test semantics retained.
# ruff: noqa: C408, TC002, TC003
"""Tests for lamindb_setup.evolve.engine._apply."""

from __future__ import annotations

import contextlib
import re
from collections.abc import Iterator, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest
from lamindb_setup.evolve import (
    DataMigration,
    Feature,
    FeatureType,
    LaminDType,
    MigrationContext,
    MigrationError,
    Record,
    RecordType,
    Schema,
    Sheet,
    drop_member_values,
    drop_values,
    fill_missing,
    keep_member_values,
    keep_values,
    reexport_sheets,
)
from lamindb_setup.evolve.authoring._declarative import (
    SchemaConfig,
    SchemaModel,
    feature,
)
from lamindb_setup.evolve.authoring._dtypes import CatDType, GeneCat
from lamindb_setup.evolve.authoring._plan import (
    Plan,
    PlanContext,
    PlanExports,
    create_plan,
)
from lamindb_setup.evolve.core import (
    MissingPrerequisite,
    PrerequisitePlan,
    SpaceUID,
    WarningCategory,
)
from lamindb_setup.evolve.engine._apply import (
    Action,
    ApplyContext,
    ApplyMode,
    DriftError,
    ExistenceCache,
    MissingPrerequisiteError,
    PlanChangeRefusedError,
    SpaceNotFoundError,
    compute_changes,
    forget_missing,
    plan_natural_keys,
    prefetch_plans,
    run_sheet_export_migrations,
    write_lock,
)
from lamindb_setup.evolve.engine._loader import plan_target
from lamindb_setup.evolve.engine._lock import LockFile, natural_key
from lamindb_setup.evolve.render import render_changes, render_changes_markdown

if TYPE_CHECKING:
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as BiontyModuleType
    from typing import Any as LamindbModuleType


@create_plan
def _apply_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("apply_ft")
    f = Feature("apply_col", dtype=LaminDType(str), type=ft)
    Schema(name="ApplySchema", features=[f])


@create_plan
def _records_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("sheet_ft")
    f = Feature("sheet_col", dtype=LaminDType(str), type=ft)
    schema = Schema(name="SheetSchema", features=[f])
    rt = RecordType("apply_rt")
    Sheet("apply_sheet", schema=schema, type=rt)
    Record("apply_record", type=rt)


@create_plan
def _records_plan_without_value(ctx: PlanContext, exports: None) -> None:
    """`_records_plan` with its one vocabulary value deleted."""
    ft = FeatureType("sheet_ft")
    f = Feature("sheet_col", dtype=LaminDType(str), type=ft)
    schema = Schema(name="SheetSchema", features=[f])
    rt = RecordType("apply_rt")
    Sheet("apply_sheet", schema=schema, type=rt)


@create_plan
def _unnamed_record_plan(ctx: PlanContext, exports: None) -> None:
    Record(type=RecordType("unnamed_rt"))


@create_plan
def _space_plan(ctx: PlanContext, exports: None) -> None:
    ctx.default_space_uid = SpaceUID("spaceUID0001")
    ft = FeatureType("space_ft")
    Feature(
        "space_col", dtype=LaminDType(str), type=ft, space_uid=SpaceUID("spaceUID0002")
    )
    Schema(name="SpaceSchema", features=[Feature("plain", dtype=LaminDType(int))])


@create_plan
def _single_space_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("ss_ft")
    f = Feature("ss_col", dtype=LaminDType(str), type=ft)
    schema = Schema(name="SsSchema", features=[f])
    rt = RecordType("ss_rt", single_space_uid=SpaceUID("spaceUID0002"))
    Sheet(
        "ss_sheet",
        schema=schema,
        type=rt,
        single_space_uid=SpaceUID("spaceUID0002"),
        is_experiment_sheet=True,
    )


@create_plan
def _reconcile_ss_v1_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("rec_ss_ft")
    f = Feature("rec_ss_col", dtype=LaminDType(str), type=ft)
    schema = Schema(name="RecSsSchema", features=[f])
    rt = RecordType("rec_ss_rt")
    Sheet("rec_ss_sheet", schema=schema, type=rt)


@create_plan
def _reconcile_ss_v2_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("rec_ss_ft")
    f = Feature("rec_ss_col", dtype=LaminDType(str), type=ft)
    schema = Schema(name="RecSsSchema", features=[f])
    rt = RecordType("rec_ss_rt", single_space_uid=SpaceUID("spaceUID0002"))
    Sheet(
        "rec_ss_sheet",
        schema=schema,
        type=rt,
        single_space_uid=SpaceUID("spaceUID0002"),
        is_experiment_sheet=True,
    )


@create_plan
def _exclude_children_plan(ctx: PlanContext, exports: None) -> None:
    schema = Schema(
        name="ExclSchema", features=[Feature("excl_col", dtype=LaminDType(str))]
    )
    rt = RecordType("excl_parent", exclude_children_from_plans=True)
    Sheet("excl_sheet", schema=schema, type=rt, exclude_children_from_plans=True)


@create_plan
def _exclude_children_off_plan(ctx: PlanContext, exports: None) -> None:
    schema = Schema(
        name="ExclSchema", features=[Feature("excl_col", dtype=LaminDType(str))]
    )
    rt = RecordType("excl_parent")
    Sheet("excl_sheet", schema=schema, type=rt)


@create_plan
def _missing_space_plan(ctx: PlanContext, exports: None) -> None:
    ctx.default_space_uid = SpaceUID("spaceUID9999")
    FeatureType("missing_space_ft")


@create_plan
def _descriptive_v1_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("drift_ft")
    Feature("drift_col", dtype=LaminDType(str), type=ft, description="first")


@create_plan
def _descriptive_v2_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("drift_ft")
    Feature(
        "drift_col",
        dtype=LaminDType(str),
        type=ft,
        description="second",
        unit="mg",
        synonyms=["alias"],
    )


@create_plan
def _descriptive_create_plan(ctx: PlanContext, exports: None) -> None:
    """Descriptive metadata declared on a feature that does not exist yet."""
    ft = FeatureType("create_ft")
    Feature(
        "organism",
        dtype=LaminDType(str),
        type=ft,
        description="Species of the sample",
        unit="mg",
        synonyms=["species", "organism_name"],
        nullable=False,
    )


@create_plan
def _descriptive_cat_create_plan(ctx: PlanContext, exports: None) -> None:
    """A categorical feature declaring `synonyms`, as a declarative override."""
    import bionty as bt

    class _OrganismSchema(SchemaModel):
        schema_config = SchemaConfig(
            name="OrganismSchema",
            default_feature_type=FeatureType("create_ft"),
        )
        organism: Feature[bt.models.Organism] = feature(
            nullable=False, synonyms=["species"]
        )


@create_plan
def _nullable_true_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("null_ft")
    Feature("null_col", dtype=LaminDType(str), type=ft, nullable=True)


@create_plan
def _nullable_false_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("null_ft")
    Feature("null_col", dtype=LaminDType(str), type=ft, nullable=False)


@create_plan
def _nullable_unset_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("null_ft")
    Feature("null_col", dtype=LaminDType(str), type=ft)


@create_plan
def _nullable_declarative_plan(ctx: PlanContext, exports: None) -> None:
    """What a plain `Feature[str]` annotation emits: `nullable=False`."""
    ft = FeatureType("null_ft")

    class _M(SchemaModel):
        schema_config = SchemaConfig(name="NullSheet", default_feature_type=ft)
        null_col: Feature[str]


@create_plan
def _dtype_int_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("dtype_ft")
    Feature("dtype_col", dtype=LaminDType(int), type=ft)


@create_plan
def _dtype_str_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("dtype_ft")
    Feature("dtype_col", dtype=LaminDType(str), type=ft)


def _dtype_refusal(before: str, after: str) -> str:
    """Match the refusal of the plan changing `dtype_col`'s dtype in place."""
    return "^" + re.escape(
        "cannot change Feature 'dtype_col' (FeatureType:dtype_ft/Feature:dtype_col) "
        f"in place:\n  dtype {before!r} \u2192 {after!r}"
    )


def _coerce_non_ints(context: MigrationContext) -> None:
    """Replace any stored scalar value that does not parse as an int with `"0"`."""
    from lamindb.models import JsonValue

    for row in list(context.feature.values.all()):
        try:
            int(row.value)
        except (TypeError, ValueError):
            row.delete(permanent=True)
            JsonValue(feature=context.feature, value="0").save()


@create_plan
def _dtype_int_migrate_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("dtype_ft")
    feature = Feature("dtype_col", dtype=LaminDType(int), type=ft)
    ctx.migrate(
        feature,
        DataMigration(
            name="coerce_non_ints",
            reason="map non-integer values to 0",
            migrate=_coerce_non_ints,
        ),
    )


def _relocate_treatment_to_compound(context: MigrationContext) -> None:
    """Relocate scalar treatment strings to Compound records (RecordJson -> RecordRecord)."""
    import lamindb as ln

    compound_type = ln.Record.get(name="Compound", is_type=True)
    rows = list(context.feature.links_recordjson.all())
    label_map: dict[str, object] = {}
    for value in {row.value for row in rows}:
        label_map[value] = (
            ln.Record.filter(name=value, type=compound_type).one_or_none()
            or ln.Record(name=value, type=compound_type).save()
        )
    for row in rows:
        row.record.features.update_values({context.feature.name: label_map[row.value]})
        row.delete(permanent=True)


@create_plan
def _treatment_str_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("trial_ft")
    Feature("treatment", dtype=LaminDType(str), type=ft)


@create_plan
def _treatment_record_migrate_plan(ctx: PlanContext, exports: None) -> None:
    import lamindb as ln

    ft = FeatureType("trial_ft")
    rt = RecordType("Compound")
    feature = Feature(
        "treatment", dtype=LaminDType(ln.Record, type__uid=rt.uid), type=ft
    )
    ctx.migrate(
        feature,
        DataMigration(
            name="treatment_to_compound",
            reason="relocate treatment strings to Compound records",
            migrate=_relocate_treatment_to_compound,
        ),
    )


@create_plan
def _dtype_float_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("dtype_ft")
    Feature("dtype_col", dtype=LaminDType(float), type=ft)


@create_plan
def _dtype_list_int_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("dtype_ft")
    Feature("dtype_col", dtype=LaminDType(list[int]), type=ft)


@create_plan
def _dtype_cat_plan(ctx: PlanContext, exports: None) -> None:
    import lamindb as ln

    ft = FeatureType("dtype_ft")
    Feature("dtype_col", dtype=LaminDType(ln.ULabel), type=ft)


@create_plan
def _dtype_cat_project_plan(ctx: PlanContext, exports: None) -> None:
    import lamindb as ln

    ft = FeatureType("dtype_ft")
    Feature("dtype_col", dtype=LaminDType(ln.Project), type=ft)


@create_plan
def _dtype_cat_record_plan(ctx: PlanContext, exports: None) -> None:
    import lamindb as ln

    ft = FeatureType("dtype_ft")
    rt = RecordType("DtypeCompound")
    Feature("dtype_col", dtype=LaminDType(ln.Record, type__uid=rt.uid), type=ft)


@create_plan
def _dtype_cat_celltype_plan(ctx: PlanContext, exports: None) -> None:
    import bionty as bt

    ft = FeatureType("dtype_ft")
    Feature("dtype_col", dtype=LaminDType(bt.CellType), type=ft)


def _relocate_dtype_str_to_project(context: MigrationContext) -> None:
    """Relocate scalar strings to Project records (RecordJson -> RecordProject)."""
    import lamindb as ln

    rows = list(context.feature.links_recordjson.all())
    label_map: dict[str, object] = {}
    for value in {row.value for row in rows}:
        label_map[value] = (
            ln.Project.filter(name=value).one_or_none() or ln.Project(name=value).save()
        )
    for row in rows:
        row.record.features.update_values({context.feature.name: label_map[row.value]})
        row.delete(permanent=True)


@create_plan
def _dtype_project_migrate_plan(ctx: PlanContext, exports: None) -> None:
    import lamindb as ln

    ft = FeatureType("dtype_ft")
    feature = Feature("dtype_col", dtype=LaminDType(ln.Project), type=ft)
    ctx.migrate(
        feature,
        DataMigration(
            name="dtype_to_project",
            reason="relocate strings to Project records",
            migrate=_relocate_dtype_str_to_project,
        ),
    )


def _relocate_dtype_str_to_celltype(context: MigrationContext) -> None:
    """Relocate scalar strings to CellType records (RecordJson -> bionty link)."""
    import bionty as bt

    rows = list(context.feature.links_recordjson.all())
    label_map: dict[str, object] = {}
    for value in {row.value for row in rows}:
        label_map[value] = (
            bt.CellType.filter(name=value).one_or_none()
            or bt.CellType(name=value).save()
        )
    for row in rows:
        row.record.features.update_values({context.feature.name: label_map[row.value]})
        row.delete(permanent=True)


@create_plan
def _dtype_celltype_migrate_plan(ctx: PlanContext, exports: None) -> None:
    import bionty as bt

    ft = FeatureType("dtype_ft")
    feature = Feature("dtype_col", dtype=LaminDType(bt.CellType), type=ft)
    ctx.migrate(
        feature,
        DataMigration(
            name="dtype_to_celltype",
            reason="relocate strings to CellType records",
            migrate=_relocate_dtype_str_to_celltype,
        ),
    )


def _relocate_ulabel_to_str(context: MigrationContext) -> None:
    """Relocate categorical ULabel links to the scalar store (RecordULabel -> RecordJson)."""
    rows = list(context.feature.links_recordulabel.all())
    for row in rows:
        row.record.features.update_values({context.feature.name: row.value.name})
        row.delete(permanent=True)


@create_plan
def _dtype_str_from_cat_migrate_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("dtype_ft")
    feature = Feature("dtype_col", dtype=LaminDType(str), type=ft)
    ctx.migrate(
        feature,
        DataMigration(
            name="ulabel_to_str",
            reason="relocate ULabel links to scalar strings",
            migrate=_relocate_ulabel_to_str,
        ),
    )


def _noop_migration(context: MigrationContext) -> None:
    """A deliberately broken migration: relocates nothing, leaving values stranded."""


def _raising_migration(context: MigrationContext) -> None:
    """A migration that fails, standing in for any unrunnable relocation."""
    raise RuntimeError("target not available")


@create_plan
def _dtype_project_raises_migrate_plan(ctx: PlanContext, exports: None) -> None:
    import lamindb as ln

    ft = FeatureType("dtype_ft")
    feature = Feature("dtype_col", dtype=LaminDType(ln.Project), type=ft)
    ctx.migrate(
        feature,
        DataMigration(
            name="dtype_to_project_raises",
            reason="broken: raises",
            migrate=_raising_migration,
        ),
    )


@create_plan
def _dtype_project_noop_migrate_plan(ctx: PlanContext, exports: None) -> None:
    import lamindb as ln

    ft = FeatureType("dtype_ft")
    feature = Feature("dtype_col", dtype=LaminDType(ln.Project), type=ft)
    ctx.migrate(
        feature,
        DataMigration(
            name="dtype_to_project_noop",
            reason="broken: does not relocate any value",
            migrate=_noop_migration,
        ),
    )


@create_plan
def _pooled_screen_str_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("PooledScreen_feature_type")
    Feature("signal_link", dtype=LaminDType(str), type=ft)


@create_plan
def _pooled_screen_url_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("PooledScreen_feature_type")
    Feature("signal_link", dtype=LaminDType("url"), type=ft)


@create_plan
def _gene_cat_plan(ctx: PlanContext, exports: None) -> None:
    # A declarative schema with a single, nullable categorical feature pointing
    # at `bionty.Gene` and matched on its `symbol` field. The `| None`
    # inside `Feature[...]` marks the feature nullable. `bt` is resolved from
    # this frame's locals when the metaclass evaluates the annotation.
    import bionty as bt

    class _GeneticPerturbationSchema(SchemaModel):
        schema_config = SchemaConfig(
            name="GeneticPerturbationSchema",
            default_feature_type=FeatureType("BioSampleFeatureType"),
        )
        genetic_perturbation: Feature[
            CatDType[bt.models.Gene, GeneCat(name_field="symbol")] | None
        ]


@create_plan
def _gene_cat_none_plan(ctx: PlanContext, exports: None) -> None:
    # Same feature, but with `name_field=None` -- get_dtype fills the default
    # name field (Gene -> symbol), so it resolves to cat[bionty.Gene.symbol] too.
    import bionty as bt

    class _GeneticPerturbationSchema(SchemaModel):
        schema_config = SchemaConfig(
            name="GeneticPerturbationSchema",
            default_feature_type=FeatureType("BioSampleFeatureType"),
        )
        genetic_perturbation: Feature[
            CatDType[bt.models.Gene, GeneCat(name_field=None)] | None
        ]


@create_plan
def _record_cat_plan(ctx: PlanContext, exports: None) -> None:
    # A Record categorical constrained by a RecordType uid. The plan carries the
    # type__uid in cat_filters and its dtype_str is the bare `cat[Record]`,
    # while lamindb inlines the uid into the stored string (`cat[Record[<uid>]]`).
    import lamindb as ln

    rt = RecordType("BioSampleType")
    ft = FeatureType("BioSampleFeatureType")
    Feature(
        "biosample_type",
        dtype=LaminDType(ln.Record, type__uid=rt.uid),
        type=ft,
        nullable=False,
    )


@create_plan
def _list_record_cat_plan(ctx: PlanContext, exports: None) -> None:
    # A *list* of a Record categorical constrained by a RecordType uid. lamindb
    # rejects `cat_filters=` on a nested dtype, so staging inlines the uid into
    # the dtype string (`list[cat[Record[<uid>]]]`).
    import lamindb as ln

    rt = RecordType("ListBioSampleType")
    ft = FeatureType("ListBioSampleFeatureType")
    Feature(
        "biosample_types",
        dtype=LaminDType(list[ln.Record], type__uid=rt.uid),
        type=ft,
        nullable=False,
    )


@create_plan
def _list_int_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("list_ft")
    Feature("list_col", dtype=LaminDType(list[int]), type=ft)


@create_plan
def _list_float_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("list_ft")
    Feature("list_col", dtype=LaminDType(list[float]), type=ft)


@create_plan
def _coerce_off_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("coerce_ft")
    Feature("coerce_col", dtype=LaminDType(str), type=ft, coerce=False)


@create_plan
def _coerce_on_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("coerce_ft")
    Feature("coerce_col", dtype=LaminDType(str), type=ft, coerce=True)


@create_plan
def _optional_schema_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("optional_ft")
    required = Feature("opt_required", dtype=LaminDType(str), type=ft)
    optional = Feature("opt_optional", dtype=LaminDType(str), type=ft)
    Schema(
        name="OptionalSchema",
        features=[required, optional],
        optionals=[optional],
    )


@create_plan
def _optional_none_plan(ctx: PlanContext, exports: None) -> None:
    """`_optional_schema_plan` with every member required."""
    ft = FeatureType("optional_ft")
    required = Feature("opt_required", dtype=LaminDType(str), type=ft)
    optional = Feature("opt_optional", dtype=LaminDType(str), type=ft)
    Schema(name="OptionalSchema", features=[required, optional])


@create_plan
def _optional_both_plan(ctx: PlanContext, exports: None) -> None:
    """`_optional_schema_plan` with every member optional."""
    ft = FeatureType("optional_ft")
    required = Feature("opt_required", dtype=LaminDType(str), type=ft)
    optional = Feature("opt_optional", dtype=LaminDType(str), type=ft)
    Schema(
        name="OptionalSchema",
        features=[required, optional],
        optionals=[required, optional],
    )


def _optional_sheet_plan(
    *, optional: bool, migration: DataMigration | None = None, name: str = "optsheet"
):
    """A sheet-backed schema whose `optsheet_optional` member may be optional.

    The sheet is what makes the schema validate *rows*: a `Record`'s `schema`
    foreign key only carries meaning on a type, so its rows are the records of
    that type -- which is exactly what an optional-to-required narrowing has to
    be judged against.
    """

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        ft = FeatureType("optsheet_ft")
        required = Feature("optsheet_required", dtype=LaminDType(str), type=ft)
        member = Feature("optsheet_optional", dtype=LaminDType(str), type=ft)
        schema = Schema(
            name="OptSheetSchema",
            features=[required, member],
            optionals=[member] if optional else [],
        )
        rt = RecordType("optsheet_rt")
        Sheet("optsheet_sheet", schema=schema, type=rt)
        if migration is not None:
            ctx.migrate(schema, migration)

    _plan.name = name
    return _plan


_optional_sheet_optional_plan = _optional_sheet_plan(optional=True, name="opt_sheet")
_optional_sheet_required_plan = _optional_sheet_plan(optional=False, name="req_sheet")
_optional_sheet_migrated_plan = _optional_sheet_plan(
    optional=False,
    migration=fill_missing("backfilled", name="backfill_optsheet"),
    name="mig_sheet",
)
_optional_sheet_noop_plan = _optional_sheet_plan(
    optional=False,
    migration=DataMigration(name="noop", migrate=lambda context: None),
    name="noop_sheet",
)
_optional_sheet_removal_migration_plan = _optional_sheet_plan(
    optional=False,
    migration=keep_member_values(feature="optsheet_optional"),
    name="wrong_narrowing_sheet",
)


@create_plan
def _declarative_optional_plan(ctx: PlanContext, exports: None) -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="DeclOptionalSchema")
        decl_required: Feature[str]
        decl_optional: Feature[str] | None


@create_plan
def _declarative_required_plan(ctx: PlanContext, exports: None) -> None:
    """`_declarative_optional_plan` with the `| None` dropped.

    The authoring form this narrowing actually reaches users through.
    """

    class _S(SchemaModel):
        schema_config = SchemaConfig(name="DeclOptionalSchema")
        decl_required: Feature[str]
        decl_optional: Feature[str]


_backfilled_members: list[str] = []


def _per_member_value(feature: Any, row: Any) -> Any:
    """Fill each newly-required member with a value of its own dtype."""
    _backfilled_members.append(feature.name)
    return "n/a" if feature.name == "opttwo_a" else 0


def _optional_two_plan(
    *, optional: bool, migrations: Sequence[DataMigration] = (), name: str
):
    """A schema narrowing *two* members of different dtypes at once.

    No single scalar can serve both, so this is what distinguishes the two ways
    of covering them: one migration branching on the feature, or one declaration
    per member.
    """

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        ft = FeatureType("opttwo_ft")
        a = Feature("opttwo_a", dtype=LaminDType(str), type=ft)
        b = Feature("opttwo_b", dtype=LaminDType(int), type=ft)
        schema = Schema(
            name="OptTwoSchema",
            features=[a, b],
            optionals=[a, b] if optional else [],
        )
        Sheet("opttwo_sheet", schema=schema, type=RecordType("opttwo_rt"))
        for migration in migrations:
            ctx.migrate(schema, migration)

    _plan.name = name
    return _plan


_optional_two_optional_plan = _optional_two_plan(optional=True, name="two_opt")
_optional_two_migrated_plan = _optional_two_plan(
    optional=False,
    migrations=[fill_missing(_per_member_value, name="backfill_opttwo")],
    name="two_mig",
)
_optional_two_per_member_plan = _optional_two_plan(
    optional=False,
    migrations=[
        fill_missing("bla", feature="opttwo_a"),
        fill_missing(-1, feature="opttwo_b"),
    ],
    name="two_each",
)


def _optional_cat_sheet_plan(*, optional: bool, name: str):
    """The same narrowing for a *categorical* member.

    Its values live in a per-registry link table rather than the scalar one, and
    `RecordRecord` is reachable from `Record` twice -- once as the annotated
    row, once as the annotating value -- so this is what proves the omission
    check tells the two apart.
    """

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        import lamindb as ln

        ft = FeatureType("optcat_ft")
        vocab = RecordType("optcat_vocab")
        Record("v1", type=vocab)
        required = Feature("optcat_required", dtype=LaminDType(str), type=ft)
        member = Feature(
            "optcat_optional",
            dtype=LaminDType(ln.Record, type__uid=vocab.uid),
            type=ft,
        )
        schema = Schema(
            name="OptCatSchema",
            features=[required, member],
            optionals=[member] if optional else [],
        )
        Sheet("optcat_sheet", schema=schema, type=RecordType("optcat_rt"))

    _plan.name = name
    return _plan


_optional_cat_optional_plan = _optional_cat_sheet_plan(optional=True, name="cat_opt")
_optional_cat_required_plan = _optional_cat_sheet_plan(optional=False, name="cat_req")


def _seed_optsheet_rows(ln: LamindbModuleType, *rows: tuple[str, bool]) -> None:
    """Create `(name, has_value)` rows under the applied sheet."""
    sheet = ln.Record.get(name="optsheet_sheet", is_type=True)
    for row_name, has_value in rows:
        features = {"optsheet_required": "x"}
        if has_value:
            features["optsheet_optional"] = "y"
        ln.Record(name=row_name, type=sheet, features=features).save()


def _member_drop_plan(
    *,
    member: bool = True,
    declared: bool = True,
    maximal: bool = False,
    migrations: Sequence[DataMigration] = (),
    name: str,
):
    """A sheet-backed schema that may stop declaring `mdrop_doomed`.

    `member` keeps it in the schema's member list, `declared` keeps it in the
    plan at all -- dropping both is what also deletes the feature itself, which
    is the path where the values would otherwise be stranded on a trashed record.
    """

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        ft = FeatureType("mdrop_ft")
        keep = Feature("mdrop_keep", dtype=LaminDType(str), type=ft)
        doomed = (
            Feature("mdrop_doomed", dtype=LaminDType(str), type=ft)
            if member or declared
            else None
        )
        schema = Schema(
            name="MDropSchema",
            features=[keep, doomed] if member and doomed is not None else [keep],
            optionals=[doomed] if member and doomed is not None else [],
            maximal_set=maximal,
        )
        Sheet("mdrop_sheet", schema=schema, type=RecordType("mdrop_rt"))
        for migration in migrations:
            ctx.migrate(schema, migration)

    _plan.name = name
    return _plan


def _seed_mdrop_rows(ln: LamindbModuleType, *rows: tuple[str, bool]) -> None:
    """Create `(name, carries_the_doomed_member)` rows under the sheet."""
    sheet = ln.Record.get(name="mdrop_sheet", is_type=True)
    for row_name, carries in rows:
        features = {"mdrop_keep": "k"}
        if carries:
            features["mdrop_doomed"] = "d"
        ln.Record(name=row_name, type=sheet, features=features).save()


_member_drop_v1 = _member_drop_plan(name="mdrop_v1")
_member_drop_bare = _member_drop_plan(member=False, name="mdrop_bare")
_member_drop_dropped = _member_drop_plan(
    member=False,
    migrations=[drop_member_values(feature="mdrop_doomed")],
    name="mdrop_dropped",
)
_member_drop_kept = _member_drop_plan(
    member=False,
    migrations=[keep_member_values(feature="mdrop_doomed")],
    name="mdrop_kept",
)
_member_drop_deleted = _member_drop_plan(
    member=False,
    declared=False,
    migrations=[drop_member_values(feature="mdrop_doomed")],
    name="mdrop_deleted",
)
_member_drop_noop = _member_drop_plan(
    member=False,
    migrations=[DataMigration(name="noop", migrate=lambda context: None)],
    name="mdrop_noop",
)
_member_drop_backfill = _member_drop_plan(
    member=False,
    migrations=[fill_missing("backfilled", feature="mdrop_doomed")],
    name="mdrop_backfill",
)
_member_drop_max_v1 = _member_drop_plan(maximal=True, name="mdrop_max_v1")
_member_drop_max_kept = _member_drop_plan(
    member=False,
    maximal=True,
    migrations=[keep_member_values(feature="mdrop_doomed")],
    name="mdrop_max_kept",
)
_member_drop_max_dropped = _member_drop_plan(
    member=False,
    maximal=True,
    migrations=[drop_member_values(feature="mdrop_doomed"), reexport_sheets()],
    name="mdrop_max_dropped",
)


def _artifact_member_drop_plan(
    *,
    member: bool = True,
    migrations: Sequence[DataMigration] = (),
    name: str,
):
    """A schema validating *artifacts*, with a categorical member it may drop.

    The counterpart of `_member_drop_plan` for the other row registry a schema
    validates.  An artifact's categorical annotations live in a link table whose
    label column is named after the label's registry (`ArtifactRecord.record`)
    rather than `value`, which is what the `Record` case never exercises.
    """

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        import lamindb as ln

        ft = FeatureType("amdrop_ft")
        rt = RecordType("AmdropVocab")
        Record("alpha", type=rt)
        keep = Feature("amdrop_keep", dtype=LaminDType(str), type=ft)
        doomed = Feature(
            "amdrop_doomed", dtype=LaminDType(ln.Record, type__uid=rt.uid), type=ft
        )
        schema = Schema(
            name="AmdropSchema",
            features=[keep, doomed] if member else [keep],
            optionals=[doomed] if member else [],
        )
        for migration in migrations:
            ctx.migrate(schema, migration)

    _plan.name = name
    return _plan


_artifact_member_drop_v1 = _artifact_member_drop_plan(name="amdrop_v1")
_artifact_member_drop_dropped = _artifact_member_drop_plan(
    member=False,
    migrations=[drop_member_values(feature="amdrop_doomed")],
    name="amdrop_dropped",
)


@create_plan
def _dupname_plan(ctx: PlanContext, exports: None) -> None:
    """Two features sharing a name under different feature types.

    Their natural keys differ (the parent chain does), but the prefetch's name
    index is keyed by `(name, is_type, parent uid)`, so both are loaded by the
    same `name__in` read.
    """
    Feature("dupname_col", dtype=LaminDType(str), type=FeatureType("dupname_ft1"))
    Feature("dupname_col", dtype=LaminDType(str), type=FeatureType("dupname_ft2"))


@create_plan
def _removal_v1_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("rm_ft")
    Feature("rm_keep", dtype=LaminDType(str), type=ft)
    Feature("rm_drop", dtype=LaminDType(str), type=ft)


@create_plan
def _removal_v2_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("rm_ft")
    Feature("rm_keep", dtype=LaminDType(str), type=ft)


_FDEL_KEY = "FeatureType:rm_ft/Feature:rm_drop"


def _feature_delete_plan(*, migrations: Sequence[DataMigration] = (), name: str):
    """`_removal_v2_plan` with a declaration for the feature it deletes.

    The feature is deliberately not a schema member: that path is already
    answered by `drop_member_values`, so what is left to cover is a feature
    annotated without any schema declaring it.
    """

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        ft = FeatureType("rm_ft")
        Feature("rm_keep", dtype=LaminDType(str), type=ft)
        for migration in migrations:
            ctx.migrate(_FDEL_KEY, migration)

    _plan.name = name
    return _plan


_feature_delete_dropped = _feature_delete_plan(
    migrations=[drop_values()], name="fdel_dropped"
)
_feature_delete_kept = _feature_delete_plan(
    migrations=[keep_values()], name="fdel_kept"
)
_feature_delete_noop = _feature_delete_plan(
    migrations=[DataMigration(name="noop", migrate=lambda context: None)],
    name="fdel_noop",
)

_CAT_FDEL_KEY = "FeatureType:crm_ft/Feature:crm_drop"


def _cat_removal_plan(
    *, declared: bool = False, migrations: Sequence[DataMigration] = (), name: str
):
    """The removal plans for a categorical (`list[cat[Record[...]]]`) feature.

    Its values live in a link table rather than in the scalar stores, so every
    check that they are gone has to look there; one presuming a categorical
    feature holds values made such a feature impossible to delete.
    """

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        import lamindb as ln

        ft = FeatureType("crm_ft")
        rt = RecordType("CrmVocab")
        Record("crm_a", type=rt)
        Feature("crm_keep", dtype=LaminDType(str), type=ft)
        if declared:
            Feature(
                "crm_drop",
                dtype=LaminDType(list[ln.Record], type__uid=rt.uid),
                type=ft,
            )
        for migration in migrations:
            ctx.migrate(_CAT_FDEL_KEY, migration)

    _plan.name = name
    return _plan


_cat_removal_v1 = _cat_removal_plan(declared=True, name="crm_v1")
_cat_removal_v2 = _cat_removal_plan(name="crm_v2")
_cat_removal_dropped = _cat_removal_plan(migrations=[drop_values()], name="crm_dropped")
_cat_removal_noop = _cat_removal_plan(
    migrations=[DataMigration(name="noop", migrate=lambda context: None)],
    name="crm_noop",
)


@create_plan
def _rename_feat_v1_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("rn_ft")
    Feature("rn_old", dtype=LaminDType(str), type=ft)


@create_plan
def _rename_feat_v2_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("rn_ft")
    f = Feature("rn_new", dtype=LaminDType(str), type=ft)
    ctx.moved(f, "rn_old")


@create_plan
def _rename_feat_v2_no_moved_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("rn_ft")
    Feature("rn_new", dtype=LaminDType(str), type=ft)


@create_plan
def _rename_two_v1_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("rn2_ft")
    Feature("rn2_a", dtype=LaminDType(str), type=ft)
    Feature("rn2_b", dtype=LaminDType(str), type=ft)


@create_plan
def _rename_two_v2_no_moved_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("rn2_ft")
    Feature("rn2_x", dtype=LaminDType(str), type=ft)
    Feature("rn2_y", dtype=LaminDType(str), type=ft)


@create_plan
def _rename_feat_bad_v2_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("rn_ft")
    # A rename bundled with a structural change (dtype str -> int) must be refused.
    f = Feature("rn_new", dtype=LaminDType(int), type=ft)
    ctx.moved(f, "rn_old")


@create_plan
def _rename_ft_v1_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("cas_old")
    Feature("cas_child", dtype=LaminDType(str), type=ft)


@create_plan
def _rename_ft_v2_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("cas_new")
    Feature("cas_child", dtype=LaminDType(str), type=ft)
    ctx.moved(ft, "cas_old")


@create_plan
def _reparent_v1_plan(ctx: PlanContext, exports: None) -> None:
    a = FeatureType("rp_a")
    FeatureType("rp_b")
    Feature("rp_x", dtype=LaminDType(str), type=a)


@create_plan
def _reparent_v2_plan(ctx: PlanContext, exports: None) -> None:
    FeatureType("rp_a")
    b = FeatureType("rp_b")
    f = Feature("rp_x", dtype=LaminDType(str), type=b)
    ctx.moved(f, "FeatureType:rp_a/Feature:rp_x")


def _schema_dtype_plan(dtype: type, name: str):
    """Build a plan declaring the `itype` schema `SD` with member dtype `dtype`."""

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        import lamindb as ln

        Schema(name="SD", features=[], itype=ln.Feature, dtype=LaminDType(dtype))

    _plan.name = name
    return _plan


_schema_dtype_int_plan = _schema_dtype_plan(int, "schema_dtype_int")
_schema_dtype_float_plan = _schema_dtype_plan(float, "schema_dtype_float")


@create_plan
def _x_record_type_plan(ctx: PlanContext, exports: None) -> None:
    RecordType("rtx_X")


@create_plan
def _x_sheet_plan(ctx: PlanContext, exports: None) -> None:
    f = Feature("rtx_c", dtype=LaminDType(str))
    s = Schema(name="rtx_XS", features=[f], optionals=[f])
    Sheet("rtx_X", schema=s)


@create_plan
def _x_back_to_record_type_plan(ctx: PlanContext, exports: None) -> None:
    f = Feature("rtx_c", dtype=LaminDType(str))
    Schema(name="rtx_XS", features=[f], optionals=[f])
    RecordType("rtx_X")


def _ls_plan(element: type, name: str):
    """Build a plan with one `list[element]` feature."""

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        Feature("ls_col", dtype=LaminDType(list[element]), type=FeatureType("ls_ft"))

    _plan.name = name
    return _plan


_ls_int_plan = _ls_plan(int, "ls_int")
_ls_str_plan = _ls_plan(str, "ls_str")


@create_plan
def _collision_v1_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("col_ft")
    Feature("col_a", dtype=LaminDType(str), type=ft)


@create_plan
def _collision_v2_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("col_ft")
    f = Feature("col_b", dtype=LaminDType(str), type=ft)
    ctx.moved(f, "col_a")


@create_plan
def _rename_member_v1_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("mem_ft")
    f = Feature("mem_old", dtype=LaminDType(str), type=ft)
    Schema(name="MemSchema", features=[f])


@create_plan
def _rename_member_v2_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("mem_ft")
    f = Feature("mem_new", dtype=LaminDType(str), type=ft)
    Schema(name="MemSchema", features=[f])
    ctx.moved(f, "mem_old")


@create_plan
def _decl_rename_v1_plan(ctx: PlanContext, exports: None) -> None:
    class M(SchemaModel):
        schema_config = SchemaConfig(
            name="DeclRnSchema", default_feature_type=FeatureType("decl_rn_ft")
        )
        old_col: Feature[str]


@create_plan
def _decl_rename_v2_plan(ctx: PlanContext, exports: None) -> None:
    class M(SchemaModel):
        schema_config = SchemaConfig(
            name="DeclRnSchema", default_feature_type=FeatureType("decl_rn_ft")
        )
        new_col: Feature[str]

    ctx.moved(M.new_col, "old_col")


@create_plan
def _schema_add_v1_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("schema_add_ft")
    f1 = Feature("schema_add_a", dtype=LaminDType(str), type=ft)
    f2 = Feature("schema_add_b", dtype=LaminDType(str), type=ft)
    Schema(name="SchemaAddSchema", features=[f1, f2])


@create_plan
def _schema_add_v2_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("schema_add_ft")
    f1 = Feature("schema_add_a", dtype=LaminDType(str), type=ft)
    f2 = Feature("schema_add_b", dtype=LaminDType(str), type=ft)
    f3 = Feature("schema_add_c", dtype=LaminDType(str), type=ft, nullable=True)
    Schema(name="SchemaAddSchema", features=[f1, f2, f3])


@create_plan
def _schema_remove_v2_plan(ctx: PlanContext, exports: None) -> None:
    """`schema_add_b` dropped from the schema *and* from the plan.

    With a lock, the feature has no declared address left at all, so the schema
    loses the member and the removal pass deletes the feature itself.
    """
    ft = FeatureType("schema_add_ft")
    f1 = Feature("schema_add_a", dtype=LaminDType(str), type=ft)
    Schema(name="SchemaAddSchema", features=[f1])


@create_plan
def _schema_unlink_v2_plan(ctx: PlanContext, exports: None) -> None:
    """`schema_add_b` dropped from the schema but still declared by the plan.

    The schema loses the member; the feature itself is still owned by the plan
    and survives.
    """
    ft = FeatureType("schema_add_ft")
    f1 = Feature("schema_add_a", dtype=LaminDType(str), type=ft)
    Feature("schema_add_b", dtype=LaminDType(str), type=ft)
    Schema(name="SchemaAddSchema", features=[f1])


@create_plan
def _schema_optional_remove_v1_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("optrm_ft")
    keep = Feature("optrm_keep", dtype=LaminDType(str), type=ft)
    drop = Feature("optrm_drop", dtype=LaminDType(str), type=ft)
    Schema(name="OptRemoveSchema", features=[keep, drop], optionals=[keep, drop])


@create_plan
def _schema_optional_remove_v2_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("optrm_ft")
    keep = Feature("optrm_keep", dtype=LaminDType(str), type=ft)
    Feature("optrm_drop", dtype=LaminDType(str), type=ft)
    Schema(name="OptRemoveSchema", features=[keep], optionals=[keep])


@create_plan
def _maximal_schema_v1_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("maxset_ft")
    a = Feature("maxset_a", dtype=LaminDType(str), type=ft)
    b = Feature("maxset_b", dtype=LaminDType(str), type=ft)
    Schema(name="MaximalSchema", features=[a, b], maximal_set=True)


@create_plan
def _maximal_schema_v2_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("maxset_ft")
    a = Feature("maxset_a", dtype=LaminDType(str), type=ft)
    Feature("maxset_b", dtype=LaminDType(str), type=ft)
    Schema(name="MaximalSchema", features=[a], maximal_set=True)


@create_plan
def _ordered_schema_v1_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("ordered_ft")
    a = Feature("ordered_a", dtype=LaminDType(str), type=ft)
    b = Feature("ordered_b", dtype=LaminDType(str), type=ft)
    c = Feature("ordered_c", dtype=LaminDType(str), type=ft)
    Schema(name="OrderedSchema", features=[a, b, c], ordered_set=True)


@create_plan
def _ordered_schema_v2_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("ordered_ft")
    a = Feature("ordered_a", dtype=LaminDType(str), type=ft)
    b = Feature("ordered_b", dtype=LaminDType(str), type=ft)
    c = Feature("ordered_c", dtype=LaminDType(str), type=ft)
    f = Feature("ordered_f", dtype=LaminDType(str), type=ft)
    Schema(name="OrderedSchema", features=[a, b, f, c], ordered_set=True)


@create_plan
def _ordered_schema_remove_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("ordered_ft")
    a = Feature("ordered_a", dtype=LaminDType(str), type=ft)
    Feature("ordered_b", dtype=LaminDType(str), type=ft)
    c = Feature("ordered_c", dtype=LaminDType(str), type=ft)
    Schema(name="OrderedSchema", features=[a, c], ordered_set=True)


@create_plan
def _ordered_schema_remove_and_reorder_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("ordered_ft")
    a = Feature("ordered_a", dtype=LaminDType(str), type=ft)
    Feature("ordered_b", dtype=LaminDType(str), type=ft)
    c = Feature("ordered_c", dtype=LaminDType(str), type=ft)
    Schema(name="OrderedSchema", features=[c, a], ordered_set=True)


@create_plan
def _index_outside_features_plan(ctx: PlanContext, exports: None) -> None:
    """`index=` names a feature the plan does not also list in `features`.

    LaminDB inserts the index into the member list itself, so a plan reading only
    its own `features` sees a member it never declared.
    """
    ft = FeatureType("idx_ft")
    idx = Feature("idx_id", dtype=LaminDType(str), type=ft)
    other = Feature("idx_other", dtype=LaminDType(str), type=ft)
    Schema(name="IdxSchema", features=[other], index=idx)


@create_plan
def _index_listed_last_plan(ctx: PlanContext, exports: None) -> None:
    """An ordered schema listing its index last; LaminDB still puts it first."""
    ft = FeatureType("idxord_ft")
    idx = Feature("idxord_id", dtype=LaminDType(str), type=ft)
    other = Feature("idxord_other", dtype=LaminDType(str), type=ft)
    Schema(name="IdxOrdSchema", features=[other, idx], index=idx, ordered_set=True)


@create_plan
def _typed_dup_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("apply_ft")
    Feature("apply_col", dtype=LaminDType(str), type=ft)


@create_plan
def _bulk_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("bulk_ft")
    features = [
        Feature(f"bulk_col_{i:03d}", dtype=LaminDType(str), type=ft) for i in range(25)
    ]
    Schema(name="BulkSchema", features=features)


@create_plan
def _atomic_plan(ctx: PlanContext, exports: None) -> None:
    FeatureType("atomic_ok")
    FeatureType("atomic_boom")


@create_plan
def _members_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("members_ft")
    a = Feature("members_a", dtype=LaminDType(str), type=ft)
    b = Feature("members_b", dtype=LaminDType(str), type=ft)
    c = Feature("members_c", dtype=LaminDType(str), type=ft)
    Schema(name="MembersOne", features=[a, b, c], ordered_set=True)
    Schema(name="MembersTwo", features=[c, a], ordered_set=True)


@create_plan
def _shared_one_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("shared_one_ft")
    a = Feature("shared_one_a", dtype=LaminDType(str), type=ft)
    b = Feature("shared_one_b", dtype=LaminDType(int), type=ft)
    Schema(name="SharedOne", features=[a, b])


@create_plan
def _shared_two_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("shared_two_ft")
    a = Feature("shared_two_a", dtype=LaminDType(str), type=ft)
    Schema(name="SharedTwo", features=[a])


@create_plan
def _codeploy_one_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("codeploy_ft")
    shared = Feature("codeploy_shared", dtype=LaminDType(str), type=ft)
    Schema(name="CodeployOne", features=[shared])


@create_plan
def _codeploy_two_plan(ctx: PlanContext, exports: None) -> None:
    """`_codeploy_one_plan`'s sibling: it declares the same feature type and feature."""
    ft = FeatureType("codeploy_ft")
    shared = Feature("codeploy_shared", dtype=LaminDType(str), type=ft)
    Schema(name="CodeployTwo", features=[shared])


@create_plan
def _adopt_plan(ctx: PlanContext, exports: None) -> None:
    FeatureType("adopt_me")


def _persist_lock(ctx: object, plan: object, path: Path) -> LockFile:
    write_lock(ctx, plan, path)  # type: ignore[arg-type]
    lock = LockFile.read(path)
    assert lock is not None
    return lock


def _write_and_read_lock(ctx: object, path: Path) -> LockFile:
    return _persist_lock(ctx, _apply_plan, path)


def test_plan_mode_shows_placeholders_and_does_not_save(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    ctx = compute_changes(_apply_plan, mode=ApplyMode.PLAN, lock=None)

    assert {c.action for c in ctx.changes} == {Action.CREATE}
    assert all(c.uid == "(known after apply)" for c in ctx.changes)
    # nothing was persisted
    assert ln.Feature.filter().count() == 0
    assert ln.Schema.filter().count() == 0


def test_apply_creates_objects_and_writes_lock(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    ctx = compute_changes(_apply_plan, mode=ApplyMode.APPLY, lock=None)

    by_kind = {c.kind: c for c in ctx.changes}
    assert by_kind["FeatureType"].action is Action.CREATE
    assert by_kind["Feature"].action is Action.CREATE
    assert by_kind["Schema"].action is Action.CREATE
    assert all(c.uid != "(known after apply)" for c in ctx.changes)

    # objects exist in the instance
    assert ln.Feature.filter(name="apply_col").one_or_none() is not None
    assert ln.Schema.filter(name="ApplySchema").one_or_none() is not None

    lock = _write_and_read_lock(ctx, tmp_path / "apply_plan.lock")
    assert lock.uid_for("FeatureType:apply_ft") is not None
    assert lock.uid_for("FeatureType:apply_ft/Feature:apply_col") is not None


def test_lookup_disambiguates_same_name_by_parent_type(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # apply the plan once: creates the typed feature `apply_ft/apply_col`
    compute_changes(_apply_plan, mode=ApplyMode.APPLY, lock=None)
    typed = ln.Feature.filter(name="apply_col", is_type=False).one_or_none()
    assert typed is not None and typed.type is not None

    # an unrelated, untyped feature shares the same name -- `type=None` has to
    # be explicit, or lamindb dedups against every type context and hands back
    # the typed feature above
    ln.Feature(name="apply_col", dtype="str", type=None).save()
    assert ln.Feature.filter(name="apply_col", is_type=False).count() == 2

    # without a lock the lookup falls back to natural filters; it must constrain
    # by the parent type instead of raising MultipleObjectsReturned, and resolve
    # to the typed feature (NO_CHANGE), not the untyped duplicate
    ctx = compute_changes(_typed_dup_plan, mode=ApplyMode.PLAN, lock=None)
    by_kind = {c.kind: c for c in ctx.changes}
    assert by_kind["Feature"].action is Action.NO_CHANGE
    assert by_kind["Feature"].uid == str(typed.uid)


def test_reapply_with_lock_is_idempotent(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    first = compute_changes(_apply_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _write_and_read_lock(first, tmp_path / "apply_plan.lock")

    second = compute_changes(_apply_plan, mode=ApplyMode.APPLY, lock=lock)
    assert {c.action for c in second.changes} == {Action.NO_CHANGE}


def test_plan_mode_reports_no_change_for_existing_objects(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_apply_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _write_and_read_lock(applied, tmp_path / "apply_plan.lock")

    preview = compute_changes(_apply_plan, mode=ApplyMode.PLAN, lock=lock)
    assert {c.action for c in preview.changes} == {Action.NO_CHANGE}


def test_compute_changes_reports_progress(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    messages: list[str] = []
    tracked: list[tuple[str, int]] = []

    def _report(message: str) -> None:
        messages.append(message)

    def _track(sequence: object, description: str) -> list[object]:
        items = list(sequence)  # type: ignore[call-overload]
        tracked.append((description, len(items)))
        return items

    compute_changes(
        _apply_plan,
        mode=ApplyMode.PLAN,
        lock=None,
        report=_report,
        track=_track,
    )

    assert "Finalizing plan" in messages
    deps = list(_apply_plan.dependencies)
    assert ("Resolving objects", len(deps)) in tracked


def test_apply_pass_uses_label_applying_changes(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    tracked: list[str] = []

    def _track(sequence: object, description: str) -> list[object]:
        tracked.append(description)
        return list(sequence)  # type: ignore[call-overload]

    compute_changes(_apply_plan, mode=ApplyMode.APPLY, lock=None, track=_track)
    assert tracked == ["Applying changes"]


def test_apply_reuses_existence_cache_across_passes(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
) -> None:
    existing_cache = ExistenceCache()
    compute_changes(
        _apply_plan, mode=ApplyMode.PLAN, lock=None, existing_cache=existing_cache
    )

    deps = [d.obj for d in _apply_plan.dependencies]
    # the preview pass resolved every object, in batched reads rather than one
    # query per object
    assert len(existing_cache) == len(deps)
    assert existing_cache.queries == 0
    assert existing_cache.prefilled == len(deps)

    resolved = existing_cache.queries + existing_cache.prefilled

    # the apply pass reuses the cached lookups instead of re-querying the
    # instance, yet still creates every object
    ctx = compute_changes(
        _apply_plan, mode=ApplyMode.APPLY, lock=None, existing_cache=existing_cache
    )

    # nothing new was resolved: every lookup the apply needed was already held
    assert existing_cache.queries + existing_cache.prefilled == resolved
    assert existing_cache.hits > 0
    assert {c.action for c in ctx.changes} == {Action.CREATE}
    assert ln.Schema.filter(name="ApplySchema").one_or_none() is not None


def test_apply_aborts_on_missing_uid_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_apply_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _write_and_read_lock(applied, tmp_path / "apply_plan.lock")

    entry = lock.entry_for("FeatureType:apply_ft")
    assert entry is not None
    entry.uid = "zzzzzzzzzzzz"  # valid format, but not in the instance

    with pytest.raises(DriftError, match="no matching object"):
        compute_changes(_apply_plan, mode=ApplyMode.APPLY, lock=lock)


def test_apply_aborts_on_config_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_apply_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _write_and_read_lock(applied, tmp_path / "apply_plan.lock")

    entry = lock.entry_for("FeatureType:apply_ft")
    assert entry is not None
    entry.config["name"] = "diverged"  # no longer matches the plan

    with pytest.raises(
        PlanChangeRefusedError,
        match="^cannot change FeatureType 'apply_ft' \\(FeatureType:apply_ft\\) in "
        "place:\n  name 'diverged' \u2192 'apply_ft'",
    ):
        compute_changes(_apply_plan, mode=ApplyMode.APPLY, lock=lock)


def test_apply_creates_record_types_sheets_and_records(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    ctx = compute_changes(_records_plan, mode=ApplyMode.APPLY, lock=None)

    by_kind = {c.kind for c in ctx.changes}
    assert {"RecordType", "Sheet", "Record"} <= by_kind
    assert all(c.action is Action.CREATE for c in ctx.changes)

    assert ln.Record.filter(name="apply_rt", is_type=True).one_or_none() is not None
    assert ln.Record.filter(name="apply_sheet", is_type=True).one_or_none() is not None
    assert (
        ln.Record.filter(name="apply_record", is_type=False).one_or_none() is not None
    )

    lock_path = tmp_path / "records_plan.lock"
    write_lock(ctx, _records_plan, lock_path)  # type: ignore[arg-type]
    lock = LockFile.read(lock_path)
    assert lock is not None
    # Non-type records are seeded, not tracked: their record type and sheet are
    # locked, they are not.
    assert {e.kind for e in lock.objects.values() if e.kind in ("RecordType", "Sheet")}
    assert [e.key for e in lock.objects.values() if e.kind == "Record"] == []

    reapplied = compute_changes(_records_plan, mode=ApplyMode.APPLY, lock=lock)
    assert {c.action for c in reapplied.changes} == {Action.NO_CHANGE}


def test_record_renamed_on_instance_does_not_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    """An out-of-band record rename is invisible rather than fatal.

    While records were tracked, `name` was structural, so a value renamed in
    the instance turned every plan carrying it -- including every plan that
    merely depends on the declaring one -- into a hard `DriftError`.  The
    lock's identity-only `seeded` section still finds it by uid.
    """
    ctx = compute_changes(_records_plan, mode=ApplyMode.APPLY, lock=None)
    lock_path = tmp_path / "records_plan.lock"
    write_lock(ctx, _records_plan, lock_path)  # type: ignore[arg-type]
    lock = LockFile.read(lock_path)
    assert lock is not None

    live = ln.Record.filter(name="apply_record", is_type=False).one()
    live.name = "renamed_out_of_band"
    live.save()

    replanned = compute_changes(_records_plan, mode=ApplyMode.PLAN, lock=lock)
    assert not [c for c in replanned.changes if c.action is Action.DELETE]
    # The lock remembers the value's uid (identity only), so the renamed
    # record is still the plan's value: left alone, not re-seeded as a
    # second record under the old name.
    record_changes = [c for c in replanned.changes if c.kind == "Record"]
    assert [c.action for c in record_changes] == [Action.NO_CHANGE]
    assert record_changes[0].uid == str(live.uid)
    assert ln.Record.filter(name="apply_record", is_type=False).count() == 0
    assert (
        ln.Record.filter(name="renamed_out_of_band", is_type=False).one_or_none()
        is not None
    )


def test_record_dropped_from_plan_is_not_deleted(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    """Removing a vocabulary value from a plan leaves the live record alone.

    `apply_removals` deletes by uid with no inbound-reference scan, so a
    tracked record vanished the moment its line was deleted from the plan --
    taking the labels of everything already validated against it with it.
    """
    ctx = compute_changes(_records_plan, mode=ApplyMode.APPLY, lock=None)
    lock_path = tmp_path / "records_plan.lock"
    write_lock(ctx, _records_plan, lock_path)  # type: ignore[arg-type]
    lock = LockFile.read(lock_path)
    assert lock is not None

    trimmed = compute_changes(
        _records_plan_without_value, mode=ApplyMode.APPLY, lock=lock
    )
    assert not [c for c in trimmed.changes if c.action is Action.DELETE]
    assert (
        ln.Record.filter(name="apply_record", is_type=False).one_or_none() is not None
    )


def test_record_is_readopted_without_a_lock(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    """Seeding is idempotent without a lock: records re-adopt by name and type."""
    compute_changes(_records_plan, mode=ApplyMode.APPLY, lock=None)
    _records_plan.reset()
    again = compute_changes(_records_plan, mode=ApplyMode.APPLY, lock=None)

    record_changes = [c for c in again.changes if c.kind == "Record"]
    assert [c.action for c in record_changes] == [Action.NO_CHANGE]
    assert ln.Record.filter(name="apply_record", is_type=False).count() == 1


def test_moved_rejects_a_record() -> None:
    rt = RecordType("moved_rt")
    ctx = PlanContext()
    with pytest.raises(TypeError, match="ctx.moved\\(\\) does not apply to Record"):
        ctx.moved(Record("new_name", type=rt), "old_name")


def test_finalize_rejects_an_unnamed_record() -> None:
    with pytest.raises(ValueError, match="declares an unnamed Record"):
        _unnamed_record_plan.finalize()


def test_render_changes_lists_objects(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    ctx = compute_changes(_apply_plan, mode=ApplyMode.PLAN, lock=None)
    text = render_changes(ctx, title="Plan: test")
    assert "Plan: test" in text
    assert "FeatureType" in text
    assert "'apply_ft'" in text
    assert "3 to create" in text


def test_effective_space_uid_prefers_explicit_then_default() -> None:
    ctx = ApplyContext(default_space_uid=SpaceUID("spaceUID0001"))
    defaulted = Feature("a", dtype=LaminDType(int))
    explicit = Feature("b", dtype=LaminDType(int), space_uid=SpaceUID("spaceUID0002"))
    assert ctx.effective_space_uid(defaulted) == "spaceUID0001"
    assert ctx.effective_space_uid(explicit) == "spaceUID0002"


def test_render_changes_markdown_renders_diff_block() -> None:
    from lamindb_setup.evolve.engine._apply import ChangeItem

    ctx = ApplyContext()
    ctx.changes = [
        ChangeItem(
            obj_id=1,
            key="FeatureType:md_ft",
            kind="FeatureType",
            name="md_ft",
            action=Action.CREATE,
            uid="(known after apply)",
        ),
        ChangeItem(
            obj_id=2,
            key="Feature:md_col",
            kind="Feature",
            name="md_col",
            action=Action.UPDATE,
            uid="feat0000000000",
            details=["description: old -> new"],
            warnings=["changed outside the plan"],
        ),
        ChangeItem(
            obj_id=3,
            key="Schema:md_schema",
            kind="Schema",
            name="md_schema",
            action=Action.NO_CHANGE,
            uid="schm0000000000",
        ),
    ]

    md = render_changes_markdown(ctx, title="Plan: md_test")
    assert md.startswith("### Plan: md_test")
    assert "**Plan:** 1 to create, 1 to update, 0 to delete, 1 unchanged." in md
    assert "```diff" in md
    assert "+ FeatureType 'md_ft'  (known after apply)" in md
    assert "~ Feature 'md_col'  feat0000000000" in md
    assert "    description: old -> new" in md
    assert "! WARNING: changed outside the plan" in md
    # no-change objects are excluded from the diff block
    assert "md_schema" not in md
    # drift warning callout is rendered
    assert "[!WARNING]" in md


def test_render_changes_markdown_no_changes() -> None:
    ctx = ApplyContext()
    ctx.changes = []
    md = render_changes_markdown(ctx, title="Plan: empty")
    assert "No changes. Instance is up to date." in md
    assert "```diff" not in md


def test_effective_space_uid_is_none_without_default() -> None:
    ctx = ApplyContext()
    assert ctx.effective_space_uid(Feature("a", dtype=LaminDType(int))) is None


def test_lock_comparison_abstains_on_reference_valued_keys() -> None:
    """The lock-vs-plan comparison is scalar-only.

    The plan's configuration addresses schema members by natural key and the
    instance snapshot by uid, so a reference-valued key is not comparable across
    the two -- and the verdict on it needs the live member list anyway (a member
    the plan *drops* reads exactly like one it makes required again).  Both
    belong to the diff `_reconcile` runs against the instance.
    """
    ctx = ApplyContext()
    existing = object()

    # A scalar key does take the "instance already matches the plan" shortcut:
    # the instance already holds the plan's value, so the blocked narrowing
    # changes nothing and is accepted.
    ctx._check_plan_edit(
        existing,
        "Feature:f",
        "Feature",
        {"kind": "Feature", "nullable": True},
        {"kind": "Feature", "nullable": False},
        current={"kind": "Feature", "nullable": False},
    )

    # A reference-valued key is not judged here at all, in either direction.
    for key in ("features", "optionals"):
        ctx._check_plan_edit(
            existing,
            "Schema:s",
            "Schema",
            {"kind": "Schema", key: ["Feature:a", "Feature:b"]},
            {"kind": "Schema", key: ["Feature:a"]},
            current={"kind": "Schema", key: ["Feature:a"]},
        )


def test_lock_comparison_accepts_a_filter_the_instance_already_has() -> None:
    # A feature's dtype and filters are one property.  The lock records them
    # apart (bare dtype, `cat_filters` by natural key) and the instance inlined
    # (`cat[Record[<uid>]]`), so "the instance already matches the plan" is
    # decided on the plan's full dtype -- here, a lock that is merely stale.
    ctx = ApplyContext()
    record = "lamindb.models.record.Record"
    ctx._check_plan_edit(
        object(),
        "Feature:f",
        "Feature",
        {"kind": "Feature", "dtype": record, "cat_filters": {"type__uid": "uA"}},
        {"kind": "Feature", "dtype": record, "cat_filters": {"type__uid": "uB"}},
        current={"kind": "Feature", "dtype": "cat[Record[uB]]"},
        desired_dtype="cat[Record[uB]]",
    )


def test_lock_records_resolved_space_uid(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    ctx = compute_changes(_space_plan, mode=ApplyMode.PLAN, lock=None)
    lock = ctx.build_lock(_space_plan.name)

    ft_entry = lock.entry_for("FeatureType:space_ft")
    assert ft_entry is not None
    assert ft_entry.config["space_uid"] == "spaceUID0001"  # plan-wide default

    feat_entry = lock.entry_for("FeatureType:space_ft/Feature:space_col")
    assert feat_entry is not None
    assert feat_entry.config["space_uid"] == "spaceUID0002"  # explicit override


def test_apply_assigns_space_to_created_objects(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    ln.Space(name="schemas-default", uid="spaceUID0001").save()
    ln.Space(name="schemas-explicit", uid="spaceUID0002").save()

    compute_changes(_space_plan, mode=ApplyMode.APPLY, lock=None)

    ft = ln.Feature.get(name="space_ft", is_type=True)
    assert ft.space.uid == "spaceUID0001"
    feat = ln.Feature.get(name="space_col", is_type=False)
    assert feat.space.uid == "spaceUID0002"
    schema = ln.Schema.get(name="SpaceSchema")
    assert schema.space.uid == "spaceUID0001"


def test_apply_errors_on_missing_space(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    with pytest.raises(SpaceNotFoundError, match="spaceUID9999"):
        compute_changes(_missing_space_plan, mode=ApplyMode.APPLY, lock=None)


def test_plan_detects_object_in_wrong_space(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    ln.Space(name="schemas-default", uid="spaceUID0001").save()
    ln.Space(name="schemas-explicit", uid="spaceUID0002").save()

    # Create the objects in the default (wrong) space, ignoring the plan's
    # space configuration.
    plain = ln.Feature(name="space_ft", dtype=None, is_type=True).save()
    assert plain.space.uid != "spaceUID0001"

    preview = compute_changes(_space_plan, mode=ApplyMode.PLAN, lock=None)
    by_kind = {c.kind: c for c in preview.changes}

    # The existing feature type is reported as an update (space move), not a
    # no-change, and the move is described in the details.
    ft_change = by_kind["FeatureType"]
    assert ft_change.action is Action.UPDATE
    assert any("spaceUID0001" in detail for detail in ft_change.details)
    # nothing was moved in PLAN mode
    assert ln.Feature.get(name="space_ft", is_type=True).space.uid != "spaceUID0001"


def test_apply_moves_object_into_correct_space(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    ln.Space(name="schemas-default", uid="spaceUID0001").save()
    ln.Space(name="schemas-explicit", uid="spaceUID0002").save()

    # Pre-create the feature type in the wrong space.
    ln.Feature(name="space_ft", dtype=None, is_type=True).save()
    assert ln.Feature.get(name="space_ft", is_type=True).space.uid != "spaceUID0001"

    ctx = compute_changes(_space_plan, mode=ApplyMode.APPLY, lock=None)
    by_kind = {c.kind: c for c in ctx.changes}
    assert by_kind["FeatureType"].action is Action.UPDATE

    # The existing object was moved into the configured space.
    assert ln.Feature.get(name="space_ft", is_type=True).space.uid == "spaceUID0001"

    # Re-applying is now a no-op for the moved object.
    again = compute_changes(_space_plan, mode=ApplyMode.PLAN, lock=None)
    moved = {c.kind: c for c in again.changes}["FeatureType"]
    assert moved.action is Action.NO_CHANGE


def test_apply_creates_feature_with_descriptive_metadata(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    """`synonyms` and `unit` are persisted on a feature the plan *creates*.

    `ln.Feature.__init__` advertises both as keyword arguments but never
    consumes them, so a plan that declared either could only ever be applied to
    an instance where the feature already existed.
    """
    ctx = compute_changes(_descriptive_create_plan, mode=ApplyMode.APPLY, lock=None)
    assert {c.kind: c for c in ctx.changes}["Feature"].action is Action.CREATE

    feat = ln.Feature.get(name="organism", is_type=False)
    assert feat.synonyms == "species|organism_name"
    assert feat.unit == "mg"
    assert feat.description == "Species of the sample"
    assert feat.nullable is False

    # The created state matches the plan, so re-applying changes nothing --
    # both without a lock and against the lock the first apply wrote.
    lock = _persist_lock(ctx, _descriptive_create_plan, tmp_path / "create.lock")
    again = compute_changes(_descriptive_create_plan, mode=ApplyMode.PLAN, lock=None)
    assert {c.kind: c for c in again.changes}["Feature"].action is Action.NO_CHANGE
    relocked = compute_changes(
        _descriptive_create_plan, mode=ApplyMode.APPLY, lock=lock
    )
    feature_change = {c.kind: c for c in relocked.changes}["Feature"]
    assert feature_change.action is Action.NO_CHANGE
    assert feature_change.warnings == []


@pytest.mark.parametrize("instance", ["bionty"], indirect=True)
def test_apply_creates_categorical_feature_with_synonyms(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    """A categorical feature carries `synonyms` through creation too.

    `cat_filters` takes its own branch in :meth:`Feature.stage`, so the
    categorical case is exercised alongside the scalar one.
    """
    compute_changes(_descriptive_cat_create_plan, mode=ApplyMode.APPLY, lock=None)

    feat = ln.Feature.get(name="organism", is_type=False)
    assert feat.synonyms == "species"
    assert feat.nullable is False
    assert feat.dtype_as_str == "cat[bionty.Organism]"

    again = compute_changes(
        _descriptive_cat_create_plan, mode=ApplyMode.PLAN, lock=None
    )
    assert {c.kind: c for c in again.changes}["Feature"].action is Action.NO_CHANGE


def test_plan_detects_descriptive_metadata_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_descriptive_v1_plan, mode=ApplyMode.APPLY, lock=None)

    preview = compute_changes(_descriptive_v2_plan, mode=ApplyMode.PLAN, lock=None)
    feat = {c.kind: c for c in preview.changes}["Feature"]
    assert feat.action is Action.UPDATE
    assert any("description" in d for d in feat.details)
    assert any("unit" in d for d in feat.details)
    assert any("synonyms" in d for d in feat.details)

    # PLAN mode does not mutate the instance.
    assert ln.Feature.get(name="drift_col", is_type=False).description == "first"


def test_apply_updates_descriptive_metadata_in_place(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_descriptive_v1_plan, mode=ApplyMode.APPLY, lock=None)

    ctx = compute_changes(_descriptive_v2_plan, mode=ApplyMode.APPLY, lock=None)
    feat_change = {c.kind: c for c in ctx.changes}["Feature"]
    assert feat_change.action is Action.UPDATE

    feat = ln.Feature.get(name="drift_col", is_type=False)
    assert feat.description == "second"
    assert feat.unit == "mg"
    assert feat.synonyms == "alias"

    # Re-applying the same plan is a no-op.
    again = compute_changes(_descriptive_v2_plan, mode=ApplyMode.PLAN, lock=None)
    assert {c.kind: c for c in again.changes}["Feature"].action is Action.NO_CHANGE


def test_apply_raises_on_validation_affecting_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_nullable_true_plan, mode=ApplyMode.APPLY, lock=None)

    with pytest.raises(DriftError, match="nullable"):
        compute_changes(_nullable_false_plan, mode=ApplyMode.PLAN, lock=None)
    # Nothing changed on the instance: it is the plan's change, refused.
    with pytest.raises(PlanChangeRefusedError, match="nullable True \u2192 False"):
        compute_changes(_nullable_false_plan, mode=ApplyMode.APPLY, lock=None)


def test_a_refusal_of_what_the_instance_changed_keeps_the_drift_wording(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    ctx = compute_changes(_nullable_false_plan, mode=ApplyMode.APPLY, lock=None)
    lock = ctx.build_lock(_nullable_false_plan.name)
    # Widened outside the plan: safe for the instance, but the plan still
    # declares False, and turning it back off is refused.
    feature = ln.Feature.get(name="null_col", is_type=False)
    feature.nullable = True
    feature.save()

    with pytest.raises(DriftError) as excinfo:
        compute_changes(_nullable_false_plan, mode=ApplyMode.PLAN, lock=lock)

    # The instance moved, not the plan: still drift, worded as before.
    message = str(excinfo.value)
    assert not isinstance(excinfo.value, PlanChangeRefusedError)
    assert message.startswith(
        "drift for 'FeatureType:null_ft/Feature:null_col': the instance diverges "
        "from the plan in validation-affecting or structural properties that "
        "cannot be changed safely:\n  nullable: instance has True, plan wants False"
    )
    assert "cannot change" not in message


def _origin(
    *,
    lock: dict[str, Any] | None = None,
    plan: dict[str, Any] | None = None,
    stored: dict[str, Any] | None = None,
    current: dict[str, Any] | None = None,
) -> Any:
    from lamindb_setup.evolve.engine._apply import _ChangeOrigin

    return _ChangeOrigin(
        lock_config=lock or {},
        plan_config=plan or {},
        stored=stored,
        current=current or {},
    )


def test_a_property_the_plan_changed_is_not_drift() -> None:
    origin = _origin(
        lock={"nullable": True},
        plan={"nullable": False},
        stored={"nullable": True},
        current={"nullable": True},
    )

    assert not origin.is_drift("nullable")


def test_a_property_the_instance_changed_is_drift() -> None:
    # The snapshot says the instance moved, even where the plan did too.
    origin = _origin(
        lock={"unit": "mg"},
        plan={"unit": "g"},
        stored={"unit": "mg"},
        current={"unit": "kg"},
    )

    assert origin.is_drift("unit")


def test_a_property_the_plan_still_declares_as_locked_is_drift() -> None:
    # No snapshot proves it, but the plan did not move, so the instance did.
    origin = _origin(lock={"flexible": True}, plan={"flexible": True}, stored=None)

    assert origin.is_drift("flexible")


def test_an_unset_snapshot_index_does_not_establish_drift() -> None:
    # The writer omits the index when none is set.
    origin = _origin(
        lock={},
        plan={"index": "FeatureType:ft/Feature:barcode"},
        stored={"name": "S"},
        current={"name": "S", "index": "uid0barcode0"},
    )

    assert not origin.is_drift("index")


def test_a_feature_dtype_change_includes_its_filters() -> None:
    origin = _origin(
        lock={"dtype": "lamindb.models.Record", "cat_filters": {"type__uid": "A"}},
        plan={"dtype": "lamindb.models.Record", "cat_filters": {"type__uid": "B"}},
        stored={"dtype": "cat[Record[A]]"},
        current={"dtype": "cat[Record[A]]"},
    )

    assert not origin.is_drift("dtype")


def test_refusals_without_a_lock_entry_are_plan_changes() -> None:
    from lamindb_setup.evolve.engine._apply import _reconcile_refusal, _Refusal

    error = _reconcile_refusal(
        "Schema",
        "S",
        "Schema:S",
        [
            _Refusal.of_values("maximal_set", False, True, " (why)"),
            _Refusal.of_text("features", "features: the plan removes 'x'"),
        ],
        None,
    )

    assert isinstance(error, PlanChangeRefusedError)
    assert str(error) == (
        "cannot change Schema 'S' (Schema:S) in place:\n"
        "  maximal_set False \u2192 True (why)\n"
        "  features: the plan removes 'x'"
    )


def test_drift_and_plan_changes_of_one_object_are_told_apart() -> None:
    from lamindb_setup.evolve.engine._apply import _reconcile_refusal, _Refusal

    origin = _origin(
        lock={"flexible": True, "maximal_set": False},
        plan={"flexible": True, "maximal_set": True},
        stored={"flexible": True, "maximal_set": False},
        current={"flexible": False, "maximal_set": False},
    )

    error = _reconcile_refusal(
        "Schema",
        "S",
        "Schema:S",
        [
            _Refusal.of_values("flexible", False, True),
            _Refusal.of_values("maximal_set", False, True, " (why)"),
        ],
        origin,
    )

    assert not isinstance(error, PlanChangeRefusedError)
    assert str(error) == (
        "drift for 'Schema:S': the instance diverges from the plan in "
        "validation-affecting or structural properties that cannot be changed "
        "safely:\n"
        "  flexible: instance has False, plan wants True\n"
        "cannot change Schema 'S' (Schema:S) in place:\n"
        "  maximal_set False \u2192 True (why)"
    )


def test_widening_nullable_edit_is_applied(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # nullable False -> True is a widening: existing records stay valid, so the
    # edit is applied in place rather than refused as drift.
    applied = compute_changes(_nullable_false_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _nullable_false_plan, tmp_path / "null.lock")

    ctx = compute_changes(_nullable_true_plan, mode=ApplyMode.APPLY, lock=lock)
    feat = {c.kind: c for c in ctx.changes}["Feature"]
    assert feat.action is Action.UPDATE
    assert ln.Feature.get(name="null_col", is_type=False).nullable is True


def test_widening_dtype_edit_is_applied(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # int -> float widens the numeric ladder; every existing int is still a
    # valid float, so the dtype is updated in place.
    applied = compute_changes(_dtype_int_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_int_plan, tmp_path / "dtype.lock")

    ctx = compute_changes(_dtype_float_plan, mode=ApplyMode.APPLY, lock=lock)
    feat = {c.kind: c for c in ctx.changes}["Feature"]
    assert feat.action is Action.UPDATE
    assert ln.Feature.get(name="dtype_col", is_type=False).dtype_as_str == "float"


def test_narrowing_dtype_edit_is_blocked(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # float -> int is a narrowing: a stored float may not be an int, so the
    # plan's edit is refused.
    applied = compute_changes(_dtype_float_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_float_plan, tmp_path / "dtype.lock")

    with pytest.raises(PlanChangeRefusedError, match=_dtype_refusal("float", "int")):
        compute_changes(_dtype_int_plan, mode=ApplyMode.PLAN, lock=lock)


def test_narrowing_dtype_applied_when_feature_has_no_values(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # float -> int is normally blocked, but the feature has no stored values, so
    # nothing can be invalidated: with the opt-in flag it applies in place.
    applied = compute_changes(_dtype_float_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_float_plan, tmp_path / "dtype.lock")

    ctx = compute_changes(
        _dtype_int_plan,
        mode=ApplyMode.APPLY,
        lock=lock,
        allow_data_validated_changes=True,
    )
    feat = {c.kind: c for c in ctx.changes}["Feature"]
    assert feat.action is Action.UPDATE
    assert any("allowed despite block" in d for d in feat.details)
    assert ln.Feature.get(name="dtype_col", is_type=False).dtype_as_str == "int"


def test_narrowing_nullable_applied_when_feature_has_no_values(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A nullable True -> False narrowing applies with the flag when nothing could
    # hold a null for the feature: no schema declares it, so no row omits it and
    # no dataset was validated with it.
    applied = compute_changes(_nullable_true_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _nullable_true_plan, tmp_path / "null.lock")

    ctx = compute_changes(
        _nullable_false_plan,
        mode=ApplyMode.APPLY,
        lock=lock,
        allow_data_validated_changes=True,
    )
    feat = {c.kind: c for c in ctx.changes}["Feature"]
    assert feat.action is Action.UPDATE
    assert ln.Feature.get(name="null_col", is_type=False).nullable is False


def test_narrowing_dtype_applied_when_all_values_validate(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # str -> int is blocked, but every stored value parses as an int, so with the
    # opt-in flag the dtype is narrowed in place.
    from lamindb.models import JsonValue

    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    JsonValue(feature=feat, value="1").save()
    JsonValue(feature=feat, value="2").save()

    ctx = compute_changes(
        _dtype_int_plan,
        mode=ApplyMode.APPLY,
        lock=lock,
        allow_data_validated_changes=True,
    )
    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.UPDATE
    assert ln.Feature.get(name="dtype_col", is_type=False).dtype_as_str == "int"


def test_narrowing_dtype_blocked_when_a_value_is_invalid(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A single non-numeric value keeps the str -> int narrowing blocked even with
    # the opt-in flag, because it would be invalidated by the change.
    from lamindb.models import JsonValue

    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    JsonValue(feature=feat, value="1").save()
    JsonValue(feature=feat, value="notanint").save()

    with pytest.raises(PlanChangeRefusedError, match=_dtype_refusal("str", "int")):
        compute_changes(
            _dtype_int_plan,
            mode=ApplyMode.PLAN,
            lock=lock,
            allow_data_validated_changes=True,
        )


def test_narrowing_dtype_migration_rewrites_blocking_values(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A declared `ctx.migrate` unblocks a narrowing the data would otherwise
    # refuse: the engine flips the dtype and the migration rewrites the value
    # that does not parse as an int, all inside one apply.
    from lamindb.models import JsonValue

    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    JsonValue(feature=feat, value="42").save()
    JsonValue(feature=feat, value="notanint").save()

    # Without the migration the narrowing is refused (a value does not validate).
    with pytest.raises(PlanChangeRefusedError, match=_dtype_refusal("str", "int")):
        compute_changes(_dtype_int_plan, mode=ApplyMode.PLAN, lock=lock)

    ctx = compute_changes(_dtype_int_migrate_plan, mode=ApplyMode.APPLY, lock=lock)
    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.UPDATE
    assert any("via coerce_non_ints" in d for d in change.details)
    assert ln.Feature.get(name="dtype_col", is_type=False).dtype_as_str == "int"
    # The blocking value was rewritten; the already-valid one was left alone.
    assert sorted(v.value for v in feat.values.all()) == ["0", "42"]


def test_narrowing_dtype_migration_not_run_in_plan_mode(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # PLAN mode previews the migration but mutates nothing: the dtype and the
    # blocking value are untouched.
    from lamindb.models import JsonValue

    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    JsonValue(feature=feat, value="notanint").save()

    ctx = compute_changes(_dtype_int_migrate_plan, mode=ApplyMode.PLAN, lock=lock)
    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any("via coerce_non_ints" in d for d in change.details)
    assert ln.Feature.get(name="dtype_col", is_type=False).dtype_as_str == "str"
    assert sorted(v.value for v in feat.values.all()) == ["notanint"]


def test_narrowing_dtype_migration_is_idempotent_after_it_ran(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Once the live dtype matches the plan, the dtype change -- and hence the
    # migration -- is skipped: re-applying is a no-op and does not re-run it.
    from lamindb.models import JsonValue

    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    JsonValue(feature=feat, value="notanint").save()

    ctx = compute_changes(_dtype_int_migrate_plan, mode=ApplyMode.APPLY, lock=lock)
    lock = _persist_lock(ctx, _dtype_int_migrate_plan, tmp_path / "dtype.lock")

    again = compute_changes(_dtype_int_migrate_plan, mode=ApplyMode.APPLY, lock=lock)
    change = {c.kind: c for c in again.changes}["Feature"]
    assert change.action is Action.NO_CHANGE
    assert sorted(v.value for v in feat.values.all()) == ["0"]


def test_plan_context_migrate_records_declaration() -> None:
    migration = DataMigration(name="m", migrate=lambda context: None)

    @create_plan
    def _plan(ctx: PlanContext, exports: None) -> None:
        ft = FeatureType("mig_ft")
        feature = Feature("mig_col", dtype=LaminDType(int), type=ft)
        ctx.migrate(feature, migration)

    _plan.finalize()

    assert [m.migration for m in _plan.ctx.migrations] == [migration]


def test_narrowing_str_to_record_migration_relocates_values(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # str -> cat[Record] is the flagship migration: the stored scalar values must
    # be relocated from RecordJson to the categorical RecordRecord link table, and
    # target Compound records created for each distinct value -- all in one apply.
    applied = compute_changes(_treatment_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _treatment_str_plan, tmp_path / "trt.lock")
    sheet = ln.Record(name="TrialSheet", is_type=True).save()
    ln.Record(name="s1", type=sheet, features={"treatment": "aspirin"}).save()
    ln.Record(name="s2", type=sheet, features={"treatment": "ibuprofen"}).save()
    s3 = ln.Record(name="s3", type=sheet, features={"treatment": "aspirin"}).save()

    ctx = compute_changes(
        _treatment_record_migrate_plan, mode=ApplyMode.APPLY, lock=lock
    )

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.UPDATE
    assert any("via treatment_to_compound" in d for d in change.details)

    feat = ln.Feature.get(name="treatment", is_type=False)
    # The dtype is now a Record categorical constrained to the Compound type.
    assert feat.dtype_as_str.startswith("cat[Record[")
    # Every scalar value was relocated: no RecordJson rows remain.
    assert list(feat.links_recordjson.values_list("value", flat=True)) == []
    # The records now carry the categorical value, resolvable back to the label.
    assert s3.features.get_values()["treatment"] == "aspirin"
    assert ln.Record.filter(name="aspirin", type__name="Compound").count() == 1


def test_narrowing_dtype_with_values_still_blocked_without_flag(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Even when all values would validate, the override is opt-in: without the
    # flag a str -> int narrowing stays blocked.
    from lamindb.models import JsonValue

    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    JsonValue(feature=feat, value="1").save()

    with pytest.raises(PlanChangeRefusedError, match=_dtype_refusal("str", "int")):
        compute_changes(_dtype_int_plan, mode=ApplyMode.PLAN, lock=lock)


def test_data_safety_evaluates_values_in_all_spaces(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A feature's values may live in any space, including one an operation would
    # exclude. They must still be evaluated: a non-numeric value stored in a
    # non-default space keeps the str -> int narrowing blocked even with the flag.
    # (If the check were scoped to the default space it would see no values, fire
    # the no-values override, and wrongly allow the narrowing.)
    from lamindb.models import JsonValue

    ln.Space(name="schemas-explicit", uid="spaceUID0002").save()
    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")

    feat = ln.Feature.get(name="dtype_col", is_type=False)
    other = ln.Space.get(uid="spaceUID0002")
    value = JsonValue(feature=feat, value="notanint")
    value.space = other
    value.save()

    with pytest.raises(PlanChangeRefusedError, match=_dtype_refusal("str", "int")):
        compute_changes(
            _dtype_int_plan,
            mode=ApplyMode.PLAN,
            lock=lock,
            allow_data_validated_changes=True,
        )


def test_blocked_change_reports_override_hint_when_no_values(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Without the flag a float -> int narrowing is blocked, but the feature has
    # no stored values, so the report tells the operator the flag would unblock
    # it (issue: "plan should report if a change could be unblocked").
    applied = compute_changes(_dtype_float_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_float_plan, tmp_path / "dtype.lock")

    with pytest.raises(DriftError) as excinfo:
        compute_changes(_dtype_int_plan, mode=ApplyMode.PLAN, lock=lock)
    assert (
        "could be applied with --allow-data-validated-changes: "
        "feature has no stored values" in str(excinfo.value)
    )


def test_blocked_change_reports_override_hint_via_reconcile(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # With no lock the block surfaces from _reconcile (plan vs live instance).
    # The empty feature already exists as str; the hint still fires for str -> int.
    compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)

    with pytest.raises(PlanChangeRefusedError) as excinfo:
        compute_changes(_dtype_int_plan, mode=ApplyMode.PLAN, lock=None)
    message = str(excinfo.value)
    assert "dtype 'str' \u2192 'int'" in message
    assert (
        "could be applied with --allow-data-validated-changes: "
        "feature has no stored values" in message
    )


def test_blocked_change_hint_reports_value_validation(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The hint also fires when the feature has values but all of them validate
    # against the new dtype: every stored string parses as an int. The hint
    # reports how many values are attached.
    from lamindb.models import JsonValue

    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    JsonValue(feature=feat, value="1").save()
    JsonValue(feature=feat, value="2").save()

    with pytest.raises(DriftError) as excinfo:
        compute_changes(_dtype_int_plan, mode=ApplyMode.PLAN, lock=lock)
    assert (
        "could be applied with --allow-data-validated-changes: "
        "all 2 stored values validate against the new dtype" in str(excinfo.value)
    )


def test_blocked_change_reports_no_hint_when_data_unsafe(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A genuinely unsafe change (a non-numeric stored value) offers no opt-in
    # hint -- the flag would not help -- but reports how many values block it.
    from lamindb.models import JsonValue

    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    JsonValue(feature=feat, value="notanint").save()

    with pytest.raises(DriftError) as excinfo:
        compute_changes(_dtype_int_plan, mode=ApplyMode.PLAN, lock=lock)
    message = str(excinfo.value)
    assert "could be applied with --allow-data-validated-changes" not in message
    assert (
        "1 of 1 stored value does not validate against the new dtype; "
        "invalid value: 'notanint'" in message
    )


def test_blocked_change_reports_no_hint_when_flag_already_set(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # With the flag already on, a change that is still blocked is genuinely
    # data-unsafe, so suggesting the flag would be misleading: no opt-in hint,
    # but the failing-value count is still reported.
    from lamindb.models import JsonValue

    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    JsonValue(feature=feat, value="notanint").save()
    JsonValue(feature=feat, value="7").save()

    with pytest.raises(DriftError) as excinfo:
        compute_changes(
            _dtype_int_plan,
            mode=ApplyMode.PLAN,
            lock=lock,
            allow_data_validated_changes=True,
        )
    message = str(excinfo.value)
    assert "could be applied with --allow-data-validated-changes" not in message
    assert (
        "1 of 2 stored values does not validate against the new dtype; "
        "invalid value: 'notanint'" in message
    )


def test_blocked_change_reports_distinct_examples_of_failing_values(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Many failing rows: the totals still lead the report, but the operator also
    # gets the distinct offenders (capped at five examples) so they can see what
    # actually blocks the change instead of only how much of it does.
    from lamindb.models import JsonValue

    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    for value in ["a", "b", "c", "d", "e", "f", "a", "b", "9"]:
        JsonValue(feature=feat, value=value).save()

    with pytest.raises(DriftError) as excinfo:
        compute_changes(_dtype_int_plan, mode=ApplyMode.PLAN, lock=lock)
    message = str(excinfo.value)
    assert (
        "8 of 9 stored values do not validate against the new dtype; "
        "6 distinct invalid values, e.g. " in message
    )
    examples = message.split("6 distinct invalid values, e.g. ")[1].split(")")[0]
    # Five of the six distinct offenders, each rendered once (row order is up to
    # the database, so only the count and the membership are asserted).
    assert len(examples.split(", ")) == 5
    assert set(examples.split(", ")) < {"'a'", "'b'", "'c'", "'d'", "'e'", "'f'"}


def test_str_to_url_dtype_change_is_blocked_as_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Reproduces the reported drift: a Feature 'signal_link' under FeatureType
    # 'PooledScreen_feature_type' exists in the instance as dtype 'str', but the
    # plan now wants 'url'. The pure taxonomy refuses str -> url as a narrowing,
    # so it blocks by default. lamindb validates 'url' as a string, though, so a
    # stored string already validates against the new dtype: the drift reports
    # that --allow-data-validated-changes would apply it in place.
    from lamindb.models import JsonValue

    compute_changes(_pooled_screen_str_plan, mode=ApplyMode.APPLY, lock=None)
    feat = ln.Feature.get(name="signal_link", is_type=False)
    assert feat.dtype_as_str == "str"
    JsonValue(feature=feat, value="https://example.com/signal").save()

    # No lock: the block surfaces from _reconcile (plan vs live instance), and
    # since nothing records the instance changing, it is the plan's change.
    with pytest.raises(PlanChangeRefusedError) as excinfo:
        compute_changes(_pooled_screen_url_plan, mode=ApplyMode.PLAN, lock=None)
    message = str(excinfo.value)
    assert message.startswith(
        "cannot change Feature 'signal_link' "
        "(FeatureType:PooledScreen_feature_type/Feature:signal_link) in place:\n"
        "  dtype 'str' \u2192 'url'"
    )
    assert "drift" not in message
    # The stored string validates as a url, so the flag would apply it in place.
    assert (
        "could be applied with --allow-data-validated-changes: "
        "all 1 stored value validate against the new dtype" in message
    )


def test_str_to_url_dtype_change_reports_override_hint_when_no_values(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Same str -> url narrowing as above, but the feature has no stored values,
    # so the flag would apply the change safely. The drift must still block by
    # default, yet tell the operator that --allow-data-validated-changes unblocks
    # it (issue: "plan should report if a change could be unblocked").
    compute_changes(_pooled_screen_str_plan, mode=ApplyMode.APPLY, lock=None)
    feat = ln.Feature.get(name="signal_link", is_type=False)
    assert feat.dtype_as_str == "str"

    # No lock: the block surfaces from _reconcile (plan vs live instance).
    with pytest.raises(PlanChangeRefusedError) as excinfo:
        compute_changes(_pooled_screen_url_plan, mode=ApplyMode.PLAN, lock=None)
    message = str(excinfo.value)
    assert "dtype 'str' \u2192 'url'" in message
    assert (
        "could be applied with --allow-data-validated-changes: "
        "feature has no stored values" in message
    )


def test_str_to_url_dtype_change_applies_with_flag(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # With --allow-data-validated-changes the str -> url narrowing applies in
    # place: lamindb validates a url as a string, so the stored string value is
    # already valid under the new dtype and no record is invalidated.
    from lamindb.models import JsonValue

    compute_changes(_pooled_screen_str_plan, mode=ApplyMode.APPLY, lock=None)
    feat = ln.Feature.get(name="signal_link", is_type=False)
    JsonValue(feature=feat, value="https://example.com/signal").save()

    compute_changes(
        _pooled_screen_url_plan,
        mode=ApplyMode.APPLY,
        lock=None,
        allow_data_validated_changes=True,
    )
    feat = ln.Feature.get(name="signal_link", is_type=False)
    assert feat.dtype_as_str == "url"


def test_cat_to_str_widening_is_applied(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # cat[ULabel] -> str is a widening: every categorical value is stored as its
    # label string, so the dtype is relaxed to a free string in place -- no
    # opt-in flag needed.
    _dtype_cat_plan.reset()  # re-finalize against this test's instance
    applied = compute_changes(_dtype_cat_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_cat_plan, tmp_path / "dtype.lock")
    assert ln.Feature.get(name="dtype_col", is_type=False).dtype_as_str == "cat[ULabel]"

    ctx = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=lock)
    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.UPDATE
    assert ln.Feature.get(name="dtype_col", is_type=False).dtype_as_str == "str"


def test_cat_to_str_widening_blocked_when_values_need_relocation(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # cat[ULabel] -> str is revalidation-safe, but a stored categorical value lives
    # in a per-registry link table an in-place flip would not relocate, so the
    # widening is refused (SAFE only holds when there is nothing to relocate).
    _dtype_cat_plan.reset()
    applied = compute_changes(_dtype_cat_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_cat_plan, tmp_path / "dtype.lock")

    label = ln.ULabel(name="aspirin").save()
    sheet = ln.Record(name="CatSheet", is_type=True).save()
    rec = ln.Record(name="c1", type=sheet).save()
    rec.features.set_values({"dtype_col": label})

    with pytest.raises(DriftError) as exc:
        compute_changes(_dtype_str_plan, mode=ApplyMode.PLAN, lock=lock)
    assert "relocated" in str(exc.value)
    # Nothing was mutated -- the feature is still categorical.
    assert ln.Feature.get(name="dtype_col", is_type=False).dtype_as_str == "cat[ULabel]"


def test_cat_to_str_widening_blocked_without_lock_when_values_exist(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The same guard applies on the no-lock (plan vs live) path in _reconcile, not
    # only the config-drift (plan vs lock) path.
    _dtype_cat_plan.reset()
    compute_changes(_dtype_cat_plan, mode=ApplyMode.APPLY, lock=None)
    label = ln.ULabel(name="aspirin").save()
    sheet = ln.Record(name="CatSheet", is_type=True).save()
    rec = ln.Record(name="c1", type=sheet).save()
    rec.features.set_values({"dtype_col": label})

    with pytest.raises(DriftError) as exc:
        compute_changes(_dtype_str_plan, mode=ApplyMode.PLAN, lock=None)
    assert "relocated" in str(exc.value)


def test_cat_record_to_str_widening_blocked_when_values_exist(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # cat[Record] -> str: the value lives in the RecordRecord link table, which the
    # generic categorical-value detection must catch just as it does for ULabel.
    _dtype_cat_record_plan.reset()
    applied = compute_changes(_dtype_cat_record_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_cat_record_plan, tmp_path / "dtype.lock")
    assert ln.Feature.get(name="dtype_col", is_type=False).dtype_as_str.startswith(
        "cat[Record["
    )

    compound_type = ln.Record.get(name="DtypeCompound", is_type=True)
    value = ln.Record(name="aspirin", type=compound_type).save()
    sheet = ln.Record(name="CatSheet", is_type=True).save()
    rec = ln.Record(name="c1", type=sheet).save()
    rec.features.set_values({"dtype_col": value})

    with pytest.raises(DriftError) as exc:
        compute_changes(_dtype_str_plan, mode=ApplyMode.PLAN, lock=lock)
    assert "relocated" in str(exc.value)
    assert ln.Feature.get(name="dtype_col", is_type=False).dtype_as_str.startswith(
        "cat[Record["
    )


def test_cat_project_to_str_widening_blocked_when_values_exist(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # cat[Project] -> str: a plain (non-ULabel, non-Record) lamindb registry whose
    # values live in the RecordProject link table.
    _dtype_cat_project_plan.reset()
    applied = compute_changes(_dtype_cat_project_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_cat_project_plan, tmp_path / "dtype.lock")
    assert (
        ln.Feature.get(name="dtype_col", is_type=False).dtype_as_str == "cat[Project]"
    )

    project = ln.Project(name="proj-a").save()
    sheet = ln.Record(name="CatSheet", is_type=True).save()
    rec = ln.Record(name="c1", type=sheet).save()
    rec.features.set_values({"dtype_col": project})

    with pytest.raises(DriftError) as exc:
        compute_changes(_dtype_str_plan, mode=ApplyMode.PLAN, lock=lock)
    assert "relocated" in str(exc.value)
    assert (
        ln.Feature.get(name="dtype_col", is_type=False).dtype_as_str == "cat[Project]"
    )


@pytest.mark.parametrize("instance", ["bionty"], indirect=True)
def test_cat_celltype_to_str_widening_blocked_when_values_exist(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    bt: BiontyModuleType,
    tmp_path: Path,
) -> None:
    # cat[bionty.CellType] -> str: a bionty registry whose values live in a bionty
    # link table (RecordCellType), a different family from the core Record* tables.
    _dtype_cat_celltype_plan.reset()
    applied = compute_changes(_dtype_cat_celltype_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_cat_celltype_plan, tmp_path / "dtype.lock")

    ct = bt.CellType(name="T cell").save()
    sheet = ln.Record(name="CatSheet", is_type=True).save()
    rec = ln.Record(name="c1", type=sheet).save()
    rec.features.set_values({"dtype_col": ct})

    with pytest.raises(DriftError) as exc:
        compute_changes(_dtype_str_plan, mode=ApplyMode.PLAN, lock=lock)
    assert "relocated" in str(exc.value)


def test_str_to_project_migration_relocates_values(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # str -> cat[Project] via a declared migration: the stored scalar values are
    # relocated from RecordJson to the RecordProject link table and a Project
    # record created per distinct value -- the same relocation as str -> cat[Record]
    # but for a plain lamindb registry.
    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    sheet = ln.Record(name="ProjSheet", is_type=True).save()
    ln.Record(name="s1", type=sheet, features={"dtype_col": "proj-a"}).save()
    ln.Record(name="s2", type=sheet, features={"dtype_col": "proj-b"}).save()
    s3 = ln.Record(name="s3", type=sheet, features={"dtype_col": "proj-a"}).save()

    ctx = compute_changes(_dtype_project_migrate_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.UPDATE
    assert any("via dtype_to_project" in d for d in change.details)

    feat = ln.Feature.get(name="dtype_col", is_type=False)
    assert feat.dtype_as_str == "cat[Project]"
    # Every scalar value was relocated: no RecordJson rows remain.
    assert list(feat.links_recordjson.values_list("value", flat=True)) == []
    assert s3.features.get_values()["dtype_col"] == "proj-a"
    assert ln.Project.filter(name="proj-a").count() == 1


def test_preview_migrations_dry_run_reports_relocation(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `plan --preview-migrations` runs the declared migration in a rolled-back
    # savepoint to *prove* it relocates every value, reporting the outcome in the
    # diff -- and persisting nothing.
    _dtype_str_plan.reset()
    _dtype_project_migrate_plan.reset()
    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    sheet = ln.Record(name="ProjSheet", is_type=True).save()
    ln.Record(name="s1", type=sheet, features={"dtype_col": "proj-a"}).save()
    ln.Record(name="s2", type=sheet, features={"dtype_col": "proj-b"}).save()

    ctx = compute_changes(
        _dtype_project_migrate_plan,
        mode=ApplyMode.PLAN,
        lock=lock,
        preview_migrations=True,
    )

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any(
        "dry-run: dtype_to_project relocated all values, none stranded" in d
        for d in change.details
    )
    # The dry-run rolled back: the dtype, the scalar values, and the Project
    # registry are all untouched.
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    assert feat.dtype_as_str == "str"
    assert sorted(feat.links_recordjson.values_list("value", flat=True)) == [
        "proj-a",
        "proj-b",
    ]
    assert ln.Project.filter(name="proj-a").count() == 0


def test_preview_migrations_off_by_default_skips_dry_run(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Without `--preview-migrations` a PLAN diff describes the migration's intent
    # but never runs it: no dry-run note, and nothing is created.
    _dtype_str_plan.reset()
    _dtype_project_migrate_plan.reset()
    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    sheet = ln.Record(name="ProjSheet", is_type=True).save()
    ln.Record(name="s1", type=sheet, features={"dtype_col": "proj-a"}).save()

    ctx = compute_changes(_dtype_project_migrate_plan, mode=ApplyMode.PLAN, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any("via dtype_to_project" in d for d in change.details)
    assert not any("dry-run" in d for d in change.details)
    assert ln.Project.filter(name="proj-a").count() == 0


def test_apply_migration_leaving_stranded_values_raises(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A migration that flips the dtype but fails to relocate every value leaves
    # rows stranded in the old store; apply raises `MigrationError` so the
    # enclosing transaction rolls the half-migrated state back.
    from django.db import transaction

    _dtype_str_plan.reset()
    _dtype_project_noop_migrate_plan.reset()
    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    sheet = ln.Record(name="ProjSheet", is_type=True).save()
    ln.Record(name="s1", type=sheet, features={"dtype_col": "proj-a"}).save()

    with pytest.raises(MigrationError, match="stranded"):
        with transaction.atomic():
            compute_changes(
                _dtype_project_noop_migrate_plan, mode=ApplyMode.APPLY, lock=lock
            )

    # The failed apply rolled back: the feature is still `str` with its value.
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    assert feat.dtype_as_str == "str"
    assert sorted(feat.links_recordjson.values_list("value", flat=True)) == ["proj-a"]


def test_preview_migrations_dry_run_reports_failure(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # When the dry-run migration itself fails (e.g. its target is not available in
    # PLAN mode), the diff reports the failure instead of claiming success -- and
    # still persists nothing.
    _dtype_str_plan.reset()
    _dtype_project_raises_migrate_plan.reset()
    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    sheet = ln.Record(name="ProjSheet", is_type=True).save()
    ln.Record(name="s1", type=sheet, features={"dtype_col": "proj-a"}).save()

    ctx = compute_changes(
        _dtype_project_raises_migrate_plan,
        mode=ApplyMode.PLAN,
        lock=lock,
        preview_migrations=True,
    )

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any("dry-run: migration would fail" in d for d in change.details)
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    assert feat.dtype_as_str == "str"
    assert sorted(feat.links_recordjson.values_list("value", flat=True)) == ["proj-a"]


@pytest.mark.parametrize("instance", ["bionty"], indirect=True)
def test_str_to_celltype_migration_relocates_values(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    bt: BiontyModuleType,
    tmp_path: Path,
) -> None:
    # str -> cat[bionty.CellType] via a declared migration: scalar values are
    # relocated from RecordJson to the bionty CellType link table, exercising the
    # relocation path for a bionty registry.
    _dtype_celltype_migrate_plan.reset()
    _dtype_str_plan.reset()
    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    sheet = ln.Record(name="CtSheet", is_type=True).save()
    ln.Record(name="s1", type=sheet, features={"dtype_col": "T cell"}).save()
    s2 = ln.Record(name="s2", type=sheet, features={"dtype_col": "B cell"}).save()

    ctx = compute_changes(_dtype_celltype_migrate_plan, mode=ApplyMode.APPLY, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.UPDATE
    assert any("via dtype_to_celltype" in d for d in change.details)

    feat = ln.Feature.get(name="dtype_col", is_type=False)
    assert feat.dtype_as_str.startswith("cat[bionty.CellType")
    assert list(feat.links_recordjson.values_list("value", flat=True)) == []
    assert s2.features.get_values()["dtype_col"] == "B cell"
    assert bt.CellType.filter(name="B cell").count() == 1


def test_cat_ulabel_to_str_migration_relocates_values(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # cat[ULabel] -> str is revalidation-safe but held back when categorical values
    # exist (they live in RecordULabel, which an in-place flip does not touch). A
    # declared migration relocates those links into the scalar store, so the
    # widening applies instead of raising drift.
    _dtype_cat_plan.reset()
    _dtype_str_from_cat_migrate_plan.reset()
    applied = compute_changes(_dtype_cat_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_cat_plan, tmp_path / "dtype.lock")

    label = ln.ULabel(name="aspirin").save()
    sheet = ln.Record(name="CatSheet", is_type=True).save()
    rec = ln.Record(name="c1", type=sheet).save()
    rec.features.set_values({"dtype_col": label})

    ctx = compute_changes(
        _dtype_str_from_cat_migrate_plan, mode=ApplyMode.APPLY, lock=lock
    )

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.UPDATE
    assert any("via ulabel_to_str" in d for d in change.details)

    feat = ln.Feature.get(name="dtype_col", is_type=False)
    assert feat.dtype_as_str == "str"
    # The categorical links were relocated to the scalar store.
    assert list(feat.links_recordulabel.all()) == []
    assert rec.features.get_values()["dtype_col"] == "aspirin"


def test_str_to_project_with_label_values_waits_for_relocation(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Every stored value is a Project label, but it is stored as a string, which
    # lamindb refuses to read under a categorical dtype: even with the opt-in
    # flag the change waits for a migration that relocates the values.
    from lamindb.models import JsonValue

    _dtype_cat_project_plan.reset()
    ln.Project(name="proj-a").save()
    ln.Project(name="proj-b").save()

    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    JsonValue(feature=feat, value="proj-a").save()
    JsonValue(feature=feat, value="proj-b").save()

    with pytest.raises(DriftError) as excinfo:
        compute_changes(
            _dtype_cat_project_plan,
            mode=ApplyMode.APPLY,
            lock=lock,
            allow_data_validated_changes=True,
        )
    assert (
        "all 2 stored values are labels the new dtype admits, but scalar values "
        "must be relocated to the categorical store; declare a ctx.migrate "
        "(e.g. map_to_records()) to do so" in str(excinfo.value)
    )
    assert ln.Feature.get(name="dtype_col", is_type=False).dtype_as_str == "str"


@pytest.mark.parametrize("instance", ["bionty"], indirect=True)
def test_str_to_celltype_with_label_values_waits_for_relocation(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    bt: BiontyModuleType,
    tmp_path: Path,
) -> None:
    # Every stored value is a CellType label, yet the strings must be relocated
    # into the CellType link table, so the flag does not flip the dtype in place.
    from lamindb.models import JsonValue

    _dtype_cat_celltype_plan.reset()
    _dtype_str_plan.reset()
    bt.CellType(name="T cell").save()
    bt.CellType(name="B cell").save()

    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    JsonValue(feature=feat, value="T cell").save()
    JsonValue(feature=feat, value="B cell").save()

    with pytest.raises(DriftError, match="all 2 stored values are labels"):
        compute_changes(
            _dtype_cat_celltype_plan,
            mode=ApplyMode.APPLY,
            lock=lock,
            allow_data_validated_changes=True,
        )
    assert ln.Feature.get(name="dtype_col", is_type=False).dtype_as_str == "str"


@pytest.mark.parametrize("instance", ["bionty"], indirect=True)
def test_declarative_gene_categorical_roundtrips(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    bt: BiontyModuleType,
    tmp_path: Path,
) -> None:
    # A declarative `cat[bionty.Gene.symbol]` feature must roundtrip cleanly:
    # plan -> instance -> lock, then a re-apply of the same plan is a no-op.
    # lamindb stores a categorical dtype in its fully-qualified form (with the
    # name field), so the plan's dtype string and the instance's must agree for
    # the re-apply to report NO_CHANGE rather than drift.
    _gene_cat_plan.reset()  # re-finalize against this test's instance

    applied = compute_changes(_gene_cat_plan, mode=ApplyMode.APPLY, lock=None)
    by_kind = {c.kind: c for c in applied.changes}
    assert by_kind["Feature"].action is Action.CREATE

    feat = ln.Feature.get(name="genetic_perturbation", is_type=False)
    assert feat.dtype_as_str == "cat[bionty.Gene.symbol]"
    assert feat.nullable is True

    lock = _persist_lock(applied, _gene_cat_plan, tmp_path / "gene.lock")

    # Re-apply the very same plan: no drift, nothing to change.
    reapplied = compute_changes(_gene_cat_plan, mode=ApplyMode.APPLY, lock=lock)
    assert {c.action for c in reapplied.changes} == {Action.NO_CHANGE}


def test_record_categorical_with_type_uid_roundtrips(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A Record categorical with a type__uid filter must roundtrip cleanly: the
    # plan's bare cat[Record] and the instance's inlined cat[Record[<uid>]] denote
    # the same dtype (the type constraint is tracked in cat_filters), so a
    # re-apply is a no-op rather than drift.
    _record_cat_plan.reset()
    applied = compute_changes(_record_cat_plan, mode=ApplyMode.APPLY, lock=None)
    feat = ln.Feature.get(name="biosample_type", is_type=False)
    assert feat.dtype_as_str.startswith("cat[Record[")

    lock = _persist_lock(applied, _record_cat_plan, tmp_path / "rec.lock")

    reapplied = compute_changes(_record_cat_plan, mode=ApplyMode.APPLY, lock=lock)
    assert {c.action for c in reapplied.changes} == {Action.NO_CHANGE}


def test_list_record_categorical_with_type_uid_roundtrips(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A list of a Record categorical: lamindb's `cat_filters=` keyword refuses
    # nested dtypes, so the type uid is inlined into the dtype string instead.
    # The stored dtype still canonicalizes to the plan's bare list[cat[Record]],
    # so a re-apply is a no-op.
    _list_record_cat_plan.reset()
    applied = compute_changes(_list_record_cat_plan, mode=ApplyMode.APPLY, lock=None)
    rt = ln.Record.get(name="ListBioSampleType", is_type=True)
    feat = ln.Feature.get(name="biosample_types", is_type=False)
    assert feat._dtype_str == f"list[cat[Record[{rt.uid}]]]"

    lock = _persist_lock(applied, _list_record_cat_plan, tmp_path / "listrec.lock")

    reapplied = compute_changes(_list_record_cat_plan, mode=ApplyMode.APPLY, lock=lock)
    assert {c.action for c in reapplied.changes} == {Action.NO_CHANGE}


def test_str_to_cat_is_blocked_without_flag(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # str -> cat[ULabel] is refused: an arbitrary string is not guaranteed to be
    # a registered label. Without the opt-in flag it stays blocked.
    _dtype_cat_plan.reset()  # re-finalize against this test's instance
    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")

    with pytest.raises(
        PlanChangeRefusedError, match=_dtype_refusal("str", "cat[ULabel]")
    ):
        compute_changes(_dtype_cat_plan, mode=ApplyMode.PLAN, lock=lock)


def test_str_to_cat_with_label_values_waits_for_relocation(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Every stored value is a ULabel label, but flipping the dtype in place would
    # leave the strings where a categorical dtype does not read them -- lamindb
    # then refuses to read the record's values at all.  So the flag does not lift
    # it; a migration relocating the values does.
    from lamindb.models import JsonValue

    _dtype_cat_plan.reset()  # re-finalize against this test's instance
    ln.ULabel(name="alpha").save()
    ln.ULabel(name="beta").save()

    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    JsonValue(feature=feat, value="alpha").save()
    JsonValue(feature=feat, value="beta").save()

    with pytest.raises(DriftError, match="all 2 stored values are labels"):
        compute_changes(
            _dtype_cat_plan,
            mode=ApplyMode.APPLY,
            lock=lock,
            allow_data_validated_changes=True,
        )
    assert ln.Feature.get(name="dtype_col", is_type=False).dtype_as_str == "str"


def test_str_to_cat_blocked_when_a_value_is_not_a_label(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A single value with no matching ULabel keeps the str -> cat[ULabel] change
    # blocked even with the opt-in flag: converting it would strand that value.
    from lamindb.models import JsonValue

    _dtype_cat_plan.reset()  # re-finalize against this test's instance
    ln.ULabel(name="alpha").save()

    applied = compute_changes(_dtype_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_str_plan, tmp_path / "dtype.lock")
    feat = ln.Feature.get(name="dtype_col", is_type=False)
    JsonValue(feature=feat, value="alpha").save()
    JsonValue(feature=feat, value="not-a-label").save()

    with pytest.raises(
        PlanChangeRefusedError, match=_dtype_refusal("str", "cat[ULabel]")
    ) as excinfo:
        compute_changes(
            _dtype_cat_plan,
            mode=ApplyMode.PLAN,
            lock=lock,
            allow_data_validated_changes=True,
        )
    assert (
        "1 of 2 stored values is not a label the new dtype admits, invalid value: "
        "'not-a-label'; scalar values must be relocated to the categorical store; "
        "declare a ctx.migrate (e.g. map_to_records(mapping=...)) to do so"
        in str(excinfo.value)
    )


def test_widening_list_element_edit_is_applied(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A list keeps its shape while its element type widens (list[int] ->
    # list[float]): every stored element stays valid, so it is applied in place.
    applied = compute_changes(_list_int_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _list_int_plan, tmp_path / "list.lock")

    ctx = compute_changes(_list_float_plan, mode=ApplyMode.APPLY, lock=lock)
    feat = {c.kind: c for c in ctx.changes}["Feature"]
    assert feat.action is Action.UPDATE
    assert ln.Feature.get(name="list_col", is_type=False).dtype_as_str == "list[float]"


def test_listify_dtype_edit_is_blocked(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Changing list-ness (scalar int -> list[int]) is refused: an existing scalar
    # value is not itself a list, so it may require a data change.
    applied = compute_changes(_dtype_int_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_int_plan, tmp_path / "dtype.lock")

    with pytest.raises(
        PlanChangeRefusedError, match=_dtype_refusal("int", "list[int]")
    ):
        compute_changes(_dtype_list_int_plan, mode=ApplyMode.PLAN, lock=lock)


def test_future_only_coerce_edit_is_applied(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # coerce only affects how future values are ingested, so a change in either
    # direction is safe and applied in place.
    applied = compute_changes(_coerce_off_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _coerce_off_plan, tmp_path / "coerce.lock")

    ctx = compute_changes(_coerce_on_plan, mode=ApplyMode.APPLY, lock=lock)
    feat = {c.kind: c for c in ctx.changes}["Feature"]
    assert feat.action is Action.UPDATE
    assert ln.Feature.get(name="coerce_col", is_type=False).coerce is True


def test_apply_then_reapply_records_snapshot_and_has_no_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    first = compute_changes(_apply_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _write_and_read_lock(first, tmp_path / "apply_plan.lock")

    # every recorded object carries a remote snapshot + hash baseline
    for entry in lock.objects.values():
        assert entry.remote_snapshot is not None
        assert entry.remote_hash is not None

    second = compute_changes(_apply_plan, mode=ApplyMode.APPLY, lock=lock)
    assert {c.action for c in second.changes} == {Action.NO_CHANGE}


def test_plan_edit_of_descriptive_field_is_allowed_with_lock(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_descriptive_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _descriptive_v1_plan, tmp_path / "drift.lock")

    # Editing only descriptive fields in the plan is an intentional change and
    # must be allowed (and pushed) rather than blocked as drift.
    ctx = compute_changes(_descriptive_v2_plan, mode=ApplyMode.APPLY, lock=lock)
    feat = {c.kind: c for c in ctx.changes}["Feature"]
    assert feat.action is Action.UPDATE
    assert ln.Feature.get(name="drift_col", is_type=False).description == "second"


def test_plan_edit_of_structural_field_is_blocked_with_lock(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_nullable_true_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _nullable_true_plan, tmp_path / "null.lock")

    with pytest.raises(PlanChangeRefusedError, match="nullable True \u2192 False"):
        compute_changes(_nullable_false_plan, mode=ApplyMode.PLAN, lock=lock)


def test_plan_edit_of_structural_field_matching_instance_refreshes_lock(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The instance already holds the plan's desired nullable=True, but the lock
    # baseline is stale (as if it predated tracking the property, so it never
    # recorded `nullable`).  Applying the plan changes nothing on the
    # instance, so it must be accepted and the lock refreshed rather than
    # aborting with configuration drift.
    applied = compute_changes(_nullable_true_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _nullable_true_plan, tmp_path / "null.lock")

    entry = next(e for e in lock.objects.values() if "nullable" in e.config)
    del entry.config["nullable"]

    ctx = compute_changes(_nullable_true_plan, mode=ApplyMode.PLAN, lock=lock)
    feat = {c.kind: c for c in ctx.changes}["Feature"]
    assert feat.action is Action.NO_CHANGE


def test_remote_structural_change_raises_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_nullable_true_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _nullable_true_plan, tmp_path / "null.lock")

    # Someone changes a validation-affecting property directly on the instance.
    feat = ln.Feature.get(name="null_col", is_type=False)
    feat.nullable = False
    feat.save()

    with pytest.raises(DriftError, match="remote drift"):
        compute_changes(_nullable_true_plan, mode=ApplyMode.PLAN, lock=lock)


def test_remote_nullable_narrowing_raises_drift_when_plan_leaves_it_unset(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A plan that says nothing about `nullable` still tracks it: LaminDB's
    # default (`True`) is captured in the lock's remote snapshot at apply
    # time, so flipping the feature to `nullable=False` by hand on the
    # instance is a narrowing that must be refused as remote drift.
    applied = compute_changes(_nullable_unset_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _nullable_unset_plan, tmp_path / "null.lock")

    entry = next(e for e in lock.objects.values() if e.kind == "Feature")
    assert "nullable" not in entry.config  # the plan has no opinion ...
    assert entry.remote_snapshot is not None
    assert entry.remote_snapshot["nullable"] is True  # ... the instance does

    feat = ln.Feature.get(name="null_col", is_type=False)
    feat.nullable = False
    feat.save()

    with pytest.raises(DriftError, match="remote drift") as excinfo:
        compute_changes(_nullable_unset_plan, mode=ApplyMode.PLAN, lock=lock)
    assert "nullable" in str(excinfo.value)


def test_untracked_nullable_false_is_repaired_when_plan_leaves_it_unset(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # Same instance-side narrowing, but with no lock baseline to compare
    # against (the object was adopted, or the lock was regenerated after the
    # manual edit).  A plan that says nothing about `nullable` still creates
    # features with LaminDB's default (`True`), so it owns that value: the
    # divergence must not pass silently but be widened back in place.
    compute_changes(_nullable_unset_plan, mode=ApplyMode.APPLY, lock=None)
    feat = ln.Feature.get(name="null_col", is_type=False)
    feat.nullable = False
    feat.save()

    ctx = compute_changes(_nullable_unset_plan, mode=ApplyMode.APPLY, lock=None)
    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.UPDATE
    assert ln.Feature.get(name="null_col", is_type=False).nullable is True

    # ... and re-applying is then a clean no-op.
    again = compute_changes(_nullable_unset_plan, mode=ApplyMode.APPLY, lock=None)
    assert {c.kind: c for c in again.changes}["Feature"].action is Action.NO_CHANGE


def test_plan_nullable_false_is_not_overridden_by_the_implicit_default(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # The implicit default only fills an *unset* plan value; a plan that states
    # `nullable=False` keeps it across re-applies.
    compute_changes(_nullable_false_plan, mode=ApplyMode.APPLY, lock=None)
    assert ln.Feature.get(name="null_col", is_type=False).nullable is False

    ctx = compute_changes(_nullable_false_plan, mode=ApplyMode.APPLY, lock=None)
    assert {c.kind: c for c in ctx.changes}["Feature"].action is Action.NO_CHANGE
    assert ln.Feature.get(name="null_col", is_type=False).nullable is False


def test_plan_can_repair_an_instance_side_nullable_false(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # Stating `nullable=True` explicitly in the plan is the repair: False ->
    # True is a widening, so it is pushed in place even without a lock.
    compute_changes(_nullable_unset_plan, mode=ApplyMode.APPLY, lock=None)
    feat = ln.Feature.get(name="null_col", is_type=False)
    feat.nullable = False
    feat.save()

    ctx = compute_changes(_nullable_true_plan, mode=ApplyMode.APPLY, lock=None)
    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.UPDATE
    assert ln.Feature.get(name="null_col", is_type=False).nullable is True


def test_declarative_nullable_narrowing_needs_the_data_override(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # A plain `Feature[str]` annotation now means `nullable=False`, so a
    # schema written under the old semantics wants to narrow True -> False.
    # The taxonomy blocks it, and the refusal names the flag that lifts it --
    # this is the documented migration path for that breaking change.
    compute_changes(_nullable_unset_plan, mode=ApplyMode.APPLY, lock=None)
    assert ln.Feature.get(name="null_col", is_type=False).nullable is True

    with pytest.raises(PlanChangeRefusedError, match="nullable True \u2192 False"):
        compute_changes(_nullable_declarative_plan, mode=ApplyMode.PLAN, lock=None)

    _nullable_declarative_plan.reset()
    ctx = compute_changes(
        _nullable_declarative_plan,
        mode=ApplyMode.APPLY,
        lock=None,
        allow_data_validated_changes=True,
    )
    assert {c.kind: c for c in ctx.changes}["Feature"].action is Action.UPDATE
    assert ln.Feature.get(name="null_col", is_type=False).nullable is False


def test_remote_descriptive_change_warns_and_auto_corrects(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_descriptive_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _descriptive_v1_plan, tmp_path / "drift.lock")

    # Someone edits a descriptive property directly on the instance.
    feat = ln.Feature.get(name="drift_col", is_type=False)
    feat.description = "changed-on-remote"
    feat.save()

    ctx = compute_changes(_descriptive_v1_plan, mode=ApplyMode.APPLY, lock=lock)
    feat_change = {c.kind: c for c in ctx.changes}["Feature"]
    assert feat_change.warnings
    assert any("auto-corrected" in w for w in feat_change.warnings)
    # the remote was snapped back to the plan
    assert ln.Feature.get(name="drift_col", is_type=False).description == "first"


def test_remote_safe_widening_warns_and_is_kept(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A validation-affecting property widened directly on the instance (int ->
    # float) is non-invalidating, so it is kept with a warning rather than
    # raising remote drift, provided the plan is updated to match.
    applied = compute_changes(_dtype_int_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dtype_int_plan, tmp_path / "dtype.lock")

    feat = ln.Feature.get(name="dtype_col", is_type=False)
    feat._dtype_str = "float"
    feat.save()

    ctx = compute_changes(_dtype_float_plan, mode=ApplyMode.PLAN, lock=lock)
    feat_change = {c.kind: c for c in ctx.changes}["Feature"]
    assert any("non-invalidating" in w for w in feat_change.warnings)


def test_remote_drift_is_a_drift_warning_announced_as_such(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_descriptive_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _descriptive_v1_plan, tmp_path / "drift.lock")
    feat = ln.Feature.get(name="drift_col", is_type=False)
    feat.description = "changed-on-remote"
    feat.save()

    preview = compute_changes(_descriptive_v1_plan, mode=ApplyMode.PLAN, lock=lock)

    feat_change = {c.kind: c for c in preview.changes}["Feature"]
    assert [w.category for w in feat_change.warnings] == [WarningCategory.DRIFT]
    banner = (
        "1 object(s) changed outside the plan and will be auto-corrected to the "
        "plan; review the WARNING lines above."
    )
    assert f"! {banner}" in render_changes(preview, title="Plan")
    assert f"> [!WARNING]\n> {banner}" in render_changes_markdown(preview, title="Plan")


def test_removal_reports_delete_and_deletes_object(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_removal_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _removal_v1_plan, tmp_path / "rm.lock")
    assert ln.Feature.filter(name="rm_drop", is_type=False).one_or_none() is not None

    ctx = compute_changes(_removal_v2_plan, mode=ApplyMode.APPLY, lock=lock)
    deletes = [c for c in ctx.changes if c.action is Action.DELETE]
    assert any(c.name == "rm_drop" for c in deletes)
    assert ln.Feature.filter(name="rm_drop", is_type=False).one_or_none() is None
    # the kept object is untouched
    assert ln.Feature.filter(name="rm_keep", is_type=False).one_or_none() is not None


def test_removal_of_an_already_trashed_object_saturates(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Dropping the object from the plan is what a soft-deleted plan object is
    # meant to be resolved with, so the record a removal deletes is routinely in
    # the trash already.  Deleting it again with lamindb's default asks, on
    # stdin, whether to erase it from the database -- and under CI that prompt
    # is an EOFError that rolls the whole apply back.
    applied = compute_changes(_removal_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _removal_v1_plan, tmp_path / "rm.lock")

    trashed = ln.Feature.get(name="rm_drop", is_type=False)
    trashed_uid = str(trashed.uid)
    trashed.delete()

    ctx = compute_changes(_removal_v2_plan, mode=ApplyMode.APPLY, lock=lock)

    deletes = [c for c in ctx.changes if c.action is Action.DELETE]
    assert any(c.name == "rm_drop" for c in deletes)
    # The deletion saturates: it stays in the trash rather than being escalated
    # into an irreversible one.
    assert ln.Feature.filter(uid=trashed_uid, branch_id=-1).one_or_none() is not None


def test_plan_mode_removal_reports_delete_without_deleting(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_removal_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _removal_v1_plan, tmp_path / "rm.lock")

    preview = compute_changes(_removal_v2_plan, mode=ApplyMode.PLAN, lock=lock)
    deletes = [c for c in preview.changes if c.action is Action.DELETE]
    assert any(c.name == "rm_drop" for c in deletes)
    # PLAN mode does not mutate the instance
    assert ln.Feature.filter(name="rm_drop", is_type=False).one_or_none() is not None


def _apply_rm_v1_with_values(ln: LamindbModuleType, tmp_path: Path) -> LockFile:
    """Apply the baseline plan and annotate the doomed feature."""
    from lamindb.models import JsonValue

    applied = compute_changes(_removal_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _removal_v1_plan, tmp_path / "rm.lock")
    feat = ln.Feature.get(name="rm_drop", is_type=False)
    JsonValue(feature=feat, value="d").save()
    return lock


def test_deleting_a_feature_with_stored_values_is_blocked(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Trashing detaches nothing, so the values would outlive the feature: keyed
    # to a trashed record, readable only by name, and not inherited by a feature
    # of the same name declared later.
    lock = _apply_rm_v1_with_values(ln, tmp_path)

    with pytest.raises(DriftError) as excinfo:
        compute_changes(_removal_v2_plan, mode=ApplyMode.PLAN, lock=lock)
    message = str(excinfo.value)
    assert "values are still stored for it" in message
    assert "drop_values()" in message
    assert "keep_values()" in message
    assert _FDEL_KEY in message

    assert ln.Feature.filter(name="rm_drop", is_type=False).one_or_none() is not None


def test_deleting_a_feature_without_values_needs_no_declaration(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The block is about what would be stranded, not about the deletion.
    applied = compute_changes(_removal_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _removal_v1_plan, tmp_path / "rm.lock")

    ctx = compute_changes(_removal_v2_plan, mode=ApplyMode.APPLY, lock=lock)
    assert any(c.action is Action.DELETE and c.name == "rm_drop" for c in ctx.changes)
    assert ln.Feature.filter(name="rm_drop", is_type=False).one_or_none() is None


def test_drop_values_clears_them_before_the_feature_is_deleted(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The declaration runs while the feature is still live, so nothing is left
    # keyed to the trashed record.
    lock = _apply_rm_v1_with_values(ln, tmp_path)

    _feature_delete_dropped.reset()
    ctx = compute_changes(_feature_delete_dropped, mode=ApplyMode.APPLY, lock=lock)
    change = next(c for c in ctx.changes if c.action is Action.DELETE)
    assert change.name == "rm_drop"
    assert any("drop_values" in detail for detail in change.details)

    assert ln.Feature.filter(name="rm_drop", is_type=False).one_or_none() is None
    trashed = ln.Feature.filter(name="rm_drop", branch_id=-1).one()
    assert list(trashed.values.all()) == []


def test_keep_values_deletes_the_feature_and_leaves_the_values(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The acknowledged retention: the values stay, and the acceptance is recorded
    # on the feature so `repair stranded-values` can tell it from an accident.
    lock = _apply_rm_v1_with_values(ln, tmp_path)

    _feature_delete_kept.reset()
    ctx = compute_changes(_feature_delete_kept, mode=ApplyMode.APPLY, lock=lock)
    change = next(c for c in ctx.changes if c.action is Action.DELETE)
    assert any("keep_values" in detail for detail in change.details)

    trashed = ln.Feature.filter(name="rm_drop", branch_id=-1).one()
    assert [v.value for v in trashed.values.all()] == ["d"]
    assert (trashed._aux or {}).get("_valuesRetained") == 1


def test_a_deletion_migration_that_clears_nothing_fails_the_apply(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A declaration is not a promise: the values are re-counted afterwards unless
    # the migration is one that keeps them deliberately.
    from django.db import transaction

    lock = _apply_rm_v1_with_values(ln, tmp_path)

    _feature_delete_noop.reset()
    with pytest.raises(MigrationError, match="left values stored"):
        with transaction.atomic():
            compute_changes(_feature_delete_noop, mode=ApplyMode.APPLY, lock=lock)

    assert ln.Feature.filter(name="rm_drop", is_type=False).one_or_none() is not None


def _apply_cat_v1(ln: LamindbModuleType, tmp_path: Path, *, annotate: bool) -> LockFile:
    """Apply the categorical baseline, optionally annotating the doomed feature."""
    _cat_removal_v1.reset()
    applied = compute_changes(_cat_removal_v1, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _cat_removal_v1, tmp_path / "crm.lock")
    if annotate:
        sheet = ln.Record(name="CrmSheet", is_type=True).save()
        ln.Record(name="crm_owner", type=sheet, features={"crm_drop": ["crm_a"]}).save()
    return lock


def _cat_link_rows() -> int:
    from lamindb.models import RecordRecord

    return RecordRecord.objects.filter(feature__name="crm_drop").count()


def test_deleting_a_categorical_feature_without_values_needs_no_declaration(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _apply_cat_v1(ln, tmp_path, annotate=False)

    _cat_removal_v2.reset()
    ctx = compute_changes(_cat_removal_v2, mode=ApplyMode.APPLY, lock=lock)

    assert any(c.action is Action.DELETE and c.name == "crm_drop" for c in ctx.changes)
    assert ln.Feature.filter(name="crm_drop", is_type=False).one_or_none() is None


def test_deleting_a_categorical_feature_with_stored_values_is_blocked(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _apply_cat_v1(ln, tmp_path, annotate=True)

    _cat_removal_v2.reset()
    with pytest.raises(DriftError, match="values are still stored for it"):
        compute_changes(_cat_removal_v2, mode=ApplyMode.PLAN, lock=lock)


def test_drop_values_lets_a_categorical_feature_be_deleted(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The re-count after the migration has to look at the link rows: presuming a
    # categorical feature holds values failed every such deletion, however
    # thoroughly the migration had cleared it.
    lock = _apply_cat_v1(ln, tmp_path, annotate=True)
    assert _cat_link_rows() == 1

    _cat_removal_dropped.reset()
    ctx = compute_changes(_cat_removal_dropped, mode=ApplyMode.APPLY, lock=lock)

    change = next(c for c in ctx.changes if c.action is Action.DELETE)
    assert change.name == "crm_drop"
    assert any("drop_values" in detail for detail in change.details)
    assert ln.Feature.filter(name="crm_drop", is_type=False).one_or_none() is None
    assert _cat_link_rows() == 0


def test_a_deletion_migration_that_leaves_categorical_values_fails_the_apply(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from django.db import transaction

    lock = _apply_cat_v1(ln, tmp_path, annotate=True)

    _cat_removal_noop.reset()
    with pytest.raises(MigrationError, match="left values stored"):
        with transaction.atomic():
            compute_changes(_cat_removal_noop, mode=ApplyMode.APPLY, lock=lock)

    assert ln.Feature.filter(name="crm_drop", is_type=False).one_or_none() is not None
    assert _cat_link_rows() == 1


def test_plan_mode_reports_the_deletion_declaration_without_running_it(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    lock = _apply_rm_v1_with_values(ln, tmp_path)

    _feature_delete_dropped.reset()
    ctx = compute_changes(_feature_delete_dropped, mode=ApplyMode.PLAN, lock=lock)
    change = next(c for c in ctx.changes if c.action is Action.DELETE)
    assert any("stored values handled by" in detail for detail in change.details)

    feat = ln.Feature.get(name="rm_drop", is_type=False)
    assert [v.value for v in feat.values.all()] == ["d"]


def test_apply_marks_optional_schema_features(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_optional_schema_plan, mode=ApplyMode.APPLY, lock=None)

    schema = ln.Schema.get(name="OptionalSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"opt_required", "opt_optional"}
    optional_names = {f.name for f in schema.optionals.get()}
    assert optional_names == {"opt_optional"}

    # Re-applying the same plan is a no-op.
    again = compute_changes(_optional_schema_plan, mode=ApplyMode.PLAN, lock=None)
    assert {c.kind: c for c in again.changes}["Schema"].action is Action.NO_CHANGE


def test_remote_optional_made_required_raises_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # LaminDB keeps the optional members in `Schema._aux`, so making one
    # required again on the instance changes neither the member list nor any
    # scalar field -- it must still be caught, because every record that omitted
    # the column stops validating.
    applied = compute_changes(_optional_schema_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _optional_schema_plan, tmp_path / "opt.lock")

    entry = next(e for e in lock.objects.values() if e.kind == "Schema")
    assert entry.remote_snapshot is not None
    assert len(entry.remote_snapshot["optionals"]) == 1

    schema = ln.Schema.get(name="OptionalSchema", is_type=False)
    schema.optionals.remove(ln.Feature.get(name="opt_optional", is_type=False))
    schema.save()

    with pytest.raises(DriftError, match="remote drift") as excinfo:
        compute_changes(_optional_schema_plan, mode=ApplyMode.PLAN, lock=lock)
    assert "optionals" in str(excinfo.value)


def test_untracked_optional_made_required_is_repaired(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # Same instance-side narrowing with no lock baseline to compare against.
    # Restoring the plan's optional member is a widening, so it is pushed in
    # place rather than passing silently.
    compute_changes(_optional_schema_plan, mode=ApplyMode.APPLY, lock=None)
    schema = ln.Schema.get(name="OptionalSchema", is_type=False)
    schema.optionals.remove(ln.Feature.get(name="opt_optional", is_type=False))
    schema.save()

    ctx = compute_changes(_optional_schema_plan, mode=ApplyMode.APPLY, lock=None)
    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert change.action is Action.UPDATE
    assert any("+optional 'opt_optional'" in d for d in change.details)
    schema = ln.Schema.get(name="OptionalSchema", is_type=False)
    assert {f.name for f in schema.optionals.get()} == {"opt_optional"}

    # ... and re-applying is then a clean no-op.
    again = compute_changes(_optional_schema_plan, mode=ApplyMode.APPLY, lock=None)
    assert {c.kind: c for c in again.changes}["Schema"].action is Action.NO_CHANGE


def test_remote_extra_optional_the_plan_does_not_declare_is_blocked(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # The reverse tampering: the instance marks a *required* member optional.
    # Re-applying the plan would make it required again -- a narrowing -- so it
    # is refused instead of silently reverted.
    compute_changes(_optional_schema_plan, mode=ApplyMode.APPLY, lock=None)
    schema = ln.Schema.get(name="OptionalSchema", is_type=False)
    schema.optionals.add(ln.Feature.get(name="opt_required", is_type=False))
    schema.save()

    with pytest.raises(DriftError, match="optionals") as excinfo:
        compute_changes(_optional_schema_plan, mode=ApplyMode.PLAN, lock=None)
    assert "opt_required" in str(excinfo.value)
    # the uid is named too: a bare name cannot say *which* record is optional,
    # and features are unique per feature type rather than per instance.
    uid = str(ln.Feature.get(name="opt_required", is_type=False).uid)
    assert uid in str(excinfo.value)


def test_dropped_optional_names_the_uid_the_plan_resolves_to() -> None:
    # A dropped optional whose name the plan *does* declare, resolving to a
    # different record, is the case the plain name misreports: it reads as "the
    # plan forgot to mark it optional" and sends the author after a plan edit
    # that cannot work, because a natural key only ever resolves to one uid.
    member = SimpleNamespace(name="signal_link")
    described = ApplyContext._describe_dropped_optional(
        "testUID00001", member, {"signal_link": "testUID00002"}
    )
    assert "testUID00001" in described
    assert "different object" in described
    assert "testUID00002" in described

    # ... and when the plan declares no optional of that name, it is exactly the
    # plan edit the message says it is.
    plain = ApplyContext._describe_dropped_optional("testUID00001", member, {})
    assert plain == "'signal_link' (uid 'testUID00001')"


def test_trashed_schema_member_is_not_diffed_as_a_member(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # Trashing a feature detaches nothing: it stays a schema member (the through
    # rows carry no branch filter) and its uid stays in the `_aux` optional
    # list, which is a bare uid list with no branch filter either.  The plan
    # addresses features by natural key, which only ever resolves to the *live*
    # record of that name, so treating the trashed uid as an optional the plan
    # dropped would be drift no plan edit can reconcile.
    compute_changes(_optional_schema_plan, mode=ApplyMode.APPLY, lock=None)
    trashed = ln.Feature.get(name="opt_optional", is_type=False)
    trashed_uid = str(trashed.uid)
    trashed.delete()

    schema = ln.Schema.get(name="OptionalSchema", is_type=False)
    assert trashed_uid in schema.optionals.get_uids()
    assert trashed_uid in {str(m.uid) for m in schema.members.all()}

    ctx = compute_changes(_optional_schema_plan, mode=ApplyMode.APPLY, lock=None)
    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert change.action is Action.UPDATE
    assert any("+feature 'opt_optional'" in d for d in change.details)
    assert any("+optional 'opt_optional'" in d for d in change.details)

    # The live replacement is now the member the plan declares, and re-applying
    # converges rather than re-reporting the same addition forever.
    live = ln.Feature.get(name="opt_optional", is_type=False)
    assert str(live.uid) != trashed_uid
    schema = ln.Schema.get(name="OptionalSchema", is_type=False)
    assert str(live.uid) in schema.optionals.get_uids()
    again = compute_changes(_optional_schema_plan, mode=ApplyMode.APPLY, lock=None)
    assert {c.kind: c for c in again.changes}["Schema"].action is Action.NO_CHANGE


def test_locked_object_in_the_trash_is_reported_as_trashed(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A lock pins uids, and lamindb exempts uid lookups from its branch filter,
    # so a plan object trashed out-of-band would otherwise stay "managed"
    # forever -- diffed against a record that validates nothing.
    applied = compute_changes(_optional_schema_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _optional_schema_plan, tmp_path / "opt.lock")

    trashed = ln.Feature.get(name="opt_optional", is_type=False)
    trashed_uid = str(trashed.uid)
    trashed.delete()

    with pytest.raises(DriftError) as excinfo:
        compute_changes(_optional_schema_plan, mode=ApplyMode.PLAN, lock=lock)
    message = str(excinfo.value)
    assert "soft-deleted" in message
    assert trashed_uid in message
    assert "branch_id=-1" in message


def test_trashed_record_is_not_reported_as_a_duplicate(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `repair duplicates` trashes the duplicate rather than deleting it, so a
    # lock pinning the trashed one leaves a live and a trashed record sharing a
    # natural key.  The prefetch loads both -- one by name, one by uid -- and
    # used to refuse with "resolve the duplicates (e.g. with repair
    # duplicates)", for a duplicate repair had already resolved.
    applied = compute_changes(_dupname_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _dupname_plan, tmp_path / "dup.lock")

    ft1 = ln.Feature.get(name="dupname_ft1", is_type=True)
    trashed = ln.Feature.filter(name="dupname_col", type=ft1).one()
    trashed_uid = str(trashed.uid)
    trashed.delete()
    ln.Feature(name="dupname_col", dtype="str", type=ft1).save()

    # The sibling under the other feature type is resolved by *name*, which is
    # what pulls every same-named record into the index the uid-pinned trashed
    # record would collide in.
    sibling_uid = str(ln.Feature.filter(name="dupname_col").exclude(type=ft1).one().uid)
    del lock.objects[
        next(key for key, e in lock.objects.items() if e.uid == sibling_uid)
    ]

    with pytest.raises(DriftError) as excinfo:
        compute_changes(_dupname_plan, mode=ApplyMode.PLAN, lock=lock)
    message = str(excinfo.value)
    assert "repair duplicates" not in message
    assert "soft-deleted" in message
    assert trashed_uid in message


def test_plan_edit_marking_a_member_optional_is_applied(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Widening the optional set from the plan side only admits more records, so
    # it is pushed in place rather than refused as configuration drift.
    applied = compute_changes(_optional_schema_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _optional_schema_plan, tmp_path / "opt.lock")

    ctx = compute_changes(_optional_both_plan, mode=ApplyMode.APPLY, lock=lock)
    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert change.action is Action.UPDATE
    schema = ln.Schema.get(name="OptionalSchema", is_type=False)
    assert {f.name for f in schema.optionals.get()} == {"opt_required", "opt_optional"}


def test_plan_edit_making_an_optional_member_required_is_blocked(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Dropping a member from the optional set *while keeping it a member*
    # invalidates every record that omitted it, so the plan edit is refused.
    applied = compute_changes(_optional_schema_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _optional_schema_plan, tmp_path / "opt.lock")

    with pytest.raises(
        PlanChangeRefusedError, match="optional \u2192 required"
    ) as excinfo:
        compute_changes(_optional_none_plan, mode=ApplyMode.PLAN, lock=lock)
    assert "optionals" in str(excinfo.value)


def test_optional_to_required_without_records_names_the_flag(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A schema that has validated nothing cannot invalidate anything, so the
    # refusal must say the opt-in flag would apply it rather than stop at "no".
    _optional_sheet_optional_plan.reset()
    applied = compute_changes(
        _optional_sheet_optional_plan, mode=ApplyMode.APPLY, lock=None
    )
    lock = _persist_lock(applied, _optional_sheet_optional_plan, tmp_path / "s.lock")

    _optional_sheet_required_plan.reset()
    with pytest.raises(DriftError) as excinfo:
        compute_changes(_optional_sheet_required_plan, mode=ApplyMode.PLAN, lock=lock)
    message = str(excinfo.value)
    assert "--allow-data-validated-changes" in message
    assert "the schema has validated no records" in message


@pytest.mark.parametrize(
    ("rows", "reason"),
    [
        ((), "the schema has validated no records"),
        ((("r1", True), ("r2", True)), "all 2 validated records already carry a value"),
    ],
)
def test_optional_to_required_is_applied_when_the_records_prove_it_safe(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    rows: tuple[tuple[str, bool], ...],
    reason: str,
) -> None:
    # The two ways the live rows can prove the narrowing safe: none exist, or
    # every one of them already carries a value for the member.
    _optional_sheet_optional_plan.reset()
    applied = compute_changes(
        _optional_sheet_optional_plan, mode=ApplyMode.APPLY, lock=None
    )
    lock = _persist_lock(applied, _optional_sheet_optional_plan, tmp_path / "s.lock")
    _seed_optsheet_rows(ln, *rows)

    _optional_sheet_required_plan.reset()
    ctx = compute_changes(
        _optional_sheet_required_plan,
        mode=ApplyMode.APPLY,
        lock=lock,
        allow_data_validated_changes=True,
    )
    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert change.action is Action.UPDATE
    assert any(reason in detail for detail in change.details)
    assert "-optional 'optsheet_optional'" in change.details

    schema = ln.Schema.get(name="OptSheetSchema", is_type=False)
    assert schema.optionals.get_uids() == []
    assert {f.name for f in schema.members} == {
        "optsheet_required",
        "optsheet_optional",
    }


def test_optional_to_required_stays_blocked_when_records_omit_the_member(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The rows that actually omitted the member are what the block protects, so
    # the refusal has to count and name them -- and point at the way out.
    _optional_sheet_optional_plan.reset()
    applied = compute_changes(
        _optional_sheet_optional_plan, mode=ApplyMode.APPLY, lock=None
    )
    lock = _persist_lock(applied, _optional_sheet_optional_plan, tmp_path / "s.lock")
    _seed_optsheet_rows(ln, ("kept", True), ("bare", False))

    _optional_sheet_required_plan.reset()
    with pytest.raises(DriftError) as excinfo:
        compute_changes(
            _optional_sheet_required_plan,
            mode=ApplyMode.APPLY,
            lock=lock,
            allow_data_validated_changes=True,
        )
    message = str(excinfo.value)
    assert "1 of 2 validated rows omits it" in message
    assert "'bare'" in message
    assert "ctx.migrate" in message

    schema = ln.Schema.get(name="OptSheetSchema", is_type=False)
    assert len(schema.optionals.get_uids()) == 1


def test_a_member_removal_migration_does_not_unblock_a_required_narrowing(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A schema takes declarations for two unrelated narrowings at once, and they
    # are looked up by the object they hang on. A member-removal cleanup left in
    # the plan therefore used to read as a backfill for a member the same schema
    # makes required, satisfying a block it does nothing about -- and the apply
    # only found that out afterwards, from rows it had already invalidated.
    _optional_sheet_optional_plan.reset()
    applied = compute_changes(
        _optional_sheet_optional_plan, mode=ApplyMode.APPLY, lock=None
    )
    lock = _persist_lock(applied, _optional_sheet_optional_plan, tmp_path / "s.lock")
    _seed_optsheet_rows(ln, ("bare", False))

    _optional_sheet_removal_migration_plan.reset()
    with pytest.raises(DriftError) as excinfo:
        compute_changes(
            _optional_sheet_removal_migration_plan, mode=ApplyMode.PLAN, lock=lock
        )
    message = str(excinfo.value)
    assert "1 of 1 validated row omits it" in message
    assert "'keep_member_values[optsheet_optional]'" in message
    assert "answers another narrowing" in message

    schema = ln.Schema.get(name="OptSheetSchema", is_type=False)
    assert len(schema.optionals.get_uids()) == 1


def test_optional_to_required_is_unblocked_by_a_declared_backfill(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    _optional_sheet_optional_plan.reset()
    applied = compute_changes(
        _optional_sheet_optional_plan, mode=ApplyMode.APPLY, lock=None
    )
    lock = _persist_lock(applied, _optional_sheet_optional_plan, tmp_path / "s.lock")
    _seed_optsheet_rows(ln, ("kept", True), ("bare", False))

    _optional_sheet_migrated_plan.reset()
    ctx = compute_changes(
        _optional_sheet_migrated_plan, mode=ApplyMode.APPLY, lock=lock
    )
    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert change.action is Action.UPDATE
    assert any("backfill_optsheet" in detail for detail in change.details)

    schema = ln.Schema.get(name="OptSheetSchema", is_type=False)
    assert schema.optionals.get_uids() == []
    bare = ln.Record.get(name="bare")
    assert bare.features.get_values()["optsheet_optional"] == "backfilled"

    # The declaration is idempotent: with the instance already narrowed there is
    # nothing to migrate, so the plan converges instead of re-running the
    # backfill on every apply.
    settled = _persist_lock(ctx, _optional_sheet_migrated_plan, tmp_path / "s2.lock")
    _optional_sheet_migrated_plan.reset()
    again = compute_changes(
        _optional_sheet_migrated_plan, mode=ApplyMode.APPLY, lock=settled
    )
    assert {c.action for c in again.changes} == {Action.NO_CHANGE}


def _seed_opttwo_row(ln: LamindbModuleType, **features: Any) -> None:
    sheet = ln.Record.get(name="opttwo_sheet", is_type=True)
    ln.Record(name="two_row", type=sheet, features=features or None).save()


def test_one_backfill_covers_every_member_narrowed_in_the_same_change(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A migration sees every narrowed member in `context.features`; the
    # `(feature, row)` signature is how one declaration tells them apart --
    # here their dtypes differ, so a single scalar could not serve both.
    _backfilled_members.clear()
    _optional_two_optional_plan.reset()
    applied = compute_changes(
        _optional_two_optional_plan, mode=ApplyMode.APPLY, lock=None
    )
    lock = _persist_lock(applied, _optional_two_optional_plan, tmp_path / "two.lock")
    _seed_opttwo_row(ln)

    _optional_two_migrated_plan.reset()
    compute_changes(_optional_two_migrated_plan, mode=ApplyMode.APPLY, lock=lock)

    assert sorted(_backfilled_members) == ["opttwo_a", "opttwo_b"]
    assert ln.Record.get(name="two_row").features.get_values() == {
        "opttwo_a": "n/a",
        "opttwo_b": 0,
    }
    assert ln.Schema.get(name="OptTwoSchema", is_type=False).optionals.get_uids() == []


def test_one_backfill_may_be_declared_per_narrowed_member(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Several `ctx.migrate` calls on the same schema are kept, not collapsed,
    # so each narrowed member can take its own declaration with a value of its
    # own dtype instead of one callable branching on the feature.
    _optional_two_optional_plan.reset()
    applied = compute_changes(
        _optional_two_optional_plan, mode=ApplyMode.APPLY, lock=None
    )
    lock = _persist_lock(applied, _optional_two_optional_plan, tmp_path / "two.lock")
    _seed_opttwo_row(ln)

    _optional_two_per_member_plan.reset()
    ctx = compute_changes(
        _optional_two_per_member_plan, mode=ApplyMode.APPLY, lock=lock
    )

    assert ln.Record.get(name="two_row").features.get_values() == {
        "opttwo_a": "bla",
        "opttwo_b": -1,
    }
    details = {c.kind: c for c in ctx.changes}["Schema"].details
    assert any("fill_missing[opttwo_a]" in detail for detail in details)
    assert any("fill_missing[opttwo_b]" in detail for detail in details)


def test_a_backfill_only_receives_the_members_the_records_do_not_cover(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Each member is vetted on its own, so one the rows already prove safe takes
    # the flag route and never reaches the migration -- which must therefore not
    # assume it owns every narrowed member.
    _backfilled_members.clear()
    _optional_two_optional_plan.reset()
    applied = compute_changes(
        _optional_two_optional_plan, mode=ApplyMode.APPLY, lock=None
    )
    lock = _persist_lock(applied, _optional_two_optional_plan, tmp_path / "two.lock")
    _seed_opttwo_row(ln, opttwo_a="already here")

    _optional_two_migrated_plan.reset()
    ctx = compute_changes(
        _optional_two_migrated_plan,
        mode=ApplyMode.APPLY,
        lock=lock,
        allow_data_validated_changes=True,
    )

    assert _backfilled_members == ["opttwo_b"]
    details = {c.kind: c for c in ctx.changes}["Schema"].details
    assert any(
        "optional 'opttwo_a': allowed despite block" in detail for detail in details
    )
    assert ln.Record.get(name="two_row").features.get_values() == {
        "opttwo_a": "already here",
        "opttwo_b": 0,
    }


def test_a_backfill_that_misses_a_record_rolls_the_apply_back(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The engine must *prove* the backfill covered every row: one left behind is
    # exactly the invalidation the block exists to prevent.
    _optional_sheet_optional_plan.reset()
    applied = compute_changes(
        _optional_sheet_optional_plan, mode=ApplyMode.APPLY, lock=None
    )
    lock = _persist_lock(applied, _optional_sheet_optional_plan, tmp_path / "s.lock")
    _seed_optsheet_rows(ln, ("bare", False))

    _optional_sheet_noop_plan.reset()
    with pytest.raises(MigrationError, match="without a value for members"):
        compute_changes(_optional_sheet_noop_plan, mode=ApplyMode.APPLY, lock=lock)


@pytest.mark.parametrize(
    ("plan_name", "expected"),
    [
        ("_optional_sheet_migrated_plan", "'backfill_optsheet' backfills every record"),
        ("_optional_sheet_noop_plan", "'noop' would leave 1 record(s)"),
    ],
)
def test_a_member_backfill_is_previewed_without_persisting_it(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    plan_name: str,
    expected: str,
) -> None:
    # A dry-run proves the backfill rather than describing it: the migration runs
    # in a savepoint that is always rolled back, so `plan` stays read-only.
    _optional_sheet_optional_plan.reset()
    applied = compute_changes(
        _optional_sheet_optional_plan, mode=ApplyMode.APPLY, lock=None
    )
    lock = _persist_lock(applied, _optional_sheet_optional_plan, tmp_path / "s.lock")
    _seed_optsheet_rows(ln, ("bare", False))

    plan = globals()[plan_name]
    plan.reset()
    ctx = compute_changes(plan, mode=ApplyMode.PLAN, lock=lock, preview_migrations=True)
    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert any(expected in detail for detail in change.details)

    schema = ln.Schema.get(name="OptSheetSchema", is_type=False)
    assert len(schema.optionals.get_uids()) == 1
    assert ln.Record.get(name="bare").features.get_values() == {
        "optsheet_required": "x"
    }


def test_categorical_member_values_count_as_carried(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A categorical member's values live in `RecordRecord`, which a row reaches
    # both as the annotated subject and as an annotating value.  Only the former
    # means "carries a value", so a row annotated with the member must not be
    # counted as omitting it.
    _optional_cat_optional_plan.reset()
    applied = compute_changes(
        _optional_cat_optional_plan, mode=ApplyMode.APPLY, lock=None
    )
    lock = _persist_lock(applied, _optional_cat_optional_plan, tmp_path / "cat.lock")
    sheet = ln.Record.get(name="optcat_sheet", is_type=True)
    ln.Record(
        name="cat_row",
        type=sheet,
        features={"optcat_required": "x", "optcat_optional": "v1"},
    ).save()

    _optional_cat_required_plan.reset()
    ctx = compute_changes(
        _optional_cat_required_plan,
        mode=ApplyMode.APPLY,
        lock=lock,
        allow_data_validated_changes=True,
    )
    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert any(
        "the one validated record already carries a value" in detail
        for detail in change.details
    )
    assert ln.Schema.get(name="OptCatSchema", is_type=False).optionals.get_uids() == []


def test_declarative_optional_removal_is_the_same_narrowing(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `Feature[X] | None` -> `Feature[X]` in a `SchemaModel` body must land
    # on the same vetted path as a hand-built `optionals=`.
    _declarative_optional_plan.reset()
    applied = compute_changes(
        _declarative_optional_plan, mode=ApplyMode.APPLY, lock=None
    )
    lock = _persist_lock(applied, _declarative_optional_plan, tmp_path / "decl.lock")
    schema = ln.Schema.get(name="DeclOptionalSchema", is_type=False)
    assert {f.name for f in schema.optionals.get()} == {"decl_optional"}

    _declarative_required_plan.reset()
    with pytest.raises(PlanChangeRefusedError, match="optional \u2192 required"):
        compute_changes(_declarative_required_plan, mode=ApplyMode.PLAN, lock=lock)

    _declarative_required_plan.reset()
    compute_changes(
        _declarative_required_plan,
        mode=ApplyMode.APPLY,
        lock=lock,
        allow_data_validated_changes=True,
    )
    schema = ln.Schema.get(name="DeclOptionalSchema", is_type=False)
    assert schema.optionals.get_uids() == []


def test_schema_without_optionals_keeps_the_key_out_of_the_snapshot(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A schema with no optional members must not record an empty `optionals`
    # list, so a lock written before the key existed does not diff against it.
    applied = compute_changes(_optional_none_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _optional_none_plan, tmp_path / "opt.lock")

    entry = next(e for e in lock.objects.values() if e.kind == "Schema")
    assert entry.remote_snapshot is not None
    assert "optionals" not in entry.remote_snapshot

    again = compute_changes(_optional_none_plan, mode=ApplyMode.APPLY, lock=lock)
    assert {c.action for c in again.changes} == {Action.NO_CHANGE}


def test_apply_adds_feature_to_existing_schema(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_schema_add_v1_plan, mode=ApplyMode.APPLY, lock=None)
    schema = ln.Schema.get(name="SchemaAddSchema", is_type=False)
    before_uid = schema.uid
    assert {m.name for m in schema.members.all()} == {"schema_add_a", "schema_add_b"}

    ctx = compute_changes(_schema_add_v2_plan, mode=ApplyMode.APPLY, lock=None)
    schema_change = next(c for c in ctx.changes if c.kind == "Schema")
    assert schema_change.action is Action.UPDATE
    assert any("schema_add_c" in d for d in schema_change.details)

    schema = ln.Schema.get(name="SchemaAddSchema", is_type=False)
    # identity is preserved; only the content hash is recomputed
    assert schema.uid == before_uid
    assert {m.name for m in schema.members.all()} == {
        "schema_add_a",
        "schema_add_b",
        "schema_add_c",
    }


def test_apply_adds_feature_to_existing_schema_with_lock(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_schema_add_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _schema_add_v1_plan, tmp_path / "schema.lock")

    ctx = compute_changes(_schema_add_v2_plan, mode=ApplyMode.APPLY, lock=lock)
    schema_change = next(c for c in ctx.changes if c.kind == "Schema")
    assert schema_change.action is Action.UPDATE

    schema = ln.Schema.get(name="SchemaAddSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {
        "schema_add_a",
        "schema_add_b",
        "schema_add_c",
    }


def test_plan_mode_reports_schema_feature_addition_without_mutating(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_schema_add_v1_plan, mode=ApplyMode.APPLY, lock=None)

    preview = compute_changes(_schema_add_v2_plan, mode=ApplyMode.PLAN, lock=None)
    schema_change = next(c for c in preview.changes if c.kind == "Schema")
    assert schema_change.action is Action.UPDATE
    assert any("schema_add_c" in d for d in schema_change.details)

    # PLAN mode does not mutate the schema's membership
    schema = ln.Schema.get(name="SchemaAddSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"schema_add_a", "schema_add_b"}


def test_reapply_after_schema_feature_addition_is_no_change(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_schema_add_v1_plan, mode=ApplyMode.APPLY, lock=None)
    compute_changes(_schema_add_v2_plan, mode=ApplyMode.APPLY, lock=None)

    again = compute_changes(_schema_add_v2_plan, mode=ApplyMode.PLAN, lock=None)
    schema_change = next(c for c in again.changes if c.kind == "Schema")
    assert schema_change.action is Action.NO_CHANGE


def test_apply_inserts_ordered_schema_feature_at_declared_position(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_ordered_schema_v1_plan, mode=ApplyMode.APPLY, lock=None)
    schema = ln.Schema.get(name="OrderedSchema", is_type=False)
    assert [m.name for m in schema.members.all()] == [
        "ordered_a",
        "ordered_b",
        "ordered_c",
    ]

    ctx = compute_changes(_ordered_schema_v2_plan, mode=ApplyMode.APPLY, lock=None)
    schema_change = next(c for c in ctx.changes if c.kind == "Schema")
    assert schema_change.action is Action.UPDATE

    schema = ln.Schema.get(name="OrderedSchema", is_type=False)
    # the new feature lands between B and C, matching the plan's declared order,
    # rather than being appended at the end
    assert [m.name for m in schema.members.all()] == [
        "ordered_a",
        "ordered_b",
        "ordered_f",
        "ordered_c",
    ]


def test_reapply_after_ordered_schema_insert_is_no_change(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    compute_changes(_ordered_schema_v1_plan, mode=ApplyMode.APPLY, lock=None)
    compute_changes(_ordered_schema_v2_plan, mode=ApplyMode.APPLY, lock=None)

    again = compute_changes(_ordered_schema_v2_plan, mode=ApplyMode.PLAN, lock=None)
    schema_change = next(c for c in again.changes if c.kind == "Schema")
    assert schema_change.action is Action.NO_CHANGE


def test_removing_an_ordered_schema_member_keeps_the_rest_in_order(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # Dropping a member leaves the survivors in plan order, so the links are
    # unlinked in place rather than rebuilt.
    compute_changes(_ordered_schema_v1_plan, mode=ApplyMode.APPLY, lock=None)

    ctx = compute_changes(_ordered_schema_remove_plan, mode=ApplyMode.APPLY, lock=None)

    schema_change = next(c for c in ctx.changes if c.kind == "Schema")
    assert "reorder features to match plan order" not in schema_change.details
    schema = ln.Schema.get(name="OrderedSchema", is_type=False)
    assert [m.name for m in schema.members.all()] == ["ordered_a", "ordered_c"]


def test_removing_an_ordered_schema_member_and_reordering_rebuilds_the_links(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # A drop that also changes the surviving order still needs the rebuild;
    # before, any drop suppressed the reorder detection outright.
    compute_changes(_ordered_schema_v1_plan, mode=ApplyMode.APPLY, lock=None)

    ctx = compute_changes(
        _ordered_schema_remove_and_reorder_plan, mode=ApplyMode.APPLY, lock=None
    )

    schema_change = next(c for c in ctx.changes if c.kind == "Schema")
    assert "reorder features to match plan order" in schema_change.details
    schema = ln.Schema.get(name="OrderedSchema", is_type=False)
    assert [m.name for m in schema.members.all()] == ["ordered_c", "ordered_a"]

    again = compute_changes(
        _ordered_schema_remove_and_reorder_plan, mode=ApplyMode.PLAN, lock=None
    )
    assert next(c for c in again.changes if c.kind == "Schema").action is (
        Action.NO_CHANGE
    )


def test_apply_removes_schema_feature_and_keeps_the_declared_feature(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    """End to end: the plan drops a member but still declares the feature.

    The schema loses the link; the feature itself is still the plan's, so it
    survives as a standalone feature.
    """
    applied = compute_changes(_schema_add_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _schema_add_v1_plan, tmp_path / "schema.lock")
    schema = ln.Schema.get(name="SchemaAddSchema", is_type=False)
    before_uid, before_hash = schema.uid, schema.hash

    ctx = compute_changes(_schema_unlink_v2_plan, mode=ApplyMode.APPLY, lock=lock)

    schema_change = next(c for c in ctx.changes if c.kind == "Schema")
    assert schema_change.action is Action.UPDATE
    assert "-feature 'schema_add_b'" in schema_change.details
    assert not [c for c in ctx.changes if c.action is Action.DELETE]

    schema = ln.Schema.get(name="SchemaAddSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"schema_add_a"}
    # identity is preserved and the content hash follows the new member set
    assert schema.uid == before_uid
    assert schema.hash != before_hash
    assert schema.n_members == 1
    # the feature is still declared by the plan, so it is not deleted
    dropped = ln.Feature.filter(name="schema_add_b", is_type=False).one_or_none()
    assert dropped is not None
    assert dropped.branch_id == 1


def test_apply_deletes_a_feature_dropped_from_both_the_schema_and_the_plan(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    """End to end: the feature has no address left in the plan at all.

    Unlinking it from the schema and deleting the record are two different
    passes, and both have to run -- the member link is what the removal pass
    would otherwise have to delete around.
    """
    applied = compute_changes(_schema_add_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _schema_add_v1_plan, tmp_path / "schema.lock")

    ctx = compute_changes(_schema_remove_v2_plan, mode=ApplyMode.APPLY, lock=lock)

    schema_change = next(c for c in ctx.changes if c.kind == "Schema")
    assert schema_change.action is Action.UPDATE
    assert "-feature 'schema_add_b'" in schema_change.details
    deleted = [c for c in ctx.changes if c.action is Action.DELETE]
    assert [c.name for c in deleted] == ["schema_add_b"]

    schema = ln.Schema.get(name="SchemaAddSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"schema_add_a"}
    # gone from the instance (soft-deleted) and forgotten by the rebuilt lock
    assert ln.Feature.filter(name="schema_add_b", is_type=False).one_or_none() is None
    rebuilt = ctx.build_lock(_schema_remove_v2_plan.name)
    assert not [key for key in rebuilt.objects if key.endswith("schema_add_b")]


def test_reapply_after_schema_feature_removal_is_no_change(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_schema_add_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _schema_add_v1_plan, tmp_path / "schema.lock")
    removed = compute_changes(_schema_unlink_v2_plan, mode=ApplyMode.APPLY, lock=lock)
    lock = _persist_lock(removed, _schema_unlink_v2_plan, tmp_path / "schema.lock")

    again = compute_changes(_schema_unlink_v2_plan, mode=ApplyMode.APPLY, lock=lock)

    assert {c.action for c in again.changes} == {Action.NO_CHANGE}


def test_plan_mode_reports_schema_feature_removal_without_mutating(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_schema_add_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _schema_add_v1_plan, tmp_path / "schema.lock")

    preview = compute_changes(_schema_unlink_v2_plan, mode=ApplyMode.PLAN, lock=lock)

    schema_change = next(c for c in preview.changes if c.kind == "Schema")
    assert schema_change.action is Action.UPDATE
    assert "-feature 'schema_add_b'" in schema_change.details
    schema = ln.Schema.get(name="SchemaAddSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"schema_add_a", "schema_add_b"}


def test_removing_an_optional_schema_member_clears_it_from_the_optional_set(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    """A dropped member must not linger in `Schema._aux`.

    Optionality is a property of membership, and `Schema.save` reads the set
    back to recompute the content hash -- a uid left behind would be hashed as a
    member the schema no longer has, and would read as an unfixable
    `optionals` drift on the next apply.
    """
    applied = compute_changes(
        _schema_optional_remove_v1_plan, mode=ApplyMode.APPLY, lock=None
    )
    lock = _persist_lock(applied, _schema_optional_remove_v1_plan, tmp_path / "o.lock")

    ctx = compute_changes(
        _schema_optional_remove_v2_plan, mode=ApplyMode.APPLY, lock=lock
    )
    assert next(c for c in ctx.changes if c.kind == "Schema").action is Action.UPDATE

    schema = ln.Schema.get(name="OptRemoveSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"optrm_keep"}
    assert {f.name for f in schema.optionals.get()} == {"optrm_keep"}
    assert schema.hash == ln.Schema.get(uid=schema.uid).hash

    lock = _persist_lock(ctx, _schema_optional_remove_v2_plan, tmp_path / "o.lock")
    again = compute_changes(
        _schema_optional_remove_v2_plan, mode=ApplyMode.APPLY, lock=lock
    )
    assert {c.action for c in again.changes} == {Action.NO_CHANGE}


def test_removing_a_member_from_a_maximal_set_schema_raises_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    """`maximal_set` forbids undeclared members, so a drop invalidates data.

    This is the one configuration in which removing a member is not a
    relaxation, and the one LaminDB's own `Schema.remove` refuses too.
    """
    applied = compute_changes(_maximal_schema_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _maximal_schema_v1_plan, tmp_path / "max.lock")

    with pytest.raises(DriftError, match="maximal_set schema") as excinfo:
        compute_changes(_maximal_schema_v2_plan, mode=ApplyMode.PLAN, lock=lock)
    assert "'maxset_b'" in str(excinfo.value)

    schema = ln.Schema.get(name="MaximalSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"maxset_a", "maxset_b"}


def _apply_mdrop_v1(
    ln: LamindbModuleType, tmp_path: Path, *rows: tuple[str, bool]
) -> LockFile:
    """Apply the baseline schema and seed `rows` under its sheet."""
    _member_drop_v1.reset()
    applied = compute_changes(_member_drop_v1, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _member_drop_v1, tmp_path / "mdrop.lock")
    _seed_mdrop_rows(ln, *rows)
    return lock


def test_removing_a_member_whose_values_are_stored_is_blocked(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Removing a member reads as a relaxation, but the values stored for it stay
    # exactly where they were -- annotating rows the schema no longer describes.
    # The refusal has to count them, name them, and offer both ways out.
    lock = _apply_mdrop_v1(ln, tmp_path, ("carries", True), ("bare", False))

    _member_drop_bare.reset()
    with pytest.raises(DriftError) as excinfo:
        compute_changes(_member_drop_bare, mode=ApplyMode.PLAN, lock=lock)
    message = str(excinfo.value)
    assert "1 of 2 validated rows carries a value for it" in message
    assert "'carries'" in message
    assert "drop_member_values" in message
    assert "keep_member_values" in message

    # nothing was unlinked
    schema = ln.Schema.get(name="MDropSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"mdrop_keep", "mdrop_doomed"}


def test_removing_a_member_no_record_carries_needs_no_declaration(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The block is about stranded values, not about the removal: rows that never
    # carried the member cannot strand anything, so the drop stays a relaxation.
    lock = _apply_mdrop_v1(ln, tmp_path, ("bare", False))

    _member_drop_bare.reset()
    ctx = compute_changes(_member_drop_bare, mode=ApplyMode.APPLY, lock=lock)
    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert change.action is Action.UPDATE
    assert "-feature 'mdrop_doomed'" in change.details

    schema = ln.Schema.get(name="MDropSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"mdrop_keep"}


def test_the_data_validated_flag_does_not_lift_the_member_removal_block(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The flag means "the live data is already safe", which for a removal is the
    # case that needs no flag. When values do exist the question is what should
    # happen to them, and only a declaration answers that.
    lock = _apply_mdrop_v1(ln, tmp_path, ("carries", True))

    _member_drop_bare.reset()
    with pytest.raises(DriftError, match="carries a value for it"):
        compute_changes(
            _member_drop_bare,
            mode=ApplyMode.PLAN,
            lock=lock,
            allow_data_validated_changes=True,
        )


def test_a_backfill_does_not_unblock_a_member_removal(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The mirror of the required-narrowing case: a backfill says nothing about
    # what happens to the values of a member the schema stops declaring, and
    # running it against a removal would not even reach a row -- its context has
    # no notion of one.
    lock = _apply_mdrop_v1(ln, tmp_path, ("carries", True))

    _member_drop_backfill.reset()
    with pytest.raises(DriftError) as excinfo:
        compute_changes(_member_drop_backfill, mode=ApplyMode.PLAN, lock=lock)
    message = str(excinfo.value)
    assert "carries a value for it" in message
    assert "'fill_missing[mdrop_doomed]'" in message
    assert "answers another narrowing" in message


def test_drop_member_values_clears_them_and_unlinks_the_member(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The declared cleanup: the member goes, and so do the values it held on the
    # rows this schema validates.
    lock = _apply_mdrop_v1(ln, tmp_path, ("carries", True), ("bare", False))
    feat = ln.Feature.get(name="mdrop_doomed", is_type=False)
    assert list(feat.links_recordjson.values_list("value", flat=True)) == ["d"]

    _member_drop_dropped.reset()
    ctx = compute_changes(_member_drop_dropped, mode=ApplyMode.APPLY, lock=lock)
    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert change.action is Action.UPDATE
    assert any("drop_member_values" in detail for detail in change.details)

    schema = ln.Schema.get(name="MDropSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"mdrop_keep"}
    # the values are gone, and the record no longer reports the member
    assert list(feat.links_recordjson.values_list("value", flat=True)) == []
    assert ln.Record.get(name="carries").features.get_values() == {"mdrop_keep": "k"}
    # the feature itself survives -- the plan still declares it
    assert ln.Feature.filter(name="mdrop_doomed", is_type=False).one_or_none()


def _register_local_storage(ln: LamindbModuleType) -> None:
    """Point the instance's `Storage` row at the root it is connected to.

    A restored SQLite snapshot can retain its original `Storage.root`.
    Align the row with the current temporary storage before saving artifacts.
    """
    import lamindb_setup

    root = lamindb_setup.settings.storage.root_as_str
    ln.Storage.objects.exclude(root=root).update(root=root)


def test_drop_member_values_clears_a_categorical_value_on_an_artifact(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The same cleanup for the other registry a schema validates. An artifact's
    # categorical annotation lives in a link table that names its label column
    # after the label's registry rather than `value`, so a walk keyed on
    # `value` never saw it -- while the count that has to be satisfied
    # afterwards did, which made the removal impossible to complete.
    _register_local_storage(ln)
    _artifact_member_drop_v1.reset()
    applied = compute_changes(_artifact_member_drop_v1, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _artifact_member_drop_v1, tmp_path / "amdrop.lock")

    payload = tmp_path / "payload.txt"
    payload.write_text("payload")
    artifact = ln.Artifact(payload, key="amdrop/payload.txt").save()
    artifact.features.add_values({"amdrop_keep": "k", "amdrop_doomed": "alpha"})
    artifact.schema = ln.Schema.get(name="AmdropSchema", is_type=False)
    artifact.save()
    feat = ln.Feature.get(name="amdrop_doomed", is_type=False)
    assert feat.links_artifactrecord.count() == 1

    _artifact_member_drop_dropped.reset()
    ctx = compute_changes(
        _artifact_member_drop_dropped, mode=ApplyMode.APPLY, lock=lock
    )
    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert change.action is Action.UPDATE

    schema = ln.Schema.get(name="AmdropSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"amdrop_keep"}
    assert feat.links_artifactrecord.count() == 0


def test_drop_member_values_is_idempotent(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # With the member already gone there is nothing to remove, so the declaration
    # is a no-op rather than something that re-runs on every apply.
    lock = _apply_mdrop_v1(ln, tmp_path, ("carries", True))

    _member_drop_dropped.reset()
    ctx = compute_changes(_member_drop_dropped, mode=ApplyMode.APPLY, lock=lock)
    settled = _persist_lock(ctx, _member_drop_dropped, tmp_path / "mdrop2.lock")

    _member_drop_dropped.reset()
    again = compute_changes(_member_drop_dropped, mode=ApplyMode.APPLY, lock=settled)
    assert {c.action for c in again.changes} == {Action.NO_CHANGE}


def test_keep_member_values_retires_the_member_and_leaves_the_data(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The non-destructive resolution: the schema stops declaring the member and
    # the values stay on the rows as undeclared extra data.
    lock = _apply_mdrop_v1(ln, tmp_path, ("carries", True))

    _member_drop_kept.reset()
    ctx = compute_changes(_member_drop_kept, mode=ApplyMode.APPLY, lock=lock)
    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert change.action is Action.UPDATE
    assert any("keep_member_values" in detail for detail in change.details)

    schema = ln.Schema.get(name="MDropSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"mdrop_keep"}
    feat = ln.Feature.get(name="mdrop_doomed", is_type=False)
    assert list(feat.links_recordjson.values_list("value", flat=True)) == ["d"]
    assert ln.Record.get(name="carries").features.get_values()["mdrop_doomed"] == "d"


def test_a_migration_that_clears_nothing_fails_the_apply(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A declaration is not a promise: the engine re-counts afterwards, so a
    # migration that leaves the values behind rolls the removal back with it.
    from django.db import transaction

    lock = _apply_mdrop_v1(ln, tmp_path, ("carries", True))

    _member_drop_noop.reset()
    with pytest.raises(MigrationError, match="stored value") as excinfo:
        with transaction.atomic():
            compute_changes(_member_drop_noop, mode=ApplyMode.APPLY, lock=lock)
    # One row carries the member, so the refusal reports one leftover: the vet
    # pass must not register the same member for cleanup twice.
    assert "left 1 stored value(s)" in str(excinfo.value)

    schema = ln.Schema.get(name="MDropSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"mdrop_keep", "mdrop_doomed"}


def test_dropping_the_feature_from_the_plan_clears_its_values_first(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The path that stranded values on a *trashed* feature: the plan drops the
    # member and the feature itself. Migrations run before the removal pass, so
    # the values are cleared while the feature is still live.
    lock = _apply_mdrop_v1(ln, tmp_path, ("carries", True))

    _member_drop_deleted.reset()
    ctx = compute_changes(_member_drop_deleted, mode=ApplyMode.APPLY, lock=lock)
    assert any(
        c.action is Action.DELETE and c.name == "mdrop_doomed" for c in ctx.changes
    )

    assert ln.Feature.filter(name="mdrop_doomed", is_type=False).one_or_none() is None
    trashed = ln.Feature.filter(name="mdrop_doomed", branch_id=-1).one()
    assert list(trashed.links_recordjson.values_list("value", flat=True)) == []
    assert ln.Record.get(name="carries").features.get_values() == {"mdrop_keep": "k"}


def test_maximal_set_removal_is_refused_by_keeping_and_allowed_by_dropping(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from lamindb_setup.evolve.authoring._migrations import SheetExportContext
    from lamindb_setup.evolve.engine import _sheet_exports as exports

    # `maximal_set` forbids a member the schema does not declare, so leaving the
    # values in place is the one resolution it cannot accept -- while deleting
    # them removes the very thing it objects to.
    _member_drop_max_v1.reset()
    applied = compute_changes(_member_drop_max_v1, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _member_drop_max_v1, tmp_path / "mmax.lock")
    _seed_mdrop_rows(ln, ("carries", True))

    sheet = ln.Record.get(name="mdrop_sheet", is_type=True)
    previous = sheet.to_artifact()
    assert exports.sheet_exports([sheet]) == {sheet.pk: [(sheet, previous)]}
    ln.Artifact.objects.filter(pk=previous.pk).update(description="edited after export")
    assert exports.sheet_exports([sheet]) == {sheet.pk: [(sheet, previous)]}
    assert exports.export_column_ids(previous) == {
        f.pk for f in sheet.schema.members.all()
    }

    _member_drop_max_kept.reset()
    with pytest.raises(DriftError, match="maximal_set schema") as excinfo:
        compute_changes(_member_drop_max_kept, mode=ApplyMode.PLAN, lock=lock)
    assert "keep_member_values cannot apply here" in str(excinfo.value)

    _member_drop_max_dropped.reset()
    ctx = compute_changes(_member_drop_max_dropped, mode=ApplyMode.APPLY, lock=lock)
    assert {c.kind: c for c in ctx.changes}["Schema"].action is Action.UPDATE
    schema = ln.Schema.get(name="MDropSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"mdrop_keep"}

    abandoned = exports.branch_name_for(_member_drop_max_dropped.name, "previous")
    branch = exports.open_branch(abandoned, description="unfinished re-export")
    assert exports.open_branch(abandoned, description="retry").pk == branch.pk
    outcome = run_sheet_export_migrations(
        ctx.pending_sheet_exports,
        plan_name=_member_drop_max_dropped.name,
        run_uid="test",
    )
    assert outcome.ok, outcome.error
    assert outcome.written == 1
    latest = ln.Artifact.get(key=previous.key)
    assert latest.stem_uid == previous.stem_uid and latest.uid != previous.uid
    assert "mdrop_doomed" in previous.load() and "mdrop_doomed" not in latest.load()
    assert outcome.closed == [abandoned]

    current = exports.SheetExport(sheet, latest)
    retry = SheetExportContext(
        schema,
        [current],
        reexport=exports.reexport,
        current_branch=exports.current_branch,
        branch_factory=lambda: exports.open_branch("retry-dedup", description="Retry"),
    )
    assert retry.write(current).pk == latest.pk
    assert not retry.written and not retry.promotions
    exports.promote_head(latest, previous)
    assert ln.Artifact.get(key=previous.key).uid == previous.uid
    latest.refresh_from_db()
    assert not latest.is_latest
    assert retry.write(exports.SheetExport(sheet, previous)).pk == latest.pk
    assert retry.promotions == [(previous, latest)] and not retry.written


def test_plan_mode_previews_the_drop_without_touching_the_values(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `plan --preview-migrations` proves the cleanup in a rolled-back savepoint.
    lock = _apply_mdrop_v1(ln, tmp_path, ("carries", True))

    _member_drop_dropped.reset()
    ctx = compute_changes(
        _member_drop_dropped,
        mode=ApplyMode.PLAN,
        lock=lock,
        preview_migrations=True,
    )
    change = {c.kind: c for c in ctx.changes}["Schema"]
    assert any(
        "accounted for every stored value" in detail for detail in change.details
    )

    feat = ln.Feature.get(name="mdrop_doomed", is_type=False)
    assert list(feat.links_recordjson.values_list("value", flat=True)) == ["d"]
    schema = ln.Schema.get(name="MDropSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"mdrop_keep", "mdrop_doomed"}


def _shared_member_fixture(ln: LamindbModuleType) -> tuple[Any, Any, Any]:
    """Two sibling component schemas declaring one feature over the same rows.

    A row is validated by whatever schema its sheet carries, so two schemas can
    only reach the *same* row through a composite that embeds them both.  Built
    directly rather than through a plan: the point is the query, and the
    annotation is written to its table without curation so no validation of the
    composite is involved.

    Returns `(component_a, component_b, feature)`.
    """
    ft = ln.Feature(name="shared_ft", is_type=True).save()
    keep = ln.Feature(name="shared_keep", dtype="str", type=ft).save()
    shared = ln.Feature(name="shared_col", dtype="str", type=ft).save()
    a = ln.Schema(name="SharedA", features=[keep, shared]).save()
    b = ln.Schema(name="SharedB", features=[shared]).save()
    composite = ln.Schema(
        name="SharedComposite", slots={"obs": a, "var": b}, otype="DataFrame"
    ).save()
    sheet = ln.Record(name="shared_sheet", is_type=True, schema=composite).save()
    row = ln.Record(name="shared_row", type=sheet).save()
    shared.links_recordjson.model(record=row, feature=shared, value="d").save()
    return a, b, shared


def test_a_member_another_schema_still_declares_is_not_dropped(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # Scoping the drop to "the rows this schema validates" does not make those
    # values exclusively this schema's: a sibling component of the same composite
    # reaches the same row, and clearing it there would invalidate that schema.
    from lamindb_setup.evolve.engine._drift_data import schema_member_conflicts

    a, b, shared = _shared_member_fixture(ln)

    assert schema_member_conflicts(a, shared) == ["SharedB"]
    assert schema_member_conflicts(b, shared) == ["SharedA"]

    # a feature only one of them declares is nobody else's business
    keep = ln.Feature.get(name="shared_keep", is_type=False)
    assert schema_member_conflicts(a, keep) == []


def test_no_conflict_when_the_other_schema_validates_no_shared_row(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # Declaring the same feature is not itself a conflict -- only sharing a row
    # that carries it is. An unrelated schema over its own rows must not block a
    # drop, or a feature reused across sheets could never be retired anywhere.
    from lamindb_setup.evolve.engine._drift_data import schema_member_conflicts

    ft = ln.Feature(name="disj_ft", is_type=True).save()
    shared = ln.Feature(name="disj_col", dtype="str", type=ft).save()
    a = ln.Schema(name="DisjA", features=[shared]).save()
    b = ln.Schema(name="DisjB", features=[shared]).save()
    for name, schema in (("disj_a", a), ("disj_b", b)):
        sheet = ln.Record(name=f"{name}_sheet", is_type=True, schema=schema).save()
        ln.Record(name=f"{name}_row", type=sheet, features={"disj_col": "d"}).save()

    assert schema_member_conflicts(a, shared) == []
    assert schema_member_conflicts(b, shared) == []


def test_an_index_declared_outside_features_is_not_removed(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # LaminDB inserts the index into the member list whatever the plan passed, so
    # a plan that declares it only as `index=` used to read it back as a member
    # to remove -- unlinking it on the next apply and leaving `Schema.index`
    # pointing at a feature the schema no longer had.
    _index_outside_features_plan.reset()
    compute_changes(_index_outside_features_plan, mode=ApplyMode.APPLY, lock=None)
    schema = ln.Schema.get(name="IdxSchema", is_type=False)
    assert [m.name for m in schema.members.all()] == ["idx_id", "idx_other"]

    _index_outside_features_plan.reset()
    again = compute_changes(
        _index_outside_features_plan, mode=ApplyMode.APPLY, lock=None
    )
    assert {c.kind: c for c in again.changes}["Schema"].action is Action.NO_CHANGE

    schema = ln.Schema.get(name="IdxSchema", is_type=False)
    assert [m.name for m in schema.members.all()] == ["idx_id", "idx_other"]
    assert schema.index is not None and schema.index.name == "idx_id"


def test_an_ordered_index_listed_last_does_not_ask_for_a_reorder(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # The index is always member zero, so an ordered schema listing it anywhere
    # else would otherwise want its links rebuilt on every single apply.
    _index_listed_last_plan.reset()
    compute_changes(_index_listed_last_plan, mode=ApplyMode.APPLY, lock=None)

    _index_listed_last_plan.reset()
    again = compute_changes(_index_listed_last_plan, mode=ApplyMode.APPLY, lock=None)
    assert {c.kind: c for c in again.changes}["Schema"].action is Action.NO_CHANGE

    schema = ln.Schema.get(name="IdxOrdSchema", is_type=False)
    assert [m.name for m in schema.members.all()] == ["idxord_id", "idxord_other"]


def test_deleting_a_feature_another_schema_declares_is_refused(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Trashing detaches nothing, so deleting a feature a schema outside the plan
    # still declares leaves that schema validating against a soft-deleted member
    # -- invisible to the diff, and what repair dangling then condemns the schema
    # itself for.
    applied = compute_changes(_removal_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _removal_v1_plan, tmp_path / "rm.lock")
    dropped = ln.Feature.get(name="rm_drop", is_type=False)
    ln.Schema(name="Outsider", features=[dropped]).save()

    with pytest.raises(DriftError, match="still declares it as a member") as excinfo:
        compute_changes(_removal_v2_plan, mode=ApplyMode.PLAN, lock=lock)
    assert "'Outsider'" in str(excinfo.value)

    # refused in APPLY too, and nothing was deleted
    with pytest.raises(DriftError, match="still declares it as a member"):
        compute_changes(_removal_v2_plan, mode=ApplyMode.APPLY, lock=lock)
    assert ln.Feature.filter(name="rm_drop", is_type=False).one_or_none() is not None
    outsider = ln.Schema.get(name="Outsider")
    assert [m.name for m in outsider.members.all()] == ["rm_drop"]


def test_a_feature_only_the_plans_own_schema_declared_is_still_deleted(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The guard must not fire on the plan's own schema: the member is unlinked in
    # the reconcile pass, before the removal pass runs, so by then nothing
    # declares it and the deletion is exactly what the plan asked for.  The
    # PLAN-mode preview -- which the CLI always runs first -- unlinks nothing,
    # and has to reach the same verdict.
    applied = compute_changes(_schema_add_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _schema_add_v1_plan, tmp_path / "sa.lock")

    preview = compute_changes(_schema_remove_v2_plan, mode=ApplyMode.PLAN, lock=lock)
    assert any(
        c.action is Action.DELETE and c.name == "schema_add_b" for c in preview.changes
    )

    ctx = compute_changes(_schema_remove_v2_plan, mode=ApplyMode.APPLY, lock=lock)
    assert any(
        c.action is Action.DELETE and c.name == "schema_add_b" for c in ctx.changes
    )
    assert ln.Feature.filter(name="schema_add_b", is_type=False).one_or_none() is None


def test_removing_a_schema_feature_without_a_lock_still_unlinks_it(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    """A first apply against a pre-existing instance has no lock to compare to.

    The member diff is computed against the instance, so the drop is applied
    just the same; only the *deletion* of the feature needs a lock entry to
    know the plan ever owned it.
    """
    compute_changes(_schema_add_v1_plan, mode=ApplyMode.APPLY, lock=None)

    ctx = compute_changes(_schema_remove_v2_plan, mode=ApplyMode.APPLY, lock=None)

    assert next(c for c in ctx.changes if c.kind == "Schema").action is Action.UPDATE
    schema = ln.Schema.get(name="SchemaAddSchema", is_type=False)
    assert {m.name for m in schema.members.all()} == {"schema_add_a"}
    assert (
        ln.Feature.filter(name="schema_add_b", is_type=False).one_or_none() is not None
    )


def test_plan_natural_keys_returns_declared_keys() -> None:
    keys = plan_natural_keys(_removal_v1_plan)

    drop = next(
        d.obj
        for d in _removal_v1_plan.dependencies
        if getattr(d.obj, "name", "") == "rm_drop"
    )
    assert natural_key(drop) in keys
    # the key set covers every declared object, not only the leaf features
    assert len(keys) == len(list(_removal_v1_plan.dependencies))


def test_claimed_key_suppresses_delete_and_keeps_object(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_removal_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _removal_v1_plan, tmp_path / "rm.lock")
    assert ln.Feature.filter(name="rm_drop", is_type=False).one_or_none() is not None

    # rm_drop is claimed by a sibling plan, so applying v2 must not delete it but
    # report it as a transfer whose destination is named via `claimed_by`.
    claimed = plan_natural_keys(_removal_v1_plan)
    drop_key = next(
        natural_key(d.obj)
        for d in _removal_v1_plan.dependencies
        if getattr(d.obj, "name", "") == "rm_drop"
    )
    ctx = compute_changes(
        _removal_v2_plan,
        mode=ApplyMode.APPLY,
        lock=lock,
        claimed_keys=claimed,
        claimed_by={drop_key: "plan_sibling.py:sibling"},
    )

    deletes = [c for c in ctx.changes if c.action is Action.DELETE]
    assert not any(c.name == "rm_drop" for c in deletes)
    transfers = [c for c in ctx.changes if c.action is Action.TRANSFER]
    transfer = next(c for c in transfers if c.name == "rm_drop")
    assert any("plan_sibling.py:sibling" in d for d in transfer.details)
    assert ln.Feature.filter(name="rm_drop", is_type=False).one_or_none() is not None


def test_claimed_key_suppresses_delete_in_plan_mode(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_removal_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _removal_v1_plan, tmp_path / "rm.lock")

    claimed = plan_natural_keys(_removal_v1_plan)
    preview = compute_changes(
        _removal_v2_plan, mode=ApplyMode.PLAN, lock=lock, claimed_keys=claimed
    )

    deletes = [c for c in preview.changes if c.action is Action.DELETE]
    assert not any(c.name == "rm_drop" for c in deletes)
    transfers = [c for c in preview.changes if c.action is Action.TRANSFER]
    assert any(c.name == "rm_drop" for c in transfers)


def test_moved_renames_feature_in_place(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_rename_feat_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _rename_feat_v1_plan, tmp_path / "rn.lock")
    old = ln.Feature.filter(name="rn_old", is_type=False).one_or_none()
    assert old is not None
    old_uid = old.uid

    # PLAN mode previews the rename as a MOVE without touching the instance.
    preview = compute_changes(_rename_feat_v2_plan, mode=ApplyMode.PLAN, lock=lock)
    moves = [c for c in preview.changes if c.action is Action.MOVE]
    move = next(c for c in moves if c.name == "rn_new")
    assert any("rn_old" in d for d in move.details)
    assert not [c for c in preview.changes if c.action is Action.CREATE]
    assert not [c for c in preview.changes if c.action is Action.DELETE]
    assert ln.Feature.filter(name="rn_old", is_type=False).one_or_none() is not None

    # APPLY mode renames the live object in place, preserving its uid.
    ctx = compute_changes(_rename_feat_v2_plan, mode=ApplyMode.APPLY, lock=lock)
    assert [c.name for c in ctx.changes if c.action is Action.MOVE] == ["rn_new"]
    assert ln.Feature.filter(name="rn_old", is_type=False).one_or_none() is None
    renamed = ln.Feature.filter(name="rn_new", is_type=False).one_or_none()
    assert renamed is not None
    assert renamed.uid == old_uid


def test_moved_is_idempotent_once_applied(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_rename_feat_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _rename_feat_v1_plan, tmp_path / "rn.lock")
    applied2 = compute_changes(_rename_feat_v2_plan, mode=ApplyMode.APPLY, lock=lock)
    lock2 = _persist_lock(applied2, _rename_feat_v2_plan, tmp_path / "rn.lock")

    # Re-applying the same plan+moves against the migrated lock is a no-op.
    again = compute_changes(_rename_feat_v2_plan, mode=ApplyMode.APPLY, lock=lock2)
    assert not [c for c in again.changes if c.action is Action.MOVE]
    assert again.move_outcome is not None
    assert again.move_outcome.already
    assert not again.move_outcome.applied


def test_rename_without_moved_is_delete_plus_create(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_rename_feat_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _rename_feat_v1_plan, tmp_path / "rn.lock")

    # Without ctx.moved the rename is a destructive delete + create.
    preview = compute_changes(
        _rename_feat_v2_no_moved_plan, mode=ApplyMode.PLAN, lock=lock
    )
    assert any(
        c.action is Action.DELETE and c.name == "rn_old" for c in preview.changes
    )
    create = next(
        c for c in preview.changes if c.action is Action.CREATE and c.name == "rn_new"
    )
    assert not [c for c in preview.changes if c.action is Action.MOVE]
    # The heuristic flags the likely-forgotten rename on the create.
    assert any("rename of deleted Feature 'rn_old'" in w for w in create.warnings)
    assert create.suspected_rename_of == "rn_old"


def test_an_undeclared_rename_is_a_rename_warning_not_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_rename_feat_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _rename_feat_v1_plan, tmp_path / "rn.lock")

    preview = compute_changes(
        _rename_feat_v2_no_moved_plan, mode=ApplyMode.PLAN, lock=lock
    )

    create = next(c for c in preview.changes if c.suspected_rename_of == "rn_old")
    assert [w.category for w in create.warnings] == [WarningCategory.RENAME]
    assert [c for c in preview.changes if c.warnings] == [create]
    terminal = render_changes(preview, title="Plan")
    markdown = render_changes_markdown(preview, title="Plan")
    banner = (
        "1 change(s) look like a rename not declared with ctx.moved; review the "
        "WARNING lines above."
    )
    assert f"! {banner}" in terminal
    assert f"> [!WARNING]\n> {banner}" in markdown
    assert "changed outside the plan" not in terminal + markdown


def test_suspected_renames_recorded_only_without_moved(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_rename_feat_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _rename_feat_v1_plan, tmp_path / "rn.lock")

    without_moved = compute_changes(
        _rename_feat_v2_no_moved_plan, mode=ApplyMode.PLAN, lock=lock
    )
    assert without_moved.suspected_renames == [("Feature", "rn_old", "rn_new")]

    with_moved = compute_changes(_rename_feat_v2_plan, mode=ApplyMode.PLAN, lock=lock)
    assert with_moved.suspected_renames == []


def test_each_create_is_suspected_of_renaming_one_deletion(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Two dropped features of one configuration and two created ones pair up:
    # each create is suspected of one rename, not one of them of both.
    applied = compute_changes(_rename_two_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _rename_two_v1_plan, tmp_path / "rn2.lock")

    preview = compute_changes(
        _rename_two_v2_no_moved_plan, mode=ApplyMode.PLAN, lock=lock
    )

    renames = preview.suspected_renames
    assert len(renames) == 2
    assert {old for _kind, old, _new in renames} == {"rn2_a", "rn2_b"}
    assert {new for _kind, _old, new in renames} == {"rn2_x", "rn2_y"}
    warned = {
        c.name: c.warnings
        for c in preview.changes
        if c.action is Action.CREATE and c.kind == "Feature"
    }
    assert set(warned) == {"rn2_x", "rn2_y"}
    assert all(len(warnings) == 1 for warnings in warned.values())


def test_apply_one_strict_renames_blocks_undeclared_rename(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from lamindb_setup.evolve.console import OutputFormat
    from lamindb_setup.evolve.engine import ApplyRefusedError
    from lamindb_setup.evolve.engine._apply_run import apply_one
    from lamindb_setup.evolve.engine._lock import lock_path

    applied = compute_changes(_rename_feat_v1_plan, mode=ApplyMode.APPLY, lock=None)
    plan_file = tmp_path / "plan_rn.py"
    write_lock(applied, _rename_feat_v1_plan, lock_path(plan_file, "slug"))

    # Strict (the default) refuses the undeclared rename before applying.
    with pytest.raises(ApplyRefusedError, match="refusing to apply"):
        apply_one(
            _rename_feat_v2_no_moved_plan,
            plan_file,
            "stemUID00001",
            "acc/inst",
            "slug",
            claimed_keys=set(),
            confirm=None,
            output_format=OutputFormat.markdown,
        )
    # Nothing was applied: the old object survives and no new one was created.
    assert ln.Feature.filter(name="rn_old", is_type=False).one_or_none() is not None
    assert ln.Feature.filter(name="rn_new", is_type=False).one_or_none() is None


def test_moved_with_structural_change_is_rejected(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_rename_feat_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _rename_feat_v1_plan, tmp_path / "rn.lock")
    with pytest.raises(DriftError):
        compute_changes(_rename_feat_bad_v2_plan, mode=ApplyMode.PLAN, lock=lock)


def test_a_declared_reparenting_moves_the_feature_whatever_the_data_flag(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A parent is identity, not data.  The flag used to lift the change for a
    # feature with no values, and since nothing applied `type` the lock then
    # recorded the new address while the feature stayed under `rp_a`.  A
    # declared move now sets the parent, so the lock and the instance agree.
    applied = compute_changes(_reparent_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _reparent_v1_plan, tmp_path / "rp.lock")

    applied = compute_changes(
        _reparent_v2_plan,
        mode=ApplyMode.APPLY,
        lock=lock,
        allow_data_validated_changes=True,
    )

    assert ln.Feature.get(name="rp_x", is_type=False).type.name == "rp_b"
    relocked = _persist_lock(applied, _reparent_v2_plan, tmp_path / "rp2.lock")
    entry = relocked.entry_for("FeatureType:rp_b/Feature:rp_x")
    assert entry is not None and entry.config["type"] == "FeatureType:rp_b"


def test_a_declared_reparenting_is_a_move_not_a_data_override(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_reparent_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _reparent_v1_plan, tmp_path / "rp.lock")

    ctx = compute_changes(_reparent_v2_plan, mode=ApplyMode.PLAN, lock=lock)

    change = {c.kind: c for c in ctx.changes}["Feature"]
    assert change.action is Action.MOVE
    assert not any("allowed despite block" in d for d in change.details)
    assert ln.Feature.get(name="rp_x", is_type=False).type.name == "rp_a"


def test_a_parent_changed_outside_the_plan_is_drift_even_with_the_data_flag(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Keeping it as "data-safe" would leave the instance under one parent and
    # the plan under another, and the next lock would hide it for good.
    applied = compute_changes(_reparent_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _reparent_v1_plan, tmp_path / "rp.lock")
    live = ln.Feature.get(name="rp_x", is_type=False)
    live.type = ln.Feature.get(name="rp_b", is_type=True)
    live.save()

    with pytest.raises(DriftError, match="remote drift") as excinfo:
        compute_changes(
            _reparent_v1_plan,
            mode=ApplyMode.APPLY,
            lock=lock,
            allow_data_validated_changes=True,
        )

    assert "type: instance now" in str(excinfo.value)


def test_a_schema_dtype_change_is_refused_without_the_flag(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The feature dtype rules used to classify it -- `int -> float` is a safe
    # widening there -- so it applied as no change: the instance kept `int`
    # and the rebuilt lock recorded `float`.
    _schema_dtype_int_plan.reset()
    applied = compute_changes(_schema_dtype_int_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _schema_dtype_int_plan, tmp_path / "sd.lock")
    _schema_dtype_float_plan.reset()

    with pytest.raises(
        PlanChangeRefusedError,
        match="^cannot change Schema 'SD' \\(Schema:SD\\) in place:\n  "
        "dtype 'int' \u2192 'float'",
    ):
        compute_changes(_schema_dtype_float_plan, mode=ApplyMode.APPLY, lock=lock)

    assert ln.Schema.get(name="SD").dtype == "int"


def test_the_flag_changes_the_dtype_of_a_schema_that_validated_nothing(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # Applied for real, so the lock and the instance agree.
    _schema_dtype_int_plan.reset()
    applied = compute_changes(_schema_dtype_int_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _schema_dtype_int_plan, tmp_path / "sd.lock")
    _schema_dtype_float_plan.reset()

    compute_changes(
        _schema_dtype_float_plan,
        mode=ApplyMode.APPLY,
        lock=lock,
        allow_data_validated_changes=True,
    )

    assert ln.Schema.get(name="SD").dtype == "float"


def test_a_schema_dtype_change_without_a_lock_is_refused(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # Without a lock only the diff against the instance can see it.
    _schema_dtype_int_plan.reset()
    compute_changes(_schema_dtype_int_plan, mode=ApplyMode.APPLY, lock=None)
    _schema_dtype_float_plan.reset()

    with pytest.raises(PlanChangeRefusedError, match="dtype 'int' \u2192 'float'"):
        compute_changes(_schema_dtype_float_plan, mode=ApplyMode.PLAN, lock=None)


def test_an_unchanged_schema_dtype_is_no_change(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The plan spells it `builtins.int` and the instance `int`.
    _schema_dtype_int_plan.reset()
    applied = compute_changes(_schema_dtype_int_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _schema_dtype_int_plan, tmp_path / "sd.lock")

    for previous in (lock, None):
        _schema_dtype_int_plan.reset()
        again = compute_changes(
            _schema_dtype_int_plan, mode=ApplyMode.APPLY, lock=previous
        )
        assert {c.action for c in again.changes} == {Action.NO_CHANGE}


@pytest.mark.parametrize("with_lock", [True, False])
def test_a_sheet_does_not_adopt_a_same_named_record_type(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    with_lock: bool,
) -> None:
    # The sheet used to find the record type by name and adopt it, schema-less;
    # the record type's lock entry was then deleted by uid -- trashing the very
    # record the sheet had just adopted, and every row under it.
    applied = compute_changes(_x_record_type_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _x_record_type_plan, tmp_path / "x.lock")
    record_type = ln.Record.get(name="rtx_X", is_type=True)
    ln.Record(name="rtx_row1", type=record_type).save()

    with pytest.raises(
        PlanChangeRefusedError, match="schema None \u2192 'Schema:rtx_XS'"
    ):
        compute_changes(
            _x_sheet_plan, mode=ApplyMode.APPLY, lock=lock if with_lock else None
        )

    record_type.refresh_from_db()
    assert record_type.branch_id == 1
    assert record_type.schema is None
    assert ln.Record.get(name="rtx_row1").branch_id == 1


def test_a_record_type_does_not_adopt_a_same_named_sheet(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The mirror case: the record type would adopt the sheet's record, and the
    # sheet's own entry would then delete it with its rows.
    applied = compute_changes(_x_sheet_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _x_sheet_plan, tmp_path / "x.lock")
    sheet = ln.Record.get(name="rtx_X", is_type=True)
    ln.Record(name="rtx_row1", type=sheet).save()

    with pytest.raises(DriftError, match="it is a sheet"):
        compute_changes(_x_back_to_record_type_plan, mode=ApplyMode.APPLY, lock=lock)

    sheet.refresh_from_db()
    assert sheet.branch_id == 1
    assert sheet.schema.name == "rtx_XS"
    assert ln.Record.get(name="rtx_row1").branch_id == 1


def test_an_unchanged_sheet_stays_unchanged(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_x_sheet_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _x_sheet_plan, tmp_path / "x.lock")

    for previous in (lock, None):
        again = compute_changes(_x_sheet_plan, mode=ApplyMode.APPLY, lock=previous)
        assert {c.action for c in again.changes} == {Action.NO_CHANGE}


def test_removals_never_delete_a_record_the_run_resolved_to() -> None:
    from lamindb_setup.evolve.engine._lock import LockEntry, LockFile

    ctx = ApplyContext()
    ctx.lock = LockFile(
        plan="p",
        objects={
            "RecordType:X": LockEntry(
                key="RecordType:X", kind="RecordType", name="X", uid="u1"
            )
        },
    )
    ctx.entries = {
        "Sheet:X": LockEntry(key="Sheet:X", kind="Sheet", name="X", uid="u1")
    }

    with pytest.raises(DriftError, match="'RecordType:X' \\(uid 'u1'\\) is the record"):
        ctx.apply_removals()


def test_a_list_element_narrowing_is_seen_in_the_lock(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # The lock and the plan used to both say `list`, so the plan-edit check
    # could not see the change at all.
    _ls_str_plan.reset()
    applied = compute_changes(_ls_str_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _ls_str_plan, tmp_path / "ls.lock")
    _ls_int_plan.reset()

    with pytest.raises(
        PlanChangeRefusedError,
        match=re.escape("dtype 'list[str]' \u2192 'list[int]'"),
    ):
        compute_changes(_ls_int_plan, mode=ApplyMode.PLAN, lock=lock)


@contextlib.contextmanager
def _untracked(plan_file: Path, *, stem_uid: str, config: Any) -> Any:
    """A `track` stand-in for `apply_one` that records no Transform/Run."""
    yield "untracked", None


def _apply_one_untracked(plan_file: Path, **kwargs: Any) -> str | None:
    from lamindb_setup.evolve.console import OutputFormat
    from lamindb_setup.evolve.engine import apply_one

    return apply_one(
        _apply_plan,
        plan_file,
        "stemUID00001",
        "acc/inst",
        "slug",
        claimed_keys=set(),
        output_format=OutputFormat.markdown,
        track=_untracked,
        **kwargs,
    )


def test_apply_one_applies_nothing_when_confirm_declines(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from lamindb_setup.evolve.core import LaminSchemasError
    from lamindb_setup.evolve.engine import ApplyAbortedError
    from lamindb_setup.evolve.engine._lock import lock_path

    asked: list[str] = []

    def decline(message: str) -> bool:
        asked.append(message)
        return False

    plan_file = tmp_path / "plan_apply.py"
    with pytest.raises(ApplyAbortedError, match="confirmation was declined") as exc:
        _apply_one_untracked(plan_file, confirm=decline)

    assert isinstance(exc.value, LaminSchemasError)
    assert len(asked) == 1
    assert asked[0].startswith("Apply ")
    assert asked[0].endswith(" change(s)?")
    assert ln.Feature.filter(name="apply_col").one_or_none() is None
    assert not lock_path(plan_file, "slug").exists()


@pytest.mark.parametrize("confirm", [None, lambda message: True])
def test_apply_one_applies_when_confirmed_or_not_asked(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    confirm: Any,
) -> None:
    from lamindb_setup.evolve.engine._lock import lock_path

    plan_file = tmp_path / "plan_apply.py"
    section = _apply_one_untracked(plan_file, confirm=confirm)

    assert section is not None
    assert "Applied" in section
    assert ln.Feature.filter(name="apply_col").one_or_none() is not None
    assert lock_path(plan_file, "slug").exists()


_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")


def _plain(text: str) -> str:
    """`text` without the ANSI styling rich adds when colour is forced."""
    return _ANSI_ESCAPE_RE.sub("", text)


@pytest.mark.parametrize("asked", [True, False])
def test_apply_one_previews_a_text_format_on_stderr_only_before_asking(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    asked: bool,
) -> None:
    # A text format renders its document only after the apply, so the prompt
    # used to be answered blind; the preview now precedes it, kept off stdout.
    seen: list[Any] = []

    def confirm(message: str) -> bool:
        seen.append(capsys.readouterr())
        return True

    section = _apply_one_untracked(
        tmp_path / "plan_apply.py", confirm=confirm if asked else None
    )

    assert section is not None
    after = capsys.readouterr()
    assert "+ Feature 'apply_col'" not in _plain(after.out + after.err)
    if asked:
        (before,) = seen
        # Under FORCE_COLOR (as in CI) rich colours the preview it prints.
        err = _plain(before.err)
        title = err.index(f"Plan: {_apply_plan.name}")
        assert title < err.index("+ Feature 'apply_col'")
        assert "apply_col" not in before.out
    else:
        assert seen == []


def test_apply_one_raises_apply_failed_when_rolled_back(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import lamindb_setup.evolve.engine._apply as apply_module
    from lamindb_setup.evolve.engine import ApplyFailedError
    from lamindb_setup.evolve.engine._lock import lock_path

    real_compute_changes = apply_module.compute_changes

    def fail_on_apply(plan: Any, *, mode: ApplyMode, **kwargs: Any) -> Any:
        if mode is ApplyMode.APPLY:
            raise RuntimeError("boom")
        return real_compute_changes(plan, mode=mode, **kwargs)

    # `apply_one` imports it from `_apply` when it runs, so patch it there.
    monkeypatch.setattr(apply_module, "compute_changes", fail_on_apply)
    plan_file = tmp_path / "plan_apply.py"
    with pytest.raises(ApplyFailedError, match="rolled back.*boom") as exc:
        _apply_one_untracked(plan_file, confirm=None)

    assert isinstance(exc.value.__cause__, RuntimeError)
    assert not lock_path(plan_file, "slug").exists()


def test_apply_one_failure_report_lists_the_unchanged_objects_too(
    tmp_path: Path, request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    import subprocess

    import lamindb_setup.evolve.engine._apply as apply_module
    from lamindb_setup.evolve.console import OutputFormat
    from lamindb_setup.evolve.engine import ApplyFailedError, apply_one
    from lamindb_setup.evolve.engine._loader import load_plan
    from lamindb_setup.evolve.engine._track import (
        ensure_all_committed_and_pushed,
        verified_session,
    )
    from lamindb_setup.evolve.introspect._template import add_stem_uid_to_plan

    def git(*args: str) -> None:
        subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True)

    git("init", "-b", "main")
    source = tmp_path / "tracked_plan.py"
    source.write_text(
        "from lamindb_setup.evolve import Feature, FeatureType, LaminDType, PlanContext, create_plan\n"
        "@create_plan\n"
        "def plan(ctx: PlanContext, exports: None):\n"
        "    Feature('apply_col', dtype=LaminDType(str), type=FeatureType('apply_ft'))\n"
    )
    stem_uid = add_stem_uid_to_plan(source)
    git("add", source.name)
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.org")
    git("commit", "--no-gpg-sign", "-m", "Add plan")
    git("clone", "--local", ".", "checkout")
    plan, path, module = load_plan(str(tmp_path / "checkout" / source.name))
    # Plan files must be imported before the fixture connects to the instance.
    ln = request.getfixturevalue("ln")
    monkeypatch.setenv("LAMIN_SCHEMA_EVOLUTION_TRACK_PREFIX", "plans")
    real_compute_changes = apply_module.compute_changes

    def fail_on_apply(plan: Any, *, mode: ApplyMode, **kwargs: Any) -> Any:
        if mode is ApplyMode.APPLY:
            raise RuntimeError("boom")
        return real_compute_changes(plan, mode=mode, **kwargs)

    ln.Feature(name="apply_ft", is_type=True, dtype=None).save()  # type: ignore[arg-type]

    def apply() -> None:
        apply_one(
            plan,
            path,
            module.PLAN_TRANSFORM_STEM_UID,
            ln.setup.settings.instance.slug,
            ln.setup.settings.instance.slug.replace("/", "-"),
            claimed_keys=set(),
            confirm=None,
            output_format=OutputFormat.markdown,
        )

    with verified_session(ensure_all_committed_and_pushed([path])):
        with monkeypatch.context() as patch:
            patch.setattr(apply_module, "compute_changes", fail_on_apply)
            with pytest.raises(ApplyFailedError, match="boom"):
                apply()
    apply()

    applied = ln.Feature.get(name="apply_col").run
    failed = ln.Run.filter(transform=applied.transform).exclude(pk=applied.pk).one()
    assert applied.status == "completed"
    assert failed.status == "errored"
    assert applied.transform.key == f"plans/{source.name}"
    assert applied.transform.stem_uid == stem_uid
    assert failed.report.run == failed
    report = failed.report.cache().read_text()
    assert "\n    FeatureType 'apply_ft'  " in report
    assert "\n  + Feature 'apply_col'  " in report
    assert "1 unchanged" in report


def test_moved_feature_type_cascades_to_children(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_rename_ft_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _rename_ft_v1_plan, tmp_path / "cas.lock")
    ft_old = ln.Feature.filter(name="cas_old", is_type=True).one_or_none()
    child = ln.Feature.filter(name="cas_child", is_type=False).one_or_none()
    assert ft_old is not None and child is not None
    ft_uid, child_uid = ft_old.uid, child.uid

    ctx = compute_changes(_rename_ft_v2_plan, mode=ApplyMode.APPLY, lock=lock)

    # Only the feature type is a MOVE; the child follows without a change.
    assert [c.name for c in ctx.changes if c.action is Action.MOVE] == ["cas_new"]
    child_change = next(c for c in ctx.changes if c.name == "cas_child")
    assert child_change.action is Action.NO_CHANGE
    assert not [c for c in ctx.changes if c.action is Action.DELETE]

    renamed_ft = ln.Feature.filter(name="cas_new", is_type=True).one_or_none()
    assert renamed_ft is not None and renamed_ft.uid == ft_uid
    still_child = ln.Feature.filter(name="cas_child", is_type=False).one_or_none()
    assert still_child is not None and still_child.uid == child_uid


def test_moved_rename_onto_existing_name_is_rejected(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_collision_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _collision_v1_plan, tmp_path / "col.lock")

    # A different object already occupies the rename target name.
    ft = ln.Feature.filter(name="col_ft", is_type=True).one()
    ln.Feature(name="col_b", dtype="str", type=ft, is_type=False).save()

    with pytest.raises(DriftError, match="already uses that name"):
        compute_changes(_collision_v2_plan, mode=ApplyMode.APPLY, lock=lock)


def test_moved_renames_schema_member_without_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_rename_member_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _rename_member_v1_plan, tmp_path / "mem.lock")
    old_uid = ln.Feature.filter(name="mem_old", is_type=False).one().uid

    ctx = compute_changes(_rename_member_v2_plan, mode=ApplyMode.APPLY, lock=lock)

    # The member feature is a MOVE; the schema that lists it is unchanged
    # (its member reference is rebased, so it is not seen as a removed feature).
    assert [c.name for c in ctx.changes if c.action is Action.MOVE] == ["mem_new"]
    schema_change = next(c for c in ctx.changes if c.kind == "Schema")
    assert schema_change.action is Action.NO_CHANGE
    assert not [c for c in ctx.changes if c.action is Action.DELETE]
    assert ln.Feature.filter(name="mem_new", is_type=False).one().uid == old_uid


def test_moved_works_with_declarative_fields(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    applied = compute_changes(_decl_rename_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _decl_rename_v1_plan, tmp_path / "decl.lock")
    old_uid = ln.Feature.filter(name="old_col", is_type=False).one().uid

    ctx = compute_changes(_decl_rename_v2_plan, mode=ApplyMode.APPLY, lock=lock)

    assert [c.name for c in ctx.changes if c.action is Action.MOVE] == ["new_col"]
    assert not [c for c in ctx.changes if c.action is Action.DELETE]
    assert ln.Feature.filter(name="old_col", is_type=False).one_or_none() is None
    assert ln.Feature.filter(name="new_col", is_type=False).one().uid == old_uid


def test_remote_rename_matching_the_plan_is_a_warning_not_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    """Renaming on the instance first and editing the plan to match must converge.

    The recorded snapshot still describes the object under its old name, so the
    remote-to-remote comparison sees the name change and would otherwise refuse
    it as unreconcilable drift -- even though the plan now declares exactly that
    rename and there is nothing left to write.  Only the lock is behind.
    """
    applied = compute_changes(_rename_member_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _rename_member_v1_plan, tmp_path / "mem.lock")
    live = ln.Feature.filter(name="mem_old", is_type=False).one()
    old_uid = live.uid
    live.name = "mem_new"
    live.save()

    ctx = compute_changes(_rename_member_v2_plan, mode=ApplyMode.APPLY, lock=lock)

    change = next(c for c in ctx.changes if c.name == "mem_new")
    assert any("already applied to the instance" in w for w in change.warnings)
    # nothing to write, but the change is surfaced so the lock refresh is visible
    assert change.action is Action.UPDATE
    assert not [c for c in ctx.changes if c.action is Action.DELETE]
    assert ln.Feature.filter(name="mem_new", is_type=False).one().uid == old_uid
    # the lock now records the new address and the new snapshot
    entry = ctx.build_lock(_rename_member_v2_plan.name).entry_for(
        "FeatureType:mem_ft/Feature:mem_new"
    )
    assert entry is not None
    assert entry.uid == str(old_uid)
    assert entry.remote_snapshot is not None
    assert entry.remote_snapshot["name"] == "mem_new"
    # and re-running is clean
    again = compute_changes(
        _rename_member_v2_plan,
        mode=ApplyMode.APPLY,
        lock=ctx.build_lock(_rename_member_v2_plan.name),
    )
    assert {c.action for c in again.changes} == {Action.NO_CHANGE}


def test_remote_rename_to_a_third_name_is_reconciled_to_the_plan(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    """A declared move licenses the rename whatever name the instance drifted to."""
    applied = compute_changes(_rename_member_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _rename_member_v1_plan, tmp_path / "mem.lock")
    live = ln.Feature.filter(name="mem_old", is_type=False).one()
    old_uid = live.uid
    live.name = "mem_something_else"
    live.save()

    ctx = compute_changes(_rename_member_v2_plan, mode=ApplyMode.APPLY, lock=lock)

    change = next(c for c in ctx.changes if c.name == "mem_new")
    assert change.action is Action.MOVE
    assert any("the plan declares a rename for it" in w for w in change.warnings)
    assert ln.Feature.filter(name="mem_new", is_type=False).one().uid == old_uid


def test_remote_rename_without_moved_is_still_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    """The relaxation is scoped to a declared move; an undeclared one still fails."""
    applied = compute_changes(_rename_member_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = _persist_lock(applied, _rename_member_v1_plan, tmp_path / "mem.lock")
    live = ln.Feature.filter(name="mem_old", is_type=False).one()
    live.name = "mem_renamed_behind_the_plans_back"
    live.save()

    with pytest.raises(DriftError, match="remote drift"):
        compute_changes(_rename_member_v1_plan, mode=ApplyMode.APPLY, lock=lock)


def test_render_move_outcome_reports_states() -> None:
    from lamindb_setup.evolve.engine._apply import MoveOutcome
    from lamindb_setup.evolve.render import render_move_outcome

    assert render_move_outcome(None, instance_slug="acme/dev") == []
    assert render_move_outcome(MoveOutcome(), instance_slug="acme/dev") == []

    applied = render_move_outcome(
        MoveOutcome(applied=[("A:old", "A:new")]), instance_slug="acme/dev"
    )
    assert any("Renamed 1 object(s) in place on acme/dev" in line for line in applied)

    unmatched = render_move_outcome(
        MoveOutcome(unmatched=[("A:old", "A:new")]), instance_slug="acme/dev"
    )
    assert any("matched nothing on acme/dev" in line for line in unmatched)

    satisfied = render_move_outcome(
        MoveOutcome(already=[("A:old", "A:new")]), instance_slug="acme/dev"
    )
    assert any(
        "already satisfied" in line and "safe to remove" in line for line in satisfied
    )


def test_prefetch_keeps_existence_queries_independent_of_object_count(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    from django.db import connection
    from django.test.utils import CaptureQueriesContext

    # Create the objects so the existence lookups have something to find.
    compute_changes(_bulk_plan, mode=ApplyMode.APPLY, lock=None)
    deps = list(_bulk_plan.dependencies)
    assert len(deps) > 20  # a sizeable plan

    # A prefetching preview issues a small, constant number of existence reads.
    with CaptureQueriesContext(connection) as prefetched:
        compute_changes(_bulk_plan, mode=ApplyMode.PLAN, lock=None, prefetch=True)

    # The same preview without prefetch issues at least one query per object.
    with CaptureQueriesContext(connection) as per_object:
        compute_changes(_bulk_plan, mode=ApplyMode.PLAN, lock=None, prefetch=False)

    # Prefetch collapses the per-object N+1 into a handful of batched queries.
    assert len(prefetched) < len(deps)
    assert len(prefetched) < len(per_object)
    assert len(per_object) >= len(deps)


def test_categorical_presence_prefetch_batches_link_table_reads(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    from django.db import connection
    from django.test.utils import CaptureQueriesContext
    from lamindb_setup.evolve.engine._drift_data import (
        _categorical_link_models,
        feature_has_any_categorical_value,
        prefetch_categorical_presence,
    )

    annotated = ln.Feature(name="presence_yes", dtype=ln.ULabel).save()
    bare = ln.Feature(name="presence_no", dtype=ln.ULabel).save()
    label = ln.ULabel(name="presence_label").save()
    sheet = ln.Record(name="PresenceSheet", is_type=True).save()
    ln.Record(name="p1", type=sheet).save().features.set_values({"presence_yes": label})
    features = [annotated, bare]

    with CaptureQueriesContext(connection) as batched:
        presence = prefetch_categorical_presence(features)
    assert presence == {annotated.pk: True, bare.pk: False}

    # The batch answers exactly what the per-object walk it replaces answers ...
    with CaptureQueriesContext(connection) as per_object:
        assert [feature_has_any_categorical_value(f) for f in features] == [True, False]

    # ... but in one statement for every feature and link table together, where
    # the walk costs one per link table for each feature that holds no values --
    # and lamindb declares a link table per categorical registry.
    n_tables = len(_categorical_link_models(bare))
    assert n_tables > 5
    assert len(batched) == 1
    assert len(per_object) >= n_tables

    # A prefetched answer is consumed without touching the instance at all.
    with CaptureQueriesContext(connection) as consumed:
        assert feature_has_any_categorical_value(bare, has_values=False) is False
    assert len(consumed) == 0


def test_link_tables_are_read_by_host_rather_than_by_a_value_column(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # lamindb names the label column `value` only when the annotated object is
    # a Record; every other host names it after the label's own registry. Keying
    # on `value` therefore made an artifact's categorical annotations invisible
    # to every value walk -- while the row-side count still saw them, so a member
    # removal could never prove its cleanup succeeded.
    from lamindb_setup.evolve.engine._drift_data import (
        _is_categorical_link_model,
        link_label_field,
        link_subject_field,
    )

    for model, subject, label in (
        (ln.models.RecordRecord, "record", "value"),
        (ln.models.RecordULabel, "record", "value"),
        (ln.models.ArtifactRecord, "artifact", "record"),
        (ln.models.ArtifactULabel, "artifact", "ulabel"),
        (ln.models.CollectionULabel, "collection", "ulabel"),
    ):
        assert _is_categorical_link_model(model), model.__name__
        assert link_subject_field(model).name == subject, model.__name__
        assert link_label_field(model).name == label, model.__name__

    # The scalar stores and the schema-membership table stay out: their value is
    # a JSON column, or they carry no label at all.
    for model in (ln.models.RecordJson, ln.models.JsonValue, ln.models.SchemaFeature):
        assert not _is_categorical_link_model(model), model.__name__


def test_prefetch_schema_members_batches_reads_and_matches_lamindb(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    from django.db import connection
    from django.test.utils import CaptureQueriesContext

    compute_changes(_members_plan, mode=ApplyMode.APPLY, lock=None)
    schemas = [
        ln.Schema.filter(name=name, is_type=False).one()
        for name in ("MembersOne", "MembersTwo")
    ]
    # What lamindb itself reports, read one schema at a time.
    expected = {s.name: [m.name for m in s.members.all()] for s in schemas}
    assert expected == {
        "MembersOne": ["members_a", "members_b", "members_c"],
        "MembersTwo": ["members_c", "members_a"],
    }

    ctx = ApplyContext()
    with CaptureQueriesContext(connection) as cap:
        ctx.prefetch_schema_members(schemas)

    # One read of the link table plus one of the member registry, for *both*
    # schemas together -- not the two-per-schema that Schema.members costs.
    assert len(cap) == 2
    assert {
        s.name: [m.name for m in ctx._members_cache[id(s)]] for s in schemas
    } == expected

    # Resolving the members now needs no further reads, and asking again does
    # not re-fetch what is already cached.
    with CaptureQueriesContext(connection) as cached:
        for schema in schemas:
            assert [m.name for m in ctx._schema_members(schema)] == expected[
                schema.name
            ]
        ctx.prefetch_schema_members(schemas)
    assert len(cached) == 0


def test_prefetch_schema_members_leaves_unmirrorable_schemas_to_lamindb(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    from types import SimpleNamespace

    from lamindb_setup.evolve.engine._apply import _schema_member_link

    compute_changes(_members_plan, mode=ApplyMode.APPLY, lock=None)
    schema = ln.Schema.filter(name="MembersOne", is_type=False).one()
    through, member_model, _fk = _schema_member_link(schema)  # type: ignore[misc]

    def _fake(**overrides: object) -> object:
        base = dict(
            is_type=False,
            itype="Feature",
            pk=schema.pk,
            _state=SimpleNamespace(db=schema._state.db),
            _get_related_name=lambda: "features",
            features=SimpleNamespace(through=through, model=member_model),
        )
        return SimpleNamespace(**{**base, **overrides})

    # A link table without a member-side foreign key, and an itype with no
    # related manager at all, are both shapes this cannot mirror.
    schema_fk = next(f for f in through._meta.fields if f.name == "schema")
    no_member_fk = _fake(
        features=SimpleNamespace(
            through=SimpleNamespace(_meta=SimpleNamespace(fields=[schema_fk])),
            model=member_model,
        )
    )
    no_manager = _fake(_get_related_name=lambda: "not_a_manager")
    assert _schema_member_link(no_member_fk) is None
    assert _schema_member_link(no_manager) is None

    # Type and composite schemas are skipped: lamindb defines their members as
    # empty rather than reading the link table.
    ctx = ApplyContext()
    ctx.prefetch_schema_members(
        [
            no_member_fk,
            no_manager,
            _fake(is_type=True),
            _fake(itype="Composite"),
        ]
    )
    assert ctx._members_cache == {}

    # Nothing cached means Schema.members still answers for them.
    assert [m.name for m in ctx._schema_members(schema)] == [
        "members_a",
        "members_b",
        "members_c",
    ]


def test_prefetch_plans_reads_every_plan_at_once_and_serves_their_diffs(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    from django.db import connection
    from django.test.utils import CaptureQueriesContext

    plans = (_shared_one_plan, _shared_two_plan)
    locks = []
    for plan in plans:
        ctx = compute_changes(plan, mode=ApplyMode.APPLY, lock=None)
        locks.append(_persist_lock(ctx, plan, tmp_path / f"{id(plan)}.lock"))
        plan.reset()

    # Diffing each plan on its own repeats the batched reads once per plan.
    with CaptureQueriesContext(connection) as per_plan:
        for plan, lock in zip(plans, locks, strict=True):
            ctx = compute_changes(plan, mode=ApplyMode.PLAN, lock=lock)
            assert {c.action for c in ctx.changes} == {Action.NO_CHANGE}

    # One shared read covers both plans: two existence queries (one uid lookup
    # per backing registry -- the locks pin every object, so no name lookup is
    # needed) plus the two-query bulk member read.
    existing_cache = ExistenceCache()
    members_cache: dict[int, list[Any]] = {}
    with CaptureQueriesContext(connection) as shared:
        prefetch_plans(
            list(zip(plans, locks, strict=True)),
            existing_cache=existing_cache,
            members_cache=members_cache,
        )
    assert len(shared) == 4
    assert len(shared) < len(per_plan)

    # Every object resolved, and the caches carry the whole diff: reusing them
    # reproduces the per-plan verdicts without touching the database again.
    assert all(record is not None for record in existing_cache.values())
    with CaptureQueriesContext(connection) as reused:
        for plan, lock in zip(plans, locks, strict=True):
            ctx = compute_changes(
                plan,
                mode=ApplyMode.PLAN,
                lock=lock,
                existing_cache=existing_cache,
                members_cache=members_cache,
            )
            assert {c.action for c in ctx.changes} == {Action.NO_CHANGE}
    assert len(reused) == 0


def test_forget_missing_lets_a_sibling_see_what_the_previous_apply_created(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # The two plans deploy together and declare the same feature type and
    # feature -- the shape a hand-off between siblings passes through.  One
    # shared read serves both, but applying the first makes part of it stale.
    plans = (_codeploy_one_plan, _codeploy_two_plan)
    existing_cache = ExistenceCache()
    members_cache: dict[int, list[Any]] = {}
    prefetch_plans(
        [(plan, None) for plan in plans],
        existing_cache=existing_cache,
        members_cache=members_cache,
    )

    first = compute_changes(
        _codeploy_one_plan,
        mode=ApplyMode.APPLY,
        lock=None,
        existing_cache=existing_cache,
        members_cache=members_cache,
    )
    assert any(
        c.action is Action.CREATE and c.name == "codeploy_shared" for c in first.changes
    )

    # Left alone, the shared read still records the feature the first plan just
    # created as missing, and the sibling would create it a second time.  Shown
    # against a copy, and read-only, so the duplicate is never written.
    stale = compute_changes(
        _codeploy_two_plan,
        mode=ApplyMode.PLAN,
        lock=None,
        existing_cache=existing_cache.copy(),
        members_cache=dict(members_cache),
    )
    assert any(
        c.action is Action.CREATE and c.name == "codeploy_shared" for c in stale.changes
    )
    _codeploy_two_plan.reset()

    forget_missing(existing_cache)
    second = compute_changes(
        _codeploy_two_plan,
        mode=ApplyMode.APPLY,
        lock=None,
        existing_cache=existing_cache,
        members_cache=members_cache,
    )
    assert not any(
        c.action is Action.CREATE and c.name == "codeploy_shared"
        for c in second.changes
    )
    assert ln.Feature.filter(name="codeploy_shared", is_type=False).count() == 1
    assert ln.Feature.filter(name="codeploy_ft", is_type=True).count() == 1


def test_create_does_not_adopt_a_same_named_record_of_another_kind(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # A plain feature that shares its name with the feature type declared below.
    # lamindb's own "did you mean" lookup matches on the name alone, so it would
    # hand this record back instead of creating the type; the apply context
    # decides existence itself, on the stricter (name, is_type, parent) key.
    ln.Feature(name="adopt_me", dtype="str").save()

    compute_changes(_adopt_plan, mode=ApplyMode.APPLY, lock=None)

    # Two distinct records, not one record quietly turned into the other.
    typed = ln.Feature.filter(name="adopt_me", is_type=True).one()
    plain = ln.Feature.filter(name="adopt_me", is_type=False).one()
    assert typed.pk != plain.pk


def test_apply_rolls_back_all_changes_on_failure(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from django.db import transaction

    _atomic_plan.finalize()
    deps = [d.obj for d in _atomic_plan.dependencies]
    boom = next(o for o in deps if getattr(o, "name", None) == "atomic_boom")

    def _fail(ctx: object) -> object:
        raise RuntimeError("boom")

    monkeypatch.setattr(boom, "commit", _fail)

    # The object mutations run inside one transaction: a mid-apply failure must
    # roll every change back, leaving the instance untouched.
    with pytest.raises(RuntimeError, match="boom"):
        with transaction.atomic():
            compute_changes(_atomic_plan, mode=ApplyMode.APPLY, lock=None)

    assert ln.Feature.filter(name="atomic_ok", is_type=True).one_or_none() is None
    assert ln.Feature.filter(name="atomic_boom", is_type=True).one_or_none() is None


def test_apply_sets_single_space_and_experiment_sheet(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    ln.Space(name="ss-default", uid="spaceUID0001").save()
    ln.Space(name="ss-target", uid="spaceUID0002").save()

    ctx = compute_changes(_single_space_plan, mode=ApplyMode.APPLY, lock=None)
    assert all(c.action is Action.CREATE for c in ctx.changes)

    rt = ln.Record.get(name="ss_rt", is_type=True)
    assert rt._aux["ss"] == "spaceUID0002"
    assert rt.settings.single_space.uid == "spaceUID0002"

    sheet = ln.Record.get(name="ss_sheet", is_type=True)
    assert sheet._aux["ss"] == "spaceUID0002"
    assert sheet._aux["isExperimentSheet"] is True
    assert sheet.settings.single_space.uid == "spaceUID0002"

    # Re-applying the same plan is a no-op (idempotent).
    lock = ctx.build_lock(_single_space_plan.name)
    reapplied = compute_changes(_single_space_plan, mode=ApplyMode.APPLY, lock=lock)
    assert {c.action for c in reapplied.changes} == {Action.NO_CHANGE}


def test_reconcile_enables_single_space_and_experiment_sheet(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    ln.Space(name="recss-default", uid="spaceUID0001").save()
    ln.Space(name="recss-target", uid="spaceUID0002").save()

    first = compute_changes(_reconcile_ss_v1_plan, mode=ApplyMode.APPLY, lock=None)
    lock = first.build_lock(_reconcile_ss_v1_plan.name)
    assert (ln.Record.get(name="rec_ss_rt", is_type=True)._aux or {}).get("ss") is None

    second = compute_changes(_reconcile_ss_v2_plan, mode=ApplyMode.APPLY, lock=lock)
    rt_change = next(
        c for c in second.changes if c.kind == "RecordType" and c.name == "rec_ss_rt"
    )
    sheet_change = next(
        c for c in second.changes if c.kind == "Sheet" and c.name == "rec_ss_sheet"
    )
    assert rt_change.action is Action.UPDATE
    assert sheet_change.action is Action.UPDATE
    assert any("single_space" in d for d in rt_change.details)
    assert any("is_experiment_sheet" in d for d in sheet_change.details)

    rt = ln.Record.get(name="rec_ss_rt", is_type=True)
    assert rt._aux["ss"] == "spaceUID0002"
    sheet = ln.Record.get(name="rec_ss_sheet", is_type=True)
    assert sheet._aux["ss"] == "spaceUID0002"
    assert sheet._aux["isExperimentSheet"] is True

    # Re-applying the v2 plan is now a no-op.
    lock2 = second.build_lock(_reconcile_ss_v2_plan.name)
    again = compute_changes(_reconcile_ss_v2_plan, mode=ApplyMode.APPLY, lock=lock2)
    assert {c.action for c in again.changes} == {Action.NO_CHANGE}


def test_lock_records_single_space_and_experiment_sheet(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    ctx = compute_changes(_single_space_plan, mode=ApplyMode.PLAN, lock=None)
    lock = ctx.build_lock(_single_space_plan.name)

    rt_entry = lock.entry_for("RecordType:ss_rt")
    assert rt_entry is not None
    assert rt_entry.config["single_space_uid"] == "spaceUID0002"

    sheet_entry = lock.entry_for("RecordType:ss_rt/Sheet:ss_sheet")
    assert sheet_entry is not None
    assert sheet_entry.config["single_space_uid"] == "spaceUID0002"
    assert sheet_entry.config["is_experiment_sheet"] is True


def test_apply_sets_exclude_children_flag(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    ctx = compute_changes(_exclude_children_plan, mode=ApplyMode.APPLY, lock=None)
    assert all(c.action is Action.CREATE for c in ctx.changes)

    rt = ln.Record.get(name="excl_parent", is_type=True)
    assert rt._aux["_excludeChildrenFromPlans"] == 1
    sheet = ln.Record.get(name="excl_sheet", is_type=True)
    assert sheet._aux["_excludeChildrenFromPlans"] == 1

    # Re-applying the same plan is a no-op (idempotent).
    lock = ctx.build_lock(_exclude_children_plan.name)
    reapplied = compute_changes(_exclude_children_plan, mode=ApplyMode.APPLY, lock=lock)
    assert {c.action for c in reapplied.changes} == {Action.NO_CHANGE}


def test_reconcile_toggles_exclude_children_off(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    first = compute_changes(_exclude_children_plan, mode=ApplyMode.APPLY, lock=None)
    lock = first.build_lock(_exclude_children_plan.name)
    parent = ln.Record.get(name="excl_parent", is_type=True)
    assert parent._aux["_excludeChildrenFromPlans"] == 1

    second = compute_changes(
        _exclude_children_off_plan, mode=ApplyMode.APPLY, lock=lock
    )
    rt_change = next(
        c for c in second.changes if c.kind == "RecordType" and c.name == "excl_parent"
    )
    assert rt_change.action is Action.UPDATE
    assert any("exclude_children_from_plans" in d for d in rt_change.details)

    rt = ln.Record.get(name="excl_parent", is_type=True)
    assert "_excludeChildrenFromPlans" not in (rt._aux or {})
    sheet = ln.Record.get(name="excl_sheet", is_type=True)
    assert "_excludeChildrenFromPlans" not in (sheet._aux or {})

    # Re-applying the off plan is now a no-op.
    lock2 = second.build_lock(_exclude_children_off_plan.name)
    again = compute_changes(
        _exclude_children_off_plan, mode=ApplyMode.APPLY, lock=lock2
    )
    assert {c.action for c in again.changes} == {Action.NO_CHANGE}


def test_lock_records_exclude_children(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    ctx = compute_changes(_exclude_children_plan, mode=ApplyMode.PLAN, lock=None)
    lock = ctx.build_lock(_exclude_children_plan.name)

    rt_entry = lock.entry_for("RecordType:excl_parent")
    assert rt_entry is not None
    assert rt_entry.config["exclude_children_from_plans"] is True

    sheet_entry = lock.entry_for("RecordType:excl_parent/Sheet:excl_sheet")
    assert sheet_entry is not None
    assert sheet_entry.config["exclude_children_from_plans"] is True


def test_record_rejects_single_space_uid() -> None:
    with pytest.raises(TypeError):
        Record("no_single_space", single_space_uid=SpaceUID("spaceUID0002"))  # type: ignore[call-arg]


# --- objects borrowed from a prerequisite plan -------------------------------
#
# `_borrower_plan` depends on `_owner_plan` and references exactly one of the
# three objects the owner declares.  The owner's body reads `_shared_state` so
# a test can re-declare it differently -- a new description, a wider dtype, a
# rename -- and check that the borrower is unaffected by a change to an object
# it does not manage.

_shared_state: dict[str, Any] = {}
_SHARED_DEFAULTS: dict[str, Any] = {
    "name": "dep_used",
    "dtype": int,
    "description": None,
    "moved_from": None,
    "drop_unused": False,
}


class _SharedExports(PlanExports):
    used: Feature
    unused: Feature


@create_plan
def _owner_plan(ctx: PlanContext, exports: _SharedExports) -> None:
    ft = FeatureType("dep_ft")
    exports.used = Feature(
        _shared_state["name"],
        dtype=LaminDType(_shared_state["dtype"]),
        type=ft,
        description=_shared_state["description"],
    )
    if not _shared_state["drop_unused"]:
        exports.unused = Feature("dep_unused", dtype=LaminDType(str), type=ft)
    if _shared_state["moved_from"] is not None:
        ctx.moved(exports.used, _shared_state["moved_from"])


@create_plan
def _borrower_plan(
    ctx: PlanContext,
    exports: None,
    owner: Plan[_SharedExports] = _owner_plan,
) -> None:
    Schema(name="BorrowerSchema", features=[owner.exports.used])


@pytest.fixture
def shared_plans() -> Iterator[None]:
    """Re-run both plan bodies from a known `_shared_state` for every test."""
    _shared_state.clear()
    _shared_state.update(_SHARED_DEFAULTS)
    _borrower_plan.reset()  # cascades to _owner_plan
    yield
    _shared_state.clear()
    _shared_state.update(_SHARED_DEFAULTS)
    _borrower_plan.reset()


def _redeclare_owner(**changes: Any) -> None:
    """Re-run both plan bodies with the owner's declaration changed."""
    _shared_state.update(changes)
    _borrower_plan.reset()


def test_borrowed_object_missing_from_the_instance_refuses_to_apply(
    instance: TestInstanceInfo, ln: LamindbModuleType, shared_plans: None
) -> None:
    """Applying out of order must refuse, not create the owner's objects itself.

    Creating them is what makes the owner's *next* rename impossible: the object
    would already sit under the name the owner is about to rename onto.
    """
    with pytest.raises(MissingPrerequisiteError, match="Apply that plan first"):
        compute_changes(_borrower_plan, mode=ApplyMode.APPLY, lock=None)

    # nothing was written
    assert ln.Feature.filter(name="dep_used", is_type=False).one_or_none() is None
    assert ln.Schema.filter(name="BorrowerSchema").one_or_none() is None

    # a preview still works, so `plan` against a fresh instance is unaffected
    preview = compute_changes(_borrower_plan, mode=ApplyMode.PLAN, lock=None)
    borrowed = next(c for c in preview.changes if c.name == "dep_used")
    assert borrowed.action is Action.CREATE
    assert any("has not been applied to this instance" in w for w in borrowed.warnings)


def test_a_missing_prerequisite_is_a_prerequisite_warning_not_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, shared_plans: None
) -> None:
    preview = compute_changes(_borrower_plan, mode=ApplyMode.PLAN, lock=None)

    warned = [c for c in preview.changes if c.warnings]
    assert {c.name for c in warned} == {"dep_ft", "dep_used", "dep_unused"}
    assert {w.category for c in warned for w in c.warnings} == {
        WarningCategory.PREREQUISITE
    }
    terminal = render_changes(preview, title="Plan")
    markdown = render_changes_markdown(preview, title="Plan")
    banner = (
        "3 object(s) are owned by a prerequisite plan that is not applied to this "
        "instance."
    )
    assert f"! {banner}" in terminal
    assert f"> [!WARNING]\n> {banner}" in markdown
    assert "changed outside the plan" not in terminal + markdown


@create_plan
def _downstream_plan(
    ctx: PlanContext,
    exports: None,
    borrower: Plan[PlanExports] = _borrower_plan,
) -> None:
    Feature("dep_downstream", dtype=LaminDType(str))


def test_a_preview_names_the_prerequisite_owning_each_missing_object(
    instance: TestInstanceInfo, ln: LamindbModuleType, shared_plans: None
) -> None:
    """The structured marker `apply_one` refuses from -- not the warning text."""
    owner = PrerequisitePlan("_owner_plan", plan_target(_owner_plan))
    borrower = PrerequisitePlan("_borrower_plan", plan_target(_borrower_plan))
    _downstream_plan.reset()
    try:
        # Owned two plans up the `depends_on` chain, or one.
        fresh = compute_changes(_downstream_plan, mode=ApplyMode.PLAN, lock=None)
        compute_changes(_owner_plan, mode=ApplyMode.APPLY, lock=None)
        _downstream_plan.reset()
        half = compute_changes(_downstream_plan, mode=ApplyMode.PLAN, lock=None)
    finally:
        _downstream_plan.reset()

    assert {(m.describe(), m.owner) for m in fresh.missing_prerequisites} == {
        ("FeatureType 'dep_ft'", owner),
        ("Feature 'dep_used'", owner),
        ("Feature 'dep_unused'", owner),
        ("Schema 'BorrowerSchema'", borrower),
    }
    assert [(m.describe(), m.owner) for m in half.missing_prerequisites] == [
        ("Schema 'BorrowerSchema'", borrower)
    ]
    # This module defines several plans, so the target has to name the variable.
    assert str(owner.target).endswith("test_apply.py:_owner_plan")


def test_apply_one_refuses_a_missing_prerequisite_before_asking_or_tracking(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    shared_plans: None,
    tmp_path: Path,
) -> None:
    from lamindb_setup.evolve.console import OutputFormat
    from lamindb_setup.evolve.engine import ApplyRefusedError, apply_one

    def unreachable(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("the refusal must come before this")

    with pytest.raises(ApplyRefusedError) as exc:
        apply_one(
            _borrower_plan,
            tmp_path / "plan_borrower.py",
            "stemUID00001",
            "acc/inst",
            "acc-inst",
            claimed_keys=set(),
            confirm=unreachable,
            output_format=OutputFormat.terminal,
            track=unreachable,
        )

    message = str(exc.value)
    assert message.startswith(
        "refusing to apply '_borrower_plan': 3 object(s) it borrows from "
        "prerequisite plan '_owner_plan' ("
    )
    assert "do not exist on acc/inst:\n" in message
    assert "  FeatureType 'dep_ft', Feature 'dep_used', Feature 'dep_unused'\n" in (
        message
    )
    assert f"(lamin evolve apply {plan_target(_owner_plan)} -i acc/inst)" in message
    assert ln.Schema.filter(name="BorrowerSchema").one_or_none() is None


def test_missing_prerequisite_message_groups_the_objects_by_owner() -> None:
    from lamindb_setup.evolve.render import missing_prerequisite_message

    base = PrerequisitePlan("base", "plans/plan_base.py")
    extra = PrerequisitePlan("extra", "plans/plan_extra.py:extra")
    missing = [
        MissingPrerequisite("FeatureType", "b_ft", "FeatureType:b_ft", base),
        MissingPrerequisite("Feature", "e_f", "Feature:e_f", extra),
        MissingPrerequisite("Feature", "b_f", "FeatureType:b_ft/Feature:b_f", base),
    ]

    message = missing_prerequisite_message("child", missing, instance="acc/inst")

    assert message == (
        "refusing to apply 'child': 3 object(s) it borrows from prerequisite plans "
        "do not exist on acc/inst:\n"
        "  from 'base' (plans/plan_base.py): FeatureType 'b_ft', Feature 'b_f'\n"
        "  from 'extra' (plans/plan_extra.py:extra): Feature 'e_f'\n"
        "Apply those plans first (lamin evolve apply plans/plan_base.py "
        "plans/plan_extra.py:extra -i acc/inst), or list them in one apply with "
        "this one -- they are applied dependencies-first."
    )


def test_missing_prerequisite_message_without_a_known_file() -> None:
    from lamindb_setup.evolve.render import missing_prerequisite_message

    unnamed = MissingPrerequisite("Schema", None, "Schema:abc", None)
    fileless = MissingPrerequisite(
        "Feature", "f", "Feature:f", PrerequisitePlan("base")
    )

    alone = missing_prerequisite_message("child", [unnamed], instance="acc/inst")
    mixed = missing_prerequisite_message(
        "child", [fileless, unnamed], instance="acc/inst"
    )

    assert alone == (
        "refusing to apply 'child': 1 object(s) it borrows from a prerequisite plan "
        "do not exist on acc/inst:\n  Schema:abc\n"
        "Apply that plan first, or list both plans in one apply -- they are "
        "applied dependencies-first."
    )
    # A plan the file of which is unknown is named, and no command is guessed.
    assert "  from 'base': Feature 'f'\n" in mixed
    assert "  from an unknown prerequisite plan: Schema:abc\n" in mixed
    assert "Apply those plans first, or list them in one apply" in mixed


# A prerequisite applied in the same run -- listed with the plan, or configured
# for the same instance -- creates what the instance lacks first.
_CREATED_IN_RUN = (
    "managed by prerequisite plan '_owner_plan' (created by it in this run)"
)
_OWNER_KEYS = {
    "FeatureType:dep_ft",
    *(f"FeatureType:dep_ft/Feature:{n}" for n in ("dep_used", "dep_unused")),
}


def test_co_applied_prerequisite_keys_are_what_listed_prerequisites_declare(
    instance: TestInstanceInfo, ln: LamindbModuleType, shared_plans: None
) -> None:
    from lamindb_setup.evolve.engine import co_applied_prerequisite_keys

    _downstream_plan.reset()
    try:
        together = co_applied_prerequisite_keys([_borrower_plan, _owner_plan])
        # Two plans up the `depends_on` chain, past one that is not listed:
        # the owner's objects are created in the run, the borrower's are not.
        skipping = co_applied_prerequisite_keys([_owner_plan, _downstream_plan])
        alone = co_applied_prerequisite_keys([_borrower_plan])
    finally:
        _downstream_plan.reset()

    assert together == [_OWNER_KEYS, set()]
    assert skipping == [set(), _OWNER_KEYS]
    assert alone == [set()]


def test_a_borrowed_object_a_co_applied_prerequisite_creates_is_not_missing(
    instance: TestInstanceInfo, ln: LamindbModuleType, shared_plans: None
) -> None:
    preview = compute_changes(
        _borrower_plan, mode=ApplyMode.PLAN, lock=None, co_applied_keys=_OWNER_KEYS
    )

    assert preview.missing_prerequisites == []
    borrowed = [c for c in preview.changes if c.borrowed]
    assert {c.name for c in borrowed} == {"dep_ft", "dep_used", "dep_unused"}
    for change in borrowed:
        assert change.action is Action.CREATE
        assert change.details == [_CREATED_IN_RUN]
        assert change.warnings == []
    own = next(c for c in preview.changes if c.name == "BorrowerSchema")
    assert (own.action, own.borrowed) == (Action.CREATE, False)
    terminal = render_changes(preview, title="Plan")
    summary = "Plan: 1 to create, 0 to update, 0 to delete, 0 unchanged, 3 to be "
    assert summary in terminal
    assert "\u2190 Feature 'dep_used'  (known after apply)" in terminal
    assert "WARNING" not in terminal
    assert "! " not in terminal


def test_a_borrowed_object_outside_the_run_is_missing_but_not_a_create(
    instance: TestInstanceInfo, ln: LamindbModuleType, shared_plans: None
) -> None:
    # Only the feature type is created in the run; the features stay missing.
    preview = compute_changes(
        _borrower_plan,
        mode=ApplyMode.PLAN,
        lock=None,
        co_applied_keys={"FeatureType:dep_ft"},
    )

    assert {m.describe() for m in preview.missing_prerequisites} == {
        "Feature 'dep_used'",
        "Feature 'dep_unused'",
    }
    by_name = {c.name: c for c in preview.changes}
    assert by_name["dep_ft"].details == [_CREATED_IN_RUN]
    assert by_name["dep_used"].details == ["managed by a prerequisite plan"]
    assert by_name["dep_used"].borrowed
    assert [w.category for w in by_name["dep_used"].warnings] == [
        WarningCategory.PREREQUISITE
    ]
    markdown = render_changes_markdown(preview, title="Plan")
    assert (
        "**Plan:** 1 to create, 0 to update, 0 to delete, 0 unchanged, 3 to be "
        "created by prerequisite plans."
    ) in markdown
    # Context lines, not creates; the plan's own schema keeps its `+`.
    assert "\n  Feature 'dep_used'  (known after apply)\n" in markdown
    assert "\n+ Schema 'BorrowerSchema'  (known after apply)\n" in markdown
    assert "> 2 object(s) are owned by a prerequisite plan that is not" in markdown


def test_an_apply_never_creates_a_borrowed_object_a_co_applied_plan_declares(
    instance: TestInstanceInfo, ln: LamindbModuleType, shared_plans: None
) -> None:
    # The keys only describe a preview: by the time this plan applies, its
    # prerequisite has run, so an object still missing is missing.
    with pytest.raises(MissingPrerequisiteError):
        compute_changes(
            _borrower_plan,
            mode=ApplyMode.APPLY,
            lock=None,
            co_applied_keys=_OWNER_KEYS,
        )

    assert not ln.Feature.filter(name__in=["dep_ft", "dep_used"]).exists()


@create_plan
def _vocabulary_plan(ctx: PlanContext, exports: None) -> None:
    Record("borrowed_value", type=RecordType("borrowed_rt"))


@create_plan
def _vocabulary_user_plan(
    ctx: PlanContext,
    exports: None,
    vocabulary: Plan[PlanExports] = _vocabulary_plan,
) -> None:
    Feature("vocabulary_user", dtype=LaminDType(str))


def test_a_borrowed_record_a_co_applied_prerequisite_seeds_is_its_create(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    from lamindb_setup.evolve.engine import co_applied_prerequisite_keys

    _vocabulary_user_plan.reset()
    (keys,) = [
        k
        for k in co_applied_prerequisite_keys([_vocabulary_plan, _vocabulary_user_plan])
        if k
    ]
    preview = compute_changes(
        _vocabulary_user_plan, mode=ApplyMode.PLAN, lock=None, co_applied_keys=keys
    )

    by_name = {c.name: c for c in preview.changes}
    value = by_name["borrowed_value"]
    assert (value.action, value.borrowed) == (Action.CREATE, True)
    assert value.details == [
        "managed by prerequisite plan '_vocabulary_plan' (created by it in this run)"
    ]
    assert by_name["borrowed_rt"].borrowed
    assert (by_name["vocabulary_user"].action, by_name["vocabulary_user"].borrowed) == (
        Action.CREATE,
        False,
    )


def test_a_rehearsal_credits_a_prerequisite_with_what_it_created(
    instance: TestInstanceInfo, ln: LamindbModuleType, shared_plans: None
) -> None:
    from lamindb_setup.evolve.core import PLACEHOLDER_UID
    from lamindb_setup.evolve.engine import preview_changes, rehearsal

    with rehearsal():
        owner = preview_changes(_owner_plan)
        borrower = preview_changes(_borrower_plan)

    assert {c.name for c in owner.changes if c.action is Action.CREATE} == {
        "dep_ft",
        "dep_used",
        "dep_unused",
    }
    borrowed = [c for c in borrower.changes if c.borrowed]
    assert {c.name for c in borrowed} == {"dep_ft", "dep_used", "dep_unused"}
    for change in borrowed:
        assert (change.action, change.uid) == (Action.CREATE, PLACEHOLDER_UID)
        assert change.details == [_CREATED_IN_RUN]
    # Exactly what a PLAN-mode diff given the run's keys reports.
    planned = compute_changes(
        _borrower_plan, mode=ApplyMode.PLAN, lock=None, co_applied_keys=_OWNER_KEYS
    )
    assert render_changes(borrower, title="Plan") == render_changes(
        planned, title="Plan"
    )
    assert not ln.Feature.filter(name__in=["dep_ft", "dep_used"]).exists()

    # Once the owner is applied, a rehearsal adopts what it finds unchanged.
    compute_changes(_owner_plan, mode=ApplyMode.APPLY, lock=None)
    with rehearsal():
        preview_changes(_owner_plan)
        adopted = preview_changes(_borrower_plan)
    assert {c.action for c in adopted.changes if c.borrowed} == {Action.NO_CHANGE}


def test_owner_can_still_rename_after_a_refused_out_of_order_apply(
    instance: TestInstanceInfo, ln: LamindbModuleType, shared_plans: None
) -> None:
    owner = compute_changes(_owner_plan, mode=ApplyMode.APPLY, lock=None)
    owner_lock = owner.build_lock(_owner_plan.name)
    original_uid = ln.Feature.get(name="dep_used", is_type=False).uid

    # the owner declares a rename; the borrower is (wrongly) applied first
    _redeclare_owner(name="dep_renamed", moved_from="dep_used")
    with pytest.raises(MissingPrerequisiteError):
        compute_changes(_borrower_plan, mode=ApplyMode.APPLY, lock=None)

    # no duplicate was created under the new name, so the owner's rename still
    # goes through in place
    compute_changes(_owner_plan, mode=ApplyMode.APPLY, lock=owner_lock)
    renamed = ln.Feature.get(name="dep_renamed", is_type=False)
    assert renamed.uid == original_uid
    assert ln.Feature.filter(name="dep_used", is_type=False).one_or_none() is None


def test_borrowed_objects_stay_out_of_the_downstream_lock(
    instance: TestInstanceInfo, ln: LamindbModuleType, shared_plans: None
) -> None:
    compute_changes(_owner_plan, mode=ApplyMode.APPLY, lock=None)
    ctx = compute_changes(_borrower_plan, mode=ApplyMode.APPLY, lock=None)

    # the borrower locks its schema and nothing of the owner's -- not the
    # feature it uses, and certainly not the one it never mentions
    assert sorted(ctx.build_lock(_borrower_plan.name).objects) == [
        "Schema:BorrowerSchema"
    ]
    # the borrowed feature is still resolved and reported, just not owned
    borrowed = next(c for c in ctx.changes if c.name == "dep_used")
    assert borrowed.action is Action.NO_CHANGE
    assert "managed by a prerequisite plan" in borrowed.details


def test_borrowed_object_edited_by_its_owner_is_not_downstream_drift(
    instance: TestInstanceInfo, ln: LamindbModuleType, shared_plans: None
) -> None:
    compute_changes(_owner_plan, mode=ApplyMode.APPLY, lock=None)
    first = compute_changes(_borrower_plan, mode=ApplyMode.APPLY, lock=None)
    lock = first.build_lock(_borrower_plan.name)

    # the owner documents and widens its feature, and applies that itself
    _redeclare_owner(description="now documented", dtype=float)
    owner_lock = compute_changes(_owner_plan, mode=ApplyMode.APPLY, lock=None)
    assert {c.action for c in owner_lock.changes} >= {Action.UPDATE}

    # the borrower sees no work and no warning: it never had an opinion on the
    # feature's description or dtype
    again = compute_changes(_borrower_plan, mode=ApplyMode.APPLY, lock=lock)
    assert {c.action for c in again.changes} == {Action.NO_CHANGE}
    assert [w for c in again.changes for w in c.warnings] == []


def test_borrowed_object_renamed_by_its_owner_is_not_deleted_downstream(
    instance: TestInstanceInfo, ln: LamindbModuleType, shared_plans: None
) -> None:
    owner = compute_changes(_owner_plan, mode=ApplyMode.APPLY, lock=None)
    owner_lock = owner.build_lock(_owner_plan.name)
    first = compute_changes(_borrower_plan, mode=ApplyMode.APPLY, lock=None)
    lock = first.build_lock(_borrower_plan.name)
    original_uid = ln.Feature.get(name="dep_used", is_type=False).uid

    _redeclare_owner(name="dep_renamed", moved_from="dep_used")
    compute_changes(_owner_plan, mode=ApplyMode.APPLY, lock=owner_lock)
    renamed = ln.Feature.filter(name="dep_renamed", is_type=False).one_or_none()
    assert renamed is not None
    assert renamed.uid == original_uid  # renamed in place, same record

    # the borrower must follow the rename, not read it as "the owner's feature
    # disappeared" and delete the record the owner just renamed in place
    again = compute_changes(_borrower_plan, mode=ApplyMode.APPLY, lock=lock)
    assert not [c for c in again.changes if c.action is Action.DELETE]
    assert ln.Feature.filter(uid=renamed.uid).one_or_none() is not None
    schema = ln.Schema.filter(name="BorrowerSchema").one_or_none()
    assert schema is not None
    assert [f.name for f in schema.features.all()] == ["dep_renamed"]


def test_borrowed_object_replaced_by_its_owner_relinks_downstream_schema(
    instance: TestInstanceInfo, ln: LamindbModuleType, shared_plans: None
) -> None:
    """Destroy + re-create is the documented escape for a narrowing no migration covers.

    It gives the feature a new uid, and the borrower's recorded schema snapshot
    addresses members by uid -- so the replacement reads as "the instance dropped
    a member".  The plan still declares it, so it is re-linked, not refused.
    """
    compute_changes(_owner_plan, mode=ApplyMode.APPLY, lock=None)
    first = compute_changes(_borrower_plan, mode=ApplyMode.APPLY, lock=None)
    lock = first.build_lock(_borrower_plan.name)

    old = ln.Feature.filter(name="dep_used", is_type=False).one_or_none()
    assert old is not None
    # what `destroy` does: drop the blocking membership, then delete
    ln.Schema.get(name="BorrowerSchema").features.remove(old)
    old.delete(permanent=True)
    compute_changes(_owner_plan, mode=ApplyMode.APPLY, lock=None)
    new = ln.Feature.filter(name="dep_used", is_type=False).one_or_none()
    assert new is not None and new.uid != old.uid

    again = compute_changes(_borrower_plan, mode=ApplyMode.APPLY, lock=lock)

    schema_change = next(c for c in again.changes if c.kind == "Schema")
    assert any("re-linked from the plan" in w for w in schema_change.warnings)
    schema = ln.Schema.filter(name="BorrowerSchema").one_or_none()
    assert schema is not None
    assert [str(f.uid) for f in schema.features.all()] == [str(new.uid)]


@pytest.mark.parametrize("format_name", ["terminal", "json"])
def test_apply_reports_export_failure_after_committing_the_plan(
    instance: TestInstanceInfo,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    format_name: str,
) -> None:
    from lamindb_setup.evolve.console import OutputFormat
    from lamindb_setup.evolve.engine import _apply, apply_one
    from lamindb_setup.evolve.engine._apply_run import SheetExportError
    from lamindb_setup.evolve.engine._lock import lock_path

    outcome = _apply.SheetExportOutcome(
        branch=SimpleNamespace(name="retry-export"), error=OSError("disk full")
    )
    monkeypatch.setattr(
        _apply, "run_sheet_export_migrations", lambda *args, **kwargs: outcome
    )
    path = tmp_path / "plan.py"
    _apply_plan.reset()
    with pytest.raises(SheetExportError, match="disk full") as exc:
        apply_one(
            _apply_plan,
            path,
            "stemUID00001",
            "acc/inst",
            "slug",
            claimed_keys=set(),
            confirm=None,
            output_format=OutputFormat(format_name),
            track=_untracked,
        )
    assert lock_path(path, "slug").is_file()
    assert exc.value.outcome is outcome
    if format_name == "terminal":
        assert exc.value.section is None
        assert "retry-export" in capsys.readouterr().err
    else:
        import json

        assert exc.value.section is not None
        assert any(
            "retry-export" in note for note in json.loads(exc.value.section)["notes"]
        )


def test_sheet_export_report_identifies_the_outstanding_version_family() -> None:
    from lamindb_setup.evolve.engine._apply import SheetExportOutcome
    from lamindb_setup.evolve.engine._apply_run import render_sheet_export_outcome

    old = SimpleNamespace(uid="old", key="sheet.csv")
    new = SimpleNamespace(uid="new", key="sheet.csv")
    outcome = SheetExportOutcome(
        branch=SimpleNamespace(name="exports"),
        merged=True,
        promotions=[(new, old), (old, new)],
        promoted=1,
        error=OSError("database unavailable"),
    )
    summary, pending = render_sheet_export_outcome(outcome)
    assert "only 1 of 2" in summary
    assert "'sheet.csv': promote uid 'new' over the current head 'old'" in pending
    outcome.error = None
    outcome.promoted = 2
    outcome.written = 1
    outcome.closed = ["previous-attempt"]
    (summary,) = render_sheet_export_outcome(outcome)
    assert (
        "re-exported 1" in summary
        and "restored 2" in summary
        and "previous-attempt" in summary
    )
    empty = SheetExportOutcome(branch=outcome.branch, merged=True)
    assert "No sheet export needed rewriting" in render_sheet_export_outcome(empty)[0]


def test_sheet_export_with_unrecorded_columns_and_with_a_preserved_snapshot(
    ln: LamindbModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    from django.db import DatabaseError
    from lamindb_setup.evolve.engine import _sheet_exports as exports

    sheet = ln.Record(name="Measurements", is_type=True).save()
    artifact = SimpleNamespace(pk=11, key="sheet.csv")
    schema = ln.Schema(
        name="Measurement schema",
        features=[ln.Feature(name="required", dtype="int").save()],
    ).save()
    options = dict(
        member_ids={1, 2},
        previous_ids={1},
        required_ids={1, 2},
        added_required=["new"],
        newly_required=["existing"],
        maximal=True,
        changed_features={3: "old column renamed"},
        index_change="index change",
    )
    monkeypatch.setattr(exports, "export_columns", lambda *_: (None, False))
    (unknown,) = exports.stale_exports(schema, [(sheet, artifact)], **options)
    assert "columns it carries are not recorded" in unknown.describe()
    assert "no column for 1 required member" in unknown.describe()
    assert "no column for new required 'new'" in unknown.describe()
    assert "'existing' was still optional" in unknown.describe()
    assert "written before the index change" in unknown.describe()
    (known,) = exports.stale_exports(
        schema, [(sheet, artifact)], recorded_columns={11: {1, 2, 3}}, **options
    )
    assert "maximal_set schema no longer declares" in known.describe()
    assert "old column renamed" in known.describe()
    assert "columns it carries are not recorded" not in known.describe()

    def unavailable():
        raise DatabaseError("metadata unavailable")

    monkeypatch.setattr(exports, "_live_exports", unavailable)
    assert exports.sheet_export_index() is None
    assert exports.sheet_exports([sheet]) is None
    assert exports.sheet_exports([]) == {}


def test_export_column_evidence_and_unmatched_sheet() -> None:
    from unittest.mock import Mock

    from lamindb_setup.evolve.engine import _sheet_exports as exports

    artifact = Mock(key="elsewhere.csv", description="edited")
    sheet = SimpleNamespace(name="Samples", uid="sheet")
    assert exports._sheet_for(artifact, [sheet]) is None
    link = artifact._links_schema.filter.return_value.first
    link.return_value = None
    assert exports.export_columns(artifact, sheet) == (None, False)
    columns = SimpleNamespace(pk=7, members=None)
    link.return_value = SimpleNamespace(schema=columns)
    assert exports.export_columns(artifact, sheet) == (None, False)
    columns.members = Mock()
    columns.members.all.return_value = [SimpleNamespace(pk=11)]
    columns.index = SimpleNamespace(pk=12)
    assert exports.export_columns(artifact, sheet) == ({11, 12}, True)


def test_schema_collision_and_unknown_shape_guards(ln) -> None:
    from lamindb_setup.evolve.core._lamindb_api import DEFAULT_MODELS
    from lamindb_setup.evolve.engine._apply import ApplyContext
    from lamindb_setup.evolve.engine._schema_collisions import classify, find_collisions

    group = ln.Schema(name="Schema group", is_type=True).save()
    assert "it is a schema type" in classify(group, managed=False).blockers
    assert find_collisions(ln.Schema, {}, using="default") == {}
    assert (
        DEFAULT_MODELS.schema_content_hash(
            object(),
            features=[],
            index=None,
            optionals=[],
            slots={},
            flexible=False,
            coerce=False,
            minimal_set=True,
            ordered_set=False,
            maximal_set=False,
        )
        is None
    )
    with pytest.raises(ValueError, match="features or slots or itype"):
        Schema(features=[])
    anonymous = Schema(features=[Feature("x", dtype=LaminDType(int))])
    assert anonymous.get(ApplyContext.standalone()) is None


def test_non_strict_uid_mismatch_is_reported_before_confirmation_and_on_empty_apply(
    ln, tmp_path: Path, capsys
) -> None:
    from lamindb_setup.evolve.console import OutputFormat
    from lamindb_setup.evolve.engine import apply_one
    from lamindb_setup.evolve.engine._lock import LockEntry, LockFile, lock_path

    path = tmp_path / "plan.py"
    origin = LockFile(plan=_apply_plan.name)
    other_uid = "SomeOtherUid"
    origin.add(
        LockEntry(
            key="FeatureType:apply_ft",
            kind="FeatureType",
            name="apply_ft",
            uid=other_uid,
        )
    )
    origin.write(lock_path(path, "other"))
    ln.Feature(name="apply_ft", is_type=True).save()
    _apply_plan.reset()

    def confirm(_):
        assert other_uid in capsys.readouterr().err
        return True

    assert _apply_one_untracked(path, strict_uids=False, confirm=confirm)
    _apply_plan.reset()
    apply_one(
        _apply_plan,
        path,
        "stemUID00001",
        "acc/inst",
        "slug",
        claimed_keys=set(),
        confirm=None,
        strict_uids=False,
        output_format=OutputFormat.terminal,
        track=_untracked,
    )
    output = capsys.readouterr().out
    assert other_uid in output and "No changes. Instance is up to date." in output
    document = _apply_one_untracked(path, strict_uids=False, confirm=None)
    assert document is not None and other_uid in document and "Applied:" in document
