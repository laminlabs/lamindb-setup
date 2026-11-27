# Adapted source tests use deliberately invalid inputs and lightweight stand-ins.
# mypy: disable-error-code="arg-type, call-overload"
"""Tests for lamindb_setup.evolve.introspect."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from lamindb_setup.evolve import (
    Feature,
    FeatureType,
    LaminDType,
    RecordType,
    Schema,
    SchemaType,
    Sheet,
)
from lamindb_setup.evolve.authoring._plan import PlanContext, create_plan
from lamindb_setup.evolve.core import SpaceUID
from lamindb_setup.evolve.engine._apply import ApplyMode, compute_changes
from lamindb_setup.evolve.introspect import introspect_instance
from lamindb_setup.evolve.introspect._read import (
    FeatureInfo,
    FeatureTypeInfo,
    InstanceContents,
    ObjectMeta,
    RecordTypeInfo,
    SchemaInfo,
    SchemaTypeInfo,
    read_instance,
)
from lamindb_setup.evolve.introspect._render import render_source
from lamindb_setup.evolve.introspect._reverse import build_plan

if TYPE_CHECKING:
    from pathlib import Path
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as BiontyModuleType
    from typing import Any as LamindbModuleType
    from typing import Any as PertdbModuleType


# --- the seed plan applied by the round-trip integration test ---------------


@create_plan
def _seed_plan(ctx: PlanContext, exports: None) -> None:
    ft = FeatureType("iso_group")
    f_str = Feature("iso_str", dtype=LaminDType(str), type=ft)
    f_int = Feature("iso_int", dtype=LaminDType(int), type=ft, description="an integer")
    st = SchemaType("IsoSchemas")
    schema = Schema(name="IsoSheet", features=[f_str, f_int], type=st)
    rt = RecordType("IsoRecords")
    Sheet("IsoExperiments", schema=schema, type=rt)


@create_plan
def _single_space_seed_plan(ctx: PlanContext, exports: None) -> None:
    f = Feature("iss_col", dtype=LaminDType(str))
    schema = Schema(name="IssSheet", features=[f])
    rt = RecordType("IssRecords", single_space_uid=SpaceUID("spaceUID0002"))
    Sheet(
        "IssExperiments",
        schema=schema,
        type=rt,
        single_space_uid=SpaceUID("spaceUID0002"),
        is_experiment_sheet=True,
    )


@create_plan
def _spaced_seed_plan(ctx: PlanContext, exports: None) -> None:
    ctx.default_space_uid = SpaceUID("spaceUID0001")
    ft = FeatureType("isp_group")
    f_str = Feature("isp_str", dtype=LaminDType(str), type=ft)
    f_int = Feature("isp_int", dtype=LaminDType(int), type=ft)
    Schema(name="IspSheet", features=[f_str, f_int])


@create_plan
def _exclude_children_seed_plan(ctx: PlanContext, exports: None) -> None:
    RecordType("Uploads", exclude_children_from_plans=True)


@create_plan
def _optionals_seed_plan(ctx: PlanContext, exports: None) -> None:
    required = Feature("iopt_required", dtype=LaminDType(str))
    optional = Feature("iopt_optional", dtype=LaminDType(str))
    Schema(name="IOptSheet", features=[required, optional], optionals=[optional])


@create_plan
def _nullable_optional_matrix_plan(ctx: PlanContext, exports: None) -> None:
    unset = Feature("imx_unset", dtype=LaminDType(str))
    explicit_true = Feature("imx_true", dtype=LaminDType(str), nullable=True)
    explicit_false = Feature("imx_false", dtype=LaminDType(str), nullable=False)
    Schema(
        name="IMatrixSheet",
        features=[unset, explicit_true, explicit_false],
        optionals=[explicit_true, explicit_false],
    )


# --- helpers for the lamindb-free unit tests --------------------------------


def _simple_contents() -> InstanceContents:
    return InstanceContents(
        feature_types=[FeatureTypeInfo(uid="uft1", name="group", type_uid=None)],
        features=[
            FeatureInfo(uid="uf1", name="col_str", type_uid="uft1", dtype_str="str"),
            FeatureInfo(
                uid="uf2",
                name="nums",
                type_uid=None,
                dtype_str="list[int]",
                nullable=False,
                description="numbers",
            ),
        ],
        schema_types=[
            SchemaTypeInfo(uid="ust1", name="Schemas", description=None, type_uid=None)
        ],
        schemas=[
            SchemaInfo(
                uid="us1",
                name="Sheet1",
                description=None,
                type_uid="ust1",
                itype="Feature",
                otype=None,
                schema_dtype=None,
                flexible=None,
                coerce=None,
                minimal_set=True,
                ordered_set=False,
                maximal_set=False,
                feature_uids=["uf1", "uf2"],
                index_feature_uid=None,
                slots={},
            )
        ],
        record_types=[
            RecordTypeInfo(
                uid="urt1",
                name="Records",
                description=None,
                type_uid=None,
                schema_uid=None,
            ),
            RecordTypeInfo(
                uid="ush1",
                name="Experiments",
                description=None,
                type_uid="urt1",
                schema_uid="us1",
            ),
        ],
    )


# --- unit tests (no instance) -----------------------------------------------


def test_build_plan_reconstructs_all_objects() -> None:
    plan = build_plan(_simple_contents())

    assert plan.errors == []
    assert {spec.kind for spec in plan.specs} == {
        "FeatureType",
        "Feature",
        "SchemaType",
        "Schema",
        "RecordType",
        "Sheet",
    }
    assert len(plan.specs) == 7


def test_drop_excluded_children_removes_subtree() -> None:
    from lamindb_setup.evolve.introspect._read import _drop_excluded_children

    def _rt(uid: str, type_uid: str | None, *, flag: bool = False) -> RecordTypeInfo:
        return RecordTypeInfo(
            uid=uid,
            name=uid,
            description=None,
            type_uid=type_uid,
            schema_uid=None,
            exclude_children=flag,
        )

    contents = InstanceContents(
        record_types=[
            _rt("root", None, flag=True),
            _rt("child", "root"),
            _rt("grandchild", "child"),
            _rt("managed", None),
        ]
    )
    _drop_excluded_children(contents)

    # The flagged root is kept (it is tracked by a plan); its whole subtree is
    # dropped, while an unrelated record type is untouched.
    assert {rt.uid for rt in contents.record_types} == {"root", "managed"}


def test_drop_excluded_children_without_flag_keeps_all() -> None:
    from lamindb_setup.evolve.introspect._read import _drop_excluded_children

    contents = InstanceContents(
        record_types=[
            RecordTypeInfo(
                uid="a", name="a", description=None, type_uid=None, schema_uid=None
            ),
            RecordTypeInfo(
                uid="b", name="b", description=None, type_uid="a", schema_uid=None
            ),
        ]
    )
    _drop_excluded_children(contents)

    assert {rt.uid for rt in contents.record_types} == {"a", "b"}


def test_render_emits_exclude_children_from_plans() -> None:
    contents = InstanceContents(
        record_types=[
            RecordTypeInfo(
                uid="root",
                name="Uploads",
                description=None,
                type_uid=None,
                schema_uid=None,
                exclude_children=True,
            ),
        ]
    )
    plan = build_plan(contents)
    source = render_source(
        plan, description="t", func="my_plan", stem_uid="abcd12345678"
    )

    compile(source, "<generated>", "exec")
    assert 'RecordType("Uploads", exclude_children_from_plans=True)' in source


def test_build_reconstructed_summary_groups_and_nests() -> None:
    from lamindb_setup.evolve.introspect._summary import build_reconstructed_summary

    plan = build_plan(_simple_contents())
    summary = build_reconstructed_summary(plan, title="Untracked objects", name="drift")

    assert summary.title == "Untracked objects"
    assert summary.plan.name == "drift"
    assert len(summary.objects) == len(plan.specs)

    by_kind_name = {(o.kind, o.name): o for o in summary.objects}
    # the feature 'col_str' nests under its FeatureType 'group'
    feature = by_kind_name[("Feature", "col_str")]
    assert feature.parent_id is not None
    parent = summary.objects_by_id[feature.parent_id]
    assert (parent.kind, parent.name) == ("FeatureType", "group")
    assert "dtype=str" in feature.details
    # the nullable=False list feature surfaces both its dtype and nullable detail
    nums = by_kind_name[("Feature", "nums")]
    assert "dtype=list[int]" in nums.details
    assert "nullable=False" in nums.details
    # the Sheet requires its schema (a non-parent dependency, shown via requires)
    sheet = by_kind_name[("Sheet", "Experiments")]
    required = {(r.kind, r.name) for r in summary.requirements_of(sheet)}
    assert ("Schema", "Sheet1") in required


def test_build_plan_external_ref_renders_cross_plan_import() -> None:
    from lamindb_setup.evolve.introspect._reverse import ExternalRef

    contents = InstanceContents(
        feature_types=[],
        features=[
            FeatureInfo(uid="uf1", name="col", type_uid="EXT1", dtype_str="str"),
        ],
        schema_types=[],
        schemas=[],
        record_types=[],
    )
    externals = {
        "EXT1": ExternalRef(
            uid="EXT1",
            module="plan_shared",
            plan_var="shared",
            exports_cls="SharedExports",
            field="group",
        )
    }

    plan = build_plan(contents, externals)

    # the out-of-scope dependency is satisfied by the external ref, not dropped
    assert plan.errors == []
    assert {spec.kind for spec in plan.specs} == {"Feature"}
    assert set(plan.external_refs) == {"EXT1"}

    source = render_source(
        plan, description="Drift", func="drift", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert "from lamindb_setup.evolve import Plan" in source
    assert "from plan_shared import SharedExports" in source
    assert "from plan_shared import shared" in source
    assert "p0: Plan[SharedExports] = shared" in source
    assert "type=p0.exports.group" in source


def test_build_plan_drops_unused_external_ref() -> None:
    from lamindb_setup.evolve.introspect._reverse import ExternalRef

    externals = {
        "EXT1": ExternalRef(
            uid="EXT1",
            module="plan_shared",
            plan_var="shared",
            exports_cls="SharedExports",
            field="group",
        )
    }
    # nothing references EXT1, so it must not be imported
    plan = build_plan(_simple_contents(), externals)
    assert plan.external_refs == {}
    source = render_source(plan, description="d", func="p", stem_uid="abcd12345678")
    assert "plan_shared" not in source


def test_render_source_compiles_and_orders_dependencies() -> None:
    plan = build_plan(_simple_contents())
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )

    # syntactically valid Python
    compile(source, "<generated>", "exec")

    assert "from lamindb_setup.evolve import Feature" in source
    assert 'PLAN_TRANSFORM_STEM_UID = "abcd12345678"' in source
    assert 'ft_group = FeatureType("group")' in source
    assert (
        'f_col_str = Feature("col_str", dtype=LaminDType(str), type=ft_group)' in source
    )
    assert (
        'f_nums = Feature("nums", dtype=LaminDType(list[int]), '
        'description="numbers", nullable=False)' in source
    )
    assert (
        's_sheet1 = Schema(name="Sheet1", features=[f_col_str, f_nums], '
        "type=st_schemas)" in source
    )
    assert (
        'sheet_experiments = Sheet("Experiments", schema=s_sheet1, type=rt_records)'
        in source
    )

    # dependencies precede their dependents
    assert source.index("ft_group = ") < source.index("f_col_str = ")
    assert source.index("f_col_str = ") < source.index("s_sheet1 = ")
    assert source.index("s_sheet1 = ") < source.index("sheet_experiments = ")


def test_render_source_includes_single_space_and_experiment_sheet() -> None:
    plan = build_plan(_simple_contents())
    plan_rt = next(s for s in plan.specs if s.kind == "RecordType")
    plan_sheet = next(s for s in plan.specs if s.kind == "Sheet")
    plan_rt.payload["single_space_uid"] = "spaceUID0002"
    plan_sheet.payload["single_space_uid"] = "spaceUID0002"
    plan_sheet.payload["is_experiment_sheet"] = True
    plan.needs_spaceuid = True

    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )

    compile(source, "<generated>", "exec")
    assert "from lamindb_setup.evolve import SpaceUID" in source
    assert (
        'rt_records = RecordType("Records", '
        'single_space_uid=SpaceUID("spaceUID0002"))' in source
    )
    assert (
        'sheet_experiments = Sheet("Experiments", schema=s_sheet1, type=rt_records, '
        'single_space_uid=SpaceUID("spaceUID0002"), is_experiment_sheet=True)' in source
    )


def test_schema_level_dtype_is_reconstructed() -> None:
    contents = _simple_contents()
    contents.schemas[0].schema_dtype = "float"

    plan = build_plan(contents)

    assert plan.errors == []
    assert plan.needs_lamindtype is True
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert "from lamindb_setup.evolve import LaminDType" in source
    assert "dtype=LaminDType(float)" in source


def test_categorical_schema_level_dtype_is_an_error() -> None:
    contents = _simple_contents()
    contents.schemas[0].schema_dtype = "cat[bionty.Gene]"

    plan = build_plan(contents)

    assert any("Sheet1" in error for error in plan.errors)
    assert all(spec.kind != "Schema" for spec in plan.specs)


def test_sheet_whose_schema_is_a_schema_type_is_an_error() -> None:
    # lamindb's `Record.schema` foreign key accepts a schema type, but a
    # `Sheet` needs a real schema, so such a record type must be excluded with
    # a diagnostic rather than rendered as an unimportable plan.
    contents = _simple_contents()
    contents.record_types[1].schema_uid = "ust1"

    plan = build_plan(contents)

    assert any(
        "Experiments" in error and "is a schema type" in error for error in plan.errors
    )
    assert all(spec.kind != "Sheet" for spec in plan.specs)


def test_composite_schema_with_slots_is_reconstructed() -> None:
    contents = _simple_contents()
    # an unnamed slot-component schema, indexed by gene
    contents.schemas.append(
        SchemaInfo(
            uid="us_var",
            name=None,
            description=None,
            type_uid=None,
            itype="bionty.Gene.ensembl_gene_id",
            otype=None,
            schema_dtype=None,
            flexible=None,
            coerce=None,
            minimal_set=True,
            ordered_set=False,
            maximal_set=False,
            feature_uids=[],
            index_feature_uid=None,
            slots={},
        )
    )
    # make Sheet1 a composite AnnData schema referencing the slot
    contents.schemas[0].feature_uids = []
    contents.schemas[0].otype = "AnnData"
    contents.schemas[0].slots = {"var": "us_var"}

    plan = build_plan(contents)

    assert plan.errors == []
    assert plan.skipped_unnamed_schemas == 0
    assert plan.needs_bionty is True
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert "import bionty as bt" in source
    assert (
        "s_gene = Schema(features=[], itype=bt.models.Gene.ensembl_gene_id)" in source
    )
    assert 'otype="AnnData"' in source
    assert 'slots={"var": s_gene}' in source
    # the slot component is defined before the composite that references it
    assert source.index("s_gene = ") < source.index("s_sheet1 = ")


def test_flexible_feature_schema_without_members_emits_itype() -> None:
    # a feature schema with no explicit features and no slots (e.g. a flexible
    # `ln.Schema(itype=ln.Feature)`) must be reconstructed with
    # `itype=ln.Feature`; otherwise `Schema(features=[], ...)` is rejected.
    contents = _simple_contents()
    contents.schemas.append(
        SchemaInfo(
            uid="us_flex",
            name="Flexible",
            description=None,
            type_uid=None,
            itype="Feature",
            otype="DataFrame",
            schema_dtype=None,
            flexible=True,
            coerce=None,
            minimal_set=True,
            ordered_set=False,
            maximal_set=False,
            feature_uids=[],
            index_feature_uid=None,
            slots={},
        )
    )

    plan = build_plan(contents)

    assert plan.errors == []
    assert plan.needs_lamindb is True
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert "import lamindb as ln" in source
    assert (
        'Schema(name="Flexible", features=[], otype="DataFrame", '
        "itype=ln.Feature, flexible=True)" in source
    )


def test_empty_composite_schema_warns_and_is_skipped() -> None:
    # a composite schema (itype='Composite') whose slots were removed has no
    # slots and no members; it cannot be reconstructed as a valid Schema, so it
    # is dropped and surfaced as a cleanup warning rather than emitted as
    # crashing `Schema(features=[], ...)` code.
    contents = _simple_contents()
    contents.schemas.append(
        SchemaInfo(
            uid="us_empty",
            name="AnnData of CELLxGENE version 5.2.0",
            description=None,
            type_uid=None,
            itype="Composite",
            otype="AnnData",
            schema_dtype="num",
            flexible=True,
            coerce=True,
            minimal_set=True,
            ordered_set=False,
            maximal_set=False,
            feature_uids=[],
            index_feature_uid=None,
            slots={},
        )
    )

    plan = build_plan(contents)

    assert plan.errors == []
    assert len(plan.warnings) == 1
    warning = plan.warnings[0]
    assert "AnnData of CELLxGENE version 5.2.0" in warning
    assert "Consider deleting it from the instance." in warning
    assert all(spec.uid != "us_empty" for spec in plan.specs)
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert "AnnData of CELLxGENE version 5.2.0" not in source


def test_empty_untyped_schema_without_members_warns_and_is_skipped() -> None:
    # an untyped schema (itype=None) with no features and no slots is likewise
    # empty: there is nothing to reconstruct and no itype to emit.
    contents = _simple_contents()
    contents.schemas.append(
        SchemaInfo(
            uid="us_empty2",
            name="CELLxGENE AnnData of ontology_id",
            description=None,
            type_uid=None,
            itype=None,
            otype="AnnData",
            schema_dtype="num",
            flexible=True,
            coerce=True,
            minimal_set=True,
            ordered_set=False,
            maximal_set=False,
            feature_uids=[],
            index_feature_uid=None,
            slots={},
        )
    )

    plan = build_plan(contents)

    assert plan.errors == []
    assert len(plan.warnings) == 1
    assert "CELLxGENE AnnData of ontology_id" in plan.warnings[0]
    assert all(spec.uid != "us_empty2" for spec in plan.specs)


def test_unreferenced_unnamed_schema_is_skipped() -> None:
    contents = _simple_contents()
    contents.schemas.append(
        SchemaInfo(
            uid="us_unnamed",
            name=None,
            description=None,
            type_uid=None,
            itype="Feature",
            otype=None,
            schema_dtype=None,
            flexible=None,
            coerce=None,
            minimal_set=True,
            ordered_set=False,
            maximal_set=False,
            feature_uids=["uf1"],
            index_feature_uid=None,
            slots={},
        )
    )

    plan = build_plan(contents)

    assert plan.errors == []
    assert plan.skipped_unnamed_schemas == 1
    assert all(spec.uid != "us_unnamed" for spec in plan.specs)


def test_out_of_scope_reference_is_an_error() -> None:
    contents = _simple_contents()
    contents.features[0].type_uid = "ghost"

    plan = build_plan(contents)

    assert any("col_str" in error for error in plan.errors)
    assert all(spec.name != "col_str" for spec in plan.specs)
    # the failing feature reports its own kind + uid
    assert any("Feature 'col_str' (uid 'uf1')" in error for error in plan.errors)
    # the schema that depends on it names that exact dependency (kind + uid)
    assert any(
        "Sheet1" in error
        and "depends on" in error
        and "Feature 'col_str' (uid 'uf1')" in error
        for error in plan.errors
    )


def test_out_of_scope_reference_records_target_uid() -> None:
    # The out-of-scope target uid is captured (with a per-target reference count)
    # so callers can map it back to an owning plan and suggest exporting it.
    contents = _simple_contents()
    contents.features[0].type_uid = "ghost"

    plan = build_plan(contents)

    assert plan.out_of_scope_uids == {"ghost": 1}


def test_unsupported_dtype_is_an_error() -> None:
    contents = _simple_contents()
    contents.features[0].dtype_str = "blob"

    plan = build_plan(contents)

    assert any("col_str" in error for error in plan.errors)
    # the error carries the object's kind, name and uid
    assert any(
        "Feature 'col_str' (uid 'uf1'): unsupported dtype: 'blob'" in error
        for error in plan.errors
    )


def test_out_of_scope_reference_to_deleted_object_is_flagged() -> None:
    contents = _simple_contents()
    contents.features[0].type_uid = "ghost"
    contents.deleted_uids = {"ghost"}

    plan = build_plan(contents)

    # the out-of-scope reference is annotated as deleted
    assert any(
        "references out-of-scope feature type (uid 'ghost', deleted)" in error
        for error in plan.errors
    )


def test_out_of_scope_reference_without_deletion_is_not_flagged() -> None:
    contents = _simple_contents()
    contents.features[0].type_uid = "ghost"

    plan = build_plan(contents)

    # an out-of-scope reference that is not trashed carries no "deleted" note
    assert any(
        "references out-of-scope feature type (uid 'ghost')" in error
        for error in plan.errors
    )
    assert all("deleted" not in error for error in plan.errors)


def test_failed_object_error_includes_creator_and_space() -> None:
    contents = _simple_contents()
    contents.features[0].dtype_str = "blob"
    contents.features[0].created_by = "alice"
    contents.features[0].space_uid = "spaceUID0001"

    plan = build_plan(contents)

    # the could-not-reconstruct object carries its uid, creator and space
    assert any(
        "Feature 'col_str' (uid 'uf1', created_by 'alice', space 'spaceUID0001'): "
        "unsupported dtype: 'blob'" in error
        for error in plan.errors
    )


def test_out_of_scope_reference_includes_target_creator_and_space() -> None:
    contents = _simple_contents()
    contents.features[0].type_uid = "ghost"
    contents.object_meta = {
        "ghost": ObjectMeta(name="group", created_by="bob", space_uid="otherSpace01")
    }

    plan = build_plan(contents)

    # the out-of-scope target is annotated with its creator and space
    assert any(
        "references out-of-scope feature type "
        "(uid 'ghost', created_by 'bob', space 'otherSpace01')" in error
        for error in plan.errors
    )


def test_num_dtype_is_reconstructed() -> None:
    contents = _simple_contents()
    contents.features[0].dtype_str = "num"

    plan = build_plan(contents)

    assert plan.errors == []
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert (
        'f_col_str = Feature("col_str", dtype=LaminDType("num"), type=ft_group)'
        in source
    )


@pytest.mark.parametrize("token", ["num", "path", "url"])
def test_string_dtypes_reconstruct_as_tokens_not_str(token: str) -> None:
    # `num` / `path` / `url` are lamindb's string dtypes; they must round
    # trip as `LaminDType("<token>")` and never degrade to `str` (or, for
    # `url`, be dropped as an unsupported dtype).
    contents = _simple_contents()
    contents.features[0].dtype_str = token

    plan = build_plan(contents)

    assert plan.errors == []
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert (
        f'f_col_str = Feature("col_str", dtype=LaminDType("{token}"), type=ft_group)'
        in source
    )
    # No stray `import pathlib` now that `path` renders as a string token.
    assert "import pathlib" not in source
    # And the reconstructed dtype resolves to the exact token, not `str`.
    assert LaminDType(token).dtype_str == token


def test_core_registry_dtypes_are_reconstructed() -> None:
    contents = _simple_contents()
    contents.features[0].dtype_str = "cat[Transform]"
    contents.features[1].dtype_str = "cat[User]"

    plan = build_plan(contents)

    assert plan.errors == []
    assert plan.needs_lamindb is True
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert "import lamindb as ln" in source
    # the default name field (Transform.key / User.handle) is dropped
    assert "dtype=LaminDType(ln.Transform)" in source
    assert "dtype=LaminDType(ln.User)" in source


def test_untyped_ulabel_dtype_is_reconstructed() -> None:
    contents = _simple_contents()
    contents.features[0].dtype_str = "cat[ULabel]"

    plan = build_plan(contents)

    assert plan.errors == []
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert "dtype=LaminDType(ln.ULabel)" in source


def test_composite_itype_schema_is_reconstructed() -> None:
    # a composite schema is read with itype="Composite"; it is reconstructed
    # from its slots, so the itype must not be treated as a registry.
    contents = _simple_contents()
    contents.schemas.append(
        SchemaInfo(
            uid="us_var",
            name=None,
            description=None,
            type_uid=None,
            itype="bionty.Gene.ensembl_gene_id",
            otype=None,
            schema_dtype=None,
            flexible=None,
            coerce=None,
            minimal_set=True,
            ordered_set=False,
            maximal_set=False,
            feature_uids=[],
            index_feature_uid=None,
            slots={},
        )
    )
    contents.schemas[0].feature_uids = []
    contents.schemas[0].itype = "Composite"
    contents.schemas[0].otype = "AnnData"
    contents.schemas[0].slots = {"var": "us_var"}

    plan = build_plan(contents)

    assert plan.errors == []
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    # only the slot component carries an itype; the composite schema does not
    assert source.count("itype=") == 1
    assert 'slots={"var": s_gene}' in source


def test_gene_indexed_schema_is_reconstructed() -> None:
    contents = _simple_contents()
    contents.schemas[0].itype = "bionty.Gene.ensembl_gene_id"
    contents.schemas[0].feature_uids = []

    plan = build_plan(contents)

    assert plan.errors == []
    assert plan.needs_bionty is True
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert "import bionty as bt" in source
    assert (
        's_sheet1 = Schema(name="Sheet1", features=[], type=st_schemas, '
        "itype=bt.models.Gene.ensembl_gene_id)" in source
    )


def test_pertdb_indexed_schema_is_reconstructed() -> None:
    contents = _simple_contents()
    contents.schemas[0].itype = "pertdb.Compound.name"
    contents.schemas[0].feature_uids = []

    plan = build_plan(contents)

    assert plan.errors == []
    assert plan.needs_pertdb is True
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert "import pertdb as pt" in source
    assert (
        's_sheet1 = Schema(name="Sheet1", features=[], type=st_schemas, '
        "itype=pt.models.Compound.name)" in source
    )


def test_space_uids_are_reconstructed() -> None:
    contents = _simple_contents()
    contents.feature_types[0].space_uid = "spaceUID0001"
    contents.features[0].space_uid = "spaceUID0002"
    contents.schemas[0].space_uid = "spaceUID0001"
    contents.record_types[1].space_uid = "spaceUID0003"

    plan = build_plan(contents)

    assert plan.errors == []
    assert plan.needs_spaceuid is True
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert "from lamindb_setup.evolve import SpaceUID" in source
    assert (
        'ft_group = FeatureType("group", space_uid=SpaceUID("spaceUID0001"))' in source
    )
    assert 'space_uid=SpaceUID("spaceUID0002")' in source
    assert (
        'sheet_experiments = Sheet("Experiments", schema=s_sheet1, type=rt_records, '
        'space_uid=SpaceUID("spaceUID0003"))' in source
    )


def test_no_space_uids_omits_import() -> None:
    plan = build_plan(_simple_contents())

    assert plan.needs_spaceuid is False
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )
    assert "SpaceUID" not in source
    assert "space_uid=" not in source


def test_the_all_space_is_left_unstated() -> None:
    # Every object lamindb creates without a space lands in its built-in "all"
    # space, so naming it on each object is noise; another space is kept.
    contents = _simple_contents()
    for group in (contents.feature_types, contents.features, contents.schemas):
        for info in group:
            info.space_uid = "a" * 12
    contents.record_types[1].space_uid = "spaceUID0003"

    plan = build_plan(contents)

    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert plan.default_space_uid is None
    assert "aaaaaaaaaaaa" not in source
    assert 'ft_group = FeatureType("group")' in source
    assert source.count("space_uid=") == 1
    assert 'type=rt_records, space_uid=SpaceUID("spaceUID0003"))' in source


def test_a_space_every_object_shares_is_the_plans_default() -> None:
    contents = _simple_contents()
    for group in (
        contents.feature_types,
        contents.features,
        contents.schema_types,
        contents.schemas,
        contents.record_types,
    ):
        for info in group:
            info.space_uid = "spaceUID0001"

    plan = build_plan(contents)

    assert plan.default_space_uid == "spaceUID0001"
    assert plan.needs_spaceuid is True
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert "from lamindb_setup.evolve import SpaceUID" in source
    assert '    ctx.default_space_uid = SpaceUID("spaceUID0001")\n' in source
    assert "space_uid=" not in source
    # it is set before the first object is declared
    assert source.index("ctx.default_space_uid") < source.index("ft_group = ")


def test_rendered_imports_are_sorted_like_ruff_sorts_them() -> None:
    # A plans repository linting with ruff's isort rules (force-single-line)
    # must accept the file as written: constants, then classes, then the rest,
    # module by module -- 'lamindb_setup.evolve.render' after
    # 'lamindb_setup.evolve', and the owning plans' modules among them.
    from lamindb_setup.evolve.introspect._reverse import ExternalRef

    contents = _simple_contents()
    contents.features[0].dtype_str = "datetime"
    contents.features[1].dtype_str = "cat[bionty.CellType[source__uid='src1']]"
    contents.features[1].space_uid = "spaceUID0002"
    contents.schemas[0].feature_uids = ["uf1", "uf2", "EXT1", "EXT2"]
    externals = {
        uid: ExternalRef(
            uid=uid, module=module, plan_var=var, exports_cls=cls, field="f"
        )
        for uid, module, var, cls in (
            ("EXT1", "plan_v10", "core", "CoreExports"),
            ("EXT2", "assays", "assays", "AssayExports"),
        )
    }

    plan = build_plan(contents, externals)
    source = render_source(
        plan, description="Test plan", func="my_plan", stem_uid="abcd12345678"
    )

    assert plan.errors == []
    imports = [line for line in source.splitlines() if line.startswith("from ")]
    assert imports == [
        "from __future__ import annotations",
        "from assays import AssayExports",
        "from assays import assays",
        "from lamindb_setup.evolve import Feature",
        "from lamindb_setup.evolve import FeatureType",
        "from lamindb_setup.evolve import LaminDType",
        "from lamindb_setup.evolve import Plan",
        "from lamindb_setup.evolve import PlanContext",
        "from lamindb_setup.evolve import RecordType",
        "from lamindb_setup.evolve import Schema",
        "from lamindb_setup.evolve import SchemaType",
        "from lamindb_setup.evolve import Sheet",
        "from lamindb_setup.evolve import SourceUID",
        "from lamindb_setup.evolve import SpaceUID",
        "from lamindb_setup.evolve import create_plan",
        "from lamindb_setup.evolve.render import TerminalPlanSummary",
        "from plan_v10 import CoreExports",
        "from plan_v10 import core",
    ]
    # one block after the standard library's, as isort separates them
    assert "import datetime\n\nfrom assays import AssayExports\n" in source


def test_import_order_ranks_constants_first_and_numbers_naturally() -> None:
    from lamindb_setup.evolve.introspect._render import _from_imports

    imports = {
        ("plan_v10", "core"),
        ("plan_v2", "core"),
        ("mod", "helper"),
        ("mod", "Helper"),
        ("mod", "HELPER"),
    }

    assert _from_imports(imports) == [
        "from mod import HELPER",
        "from mod import Helper",
        "from mod import helper",
        "from plan_v2 import core",
        "from plan_v10 import core",
    ]


def test_introspect_instance_refuses_to_overwrite(tmp_path: Path) -> None:
    output = tmp_path / "plan.py"
    output.write_text("# existing\n", encoding="utf-8")

    with pytest.raises(FileExistsError):
        introspect_instance("does/not-matter", output)


# --- integration tests (real instance) --------------------------------------


def test_introspect_roundtrips_as_noop(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    compute_changes(_seed_plan, mode=ApplyMode.APPLY, lock=None)

    output = tmp_path / "introspected_plan.py"
    result = introspect_instance(instance.slug, output, name="introspected")

    assert result.ok, (result.errors, result.verification)
    assert result.errors == []
    assert result.skipped_unnamed_schemas == 0
    assert result.reconstructed == 7
    assert result.actionable_changes == 0
    assert output.exists()
    source = output.read_text(encoding="utf-8")
    compile(source, str(output), "exec")
    # the objects sit in the "all" space a plan creates them in by default
    assert "SpaceUID" not in source
    assert "space_uid" not in source


def test_introspect_roundtrips_a_space_every_object_shares(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    ln.Space(name="iso-space", uid="spaceUID0001").save()
    compute_changes(_spaced_seed_plan, mode=ApplyMode.APPLY, lock=None)
    assert {f.space.uid for f in ln.Feature.filter(name__startswith="isp_")} == {
        "spaceUID0001"
    }

    output = tmp_path / "introspected_spaced.py"
    result = introspect_instance(
        instance.slug, output, name="introspected", space_uids=["spaceUID0001"]
    )

    assert result.ok, (result.errors, result.verification)
    assert result.reconstructed == 4
    assert result.actionable_changes == 0
    source = output.read_text(encoding="utf-8")
    assert 'ctx.default_space_uid = SpaceUID("spaceUID0001")' in source
    assert "space_uid=" not in source


def test_introspect_roundtrips_single_space_and_experiment_sheet(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    ln.Space(name="iss-default", uid="spaceUID0001").save()
    ln.Space(name="iss-target", uid="spaceUID0002").save()
    compute_changes(_single_space_seed_plan, mode=ApplyMode.APPLY, lock=None)

    output = tmp_path / "introspected_single_space.py"
    result = introspect_instance(instance.slug, output, name="introspected")

    assert result.ok, (result.errors, result.verification)
    assert result.actionable_changes == 0

    source = output.read_text(encoding="utf-8")
    assert 'single_space_uid=SpaceUID("spaceUID0002")' in source
    assert "is_experiment_sheet=True" in source


def test_introspect_roundtrips_optional_schema_members(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # A schema's optional members live in `Schema._aux` rather than on the
    # member links, so they have to be read and rendered explicitly.  Without
    # that, the reconstructed plan would declare every member required -- a
    # narrowing the drift taxonomy refuses, so the round-trip would not verify.
    compute_changes(_optionals_seed_plan, mode=ApplyMode.APPLY, lock=None)

    output = tmp_path / "introspected_optionals.py"
    result = introspect_instance(instance.slug, output, name="introspected")

    assert result.ok, (result.errors, result.verification)
    assert result.actionable_changes == 0

    source = output.read_text(encoding="utf-8")
    assert "optionals=[f_iopt_optional]" in source


def test_introspect_roundtrips_nullable_and_optional_matrix(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # `nullable` and `optionals` are independent axes and are stored in
    # different places (a column vs. `Schema._aux`); cover them together so a
    # regression in either one shows up as a non-idempotent round-trip.
    compute_changes(_nullable_optional_matrix_plan, mode=ApplyMode.APPLY, lock=None)

    def live() -> dict[str, object]:
        schema = ln.Schema.get(name="IMatrixSheet")
        optional_uids = set(schema.optionals.get_uids())
        return {
            "nullable": {
                f.name: f.nullable
                for f in ln.Feature.filter(name__startswith="imx_").all()
            },
            "optionals": sorted(
                m.name for m in schema.members.all() if m.uid in optional_uids
            ),
        }

    before = live()
    # A plan that leaves `nullable` unset still creates the feature nullable,
    # so an unset and an explicit `True` are indistinguishable on the instance.
    assert before["nullable"] == {
        "imx_unset": True,
        "imx_true": True,
        "imx_false": False,
    }
    assert before["optionals"] == ["imx_false", "imx_true"]

    output = tmp_path / "introspected_matrix.py"
    result = introspect_instance(instance.slug, output, name="introspected")

    assert result.ok, (result.errors, result.verification)
    assert result.actionable_changes == 0
    assert live() == before

    source = output.read_text(encoding="utf-8")
    assert "nullable=False" in source
    assert "optionals=[f_imx_true, f_imx_false]" in source


def test_introspect_excludes_runtime_children_of_flagged_root(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    compute_changes(_exclude_children_seed_plan, mode=ApplyMode.APPLY, lock=None)
    parent = ln.Record.get(name="Uploads", is_type=True)
    # A user creates a sheet (record type) at runtime under the flagged parent;
    # its uid is unknown to any plan, so it can only be excluded by its parent
    # chain, not by uid.
    ln.Record(name="UserSheetA", is_type=True, type=parent).save()

    output = tmp_path / "introspected_excl.py"
    result = introspect_instance(instance.slug, output, name="introspected")

    source = output.read_text(encoding="utf-8")
    # The runtime child is excluded from the reconstructed (drift) plan, while
    # the flagged root is kept and re-emits the flag.
    assert "UserSheetA" not in source
    assert 'RecordType("Uploads"' in source
    assert "exclude_children_from_plans=True" in source
    assert result.ok, (result.errors, result.verification)


def test_read_instance_invokes_tracker_per_loop(
    instance: TestInstanceInfo, ln: LamindbModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    from lamindb_setup.evolve.introspect import _read

    monkeypatch.setattr(_read, "_BATCH_SIZE", 1)
    first = ln.Feature(name="batch_a", dtype="str").save()
    first.add_synonym("alias1")
    first.add_synonym("alias2")
    second = ln.Feature(name="batch_b", dtype="str").save()
    seen: list[tuple[str, int]] = []

    def tracker(sequence: object, description: str) -> object:
        items = list(sequence)  # type: ignore[call-overload]
        seen.append((description, len(items)))
        return items

    sheet = ln.Record(name="Batched sheets", is_type=True).save()
    sheet.settings.single_space = True
    sheet.save()
    contents = read_instance(track=tracker)
    saved_sheet = next(
        record for record in contents.record_types if record.uid == sheet.uid
    )
    assert saved_sheet.single_space_uid == sheet.space.uid
    assert {f.uid for f in contents.features if f.name.startswith("batch_")} == {
        first.uid,
        second.uid,
    }
    assert set(next(f.synonyms for f in contents.features if f.uid == first.uid)) == {
        "alias1",
        "alias2",
    }

    assert [description for description, _ in seen] == [
        "Reading feature types",
        "Reading features",
        "Reading schema types",
        "Reading schemas",
        "Reading record types",
    ]


def test_read_instance_filters_to_multiple_spaces(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    ln.Space(name="space-one", uid="spaceUID0001").save()
    ln.Space(name="space-two", uid="spaceUID0002").save()
    ln.Space(name="space-three", uid="spaceUID0003").save()

    one = ln.Space.get(uid="spaceUID0001")
    two = ln.Space.get(uid="spaceUID0002")
    three = ln.Space.get(uid="spaceUID0003")

    ln.Feature(name="feat_one", dtype=int, space=one).save()
    ln.Feature(name="feat_two", dtype=int, space=two).save()
    ln.Feature(name="feat_three", dtype=int, space=three).save()

    contents = read_instance(space_uids=["spaceUID0001", "spaceUID0002"])

    names = {f.name for f in contents.features}
    assert "feat_one" in names
    assert "feat_two" in names
    assert "feat_three" not in names


def test_read_instance_excludes_spaces(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    ln.Space(name="space-one", uid="spaceUID0001").save()
    ln.Space(name="space-two", uid="spaceUID0002").save()

    one = ln.Space.get(uid="spaceUID0001")
    two = ln.Space.get(uid="spaceUID0002")

    ln.Feature(name="feat_one", dtype=int, space=one).save()
    ln.Feature(name="feat_two", dtype=int, space=two).save()

    contents = read_instance(exclude_space_uids=["spaceUID0002"])

    names = {f.name for f in contents.features}
    assert "feat_one" in names
    assert "feat_two" not in names


def test_introspect_reconstructs_record_typed_dtype(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    record_type = ln.Record(name="Compound", is_type=True).save()
    ln.Feature(name="compound", dtype=record_type).save()

    plan = build_plan(read_instance())

    assert plan.errors == []
    feature = next(spec for spec in plan.specs if spec.name == "compound")
    assert feature.payload["dtype"].registry_expr == "ln.Record"
    assert feature.payload["dtype"].type_uid == record_type.uid
    source = render_source(
        plan, description="typed", func="typed", stem_uid="abcd12345678"
    )
    assert "type__uid=rt_compound.uid" in source


def test_introspect_reconstructs_union_dtype(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    from lamindb_setup.evolve.introspect._summary import build_reconstructed_summary

    ln.Feature(name="either", dtype=[ln.ULabel, ln.Record]).save()

    plan = build_plan(read_instance())

    assert plan.errors == []
    feature = next(spec for spec in plan.specs if spec.name == "either")
    dtype = feature.payload["dtype"]
    assert dtype.union_members == [("ln.ULabel", None), ("ln.Record", None)]

    source = render_source(
        plan, description="t", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert "LaminDType([ln.ULabel, ln.Record])" in source

    summary = build_reconstructed_summary(plan, title="Instance", name="my_plan")
    obj = next(o for o in summary.objects if o.name == "either")
    assert "dtype=cat[ln.ULabel|ln.Record]" in obj.details


@pytest.mark.parametrize("instance", ["bionty,pertdb"], indirect=True)
def test_introspect_reconstructs_pertdb_dtype(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    pt: PertdbModuleType,
) -> None:
    ln.Feature(name="compound", dtype=pt.Compound).save()

    plan = build_plan(read_instance())

    assert plan.errors == []
    assert plan.needs_pertdb is True
    feature = next(spec for spec in plan.specs if spec.name == "compound")
    assert feature.payload["dtype"].registry_expr == "pt.models.Compound"


@pytest.mark.parametrize("instance", ["bionty"], indirect=True)
def test_introspect_reconstructs_source_filtered_dtype(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    bt: BiontyModuleType,
) -> None:
    source = bt.Source.filter(entity="bionty.CellType").first()
    ln.Feature(
        name="cell_type", dtype=bt.CellType, cat_filters={"source__uid": source.uid}
    ).save()

    plan = build_plan(read_instance())

    assert plan.errors == []
    assert plan.needs_bionty is True
    assert plan.needs_sourceuid is True
    feature = next(spec for spec in plan.specs if spec.name == "cell_type")
    assert feature.payload["dtype"].registry_expr == "bt.models.CellType"
    assert feature.payload["dtype"].field_name is None
    assert feature.payload["dtype"].filters == {"source__uid": source.uid}

    source_text = render_source(
        plan, description="t", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source_text, "<generated>", "exec")
    assert "from lamindb_setup.evolve import SourceUID" in source_text
    assert (
        f'dtype=LaminDType(bt.models.CellType, source__uid=SourceUID("{source.uid}"))'
        in source_text
    )


@pytest.mark.parametrize("instance", ["bionty"], indirect=True)
def test_introspect_reconstructs_field_and_source_filtered_dtype(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    bt: BiontyModuleType,
) -> None:
    source = bt.Source.filter(entity="bionty.Organism").first()
    ln.Feature(
        name="organism",
        dtype=bt.Organism.scientific_name,
        cat_filters={"source__uid": source.uid},
    ).save()

    plan = build_plan(read_instance())

    assert plan.errors == []
    assert plan.needs_sourceuid is True
    feature = next(spec for spec in plan.specs if spec.name == "organism")
    assert feature.payload["dtype"].field_name == "scientific_name"
    assert feature.payload["dtype"].filters == {"source__uid": source.uid}

    source_text = render_source(
        plan, description="t", func="my_plan", stem_uid="abcd12345678"
    )
    compile(source_text, "<generated>", "exec")
    assert (
        f"dtype=LaminDType(bt.models.Organism.scientific_name, "
        f'source__uid=SourceUID("{source.uid}"))' in source_text
    )


def test_introspect_reports_unreconstructable_dtype(
    instance: TestInstanceInfo, ln: LamindbModuleType, tmp_path: Path
) -> None:
    # bare `cat` is no longer accepted by lamindb, but a feature created with
    # it by an older version survives in an instance and is not declaratively
    # manageable. Write the legacy dtype past lamindb's validation.
    feature = ln.Feature(name="label", dtype=str).save()
    ln.Feature.filter(pk=feature.pk).update(_dtype_str="cat")

    output = tmp_path / "broken_plan.py"
    result = introspect_instance(instance.slug, output, verify=False)

    assert not result.ok
    assert any("label" in error for error in result.errors)
    assert output.exists()  # the plan is still written for inspection


def test_reconstructed_plan_preserves_schema_and_feature_settings() -> None:
    from lamindb_setup.evolve.introspect._summary import build_reconstructed_summary

    contents = _simple_contents()
    contents.feature_types.append(FeatureTypeInfo("child_ft", "Child", "uft1"))
    feature = contents.features[1]
    feature.type_uid = "child_ft"
    feature.synonyms = ["counts", "values"]
    feature.unit = "mg"
    feature.default_value = [1, 2]
    feature.coerce = True
    contents.schema_types.append(
        SchemaTypeInfo("child_st", "ChildSchema", "child group", "ust1")
    )
    schema = contents.schemas[0]
    schema.type_uid = "child_st"
    schema.description = "measurements"
    schema.otype = "DataFrame"
    schema.index_feature_uid = "uf1"
    schema.flexible = schema.coerce = schema.ordered_set = schema.maximal_set = True
    schema.minimal_set = False
    contents.record_types.append(
        RecordTypeInfo("nested", "Nested records", None, "urt1", None)
    )
    contents.record_types[0].description = "record group"
    contents.record_types[1].description = "data sheet"
    contents.record_types[1].exclude_children = True

    plan = build_plan(contents)
    assert not plan.errors
    source = render_source(
        plan, description="Measurements", func="measurements", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    for value in (
        "type=ft_group",
        "type=st_schemas",
        'synonyms=["counts", "values"]',
        'unit="mg"',
        "default_value=[1, 2]",
        'RecordType("Nested records", type=rt_records)',
        "index=f_col_str",
        'description="measurements"',
        "flexible=True",
        "coerce=True",
        "minimal_set=False",
        "ordered_set=True",
        "maximal_set=True",
        'description="child group"',
        'description="record group"',
        'description="data sheet"',
        "exclude_children_from_plans=True",
    ):
        assert value in source
    summary = build_reconstructed_summary(plan, title="Source", name="measurements")
    details = {obj.name: obj.details for obj in summary.objects}
    assert (
        "otype='DataFrame'" in details["Sheet1"] and "features=2" in details["Sheet1"]
    )
    assert "nullable=False" in details["nums"]


@pytest.mark.parametrize(
    ("dtype", "message"),
    [
        (None, "has no dtype"),
        ("cat[UnknownRegistry]", "categorical"),
        ("cat[Record]", "Record"),
        ("cat[ULabel[unknown=value]]", "filter"),
    ],
)
def test_unrepresentable_feature_does_not_hide_other_objects(
    dtype: str | None, message: str
) -> None:
    contents = _simple_contents()
    contents.features[0].dtype_str = dtype
    plan = build_plan(contents)
    assert any(message in error for error in plan.errors)
    assert any(obj.uid == "uf2" for obj in plan.specs)
    assert all(obj.uid != "uf1" for obj in plan.specs)


def test_scaffold_can_be_loaded_and_a_manual_file_gets_one_stem_uid(
    tmp_path: Path,
) -> None:
    from lamindb_setup.evolve.engine._loader import StemUidError
    from lamindb_setup.evolve.introspect._template import (
        add_stem_uid_to_plan,
        plan_slug,
        render_plan,
    )

    with pytest.raises(ValueError, match="alphanumeric"):
        plan_slug("!!")
    path = tmp_path / "plan.py"
    with pytest.raises(StemUidError, match="does not exist"):
        add_stem_uid_to_plan(path)
    path.write_text("# manually maintained plan\n")
    uid = add_stem_uid_to_plan(path)
    with pytest.raises(StemUidError, match="already defines"):
        add_stem_uid_to_plan(path)
    assert path.read_text().count(uid) == 1
    for minimal in (True, False):
        source = render_plan(plan_slug("Lab records"), "records", uid, minimal=minimal)
        compile(source, str(path), "exec")
        assert f'PLAN_TRANSFORM_STEM_UID = "{uid}"' in source
        assert "def records(" in source


@pytest.mark.parametrize(
    ("returncode", "stdout", "stderr", "expected"),
    [
        (0, 'connecting\n{"changes": []}\n\n', "", {"changes": []}),
        (0, "not a report\n", "", {"error": "could not parse verification output"}),
        (1, "loading", "could not import plan", {"error": "could not import plan"}),
    ],
)
def test_verification_reports_subprocess_failures_and_ignores_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    returncode: int,
    stdout: str,
    stderr: str,
    expected: dict,
) -> None:
    import subprocess

    from lamindb_setup.evolve.introspect import _verify

    result = subprocess.CompletedProcess([], returncode, stdout, stderr)
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: result)
    assert (
        _verify.run_verification(tmp_path / "plan.py", "plan", "test/demo") == expected
    )


@pytest.mark.parametrize(
    ("itype", "expected"),
    [
        ("SQLRecord", "ln.models.SQLRecord"),
        ("User.handle", "ln.User.handle"),
        ("User.name.extra", None),
        ("bionty.Gene.field.extra", None),
    ],
)
def test_schema_index_registry_is_reconstructed_or_reported(
    itype: str, expected: str | None
) -> None:
    contents = _simple_contents()
    contents.schemas[0].itype = itype
    contents.schemas[0].feature_uids = []
    plan = build_plan(contents)
    if expected is None:
        assert any(f"unsupported itype: {itype!r}" in error for error in plan.errors)
    else:
        assert not plan.errors
        source = render_source(
            plan, description="schema", func="schema", stem_uid="abcd12345678"
        )
        assert f"itype={expected}" in source


def test_colliding_names_produce_distinct_variables_in_generated_source() -> None:
    contents = _simple_contents()
    contents.features[0].name = "cell-count"
    contents.features[1].name = "cell count"
    plan = build_plan(contents)
    variables = {spec.varname for spec in plan.specs if spec.kind == "Feature"}
    assert variables == {"f_cell_count", "f_cell_count_2"}
    source = render_source(
        plan, description="counts", func="counts", stem_uid="abcd12345678"
    )
    compile(source, "<generated>", "exec")
    assert "features=[f_cell_count" in source and "f_cell_count_2" in source


def test_verification_lists_changes_without_applying_them(
    instance, ln, tmp_path: Path, capsys
) -> None:
    from lamindb_setup.evolve.introspect import _verify

    path = tmp_path / "new_plan.py"
    path.write_text(
        "from lamindb_setup.evolve import FeatureType, PlanContext, create_plan\n"
        "@create_plan\n"
        "def plan(ctx: PlanContext, exports: None) -> None:\n"
        "    FeatureType('new group')\n"
    )
    assert _verify.main([]) == 2
    assert "usage:" in capsys.readouterr().err
    report = _verify.run_verification(path, "plan", instance.slug)
    assert "error" not in report, report
    assert report["counts"]["create"] == 1
    assert report["changes"] == [
        {"action": "create", "object": "FeatureType 'new group'"}
    ]
    assert not ln.Feature.filter(name="new group").exists()


@pytest.mark.parametrize(
    ("components", "message"),
    [
        ([], "unsupported dtype"),
        ([{"registry_str": "UnknownRegistry"}], "unsupported categorical registry"),
        (
            [
                {
                    "registry_str": "Record",
                    "field_str": "uid",
                    "filter_str": "type__uid=abc",
                }
            ],
            "field dtype with a type uid",
        ),
        (
            [{"registry_str": "ULabel", "filter_str": "broken="}],
            "unparsable categorical filters",
        ),
        (
            [
                {"registry_str": "ULabel", "type_uid": "typed"},
                {"registry_str": "Record"},
            ],
            "union dtypes with filters",
        ),
    ],
)
def test_unrepresentable_categorical_components_are_reported(
    ln, monkeypatch, components, message
) -> None:
    from lamindb.models import feature as feature_module

    # Exercise the boundary with lamindb's registry parser independently of its version.
    monkeypatch.setattr(feature_module, "parse_dtype", lambda dtype: components)
    contents = _simple_contents()
    contents.features[0].dtype_str = "cat[ULabel]"
    plan = build_plan(contents)
    assert any(message in error for error in plan.errors)
    assert not any(spec.uid == "uf1" for spec in plan.specs)


def test_reconstructed_categorical_options_and_anonymous_slot_names() -> None:
    from lamindb_setup.evolve.introspect._render import _dtype_expr, _render_object
    from lamindb_setup.evolve.introspect._reverse import (
        DtypeInfo,
        ObjectSpec,
        _assign_varnames,
        _toposort,
    )
    from lamindb_setup.evolve.introspect._summary import _dtype_label

    dtype = DtypeInfo(
        registry_expr="ln.Artifact",
        field_name="uid",
        filters={"schema__uid": "s1", "is_type": "False"},
    )
    assert (
        _dtype_expr(dtype, {"s1": "measurement"})
        == "LaminDType(ln.Artifact.uid, schema__uid=measurement.uid, is_type=False)"
    )
    assert _dtype_label(dtype) == "ln.Artifact.uid"
    assert _dtype_label(DtypeInfo(registry_expr="ln.Record")) == "ln.Record"
    union = DtypeInfo(
        is_list=True, union_members=[("ln.Artifact", "uid"), ("ln.Record", None)]
    )
    assert _dtype_expr(union, {}) == "LaminDType(list[[ln.Artifact.uid, ln.Record]])"
    assert _dtype_label(DtypeInfo()) == "?"
    specs = [
        ObjectSpec(str(i), "Schema", None, payload=payload)
        for i, payload in enumerate(
            ({"otype": "DataFrame"}, {"itype_expr": "plugin.custom"}, {})
        )
    ]
    _assign_varnames(specs)
    assert [spec.varname for spec in specs] == ["s_dataframe", "s_custom", "s_schema"]
    specs[0].deps = {specs[1].uid}
    specs[1].deps = {specs[0].uid}
    with pytest.raises(RuntimeError, match="cyclic"):
        _toposort({spec.uid: spec for spec in specs})
    with pytest.raises(ValueError, match="unknown object kind"):
        _render_object(ObjectSpec("x", "Unknown", "mystery"), {})


def test_read_handles_sparse_metadata_and_typed_schema_members(ln, bt) -> None:
    from pathlib import Path
    from types import SimpleNamespace

    from lamindb_setup.evolve.introspect import _read
    from lamindb_setup.evolve.introspect._run import IntrospectionResult

    sparse = SimpleNamespace(_dtype_str="int")
    assert _read._created_by(sparse) is None and _read._created_at(sparse) is None
    assert _read._created_at(SimpleNamespace(created_at="2024-01-01")) == "2024-01-01"
    assert _read._synonyms(["old name"]) == ["old name"]
    assert _read._dtype_str(sparse) == "int"
    schema = ln.Schema(
        name="Gene index", itype=bt.Gene.ensembl_gene_id, maximal_set=True
    ).save()
    composite = ln.Schema(name="Dataset", otype="AnnData", slots={"var": schema}).save()
    # Composite is also a persisted itype in instances that predate the current writer.
    ln.Schema.objects.filter(pk=composite.pk).update(itype="Composite")
    by_uid = {item.uid: item for item in read_instance().schemas}
    assert by_uid[schema.uid].feature_uids == []
    assert by_uid[composite.uid].slots == {"var": schema.uid}
    result = IntrospectionResult(
        output=Path("unused"),
        func="p",
        stem_uid="u",
        reconstructed=0,
        skipped_unnamed_schemas=0,
    )
    assert result.actionable_changes == 0
