# Adapted source tests include deliberately invalid declarations and dynamic models.
# mypy: disable-error-code="list-item"
from __future__ import annotations

import types

import pytest
from lamindb_setup.evolve.authoring._dtypes import LaminDType
from lamindb_setup.evolve.authoring._features import Feature, FeatureType
from lamindb_setup.evolve.authoring._plan import (
    Plan,
    PlanContext,
    PlanExports,
    PlanNotFinalizedError,
    create_plan,
    plan_declaration_blocker,
)
from lamindb_setup.evolve.authoring._records import Record, RecordType, Sheet
from lamindb_setup.evolve.authoring._schemas import Schema, SchemaType
from lamindb_setup.evolve.core import SpaceUID
from lamindb_setup.evolve.render import TerminalPlanSummary


@create_plan
def example_plan(ctx: PlanContext, exports: None) -> None:
    ctx.default_space_uid = SpaceUID("abcdEF12abcd")
    ft = FeatureType("ft")
    Feature("col", dtype=LaminDType(str), type=ft)
    Schema(name="MySchema", features=[Feature("a", dtype=LaminDType(int), type=ft)])


def test_create_plan_returns_a_plan() -> None:
    assert isinstance(example_plan, Plan)


def test_schema_index_may_have_a_non_str_dtype() -> None:
    # Only record sheets pin the index to `str`; a curation schema -- an
    # AnnData `var` slot, say -- may index on any dtype.
    idx = Feature("gene_id", dtype=LaminDType(int))
    schema = Schema(name="IntIndex", features=[idx], index=idx)
    assert schema.index is idx


def test_sheet_index_must_be_str_dtype() -> None:
    idx = Feature("sample_id", dtype=LaminDType(int))
    schema = Schema(name="BadIndex", features=[idx], index=idx)
    with pytest.raises(ValueError, match="index feature 'sample_id' must have dtype"):
        Sheet("BadSheet", schema=schema, type=RecordType("BadSheetType"))


def test_sheet_index_str_dtype_is_accepted() -> None:
    idx = Feature("sample_id", dtype=LaminDType(str))
    schema = Schema(name="GoodIndex", features=[idx], index=idx)
    sheet = Sheet("GoodSheet", schema=schema, type=RecordType("GoodSheetType"))
    assert sheet.schema.index is idx


def test_sheet_schema_must_not_be_a_schema_type() -> None:
    # lamindb's `Record.schema` foreign key accepts a schema type, so an
    # instance can hold this state; a schema type declares no features though,
    # so it cannot constrain a sheet.
    schema_type = SchemaType("MySchemas")
    with pytest.raises(TypeError, match="'schema' must be a Schema, got SchemaType"):
        Sheet("TypeSheet", schema=schema_type)  # type: ignore[arg-type]


def test_schema_optional_must_be_a_member() -> None:
    # LaminDB silently drops an optional that is not a member, so the plan would
    # keep re-reporting it as missing on every apply and never converge.
    stray = Feature("stray", dtype=LaminDType(str))
    member = Feature("member", dtype=LaminDType(str))
    with pytest.raises(ValueError, match=r"optionals \['stray'\] are not among"):
        Schema(name="StrayOptional", features=[member], optionals=[stray])


def test_schema_optional_rejects_a_same_named_copy() -> None:
    # Membership is by identity, matching how `stage` marks optionals.
    member = Feature("member", dtype=LaminDType(str))
    with pytest.raises(ValueError, match=r"optionals \['member'\] are not among"):
        Schema(
            name="CopiedOptional",
            features=[member],
            optionals=[Feature("member", dtype=LaminDType(str))],
        )


def test_schema_index_cannot_be_optional() -> None:
    # lamindb re-inserts the index into the member list itself, so `stage`
    # never sees it and the optional marker would be dropped.
    idx = Feature("sample_id", dtype=LaminDType(str))
    other = Feature("value", dtype=LaminDType(str))
    with pytest.raises(ValueError, match="index feature 'sample_id' cannot be"):
        Schema(name="OptionalIndex", features=[idx, other], index=idx, optionals=[idx])


def test_schema_optional_that_is_a_member_is_accepted() -> None:
    required = Feature("required", dtype=LaminDType(str))
    optional = Feature("optional", dtype=LaminDType(str))
    schema = Schema(
        name="GoodOptional", features=[required, optional], optionals=[optional]
    )
    assert schema.optionals == [optional]


def test_plan_is_abstract() -> None:
    with pytest.raises(TypeError):
        Plan("test")  # type: ignore[abstract]


def test_summary_describes_everything_without_materializing() -> None:
    summary = TerminalPlanSummary().render(example_plan)
    assert "Plan summary: example_plan" in summary
    assert "FeatureType 'ft'" in summary
    assert "Feature 'col'" in summary
    assert "dtype=str" in summary
    assert "Schema 'MySchema'" in summary
    assert "features=1" in summary
    assert "space 'abcdEF12abcd'" in summary


def test_ctx_default_space_is_captured() -> None:
    example_plan.finalize()
    assert example_plan.ctx.default_space_uid == SpaceUID("abcdEF12abcd")


def test_plan_declaration_allows_its_parent_package() -> None:
    reason = plan_declaration_blocker({"lamindb": object()}, instance_connected=False)
    assert reason is None


def test_plan_declaration_blocked_once_an_instance_is_connected() -> None:
    reason = plan_declaration_blocker({}, instance_connected=True)
    assert reason is not None
    assert "instance is already connected" in reason


def test_plan_declaration_allowed_on_a_clean_process() -> None:
    assert plan_declaration_blocker({}, instance_connected=False) is None


def test_plan_construction_consults_the_blocker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The wiring: `Plan` asks the rule about the *real* process state.

    The rule itself is covered by the pure tests above; this one only pins that
    the guard is actually consulted, which needs the ambient state it reads.
    """
    from lamindb_setup import settings
    from lamindb_setup.core import django as _django

    monkeypatch.setattr(_django, "IS_SETUP", True)
    monkeypatch.setattr(
        settings, "_instance_settings", types.SimpleNamespace(slug="lamin/test")
    )
    with pytest.raises(RuntimeError, match="instance is already connected"):

        @create_plan
        def _p(ctx: PlanContext, exports: None) -> None:
            pass


def test_no_plan_is_active_outside_run() -> None:
    assert Plan.active() is None


class _ExampleExports(PlanExports):
    feature_0: Feature
    schema_0: Schema


@create_plan
def example_exports_plan(ctx: PlanContext, exports: _ExampleExports) -> None:
    ft = FeatureType("ft")
    exports.feature_0 = f0 = Feature("col", dtype=LaminDType(str), type=ft)
    exports.schema_0 = Schema(name="MySchema", features=[f0])


def test_plan_exports_subclass_accepts_allowed_types() -> None:
    assert set(_ExampleExports.__annotations__) == {"feature_0", "schema_0"}


def test_plan_exports_subclass_rejects_unsupported_type() -> None:
    with pytest.raises(TypeError, match="unsupported type"):

        class _Bad(PlanExports):
            value: int


def test_plan_populates_declared_exports() -> None:
    example_exports_plan.finalize()
    exports = example_exports_plan.exports
    assert isinstance(exports, _ExampleExports)
    assert isinstance(exports.feature_0, Feature)
    assert exports.feature_0.name == "col"
    assert isinstance(exports.schema_0, Schema)
    assert exports.schema_0.name == "MySchema"


def test_exports_requires_finalized_plan() -> None:
    @create_plan
    def _p(ctx: PlanContext, exports: _ExampleExports) -> None:
        pass

    with pytest.raises(PlanNotFinalizedError):
        _ = _p.exports


def test_plan_without_exports_has_base_exports() -> None:
    example_plan.finalize()
    assert type(example_plan.exports) is PlanExports


def test_dependency_meta_unpacks_collections_into_requires() -> None:
    captured: dict[str, object] = {}

    @create_plan
    def _p(ctx: PlanContext, exports: None) -> None:
        ft = FeatureType("ft")
        captured["a"] = Feature("a", dtype=LaminDType(str), type=ft)
        captured["b"] = Feature("b", dtype=LaminDType(int), type=ft)
        captured["schema"] = Schema(name="s", features=[captured["a"], captured["b"]])

    _p.finalize()

    schema_dep = next(dep for dep in _p.dependencies if dep.obj is captured["schema"])
    # both features nested in the `features` list are recorded as requirements
    assert captured["a"] in schema_dep.requires
    assert captured["b"] in schema_dep.requires


def test_create_plan_rejects_unannotated_second_parameter() -> None:
    with pytest.raises(TypeError, match="must have type annotations"):

        @create_plan
        def _p(ctx: PlanContext, exports) -> None:  # type: ignore[no-untyped-def]
            pass


def test_plan_dependency_declared_via_typed_default_is_auto_finalized() -> None:
    @create_plan
    def _base(ctx: PlanContext, exports: _ExampleExports) -> None:
        ft = FeatureType("base_ft")
        exports.feature_0 = Feature("base_col", dtype=LaminDType(str), type=ft)
        exports.schema_0 = Schema(name="base_schema", features=[exports.feature_0])

    @create_plan
    def _child(
        ctx: PlanContext,
        exports: None,
        base: Plan[_ExampleExports] = _base,
    ) -> None:
        FeatureType("child_ft")
        _ = base.exports.feature_0

    with pytest.raises(PlanNotFinalizedError):
        _ = _base.exports

    _child.finalize()

    assert _child.depends_on == (_base,)
    assert _base.exports.feature_0.name == "base_col"
    assert any(
        isinstance(dep.obj, Feature) and dep.obj.name == "base_col"
        for dep in _child.dependencies
    )


def test_reset_reruns_the_plan_body_on_the_next_finalize() -> None:
    runs = [0]

    @create_plan
    def _p(ctx: PlanContext, exports: None) -> None:
        runs[0] += 1
        FeatureType("reset_ft")

    _p.finalize()
    _p.finalize()  # cached: does not re-run the body
    assert runs[0] == 1

    _p.reset()
    with pytest.raises(PlanNotFinalizedError):
        _ = _p.dependencies  # reset discards the finalized state

    _p.finalize()
    assert runs[0] == 2  # the body ran again against a fresh finalize


def test_failed_finalize_does_not_cache_partial_state() -> None:
    runs = [0]

    @create_plan
    def _p(ctx: PlanContext, exports: None) -> None:
        runs[0] += 1
        Record()

    for attempt in (1, 2):
        with pytest.raises(ValueError, match="declares an unnamed Record"):
            _p.finalize()
        assert runs[0] == attempt
        with pytest.raises(PlanNotFinalizedError):
            _ = _p.dependencies


def test_reset_rebuilds_objects_so_no_state_leaks_across_finalizes() -> None:
    @create_plan
    def _p(ctx: PlanContext, exports: None) -> None:
        FeatureType("rebuilt_ft")

    _p.finalize()
    first = [dep.obj for dep in _p.dependencies]

    _p.reset()
    _p.finalize()
    second = [dep.obj for dep in _p.dependencies]

    # A fresh finalize builds brand-new objects rather than reusing the ones
    # captured against the previous finalize (which would go stale on reconnect).
    assert {id(o) for o in first}.isdisjoint({id(o) for o in second})


def test_reset_cascades_to_dependency_plans() -> None:
    base_runs = [0]

    @create_plan
    def _base(ctx: PlanContext, exports: _ExampleExports) -> None:
        base_runs[0] += 1
        ft = FeatureType("cascade_base_ft")
        exports.feature_0 = Feature("cascade_col", dtype=LaminDType(str), type=ft)
        exports.schema_0 = Schema(name="cascade_schema", features=[exports.feature_0])

    @create_plan
    def _child(
        ctx: PlanContext,
        exports: None,
        base: Plan[_ExampleExports] = _base,
    ) -> None:
        _ = base.exports.feature_0

    _child.finalize()
    assert base_runs[0] == 1

    _child.reset()
    # Resetting the child also resets its prerequisite, so both re-run.
    _child.finalize()
    assert base_runs[0] == 2


def test_create_plan_rejects_plan_annotation_without_default_plan() -> None:
    @create_plan
    def _base(ctx: PlanContext, exports: _ExampleExports) -> None:
        ft = FeatureType("base_ft")
        exports.feature_0 = Feature("base_col", dtype=LaminDType(str), type=ft)
        exports.schema_0 = Schema(name="base_schema", features=[exports.feature_0])

    with pytest.raises(TypeError, match="without a default Plan value"):

        @create_plan  # type: ignore
        def _child(
            ctx: PlanContext,
            exports: None,
            base: Plan[_ExampleExports],
        ) -> None:
            _ = _base


def test_create_plan_rejects_plan_default_without_plan_annotation() -> None:
    @create_plan
    def _base(ctx: PlanContext, exports: _ExampleExports) -> None:
        ft = FeatureType("base_ft")
        exports.feature_0 = Feature("base_col", dtype=LaminDType(str), type=ft)
        exports.schema_0 = Schema(name="base_schema", features=[exports.feature_0])

    with pytest.raises(TypeError, match="does not annotate it as Plan"):

        @create_plan  # type: ignore
        def _child(
            ctx: PlanContext,
            exports: None,
            base: _ExampleExports = _base,  # type: ignore[assignment]
        ) -> None:
            _ = base


def test_inherited_dependencies_are_the_complement_of_the_plan_s_own() -> None:
    @create_plan
    def _upstream(ctx: PlanContext, exports: _ExampleExports) -> None:
        ft = FeatureType("inh_ft")
        exports.feature_0 = Feature("inh_col", dtype=LaminDType(str), type=ft)
        exports.schema_0 = Schema(name="inh_schema", features=[exports.feature_0])

    @create_plan
    def _downstream(
        ctx: PlanContext,
        exports: None,
        upstream: Plan[_ExampleExports] = _upstream,
    ) -> None:
        Schema(name="inh_downstream", features=[upstream.exports.feature_0])

    _downstream.finalize()

    own = {id(dep.obj) for dep in _downstream.own_dependencies}
    inherited = {id(dep.obj) for dep in _downstream.inherited_dependencies}
    everything = {id(dep.obj) for dep in _downstream.dependencies}
    assert own | inherited == everything
    assert not own & inherited
    # the borrowed feature belongs to the upstream plan, the schema to this one
    assert [type(dep.obj).__name__ for dep in _downstream.own_dependencies] == [
        "Schema"
    ]
    assert {dep.obj for dep in _upstream.dependencies} == {
        dep.obj for dep in _downstream.inherited_dependencies
    }


def test_inherited_moves_are_collected_transitively() -> None:
    @create_plan
    def _root(ctx: PlanContext, exports: _ExampleExports) -> None:
        ft = FeatureType("mv_ft")
        exports.feature_0 = Feature("mv_new", dtype=LaminDType(str), type=ft)
        exports.schema_0 = Schema(name="mv_schema", features=[exports.feature_0])
        ctx.moved(exports.feature_0, "mv_old")

    @create_plan
    def _middle(
        ctx: PlanContext,
        exports: _ExampleExports,
        root: Plan[_ExampleExports] = _root,
    ) -> None:
        ft = FeatureType("mv_mid_ft")
        exports.feature_0 = Feature("mv_mid_new", dtype=LaminDType(str), type=ft)
        exports.schema_0 = Schema(
            name="mv_mid_schema", features=[root.exports.feature_0]
        )
        ctx.moved(exports.feature_0, "mv_mid_old")

    @create_plan
    def _leaf(
        ctx: PlanContext,
        exports: None,
        middle: Plan[_ExampleExports] = _middle,
    ) -> None:
        Schema(name="mv_leaf_schema", features=[middle.exports.feature_0])

    _leaf.finalize()

    assert _root.inherited_moves == ()
    assert [mv.from_ for mv in _middle.inherited_moves] == ["mv_old"]
    # the leaf sees its prerequisite's move and the one that prerequisite inherits
    assert sorted(mv.from_ for mv in _leaf.inherited_moves) == ["mv_mid_old", "mv_old"]
    # a plan's own moves are never part of its inherited ones
    assert [mv.from_ for mv in _leaf.ctx.moves] == []


def test_inherited_dependencies_requires_a_finalized_plan() -> None:
    @create_plan
    def _p(ctx: PlanContext, exports: None) -> None:
        FeatureType("inh_unfinalized_ft")

    with pytest.raises(PlanNotFinalizedError):
        _ = _p.inherited_dependencies


def test_uid_promises_match_saved_declarations_and_lock_metadata(ln) -> None:
    from lamindb_setup.evolve.engine._apply import ApplyContext
    from lamindb_setup.evolve.engine._lock import serialize_config, serialize_existing

    ctx = ApplyContext.standalone()
    with FeatureType("assay features") as feature_type:
        feature = Feature("concentration", dtype=LaminDType(float), type=feature_type)
    with SchemaType("assay schemas") as schema_type:
        schema = Schema(
            name="Concentrations",
            features=[feature],
            type=schema_type,
            description="Concentration measurements",
            coerce=True,
        )
    with RecordType("assays", description="assay records") as record_type:
        record = Record(
            "assay A",
            type=record_type,
            description="first assay",
            reference="A1",
            reference_type="accession",
        )
    for declaration in (feature_type, feature, schema, record):
        promise = declaration.uid
        assert declaration.name in repr(promise)
        assert promise.resolve(ctx) == declaration.materialize(ctx).uid
    for model, kind, keys in [
        (record, "Record", ("description", "reference", "reference_type")),
        (record_type, "RecordType", ("description",)),
        (schema, "Schema", ("description", "coerce")),
    ]:
        config = serialize_config(model)
        snapshot = serialize_existing(model.materialize(ctx), kind)
        for key in keys:
            assert snapshot[key] == config[key] == getattr(model, key)


def test_plan_cycles_and_repeated_prerequisites() -> None:
    @create_plan
    def first(ctx: PlanContext, exports: None) -> None:
        pass

    @create_plan
    def second(ctx: PlanContext, exports: None) -> None:
        pass

    with pytest.raises(PlanNotFinalizedError, match="finalized"):
        _ = first.ctx
    with pytest.raises(ValueError, match="depend on itself"):
        first.add_plan_dependency(first)
    second.add_plan_dependency(first)
    second.add_plan_dependency(first)
    assert second.depends_on == (first,)
    first.add_plan_dependency(second)
    with pytest.raises(RuntimeError, match="circular plan dependency"):
        second.finalize()


def test_create_plan_reports_invalid_function_signatures() -> None:
    def no_exports(ctx: PlanContext) -> None:
        pass

    def wrong_exports(ctx: PlanContext, exports: int) -> None:
        pass

    def bad_prerequisite(
        ctx: PlanContext,
        exports: None,
        base: Plan[_ExampleExports] = 1,  # type: ignore[assignment]
    ) -> None:
        pass

    for fn, message in [
        (no_exports, "second positional"),
        (wrong_exports, "not annotated with"),
        (bad_prerequisite, "default"),
    ]:
        with pytest.raises(TypeError, match=message):
            create_plan(fn)  # type: ignore[call-overload]


def test_transitive_prerequisite_is_listed_once_when_also_declared_directly() -> None:
    @create_plan
    def root(ctx: PlanContext, exports: None) -> None:
        FeatureType("shared")

    @create_plan
    def left(ctx: PlanContext, exports: None) -> None:
        FeatureType("left")

    @create_plan
    def right(ctx: PlanContext, exports: None) -> None:
        FeatureType("right")

    left.add_plan_dependency(root)
    right.add_plan_dependency(root)
    right.add_plan_dependency(left)
    right.finalize()
    assert [
        obj.name
        for dep in right.dependencies
        if isinstance(obj := dep.obj, FeatureType)
    ] == ["shared", "left", "right"]


def test_callable_plan_and_nested_finalization() -> None:
    from functools import partial, update_wrapper

    from lamindb_setup.evolve.core._graph import Dependency
    from lamindb_setup.evolve.engine._loader import co_applied_prerequisite_keys

    def body(ctx, exports, *args, **kwargs):
        shared = FeatureType("shared")
        active = Plan.active()
        assert active is not None
        active.add_dependency(Dependency(shared))

    # An unresolved return annotation does not invalidate the exports annotation.
    body.__annotations__ = {"exports": type(None), "return": "OptionalPluginType"}
    shared_plan = create_plan(update_wrapper(partial(body), body))
    shared_plan.finalize()
    assert len(shared_plan.dependencies) == 1
    assert shared_plan.name == "body"

    @create_plan
    def nested(ctx: PlanContext, exports: None) -> None:
        shared_plan.finalize()

    shared_plan.reset()
    with pytest.raises(RuntimeError, match="nested plans"):
        nested.finalize()
    nested.add_plan_dependency(shared_plan)

    @create_plan
    def top(ctx: PlanContext, exports: None) -> None:
        pass

    top.add_plan_dependency(shared_plan)
    top.add_plan_dependency(nested)
    assert co_applied_prerequisite_keys([shared_plan, nested, top])[-1] == {
        "FeatureType:shared"
    }


def test_custom_plan_and_dependency_graph_contracts() -> None:
    from lamindb_setup.evolve.core._graph import (
        Dependency,
        DependencyDAG,
        DependencyMeta,
        Materializable,
        Promise,
    )

    class CustomPlan(Plan[PlanExports]):
        @property
        def _exports_cls(self):
            return PlanExports

        def run(self, ctx, exports):
            pass

    custom = CustomPlan("custom")
    assert custom.defining_module == __name__
    item = Materializable()
    for operation in (item.stage, item.get):
        with pytest.raises(NotImplementedError):
            operation(None)
    promise = Promise(item, lambda obj, ctx: obj)
    assert "lambda" in repr(promise)
    assert list(DependencyMeta._iter_materializables([promise])) == [item]
    graph = DependencyDAG([Dependency(item, requires=[item])])
    with pytest.raises(ValueError, match="circular dependency"):
        graph.ordered()


def test_record_type_post_save_clears_only_the_plan_exclusion(ln) -> None:
    from lamindb_setup.evolve.engine._apply import ApplyContext

    ctx = ApplyContext.standalone()
    saved = RecordType("in scope", exclude_children_from_plans=True).commit(ctx)
    assert saved._aux["_excludeChildrenFromPlans"] == 1
    saved._aux["descriptionHint"] = "keep"
    saved.save()
    RecordType("in scope")._post_save(ctx, saved)
    saved.refresh_from_db()
    assert saved._aux == {"descriptionHint": "keep"}
