# Adapted source tests include deliberately invalid declarations and dynamic models.
# mypy: disable-error-code="assignment,misc,var-annotated"
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import pytest
from lamindb_setup.evolve.authoring._declarative import (
    FeatureGroup,
    GroupConfig,
    SchemaConfig,
    SchemaModel,
    feature,
)
from lamindb_setup.evolve.authoring._dtypes import (
    CatDType,
    DType,
    GeneCat,
    LaminDType,
    ListDType,
    RecordCat,
    ULabelCat,
    UnionCat,
    get_dtype,
)
from lamindb_setup.evolve.authoring._features import Feature, FeatureType
from lamindb_setup.evolve.authoring._plan import (
    PlanContext,
    PlanExports,
    create_plan,
)
from lamindb_setup.evolve.authoring._records import RecordType
from lamindb_setup.evolve.authoring._schemas import Schema, SchemaType
from lamindb_setup.evolve.core import Promise, SpaceUID
from lamindb_setup.evolve.engine._apply import ApplyContext
from lamindb_setup.evolve.render import TerminalPlanSummary

if TYPE_CHECKING:
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as BiontyModuleType
    from typing import Any as LamindbModuleType


class _BasicSchema(SchemaModel):
    schema_config = SchemaConfig(name="BasicSchema")
    a: Feature[str]
    b: Feature[int]


def test_emits_a_real_schema() -> None:
    assert isinstance(_BasicSchema.schema, Schema)
    assert _BasicSchema.schema.name == "BasicSchema"


def test_fields_become_features_in_declaration_order() -> None:
    assert [f.name for f in _BasicSchema.schema.features] == ["a", "b"]


def test_dtype_is_inferred_from_the_generic_argument() -> None:
    features = _BasicSchema.schema.features
    assert features[0].dtype.dtype is str
    assert features[1].dtype.dtype is int


def test_fields_are_exposed_as_feature_attributes() -> None:
    assert isinstance(_BasicSchema.a, Feature)
    assert _BasicSchema.a is _BasicSchema.schema.features[0]
    assert _BasicSchema.b is _BasicSchema.schema.features[1]


_DEFAULT_FT = FeatureType("BasicFeature")


class _TypedSchema(SchemaModel):
    schema_config = SchemaConfig(name="TypedSchema", default_feature_type=_DEFAULT_FT)
    a: Feature[str]
    b: Feature[str] = feature(type=FeatureType("OtherFeature"))


def test_default_feature_type_is_applied() -> None:
    assert _TypedSchema.a.type is _DEFAULT_FT


def test_explicit_feature_type_overrides_the_default() -> None:
    b_type = _TypedSchema.b.type
    assert b_type is not None
    assert b_type.name == "OtherFeature"


class _OverrideSchema(SchemaModel):
    schema_config = SchemaConfig(name="OverrideSchema")
    organism: Feature[str] = feature(
        name="organism_name",
        description="Species of the sample",
        nullable=True,
        synonyms=["species"],
        unit="mg",
    )


def test_feature_overrides_are_applied() -> None:
    organism = _OverrideSchema.organism
    assert organism.name == "organism_name"
    assert organism.description == "Species of the sample"
    assert organism.nullable is True
    assert organism.synonyms == ["species"]
    assert organism.unit == "mg"


class _DocstringSchema(SchemaModel):
    """A documented schema.

    Features
    --------
    sample_id : Feature[str]
        The unique sample identifier.
    organism
        Species of the
        sample.
    replicate : Feature[int]
    """

    schema_config = SchemaConfig(name="DocstringSchema")
    sample_id: Feature[str]
    organism: Feature[str]
    replicate: Feature[int]


def test_docstring_features_supply_descriptions() -> None:
    assert _DocstringSchema.sample_id.description == "The unique sample identifier."


def test_docstring_wrapped_description_is_joined() -> None:
    assert _DocstringSchema.organism.description == "Species of the sample."


def test_docstring_entry_without_body_leaves_description_unset() -> None:
    assert _DocstringSchema.replicate.description is None


class _DocstringOverrideSchema(SchemaModel):
    """Explicit feature(description=...) wins over the docstring.

    Features
    --------
    organism
        From the docstring.
    """

    schema_config = SchemaConfig(name="DocstringOverrideSchema")
    organism: Feature[str] = feature(description="From feature()")


def test_explicit_description_wins_over_docstring() -> None:
    assert _DocstringOverrideSchema.organism.description == "From feature()"


def test_docstring_comma_separated_names_share_a_description() -> None:
    class _S(SchemaModel):
        """Shared docstring description.

        Features
        --------
        a, b : Feature[int]
            A shared description.
        """

        schema_config = SchemaConfig(name="SharedDocstringSchema")
        a: Feature[int]
        b: Feature[int]

    assert _S.a.description == "A shared description."
    assert _S.b.description == "A shared description."


def test_docstring_without_features_section_is_ignored() -> None:
    class _S(SchemaModel):
        """A schema with no Features section."""

        schema_config = SchemaConfig(name="NoFeaturesSectionSchema")
        a: Feature[str]

    assert _S.a.description is None


def test_docstring_unknown_field_raises() -> None:
    with pytest.raises(ValueError, match="do not match any declared field"):

        class _S(SchemaModel):
            """Documents a field that does not exist.

            Features
            --------
            missing
                Not a real field.
            """

            schema_config = SchemaConfig(name="BadDocstringSchema")
            a: Feature[str]


def test_docstring_features_stop_at_next_section() -> None:
    class _S(SchemaModel):
        """A schema with a trailing section.

        Features
        --------
        a
            The a feature.

        Notes
        -----
        This should not be parsed as a feature.
        """

        schema_config = SchemaConfig(name="TrailingSectionSchema")
        a: Feature[str]

    assert _S.a.description == "The a feature."


def test_docstring_features_work_for_feature_group() -> None:
    class _G(FeatureGroup):
        """A documented feature group.

        Features
        --------
        sample_id
            The sample identifier.
        """

        group_config = GroupConfig(feature_type="grp_feature")
        sample_id: Feature[str]

    assert _G.sample_id.description == "The sample identifier."


class _CoerceSchema(SchemaModel):
    schema_config = SchemaConfig(name="CoerceSchema", coerce=True)
    a: Feature[str]


def test_schema_level_coerce_is_applied() -> None:
    assert _CoerceSchema.schema.coerce is True


def test_schema_level_coerce_defaults_to_unset() -> None:
    assert _BasicSchema.schema.coerce is None


def test_matching_dtype_override_is_accepted() -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="MatchingDtype")
        a: Feature[str] = feature(dtype=LaminDType(str))

    assert _S.a.dtype.dtype is str


def test_inner_optional_marks_the_feature_nullable() -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="InnerNullable")
        a: Feature[str]
        b: Feature[str | None]

    # The annotation decides: `Feature[str]` is "a str", so not nullable.
    assert _S.a.nullable is False
    assert _S.b.nullable is True
    assert _S.b.dtype.dtype is str
    # a nullable feature is not, by itself, an optional schema member
    assert _S.schema.optionals == []


def test_plain_annotation_is_not_nullable() -> None:
    # Leaving `nullable` unset would let lamindb default it to True, which
    # would make the two annotations emit identical features.
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="PlainNotNullable")
        a: Feature[str]
        b: Feature[str | None]

    assert _S.a.nullable != _S.b.nullable


def test_explicit_nullable_true_wins_over_plain_annotation() -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="PlainNullableTrue")
        a: Feature[str] = feature(nullable=True)

    assert _S.a.nullable is True


def test_inner_optional_is_equivalent_to_feature_nullable() -> None:
    class _Shorthand(SchemaModel):
        schema_config = SchemaConfig(name="InnerNullableShorthand")
        a: Feature[str | None]

    class _Explicit(SchemaModel):
        schema_config = SchemaConfig(name="InnerNullableExplicit")
        a: Feature[str] = feature(nullable=True)

    assert _Shorthand.a.nullable == _Explicit.a.nullable


def test_inner_optional_works_with_new_dtype_forms() -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="InnerNullableNewDtype")
        n: Feature[DType[int] | None]
        lst: Feature[ListDType[int] | None]

    assert _S.n.nullable is True
    assert _S.n.dtype.dtype_str == "int"
    assert _S.lst.nullable is True
    assert _S.lst.dtype.dtype_str == "list[int]"


def test_outer_optional_marks_the_field_optional() -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="OuterOptional")
        a: Feature[str]
        b: Feature[str] | None

    # `| None` on the whole annotation is an optional schema member, not
    # nullable.
    assert _S.b.nullable is False
    assert _S.b.dtype.dtype is str
    assert _S.schema.optionals == [_S.b]


def test_outer_optional_is_equivalent_to_feature_optional() -> None:
    class _Shorthand(SchemaModel):
        schema_config = SchemaConfig(name="OuterOptionalShorthand")
        a: Feature[str]
        b: Feature[str] | None

    class _Explicit(SchemaModel):
        schema_config = SchemaConfig(name="OuterOptionalExplicit")
        a: Feature[str]
        b: Feature[str] = feature(optional=True)

    assert _Shorthand.schema.optionals == [_Shorthand.b]
    assert _Explicit.schema.optionals == [_Explicit.b]


def test_inner_and_outer_optional_compose() -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="OptionalAndNullable")
        a: Feature[str]
        b: Feature[str | None] | None

    assert _S.b.nullable is True
    assert _S.b.dtype.dtype is str
    assert _S.schema.optionals == [_S.b]


def test_outer_optional_combines_with_feature_override() -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="OptionalOverride")
        a: Feature[str]
        b: Feature[str] | None = feature(description="Species")

    assert _S.b.description == "Species"
    assert _S.schema.optionals == [_S.b]


def test_explicit_optional_false_wins_over_outer_optional_annotation() -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="OptionalFalse")
        a: Feature[str]
        b: Feature[str] | None = feature(optional=False)

    assert _S.schema.optionals == []


def test_explicit_nullable_false_wins_over_inner_optional_annotation() -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="InnerNullableFalse")
        a: Feature[str | None] = feature(nullable=False)

    assert _S.a.nullable is False


def test_index_field_may_not_be_optional() -> None:
    with pytest.raises(ValueError, match="index .* may not be optional"):

        class _S(SchemaModel):
            schema_config = SchemaConfig(name="OptionalIndex", index="a")
            a: Feature[str] | None


def test_index_field_may_have_a_non_str_dtype() -> None:
    # The `str` requirement belongs to record sheets, not to schemas at large.
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="IntIndex", index="a")
        a: Feature[int]

    assert _S.schema.index is _S.a


def test_inner_optional_works_in_feature_group() -> None:
    class _G(FeatureGroup):
        group_config = GroupConfig(feature_type="null_group_feature")
        a: Feature[str | None]

    assert _G.a.nullable is True


def test_outer_optional_in_feature_group_raises() -> None:
    with pytest.raises(ValueError, match="optionality only applies to a SchemaModel"):

        class _G(FeatureGroup):
            group_config = GroupConfig(feature_type="opt_group_feature")
            a: Feature[str] | None


def test_mismatched_dtype_override_raises() -> None:
    with pytest.raises(ValueError, match="base dtypes must match"):

        class _S(SchemaModel):
            schema_config = SchemaConfig(name="MismatchDtype")
            a: Feature[str] = feature(dtype=LaminDType(int))


def test_new_dtype_simple_forms_are_resolved() -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="NewSimpleDtype")
        n: Feature[DType[int]]
        s: Feature[DType[str]]
        lst: Feature[ListDType[int]]

    assert _S.n.dtype.dtype_str == "int"
    assert _S.s.dtype.dtype_str == "str"
    assert _S.lst.dtype.dtype_str == "list[int]"
    assert _S.n.dtype.cat_filters is None


def test_new_dtype_matching_override_is_accepted() -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="NewMatchingDtype")
        a: Feature[DType[int]] = feature(dtype=get_dtype(DType[int]))

    assert _S.a.dtype.dtype_str == "int"


def test_new_dtype_mismatched_override_raises() -> None:
    with pytest.raises(ValueError, match="base dtypes must match"):

        class _S(SchemaModel):
            schema_config = SchemaConfig(name="NewMismatchDtype")
            a: Feature[DType[int]] = feature(dtype=get_dtype(DType[str]))


def test_new_dtype_categorical_form_is_resolved(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="NewCategoricalDtype")
        label: Feature[CatDType[ln.ULabel, ULabelCat(type__uid="uTEST00000000")]]

    feat = _S.label
    assert feat.dtype.dtype_str == "cat[ULabel]"
    assert feat.dtype.cat_filters == {"type__uid": "uTEST00000000"}


def test_union_marker_requires_an_actual_registry_union(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    with pytest.raises(ValueError, match="union of at least two"):
        get_dtype(CatDType[ln.Record, UnionCat()])


def test_new_dtype_list_of_categorical_is_resolved(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # A list of a categorical: the filters stay on the resolved dtype and are
    # inlined into the dtype string at staging time.
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="NewListCategoricalDtype")
        labels: Feature[
            ListDType[CatDType[ln.ULabel, ULabelCat(type__uid="uTEST00000000")]]
        ]
        refs: Feature[list[CatDType[ln.Record, RecordCat(type__uid="uTEST00000000")]]]

    assert _S.labels.dtype.dtype_str == "list[cat[ULabel]]"
    assert _S.labels.dtype.cat_filters == {"type__uid": "uTEST00000000"}
    assert _S.refs.dtype.dtype_str == "list[cat[Record]]"
    assert _S.refs.dtype.cat_filters == {"type__uid": "uTEST00000000"}


def test_union_categorical_form_is_resolved(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="UnionCategorical")
        either: Feature[CatDType[ln.ULabel | ln.Record, UnionCat()]]

    feat = _S.either
    assert feat.dtype.dtype_str == "cat[ULabel|Record]"
    assert feat.dtype.cat_filters is None


def test_union_categorical_with_field_selector(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="UnionCategoricalField")
        either: Feature[
            CatDType[ln.ULabel | ln.Record, UnionCat(fields={ln.ULabel: "uid"})]
        ]

    feat = _S.either
    assert feat.dtype.dtype_str == "cat[ULabel.uid|Record]"
    assert feat.dtype.cat_filters is None


def test_union_categorical_requires_union_marker(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    with pytest.raises(ValueError, match="requires a UnionCat"):

        class _S(SchemaModel):
            schema_config = SchemaConfig(name="UnionCategoricalNoMarker")
            either: Feature[
                CatDType[ln.ULabel | ln.Record, ULabelCat(type__uid="uTEST00000000")]
            ]


def test_nullable_union_categorical_is_resolved(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="NullableUnionCategorical")
        either: Feature[CatDType[ln.ULabel | ln.Record, UnionCat()] | None]

    feat = _S.either
    assert feat.dtype.dtype_str == "cat[ULabel|Record]"
    assert feat.nullable is True


def test_optional_single_categorical_inside_catdtype_is_resolved(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # `CatDType[A | None, ACat(...)]` is a single categorical made optional by
    # `| None` -- not a union -- so the `None` is dropped and the sole member
    # resolves as a plain categorical (with its cat_filters intact).
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="OptionalSingleCategorical")
        label: Feature[CatDType[ln.ULabel | None, ULabelCat(type__uid="uTEST00000000")]]

    feat = _S.label
    assert feat.dtype.dtype_str == "cat[ULabel]"
    assert feat.dtype.cat_filters == {"type__uid": "uTEST00000000"}


def test_record_relation_with_string_uid_is_resolved(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="RecordRelationString")
        ref: Feature[
            CatDType[
                ln.Record,
                RecordCat(type__uid="uTEST00000000", is_type=True),
            ]
        ]

    feat = _S.ref
    assert feat.dtype.dtype_str == "cat[Record]"
    assert feat.dtype.cat_filters == {"type__uid": "uTEST00000000", "is_type": True}


def test_record_relation_uid_promise_is_carried(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # A `.uid` promise to another plan-managed object is carried verbatim into
    # the feature's `cat_filters` (resolved to a concrete uid during apply).
    rt = RecordType("SomeRecordType")
    st = SchemaType("SomeSchemaType")

    class _S(SchemaModel):
        schema_config = SchemaConfig(name="RecordRelationPromise")
        ref: Feature[
            CatDType[
                ln.Record,
                RecordCat(type__uid=rt.uid, is_type=True, schema__uid=st.uid),
            ]
        ]

    cat_filters = _S.ref.dtype.cat_filters
    assert cat_filters is not None
    assert isinstance(cat_filters["type__uid"], Promise)
    assert cat_filters["type__uid"].get_arg() is rt
    assert isinstance(cat_filters["schema__uid"], Promise)
    assert cat_filters["schema__uid"].get_arg() is st
    assert cat_filters["is_type"] is True


def test_classvar_annotations_are_ignored() -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="WithClassVar")
        note: ClassVar[str] = "not a feature"
        a: Feature[str]

    assert [f.name for f in _S.schema.features] == ["a"]


def test_non_feature_annotation_raises() -> None:
    with pytest.raises(TypeError, match="must be annotated as"):

        class _S(SchemaModel):
            schema_config = SchemaConfig(name="BadAnnotation")
            a: Feature[str]
            b: int


def test_function_local_annotation_is_resolved() -> None:
    import datetime as _local_datetime

    class _S(SchemaModel):
        schema_config = SchemaConfig(name="LocalSchema")
        when: Feature[_local_datetime.date]

    assert _S.when.dtype.dtype is _local_datetime.date


def test_index_is_set_from_config() -> None:
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="IndexedSchema", index="sample_id")
        sample_id: Feature[str]
        value: Feature[int]

    assert _S.schema.index is _S.sample_id


def test_unknown_index_raises() -> None:
    with pytest.raises(ValueError, match="does not match any"):

        class _S(SchemaModel):
            schema_config = SchemaConfig(name="BadIndex", index="missing")
            a: Feature[str]


def test_index_feature_is_linked_once_on_materialize(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # The index feature is both a declared field and the schema's index. lamindb
    # re-inserts the index into the feature list, so it must not also be passed
    # in `features` or the SchemaFeature unique constraint is violated.
    class _S(SchemaModel):
        schema_config = SchemaConfig(name="IndexedMaterialized", index="sample_id")
        sample_id: Feature[str]
        value: Feature[int]

    ln_schema = _S.schema.materialize(ApplyContext.standalone())

    member_names = [f.name for f in ln_schema.members]
    assert member_names.count("sample_id") == 1
    assert set(member_names) == {"sample_id", "value"}


def test_empty_schema_raises() -> None:
    with pytest.raises(ValueError, match="at least one Feature"):

        class _S(SchemaModel):
            schema_config = SchemaConfig(name="EmptySchema")


def test_duplicate_feature_name_raises() -> None:
    with pytest.raises(ValueError, match="duplicate feature name"):

        class _S(SchemaModel):
            schema_config = SchemaConfig(name="DuplicateSchema")
            a: Feature[str]
            b: Feature[int] = feature(name="a")


def test_unknown_config_key_raises() -> None:
    with pytest.raises(TypeError, match="unknown schema_config keys"):

        class _S(SchemaModel):
            schema_config = {"nam": "typo"}  # type: ignore[typeddict-unknown-key]
            a: Feature[str]


def test_shared_feature_is_the_same_object() -> None:
    shared = Feature("sample_id", dtype=LaminDType(str))

    class _A(SchemaModel):
        schema_config = SchemaConfig(name="ShareA")
        sample_id: Feature[str] = shared

    class _B(SchemaModel):
        schema_config = SchemaConfig(name="ShareB")
        sample_id: Feature[str] = shared

    assert _A.sample_id is shared
    assert _B.sample_id is shared
    assert _A.sample_id is _B.sample_id


def test_field_can_reuse_another_models_feature() -> None:
    class _Source(SchemaModel):
        schema_config = SchemaConfig(name="ReuseSource")
        sample_id: Feature[str]

    class _Consumer(SchemaModel):
        schema_config = SchemaConfig(name="ReuseConsumer")
        sample_id: Feature[str] = _Source.sample_id
        value: Feature[int]

    assert _Consumer.sample_id is _Source.sample_id


def test_reused_feature_keeps_its_own_feature_type() -> None:
    own_ft = FeatureType("OwnFeature")
    default_ft = FeatureType("DefaultFeature")
    shared = Feature("sample_id", dtype=LaminDType(str), type=own_ft)

    class _S(SchemaModel):
        schema_config = SchemaConfig(
            name="ReuseKeepsType", default_feature_type=default_ft
        )
        sample_id: Feature[str] = shared
        value: Feature[int]

    assert _S.sample_id.type is own_ft
    assert _S.value.type is default_ft


def test_reused_feature_dtype_mismatch_raises() -> None:
    shared = Feature("sample_id", dtype=LaminDType(int))
    with pytest.raises(ValueError, match="base dtypes must match"):

        class _S(SchemaModel):
            schema_config = SchemaConfig(name="ReuseMismatch")
            sample_id: Feature[str] = shared


def test_reused_feature_categorical_filter_mismatch_raises(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    shared = Feature(
        "record",
        dtype=LaminDType(ln.Record, type__uid="a" * 20),
    )
    with pytest.raises(ValueError, match="including categorical filters"):

        class _S(SchemaModel):
            schema_config = SchemaConfig(name="ReuseFilterMismatch")
            record: Feature[CatDType[ln.Record, RecordCat(type__uid="b" * 20)]] = shared


def test_dtype_override_categorical_filter_mismatch_raises(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    with pytest.raises(ValueError, match="including categorical filters"):

        class _S(SchemaModel):
            schema_config = SchemaConfig(name="OverrideFilterMismatch")
            record: Feature[CatDType[ln.Record, RecordCat(type__uid="b" * 20)]] = (
                feature(dtype=LaminDType(ln.Record, type__uid="a" * 20))
            )


def test_reused_feature_accepts_equivalent_promised_filters(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    record_type = RecordType("PromiseTarget")
    shared = Feature(
        "record",
        dtype=LaminDType(ln.Record, type__uid=record_type.uid),
    )

    class _S(SchemaModel):
        schema_config = SchemaConfig(name="ReusePromisedFilter")
        record: Feature[CatDType[ln.Record, RecordCat(type__uid=record_type.uid)]] = (
            shared
        )

    assert _S.record is shared


def test_reused_feature_name_mismatch_raises() -> None:
    shared = Feature("sample_id", dtype=LaminDType(str))
    with pytest.raises(ValueError, match="must match the feature name"):

        class _S(SchemaModel):
            schema_config = SchemaConfig(name="ReuseNameMismatch")
            other: Feature[str] = shared


def test_unannotated_feature_attribute_raises() -> None:
    loose = Feature("loose", dtype=LaminDType(str))
    with pytest.raises(TypeError, match="not annotated as Feature"):

        class _S(SchemaModel):
            schema_config = SchemaConfig(name="LooseFeature")
            a: Feature[str]
            b = loose


def test_inheritance_raises() -> None:
    class _Base(SchemaModel):
        schema_config = SchemaConfig(name="BaseSchema")
        a: Feature[str]

    with pytest.raises(NotImplementedError, match="inheritance is on the TODO list"):

        class _Child(_Base):
            b: Feature[int]


class _ModelPlanExports(PlanExports):
    schema_0: Schema


@create_plan
def _model_plan(ctx: PlanContext, exports: _ModelPlanExports) -> None:
    ft = FeatureType("ModelFeature")

    class _Experiment(SchemaModel):
        schema_config = SchemaConfig(
            name="ExperimentSchema",
            type=SchemaType("ExperimentSchema"),
            default_feature_type=ft,
            index="sample_id",
        )
        sample_id: Feature[str]
        value: Feature[int]

    exports.schema_0 = _Experiment.schema


def test_plan_collects_emitted_objects() -> None:
    _model_plan.finalize()
    deps = list(_model_plan.dependencies)
    kinds = [type(dep.obj).__name__ for dep in deps]
    assert kinds.count("Feature") >= 2
    assert kinds.count("Schema") == 1


def test_schema_is_ordered_after_its_features() -> None:
    _model_plan.finalize()
    ordered = list(_model_plan.dependencies)
    objs = [dep.obj for dep in ordered]
    schema = next(o for o in objs if isinstance(o, Schema))
    schema_pos = objs.index(schema)
    feature_positions = [i for i, o in enumerate(objs) if isinstance(o, Feature)]
    assert all(pos < schema_pos for pos in feature_positions)


def test_plan_populates_model_exports() -> None:
    _model_plan.finalize()
    exported = _model_plan.exports.schema_0
    assert isinstance(exported, Schema)
    assert exported.name == "ExperimentSchema"


def test_summary_renders_model_plan() -> None:
    summary = TerminalPlanSummary().render(_model_plan)
    assert "Schema 'ExperimentSchema'" in summary
    assert "Feature 'sample_id'" in summary


@create_plan
def _shared_feature_plan(ctx: PlanContext, exports: None) -> None:
    shared = Feature("sample_id", dtype=LaminDType(str))

    class _A(SchemaModel):
        schema_config = SchemaConfig(name="SharedA")
        sample_id: Feature[str] = shared
        a_value: Feature[int]

    class _B(SchemaModel):
        schema_config = SchemaConfig(name="SharedB")
        sample_id: Feature[str] = shared
        b_value: Feature[int]


def test_shared_feature_is_a_single_dependency_node() -> None:
    _shared_feature_plan.finalize()
    deps = list(_shared_feature_plan.dependencies)
    sample_features = [
        dep.obj
        for dep in deps
        if isinstance(dep.obj, Feature) and dep.obj.name == "sample_id"
    ]
    assert len(sample_features) == 1


class _ExperimentFeatures(FeatureGroup):
    group_config = GroupConfig(feature_type=FeatureType("experiment_feature"))
    sample_id: Feature[str]
    replicate: Feature[int]


def test_group_emits_a_feature_type() -> None:
    assert isinstance(_ExperimentFeatures.feature_type, FeatureType)
    assert _ExperimentFeatures.feature_type.name == "experiment_feature"


def test_group_fields_become_features_typed_by_the_group() -> None:
    assert isinstance(_ExperimentFeatures.sample_id, Feature)
    assert _ExperimentFeatures.sample_id.name == "sample_id"
    assert _ExperimentFeatures.sample_id.dtype.dtype is str
    assert _ExperimentFeatures.sample_id.type is _ExperimentFeatures.feature_type
    assert _ExperimentFeatures.replicate.type is _ExperimentFeatures.feature_type


def test_group_builds_feature_type_from_a_string() -> None:
    class _G(FeatureGroup):
        group_config = GroupConfig(feature_type="strized")
        a: Feature[str]

    assert isinstance(_G.feature_type, FeatureType)
    assert _G.feature_type.name == "strized"
    assert _G.a.type is _G.feature_type


def test_group_applies_default_feature_space() -> None:
    space = SpaceUID("spaceUID0001")

    class _G(FeatureGroup):
        group_config = GroupConfig(feature_type="ft", default_feature_space=space)
        a: Feature[str]

    assert _G.a.space_uid is space


def test_group_feature_override_wins_over_group_type() -> None:
    other = FeatureType("other")

    class _G(FeatureGroup):
        group_config = GroupConfig(feature_type="ft")
        a: Feature[str] = feature(type=other, description="hi")

    assert _G.a.type is other
    assert _G.a.description == "hi"


def test_group_reused_feature_keeps_its_own_type() -> None:
    own_type = FeatureType("own")
    shared = Feature("sample_id", dtype=LaminDType(str), type=own_type)

    class _G(FeatureGroup):
        group_config = GroupConfig(feature_type="ft")
        sample_id: Feature[str] = shared

    assert _G.sample_id is shared
    assert _G.sample_id.type is own_type


def test_group_requires_feature_type() -> None:
    with pytest.raises(ValueError, match="must set 'feature_type'"):

        class _G(FeatureGroup):
            group_config = GroupConfig()  # type: ignore[typeddict-item]
            a: Feature[str]


def test_group_rejects_unknown_config_keys() -> None:
    with pytest.raises(TypeError, match="unknown group_config keys"):

        class _G(FeatureGroup):
            group_config = {"feature_type": "ft", "bogus": 1}  # type: ignore[assignment]
            a: Feature[str]


def test_group_requires_at_least_one_field() -> None:
    with pytest.raises(ValueError, match="at least one Feature"):

        class _G(FeatureGroup):
            group_config = GroupConfig(feature_type="ft")


def test_group_rejects_duplicate_feature_names() -> None:
    with pytest.raises(ValueError, match="duplicate feature name"):

        class _G(FeatureGroup):
            group_config = GroupConfig(feature_type="ft")
            a: Feature[str] = feature(name="dup")
            b: Feature[str] = feature(name="dup")


def test_group_rejects_feature_type_field_name() -> None:
    with pytest.raises(ValueError, match="reserved attribute"):

        class _G(FeatureGroup):
            group_config = GroupConfig(feature_type="ft")
            feature_type: Feature[str]  # type: ignore[assignment]


def test_schema_model_rejects_schema_field_name() -> None:
    with pytest.raises(ValueError, match="'schema' is a reserved attribute"):

        class _S(SchemaModel):
            schema_config = SchemaConfig(name="ReservedSchema")
            schema: Feature[str]
            retained: Feature[int]


@create_plan
def _group_plan(ctx: PlanContext, exports: None) -> None:
    class _Features(FeatureGroup):
        group_config = GroupConfig(feature_type=FeatureType("plan_feature"))
        sample_id: Feature[str]
        replicate: Feature[int]


def test_group_registers_type_and_features_with_the_plan() -> None:
    _group_plan.finalize()
    deps = list(_group_plan.dependencies)
    kinds = [type(dep.obj).__name__ for dep in deps]
    assert kinds.count("FeatureType") == 1
    assert kinds.count("Feature") == 2


def test_group_features_are_ordered_after_their_type() -> None:
    _group_plan.finalize()
    objs = [dep.obj for dep in _group_plan.dependencies]
    feature_type = next(o for o in objs if isinstance(o, FeatureType))
    type_pos = objs.index(feature_type)
    feature_positions = [i for i, o in enumerate(objs) if isinstance(o, Feature)]
    assert all(type_pos < pos for pos in feature_positions)


@pytest.mark.parametrize("instance", ["bionty"], indirect=True)
def test_reuse_bare_annotation_accepts_default_name_field_feature(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    bt: BiontyModuleType,
) -> None:
    # A bare `Feature[bt.models.Gene]` annotation resolves to cat[bionty.Gene],
    # while the reused feature carries Gene's default name field
    # (cat[bionty.Gene.symbol]). Both denote the same match, so reuse is accepted.
    shared = Feature(
        "genetic_perturbation",
        dtype=get_dtype(CatDType[bt.models.Gene, GeneCat(name_field="symbol")]),
    )
    assert shared.dtype.dtype_str == "cat[bionty.Gene.symbol]"

    class _S(SchemaModel):
        schema_config = SchemaConfig(
            name="ReuseBareGene", default_feature_type=FeatureType("reuse_ft")
        )
        genetic_perturbation: Feature[bt.models.Gene] = shared

    assert _S.genetic_perturbation is shared


@pytest.mark.parametrize("instance", ["bionty"], indirect=True)
def test_reuse_rejects_non_default_name_field_mismatch(
    instance: TestInstanceInfo,
    ln: LamindbModuleType,
    bt: BiontyModuleType,
) -> None:
    # A non-default name field is a genuinely different match, so a bare
    # `Feature[bt.models.Gene]` annotation must not accept a reused
    # cat[bionty.Gene.ensembl_gene_id] feature.
    shared = Feature(
        "genetic_perturbation",
        dtype=get_dtype(
            CatDType[bt.models.Gene, GeneCat(name_field="ensembl_gene_id")]
        ),
    )
    with pytest.raises(ValueError, match="base dtypes must match"):

        class _S(SchemaModel):
            schema_config = SchemaConfig(
                name="ReuseNonDefaultGene",
                default_feature_type=FeatureType("reuse_ft2"),
            )
            genetic_perturbation: Feature[bt.models.Gene] = shared
