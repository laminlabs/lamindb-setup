# Adapted source tests include deliberately invalid declarations and dynamic models.
# mypy: disable-error-code="call-overload,misc,var-annotated"
"""Tests for lamindb_setup.evolve.authoring._dtypes.LaminDType."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from lamindb_setup.evolve.authoring._dtypes import (
    CatDType,
    LaminDType,
    ListDType,
    RecordCat,
    dtype_filter_terms,
    dtypes_match,
    get_dtype,
    inline_cat_filters,
    resolved_dtype_str,
)

if TYPE_CHECKING:
    from types import SimpleNamespace as TestInstanceInfo
    from typing import Any as BiontyModuleType
    from typing import Any as LamindbModuleType


def test_list_of_python_type() -> None:
    dt = LaminDType(list[int])
    assert dt.dtype == list[int]
    assert dt.cat_filters is None


def test_list_of_str() -> None:
    dt = LaminDType(list[str])
    assert dt.dtype == list[str]
    assert dt.cat_filters is None


def test_bare_list_is_unsupported() -> None:
    with pytest.raises(ValueError, match="Unsupported dtype"):
        LaminDType(list)


def test_num_dtype() -> None:
    dt = LaminDType(int | float)
    assert dt.dtype == "num"
    assert dt.cat_filters is None


def test_num_dtype_order_insensitive() -> None:
    dt = LaminDType(float | int)
    assert dt.dtype == "num"
    assert dt.cat_filters is None


def test_num_dtype_string_alias() -> None:
    dt = LaminDType("num")
    assert dt.dtype == "num"
    assert dt.cat_filters is None


def test_num_dtype_rejects_cat_filters() -> None:
    with pytest.raises(ValueError, match="'num'"):
        LaminDType(int | float, source__uid="x")  # type: ignore[call-overload]


def test_path_dtype() -> None:
    dt = LaminDType("path")
    assert dt.dtype == "path"
    assert dt.cat_filters is None


def test_url_dtype() -> None:
    dt = LaminDType("url")
    assert dt.dtype == "url"
    assert dt.cat_filters is None


def test_path_url_dtype_rejects_cat_filters() -> None:
    with pytest.raises(ValueError, match="'path'"):
        LaminDType("path", source__uid="x")  # type: ignore[call-overload]
    with pytest.raises(ValueError, match="'url'"):
        LaminDType("url", source__uid="x")  # type: ignore[call-overload]


def test_unsupported_union_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unsupported dtype"):
        LaminDType(int | str)  # type: ignore[arg-type]


def test_list_rejects_multiple_inner_types() -> None:
    with pytest.raises(ValueError, match="exactly one inner data type"):
        LaminDType(list[int, str])  # type: ignore[misc]


def test_list_rejects_unsupported_inner_type() -> None:
    with pytest.raises(ValueError, match="Unsupported dtype"):
        LaminDType(list[object])  # type: ignore[type-var]


def test_list_of_field_attr(instance: TestInstanceInfo, ln: LamindbModuleType) -> None:
    dt = LaminDType(list[ln.ULabel.name])
    assert dt.dtype == list[ln.ULabel.name]
    assert dt.cat_filters is None


def test_list_of_sqlrecord(instance: TestInstanceInfo, ln: LamindbModuleType) -> None:
    from lamindb.models import SQLRecord

    dt = LaminDType(list[SQLRecord])
    assert dt.dtype == list[SQLRecord]
    assert dt.cat_filters is None


def test_list_of_categorical_passes_cat_filters(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    from lamindb.models import ULabel

    dt = LaminDType(list[ULabel], type__uid="uTEST00000000")
    assert dt.dtype == list[ULabel]
    assert dt.cat_filters == {"type__uid": "uTEST00000000"}


def test_ulabel_without_type_uid(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    dt = LaminDType(ln.ULabel)
    assert dt.dtype is ln.ULabel
    assert dt.cat_filters is None


def test_union_categorical_from_list(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    dt = LaminDType([ln.ULabel, ln.Record])
    assert dt.dtype == [ln.ULabel, ln.Record]
    assert dt.dtype_str == "cat[ULabel|Record]"
    assert dt.cat_filters is None


def test_union_categorical_from_union_type(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    dt = LaminDType(ln.ULabel | ln.Record)
    assert dt.dtype_str == "cat[ULabel|Record]"
    assert dt.cat_filters is None


def test_union_categorical_with_field_selectors(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    dt = LaminDType([ln.ULabel.uid, ln.Record.uid])
    assert dt.dtype_str == "cat[ULabel.uid|Record.uid]"
    assert dt.cat_filters is None


def test_union_categorical_rejects_cat_filters(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    with pytest.raises(ValueError, match="incompatible with union dtypes"):
        LaminDType([ln.ULabel, ln.Record], source__uid="x")  # type: ignore[call-overload]


def test_union_categorical_requires_two_members(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    with pytest.raises(ValueError, match="at least two"):
        LaminDType([ln.ULabel])


def test_core_registry_dtypes(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    for registry in (ln.Transform, ln.Project, ln.User, ln.Reference):
        dt = LaminDType(registry)
        assert dt.dtype is registry
        assert dt.cat_filters is None


def test_core_registry_rejects_cat_filters(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    with pytest.raises(ValueError, match="blocked on purpose"):
        LaminDType(ln.Transform, type__uid="x")  # type: ignore[call-overload]


@pytest.mark.parametrize("instance", ["bionty"], indirect=True)
def test_field_attr_with_source_filter(
    instance: TestInstanceInfo, ln: LamindbModuleType, bt: BiontyModuleType
) -> None:
    dt = LaminDType(bt.Organism.scientific_name, source__uid="abc12345")
    assert dt.dtype == bt.Organism.scientific_name
    assert dt.cat_filters == {"source__uid": "abc12345"}


def test_inline_cat_filters_uses_typed_shorthand() -> None:
    # `Record` / `ULabel` subtypes use lamindb's positional shorthand.
    assert (
        inline_cat_filters("list[cat[Record]]", {"type__uid": "uTEST00000000"})
        == "list[cat[Record[uTEST00000000]]]"
    )
    assert (
        inline_cat_filters("list[cat[ULabel]]", {"type__uid": "uTEST00000000"})
        == "list[cat[ULabel[uTEST00000000]]]"
    )


def test_inline_cat_filters_renders_keyword_filters() -> None:
    assert (
        inline_cat_filters("list[cat[bionty.CellType]]", {"source__uid": "abc12345"})
        == "list[cat[bionty.CellType[source__uid='abc12345']]]"
    )
    assert (
        inline_cat_filters(
            "list[cat[Record]]", {"type__uid": "uTEST00000000", "is_type": True}
        )
        == "list[cat[Record[uTEST00000000, is_type='True']]]"
    )


def test_inline_cat_filters_rejects_non_list_and_union() -> None:
    with pytest.raises(ValueError, match="list of a categorical"):
        inline_cat_filters("cat[Record]", {"type__uid": "x"})
    with pytest.raises(ValueError, match="union dtypes"):
        inline_cat_filters("list[cat[Record|ULabel]]", {"type__uid": "x"})


def test_resolved_dtype_str_without_filters_is_the_bare_dtype() -> None:
    assert resolved_dtype_str("cat[Record]", None) == "cat[Record]"
    assert resolved_dtype_str("list[cat[Record]]", {}) == "list[cat[Record]]"
    assert resolved_dtype_str("str", None) == "str"


def test_resolved_dtype_str_inlines_a_list_like_feature_stage() -> None:
    filters = {"type__uid": "uTEST00000000", "is_type": True}
    assert resolved_dtype_str("list[cat[Record]]", filters) == inline_cat_filters(
        "list[cat[Record]]", filters
    )


@pytest.mark.parametrize(
    ("dtype_str", "cat_filters", "expected"),
    [
        ("cat[Record]", {"type__uid": "uT0"}, "cat[Record[uT0]]"),
        ("cat[ULabel]", {"type__uid": "uT0"}, "cat[ULabel[uT0]]"),
        ("cat[Record]", {"type": "uT0"}, "cat[Record[uT0]]"),
        (
            "cat[Record]",
            {"type__uid": "uT0", "is_type": True, "schema__uid": "sT0"},
            "cat[Record[uT0, is_type=True, schema__uid=sT0]]",
        ),
        (
            "cat[bionty.CellType]",
            {"source__uid": "abc12345"},
            "cat[bionty.CellType[source__uid=abc12345]]",
        ),
        ("cat[Artifact]", {"schema__uid": "sT0"}, "cat[Artifact[schema__uid=sT0]]"),
        (
            "cat[Record]",
            {"type__uid": "uT0", "name": "a,b"},
            'cat[Record[uT0, name="a,b"]]',
        ),
        (
            "cat[Record]",
            {"type__uid": "uT0", "name": 'a,"b'},
            "cat[Record[uT0, name='a,\"b']]",
        ),
    ],
)
def test_resolved_dtype_str_serializes_a_scalar_like_lamindb(
    dtype_str: str, cat_filters: dict[str, object], expected: str
) -> None:
    assert resolved_dtype_str(dtype_str, cat_filters) == expected


def test_resolved_dtype_str_uses_a_records_uid() -> None:
    class _Rec:
        _meta = object()
        uid = "sT0"

    assert (
        resolved_dtype_str("cat[Artifact]", {"schema": _Rec()})
        == "cat[Artifact[schema__uid=sT0]]"
    )


@pytest.mark.parametrize(
    ("dtype_str", "cat_filters", "match"),
    [
        ("cat[Record|ULabel]", {"type__uid": "x"}, "union dtypes"),
        ("str", {"type__uid": "x"}, "categorical dtype"),
        ("cat[Record[uT0]]", {"is_type": True}, "already filtered"),
        ("cat[Record]", {"type__uid": ""}, "Empty value in filter type__uid"),
        ("cat[Record]", {"type__uid": "uT0", "name": None}, "Empty value"),
        ("cat[Record]", {"type": "a", "type__uid": "b"}, "Conflicting type"),
        ("cat[Record]", {"name": "a,'\""}, "both quote characters"),
    ],
)
def test_resolved_dtype_str_rejects_what_lamindb_rejects(
    dtype_str: str, cat_filters: dict[str, object], match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        resolved_dtype_str(dtype_str, cat_filters)


def test_resolved_dtype_str_matches_lamindbs_own_serialization(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # The scalar branch reimplements what `ln.Feature(cat_filters=...)` does,
    # because building a Feature for an *existing* one is refused for a new
    # dtype.  Pin the two together so a lamindb upgrade cannot split them.
    record_type = ln.Record(name="ParityType", is_type=True).save()
    schema_uid = "sPARITY00000000"
    cases = [
        ("cat[Record]", ln.Record, {"type__uid": record_type.uid}),
        ("cat[Record]", ln.Record, {"type": record_type}),
        ("cat[Record]", ln.Record, {"type__uid": record_type.uid, "is_type": True}),
        (
            "cat[Record]",
            ln.Record,
            {"type__uid": record_type.uid, "schema__uid": schema_uid},
        ),
        ("cat[ULabel]", ln.ULabel, {"type__uid": "uPARITY000000000"}),
        ("cat[Artifact]", ln.Artifact, {"schema__uid": schema_uid}),
        ("cat[Record]", ln.Record, {"type__uid": record_type.uid, "name": "a,b"}),
    ]
    for index, (bare, registry, cat_filters) in enumerate(cases):
        expected = ln.Feature(
            name=f"parity_{index}", dtype=registry, cat_filters=dict(cat_filters)
        )._dtype_str
        assert resolved_dtype_str(bare, cat_filters) == expected, cat_filters


@pytest.mark.parametrize(
    "value,expected",
    [
        ("int", {}),
        (None, {}),
        ("cat[ULabel|Record]", {"ULabel": frozenset(), "Record": frozenset()}),
        ("cat[Record[uT0]]", {"Record": frozenset({"type__uid=uT0"})}),
        ("list[cat[Record[uT0]]]", {"Record": frozenset({"type__uid=uT0"})}),
        (
            "cat[Record[uT0, is_type=True, name='a,b']]",
            {"Record": frozenset({"type__uid=uT0", "is_type=True", "name=a,b"})},
        ),
        ("cat[Record[uT0].name]", {"Record": frozenset({"type__uid=uT0"})}),
        (
            'cat[bionty.Gene.ensembl_gene_id[source__uid="s1"]]',
            {"Gene.ensembl_gene_id": frozenset({"source__uid=s1"})},
        ),
        ("bionty.models.Gene", {"Gene": frozenset()}),
    ],
)
def test_dtype_filter_terms(value: str | None, expected: dict) -> None:
    assert dtype_filter_terms(value) == expected


def test_dtypes_match_compares_filters_but_not_their_spelling() -> None:
    assert dtypes_match(
        "cat[Record[uA, is_type=True]]", "cat[Record[is_type='True', uA]]"
    )
    assert dtypes_match("cat[bionty.Gene.symbol]", "cat[bionty.Gene]")
    assert dtypes_match("builtins.int", "int")
    assert not dtypes_match("cat[Record[uA]]", "cat[Record[uB]]")
    assert not dtypes_match("cat[Record[uA]]", "cat[Record]")
    assert not dtypes_match("list[cat[Record[uA]]]", "cat[Record[uA]]")


def test_resolved_dtype_str_round_trips_through_dtypes_match(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    # The engine diffs a plan's full dtype against the instance's with
    # `dtypes_match`, so what lamindb stores for a set of filters must match
    # what `resolved_dtype_str` spells for them, whatever the order.
    record_type = ln.Record(name="MatchType", is_type=True).save()
    filters = {"is_type": True, "type__uid": record_type.uid}
    stored = ln.Feature(name="match_f", dtype=ln.Record, cat_filters=filters)._dtype_str
    reordered = {"type__uid": record_type.uid, "is_type": True}
    assert dtypes_match(resolved_dtype_str("cat[Record]", reordered), stored)


def test_get_dtype_list_of_categorical(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    import lamindb.models

    resolved = get_dtype(
        ListDType[CatDType[lamindb.models.Record, RecordCat(type__uid="uTEST00000000")]]
    )
    assert resolved.dtype == "list[cat[Record]]"
    assert resolved.cat_filters == {"type__uid": "uTEST00000000"}


def test_no_live_class_is_stored(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    """A categorical dtype keeps only an FQN spec, never the live class object."""
    dt = LaminDType(ln.Artifact)
    assert vars(dt).keys() == {"_spec", "cat_filters"}
    assert dt._spec == ("class", ("lamindb.models.artifact", "Artifact"))
    # `dtype` is a property that re-resolves the class on access.
    assert dt.dtype is ln.Artifact
    assert dt.dtype_str == "cat[Artifact]"


def test_dtype_spec_resolves_current_class_not_stale(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    """Building from a *stale* class still resolves to the connected instance's.

    Simulates the class object left over from a previous connection (same FQN,
    different object) that used to crash lamindb's `isinstance(dtype, Registry)`
    metaclass check.  The spec names the class, so resolution yields the live one.
    """
    stale = type("Artifact", (), {"__module__": "lamindb.models.artifact"})
    assert stale is not ln.Artifact

    dt = LaminDType(stale)
    assert dt._spec == ("class", ("lamindb.models.artifact", "Artifact"))
    assert dt.dtype is ln.Artifact
    assert dt.dtype is not stale
    assert dt.dtype_str == "cat[Artifact]"


def test_field_spec_captures_model_fqn_and_field(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    """A field-attr dtype is captured as (module, qualname, field_name)."""
    dt = LaminDType(ln.ULabel.name)
    assert dt._spec == ("field", ("lamindb.models.ulabel", "ULabel", "name"))
    assert dt.dtype is ln.ULabel.name


def test_union_spec_captures_each_member(
    instance: TestInstanceInfo, ln: LamindbModuleType
) -> None:
    dt = LaminDType([ln.ULabel, ln.Record])
    assert dt._spec == (
        "union",
        (
            ("class", ("lamindb.models.ulabel", "ULabel")),
            ("class", ("lamindb.models.record", "Record")),
        ),
    )
    assert dt.dtype == [ln.ULabel, ln.Record]


def test_annotation_markers_and_python_type_normalization() -> None:
    from pathlib import Path
    from typing import Annotated

    from lamindb_setup.evolve import DType, ListDType, get_dtype

    for marker in (DType, ListDType):
        with pytest.raises(TypeError, match="annotation-only"):
            marker()
    assert get_dtype(Path).dtype == "path"
    assert get_dtype(dict[str, str]).dtype == get_dtype(dict).dtype
    with pytest.raises(ValueError, match="Unsupported simple dtype"):
        get_dtype(complex)
    with pytest.raises(ValueError, match="metadata argument"):
        get_dtype(Annotated[str, "unit"])
    with pytest.raises(ValueError, match="not applicable"):
        LaminDType(int, is_type=True)


def test_custom_registry_filters_preserve_unknown_registry_and_nested_values() -> None:
    from lamindb_setup.evolve.authoring._dtypes import (
        DangerousCat,
        UnionCat,
        canonical_dtype_str,
        dtype_filter_terms,
    )

    filters = DangerousCat(name_field="symbol", source__uid="uid1", absent=None)
    assert filters.name_field == "symbol" and filters.as_cat_filters() == {
        "source__uid": "uid1"
    }
    assert UnionCat().as_cat_filters() == {}
    unknown = "cat[plugin.Measurement[labels=[x,y]]]"
    assert canonical_dtype_str(unknown) == unknown
    assert canonical_dtype_str(None) is None
    assert canonical_dtype_str("") == ""
    assert dtype_filter_terms("cat[Record[labels=[x,y],is_type=True]]") == {
        "Record": frozenset({"labels=[x,y]", "is_type=True"})
    }


def test_invalid_dtype_spec_and_single_member_union_are_rejected() -> None:
    from lamindb_setup.evolve.authoring import _dtypes

    with pytest.raises(ValueError, match="unknown dtype spec"):
        _dtypes._resolve_dtype_spec(("invalid", int))
    with pytest.raises(ValueError, match="at least two"):
        _dtypes._resolve_union_cat(int | None, (_dtypes.UnionCat(),))
