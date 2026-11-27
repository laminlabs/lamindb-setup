"""Tests for the schema-drift taxonomy in lamindb_setup.evolve.engine._drift."""

from __future__ import annotations

import pytest
from lamindb_setup.evolve.authoring._dtypes import canonical_dtype_str
from lamindb_setup.evolve.engine._drift import Verdict, caveat, classify


def test_no_change_is_safe() -> None:
    assert classify("Feature", "nullable", True, True) is Verdict.SAFE


# --- boolean widening (False -> True only) ---------------------------------
def test_widen_false_to_true_is_safe() -> None:
    assert classify("Feature", "nullable", False, True) is Verdict.SAFE
    # `None` (unset) is treated as False.
    assert classify("Feature", "nullable", None, True) is Verdict.SAFE


def test_narrow_true_to_false_is_blocked() -> None:
    assert classify("Feature", "nullable", True, False) is Verdict.BLOCKED
    assert classify("Feature", "nullable", True, None) is Verdict.BLOCKED


# --- Schema.flexible: judged against maximal_set ---------------------------
_LENIENT = {"maximal_set": False}
_STRICT = {"maximal_set": True}


@pytest.mark.parametrize("context", [_LENIENT, _STRICT, None], ids=str)
def test_turning_flexible_on_is_safe_whatever_maximal_set_says(
    context: dict[str, bool] | None,
) -> None:
    assert classify("Schema", "flexible", False, True, context=context) is (
        Verdict.SAFE
    )
    assert classify("Schema", "flexible", None, True, context=context) is (Verdict.SAFE)


def test_turning_flexible_off_is_safe_when_extra_columns_are_tolerated() -> None:
    # With maximal_set=False, lamindb never fails a column nothing validates, so
    # dropping the flexible match only skips checks a dataset already passed.
    assert classify("Schema", "flexible", True, False, context=_LENIENT) is (
        Verdict.SAFE
    )
    # lamindb reads an unset flag as off.
    assert classify("Schema", "flexible", True, None, context=_LENIENT) is (
        Verdict.SAFE
    )


def test_turning_flexible_off_is_blocked_when_extra_columns_are_forbidden() -> None:
    # With maximal_set=True the admitted columns shrink to the members.
    assert classify("Schema", "flexible", True, False, context=_STRICT) is (
        Verdict.BLOCKED
    )


@pytest.mark.parametrize("context", [None, {}, {"maximal_set": None}], ids=str)
def test_turning_flexible_off_without_maximal_set_stays_blocked(
    context: dict[str, None] | None,
) -> None:
    assert classify("Schema", "flexible", True, False, context=context) is (
        Verdict.BLOCKED
    )


def test_unset_and_off_are_the_same_flexible() -> None:
    assert classify("Schema", "flexible", None, False, context=_STRICT) is (
        Verdict.SAFE
    )


def test_turning_flexible_on_under_lenient_maximal_set_carries_a_caveat() -> None:
    warning = caveat("Schema", "flexible", False, True, context=_LENIENT)
    assert warning is not None
    assert "matched by name alone" in warning
    assert "already validated" in warning
    assert caveat("Schema", "flexible", None, True, context=_LENIENT) == warning


@pytest.mark.parametrize(
    ("old", "new", "context"),
    [
        (False, True, _STRICT),  # every column that passed was a member
        (False, True, None),  # unknown: no claim either way
        (True, False, _LENIENT),  # turning it off never adds checks
        (True, True, _LENIENT),
    ],
)
def test_no_caveat_otherwise(
    old: bool, new: bool, context: dict[str, bool] | None
) -> None:
    assert caveat("Schema", "flexible", old, new, context=context) is None


def test_caveat_is_specific_to_flexible() -> None:
    assert caveat("Feature", "nullable", False, True, context=_LENIENT) is None


# --- future-only fields (safe either direction) ----------------------------
@pytest.mark.parametrize(
    "kind,field",
    [("Feature", "coerce"), ("Feature", "default_value"), ("Schema", "coerce")],
)
def test_future_only_fields_are_safe_both_ways(kind: str, field: str) -> None:
    assert classify(kind, field, False, True) is Verdict.SAFE
    assert classify(kind, field, True, False) is Verdict.SAFE


# --- set widening (superset only) ------------------------------------------
def test_growing_the_optional_set_is_safe() -> None:
    # Marking one more schema member optional only admits more records.
    assert classify("Schema", "optionals", ["a"], ["a", "b"]) is Verdict.SAFE
    # `None` (unset / key absent) is treated as the empty set.
    assert classify("Schema", "optionals", None, ["a"]) is Verdict.SAFE


def test_shrinking_the_optional_set_is_blocked() -> None:
    # Making an optional member required again invalidates every record that
    # omitted it.
    assert classify("Schema", "optionals", ["a", "b"], ["a"]) is Verdict.BLOCKED
    assert classify("Schema", "optionals", ["a"], None) is Verdict.BLOCKED
    # Swapping one member for another shrinks the set as well as growing it.
    assert classify("Schema", "optionals", ["a"], ["b"]) is Verdict.BLOCKED


def test_optional_set_order_is_irrelevant() -> None:
    assert classify("Schema", "optionals", ["a", "b"], ["b", "a"]) is Verdict.SAFE


def test_default_value_change_is_safe() -> None:
    assert classify("Feature", "default_value", 1, 2) is Verdict.SAFE


# --- scalar dtype widening --------------------------------------------------
@pytest.mark.parametrize(
    "old,new",
    [
        ("int", "float"),
        ("int", "num"),
        ("float", "num"),
        ("int", "str"),
        ("float", "str"),
        ("bool", "str"),
        ("num", "str"),
    ],
)
def test_scalar_widening_is_safe(old: str, new: str) -> None:
    assert classify("Feature", "dtype", old, new) is Verdict.SAFE


@pytest.mark.parametrize(
    "old,new",
    [
        ("float", "int"),
        ("num", "int"),
        ("num", "float"),
        ("str", "int"),
        ("str", "float"),
        ("float", "bool"),
    ],
)
def test_scalar_narrowing_is_blocked(old: str, new: str) -> None:
    assert classify("Feature", "dtype", old, new) is Verdict.BLOCKED


def test_serialize_config_dtype_format_is_normalized() -> None:
    # `serialize_config` renders builtins by import path and string dtypes as a
    # quoted repr; both must classify like their canonical tokens.
    assert (
        classify("Feature", "dtype", "builtins.int", "builtins.float") is Verdict.SAFE
    )
    assert (
        classify("Feature", "dtype", "builtins.float", "builtins.int")
        is Verdict.BLOCKED
    )
    assert classify("Feature", "dtype", "builtins.int", "'num'") is Verdict.SAFE


# --- categorical dtype (union of registries) -------------------------------
def test_cat_add_registry_is_safe() -> None:
    assert (
        classify("Feature", "dtype", "cat[ULabel]", "cat[ULabel|Record]")
        is Verdict.SAFE
    )


def test_cat_drop_registry_is_blocked() -> None:
    assert (
        classify("Feature", "dtype", "cat[ULabel|Record]", "cat[ULabel]")
        is Verdict.BLOCKED
    )


def test_cat_replace_registry_is_blocked() -> None:
    assert classify("Feature", "dtype", "cat[ULabel]", "cat[Record]") is Verdict.BLOCKED


def test_cat_component_with_nested_brackets_splits_on_top_level_pipe() -> None:
    old = "cat[Record[rtUID0001, is_type='True']]"
    new = "cat[Record[rtUID0001, is_type='True']|ULabel]"
    assert classify("Feature", "dtype", old, new) is Verdict.SAFE


# --- categorical name-field / registry-path equivalence --------------------
def test_cat_bare_and_default_name_field_are_equivalent() -> None:
    # A bare cat[bionty.Gene] already matches on Gene's default name field
    # (symbol), so it is the same match as cat[bionty.Gene.symbol].
    assert (
        classify("Feature", "dtype", "cat[bionty.Gene]", "cat[bionty.Gene.symbol]")
        is Verdict.SAFE
    )
    assert (
        classify("Feature", "dtype", "cat[bionty.Gene.symbol]", "cat[bionty.Gene]")
        is Verdict.SAFE
    )


def test_cat_non_default_name_field_is_distinct() -> None:
    # A non-default field selects a different match, so it is a real change.
    assert (
        classify(
            "Feature",
            "dtype",
            "cat[bionty.Gene.ensembl_gene_id]",
            "cat[bionty.Gene]",
        )
        is Verdict.BLOCKED
    )


def test_legacy_registry_class_path_is_a_categorical() -> None:
    # An older lock stored a single categorical as the registry class path; it
    # must compare equal to the current cat[...] form, not as a scalar->cat
    # shape change.
    assert (
        classify("Feature", "dtype", "bionty.models.Gene", "cat[bionty.Gene.symbol]")
        is Verdict.SAFE
    )
    assert (
        classify("Feature", "dtype", "lamindb.models.ulabel.ULabel", "cat[ULabel]")
        is Verdict.SAFE
    )


@pytest.mark.parametrize(
    "value,expected",
    [
        ("cat[bionty.Gene]", "cat[Gene]"),
        ("cat[bionty.Gene.symbol]", "cat[Gene]"),
        ("bionty.models.Gene", "cat[Gene]"),
        ("cat[bionty.Gene.ensembl_gene_id]", "cat[Gene.ensembl_gene_id]"),
        ("cat[ULabel|bionty.Gene.symbol]", "cat[Gene|ULabel]"),
        ("cat[Record[oBmufFIl7Yumkac2]]", "cat[Record]"),
        (
            "cat[bionty.ExperimentalFactor[source__uid='2v5MAvnN']]",
            "cat[ExperimentalFactor]",
        ),
        ("'cat[bionty.Gene.symbol]'", "cat[Gene]"),
        ("builtins.int", "int"),
        ("'num'", "num"),
        ("list[builtins.int]", "list[int]"),
        ("int", "int"),
    ],
)
def test_canonical_dtype_str(value: str, expected: str) -> None:
    assert canonical_dtype_str(value) == expected


# --- Schema.dtype ----------------------------------------------------
def test_a_schema_dtype_change_is_blocked_whatever_the_feature_rules_say() -> None:
    # `int -> float` widens a *feature*; nothing changes a schema's dtype.
    assert classify("Schema", "dtype", "builtins.int", "builtins.float") is (
        Verdict.BLOCKED
    )
    assert classify("Schema", "dtype", None, "builtins.int") is Verdict.BLOCKED
    assert classify("Feature", "dtype", "builtins.int", "builtins.float") is (
        Verdict.SAFE
    )


def test_a_respelled_schema_dtype_is_no_change() -> None:
    assert classify("Schema", "dtype", "builtins.int", "int") is Verdict.SAFE


# --- categorical filters ---------------------------------------------
# A filter only narrows the labels a value may be: dropping one is a widening,
# adding or changing one may strand a value that satisfied the old filter.
@pytest.mark.parametrize(
    "old,new",
    [
        ("cat[Record[A]]", "cat[Record]"),
        ("cat[Record[A, is_type=True]]", "cat[Record[A]]"),
        ("cat[bionty.CellType[source__uid=s1]]", "cat[bionty.CellType]"),
        ("list[cat[Record[A]]]", "list[cat[Record]]"),
        # Spelling only: quotes and term order are not part of the filter.
        ("cat[Record[A, is_type='True']]", "cat[Record[is_type=True, A]]"),
        ("cat[bionty.CellType[source__uid='s1']]", "cat[CellType[source__uid=s1]]"),
    ],
)
def test_dropping_a_cat_filter_is_safe(old: str, new: str) -> None:
    assert classify("Feature", "dtype", old, new) is Verdict.SAFE


@pytest.mark.parametrize(
    "old,new",
    [
        ("cat[Record[A]]", "cat[Record[B]]"),
        ("cat[Record]", "cat[Record[A]]"),
        ("cat[Record[A]]", "cat[Record[A, is_type=True]]"),
        (
            "cat[bionty.CellType[source__uid=s1]]",
            "cat[bionty.CellType[source__uid=s2]]",
        ),
        ("list[cat[Record[A]]]", "list[cat[Record[B]]]"),
        # A legacy bare registry path carries no filter, so adding one narrows it.
        ("bionty.models.CellType", "cat[bionty.CellType[source__uid=s1]]"),
    ],
)
def test_adding_or_changing_a_cat_filter_is_blocked(old: str, new: str) -> None:
    assert classify("Feature", "dtype", old, new) is Verdict.BLOCKED


def test_a_filter_on_a_registry_added_to_a_union_is_new_territory() -> None:
    # No stored value can have been a label of a registry the change adds.
    assert classify("Feature", "dtype", "cat[ULabel]", "cat[ULabel|Record[A]]") is (
        Verdict.SAFE
    )


def test_plan_cat_filters_follow_the_same_rule() -> None:
    a = {"type__uid": {"ref": "RecordType:A"}}
    b = {"type__uid": {"ref": "RecordType:B"}}
    a_types = {**a, "is_type": True}
    assert classify("Feature", "cat_filters", a, None) is Verdict.SAFE
    assert classify("Feature", "cat_filters", a_types, a) is Verdict.SAFE
    assert classify("Feature", "cat_filters", a, b) is Verdict.BLOCKED
    assert classify("Feature", "cat_filters", None, a) is Verdict.BLOCKED
    assert classify("Feature", "cat_filters", a, a_types) is Verdict.BLOCKED
    assert classify("Feature", "cat_filters", "garbage", None) is Verdict.BLOCKED


# --- categorical <-> scalar cross-shape changes ----------------------------
# `cat[...] -> str` is safe (every label renders as its name string); the
# reverse (any `<scalar> -> cat[...]`) is blocked and only lifted by the
# data-aware override once every stored value is a valid label.
@pytest.mark.parametrize(
    "old",
    [
        "cat[ULabel]",
        "cat[ULabel|Record]",
        "cat[Record[rtUID0001, is_type='True']]",
    ],
)
def test_cat_to_str_is_safe(old: str) -> None:
    assert classify("Feature", "dtype", old, "str") is Verdict.SAFE


@pytest.mark.parametrize("new", ["int", "float", "num", "path", "url"])
def test_cat_to_non_str_scalar_is_blocked(new: str) -> None:
    assert classify("Feature", "dtype", "cat[ULabel]", new) is Verdict.BLOCKED


@pytest.mark.parametrize("old", ["str", "int", "float"])
def test_scalar_to_cat_is_blocked(old: str) -> None:
    assert classify("Feature", "dtype", old, "cat[ULabel]") is Verdict.BLOCKED


def test_list_of_cat_to_str_is_blocked() -> None:
    # A list is not collapsed to a scalar even when its element is categorical.
    assert classify("Feature", "dtype", "list[cat[ULabel]]", "str") is Verdict.BLOCKED


# --- structural / cross-shape dtype changes --------------------------------
@pytest.mark.parametrize(
    "old,new",
    [
        ("int", "cat[ULabel]"),
        ("cat[ULabel]", "int"),
    ],
)
def test_dtype_shape_change_is_blocked(old: str, new: str) -> None:
    assert classify("Feature", "dtype", old, new) is Verdict.BLOCKED


# --- transitions from and to list types ------------------------------------
# Any change of list-ness (scalar/cat <-> list) is blocked in either direction:
# a list may hold several values (so it cannot collapse to a scalar) and an
# existing scalar value is not itself a list.
@pytest.mark.parametrize(
    "old,new",
    [
        # listify: scalar/cat -> list
        ("int", "list[int]"),
        ("str", "list[str]"),
        ("cat[ULabel]", "list[cat[ULabel]]"),
        # listify bundled with an element change (still blocked)
        ("int", "list[float]"),
        ("float", "list[int]"),
        # delist: list -> scalar/cat
        ("list[int]", "int"),
        ("list[str]", "str"),
        ("list[cat[ULabel]]", "cat[ULabel]"),
    ],
)
def test_list_ness_change_is_blocked(old: str, new: str) -> None:
    assert classify("Feature", "dtype", old, new) is Verdict.BLOCKED


def test_list_identity_is_safe() -> None:
    assert classify("Feature", "dtype", "list[int]", "list[int]") is Verdict.SAFE


@pytest.mark.parametrize(
    "old,new",
    [
        ("list[int]", "list[float]"),
        ("list[int]", "list[num]"),
        ("list[int]", "list[str]"),
        ("list[float]", "list[str]"),
    ],
)
def test_list_element_widening_is_safe(old: str, new: str) -> None:
    assert classify("Feature", "dtype", old, new) is Verdict.SAFE


@pytest.mark.parametrize(
    "old,new",
    [
        ("list[float]", "list[int]"),
        ("list[num]", "list[int]"),
        ("list[str]", "list[int]"),
    ],
)
def test_list_element_narrowing_is_blocked(old: str, new: str) -> None:
    assert classify("Feature", "dtype", old, new) is Verdict.BLOCKED


def test_list_of_cat_widening_is_safe() -> None:
    assert (
        classify("Feature", "dtype", "list[cat[ULabel]]", "list[cat[ULabel|Record]]")
        is Verdict.SAFE
    )


def test_list_of_cat_narrowing_is_blocked() -> None:
    assert (
        classify("Feature", "dtype", "list[cat[ULabel|Record]]", "list[cat[ULabel]]")
        is Verdict.BLOCKED
    )


def test_unparseable_dtype_is_blocked() -> None:
    assert classify("Feature", "dtype", None, "int") is Verdict.BLOCKED
    assert classify("Feature", "dtype", 123, 456) is Verdict.BLOCKED


# --- schema set flags -------------------------------------------------
@pytest.mark.parametrize("field", ["maximal_set", "minimal_set", "ordered_set"])
def test_dropping_a_set_flag_is_safe(field: str) -> None:
    # Each flag constrains a dataset more when True, so dropping it only admits more.
    assert classify("Schema", field, True, False) is Verdict.SAFE


@pytest.mark.parametrize("field", ["maximal_set", "minimal_set", "ordered_set"])
def test_raising_a_set_flag_is_blocked(field: str) -> None:
    assert classify("Schema", field, False, True) is Verdict.BLOCKED


def test_turning_maximal_set_and_flexible_off_together_is_safe() -> None:
    # `flexible` is judged against `maximal_set` as the plan leaves it.
    assert classify("Schema", "maximal_set", True, False) is Verdict.SAFE
    assert classify("Schema", "flexible", True, False, context=_LENIENT) is (
        Verdict.SAFE
    )


# --- everything else defaults to blocked -----------------------------------
@pytest.mark.parametrize(
    "kind,field,old,new",
    [
        ("Schema", "otype", "DataFrame", "AnnData"),
        ("Schema", "minimal_set", False, True),
        ("Feature", "unit", "kg", "g"),
    ],
)
def test_unknown_or_structural_fields_are_blocked(
    kind: str, field: str, old: object, new: object
) -> None:
    assert classify(kind, field, old, new) is Verdict.BLOCKED


@pytest.mark.parametrize(
    "old,new,verdict",
    [
        ("schemaUID00000001", None, Verdict.SAFE),  # a sheet demoted
        (None, "schemaUID00000001", Verdict.BLOCKED),  # a schema attached
        ("schemaUID00000001", "schemaUID00000002", Verdict.BLOCKED),
    ],
)
def test_a_record_type_only_loses_a_schema_safely(
    old: str | None, new: str | None, verdict: Verdict
) -> None:
    assert classify("RecordType", "schema", old, new) is verdict


def test_a_sheet_never_loses_its_schema_safely() -> None:
    assert classify("Sheet", "schema", "schemaUID00000001", None) is Verdict.BLOCKED


def test_scalar_set_values_and_change_ownership() -> None:
    from lamindb_setup.evolve.authoring._declarations import describe_target
    from lamindb_setup.evolve.core import Action, ChangeItem
    from lamindb_setup.evolve.engine._drift import _as_set

    assert _as_set("kg") == {"kg"}
    assert describe_target("Feature:weight") == "'Feature:weight'"
    for action, borrowed, own in [
        (Action.CREATE, False, True),
        (Action.CREATE, True, False),
        (Action.NO_CHANGE, False, False),
    ]:
        change = ChangeItem(
            1, "Feature:x", "Feature", "x", action, "uid", borrowed=borrowed
        )
        assert change.is_own_change is own
