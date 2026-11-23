# Adapted source tests include deliberately invalid declarations and dynamic models.
# mypy: disable-error-code="arg-type,var-annotated"
"""Offline checks of a plan's `ctx.migrate` and `ctx.moved` declarations.

`validate` used to accept a declaration that could never do what it says: a
schema backfill attached to a feature, a dtype helper on a schema, a string that
is no natural key, a removal of an object the plan still declares.  Finalizing
the plan now refuses each of them, without lamindb.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from lamindb_setup.evolve import (
    DataMigration,
    DeclarationTarget,
    Feature,
    FeatureType,
    LaminDType,
    LaminSchemasError,
    Plan,
    PlanContext,
    PlanDeclarationError,
    PlanExports,
    Record,
    RecordType,
    Schema,
    SchemaType,
    Sheet,
    accept_added_member,
    accept_flexible_change,
    accept_index_change,
    accept_maximal_set,
    accept_minimal_set,
    accept_non_null,
    accept_ordered_set,
    accept_slot_change,
    accept_unit_change,
    create_plan,
    detach_from_schemas,
    drop_member_values,
    drop_nonmember_annotations,
    drop_record,
    drop_rows,
    drop_values,
    fill_missing,
    fill_nulls,
    keep_member_values,
    keep_stale_exports,
    keep_validated,
    keep_values,
    map_to_records,
    map_to_str,
    map_values,
    merge_schema,
    reexport_sheets,
    remap_records,
    rescale,
    revalidate_rows,
    unwrap_list,
    update_record,
    wrap_in_list,
)
from lamindb_setup.evolve.authoring import (
    ROWS_REMOVAL,
    named_key,
    parse_natural_key,
)

Declare = Callable[[PlanContext, dict[str, Any]], None]


def _noop(context: Any) -> None:
    """A hand-written migration's callable; never run offline."""


def _objects() -> dict[str, Any]:
    """One declared object of every kind a declaration can be attached to."""
    ft = FeatureType("m_ft")
    a = Feature("m_a", dtype=LaminDType(int), type=ft)
    schema = Schema(name="MSchema", features=[a], type=SchemaType("m_st"))
    sheet_schema = Schema(name="MSheetSchema", features=[a])
    vocab = RecordType("MVocab")
    return {
        "Feature": a,
        "FeatureType": ft,
        "Schema": schema,
        "Sheet": Sheet("MSheet", schema=sheet_schema),
        "RecordType": vocab,
        "Record": Record("m_r", type=vocab),
    }


def _plan(declare: Declare) -> Plan[Any]:
    @create_plan
    def m(ctx: PlanContext, exports: None) -> None:
        declare(ctx, _objects())

    return m


def _finalize(declare: Declare) -> None:
    _plan(declare).finalize()


# --- the issue's examples -----------------------------------------------------


@pytest.mark.parametrize(
    ("declare", "message"),
    [
        pytest.param(
            lambda ctx, o: ctx.migrate(o["Feature"], fill_missing("x")),
            "plan 'm': fill_missing() answers a Schema (a member it makes "
            "required), not Feature 'm_a'.",
            id="schema-helper-on-a-feature",
        ),
        pytest.param(
            lambda ctx, o: ctx.migrate(o["Schema"], map_values()),
            "plan 'm': map_values() answers a Feature (a change to its dtype), "
            "not Schema 'MSchema'.",
            id="dtype-helper-on-a-schema",
        ),
        pytest.param(
            lambda ctx, o: ctx.migrate("this is not a key", drop_values()),
            "plan 'm': ctx.migrate('this is not a key', drop_values()) names no "
            "object: 'this is not a key' is not a natural key (it does not start "
            "with 'Kind:name'",
            id="malformed-key",
        ),
        pytest.param(
            lambda ctx, o: ctx.migrate("FeatureType:m_ft/Feature:m_a", drop_values()),
            "plan 'm': drop_values() is declared for 'FeatureType:m_ft/Feature:m_a',"
            " which the plan still declares.",
            id="drop-of-a-declared-feature",
        ),
    ],
)
def test_the_issue_examples_fail_to_finalize(declare: Declare, message: str) -> None:
    with pytest.raises(PlanDeclarationError) as excinfo:
        _finalize(declare)

    assert str(excinfo.value).startswith(message)


def test_the_error_reads_as_a_one_line_cli_error() -> None:
    # A LaminSchemasError is what the CLI renders as `Error: <message>`; a
    # ValueError is what the unnamed-record check raised before it.
    assert issubclass(PlanDeclarationError, LaminSchemasError)
    assert issubclass(PlanDeclarationError, ValueError)


def test_an_unnamed_record_is_a_declaration_error() -> None:
    with pytest.raises(PlanDeclarationError, match="declares an unnamed Record"):
        _finalize(lambda ctx, o: Record(type=o["RecordType"]))


# --- target kinds, helper by helper -------------------------------------------

#: Every target a declaration can be attached to.  The keys name objects the
#: plan does not declare, as a removal must.
_TARGETS: dict[str, Callable[[dict[str, Any]], Any]] = {
    "Feature": lambda o: o["Feature"],
    "FeatureType": lambda o: o["FeatureType"],
    "Schema": lambda o: o["Schema"],
    "Sheet": lambda o: o["Sheet"],
    "RecordType": lambda o: o["RecordType"],
    "Record": lambda o: o["Record"],
    "key:Feature": lambda o: "FeatureType:m_ft/Feature:gone",
    "key:FeatureType": lambda o: "FeatureType:gone_ft",
    "key:Schema": lambda o: "SchemaType:m_st/Schema:Gone",
    "key:RecordType": lambda o: "RecordType:GoneType",
    "key:Sheet": lambda o: "Sheet:GoneSheet",
    "key:Record": lambda o: "RecordType:MVocab/Record:gone",
}

_FEATURE = ({"Feature"}, set())
_SCHEMA = ({"Schema"}, set())
_EXPORTS = ({"Schema", "Sheet"}, set())

#: What the docstring of `PlanContext.migrate` says each helper answers for:
#: `(objects, removals)`, the kinds of plan object it attaches to and the
#: leaf kinds of the natural keys it answers a removal for.
_HELPERS: dict[str, tuple[Callable[[], DataMigration], tuple[set[str], set[str]]]] = {
    "map_to_records": (map_to_records, _FEATURE),
    "remap_records": (remap_records, _FEATURE),
    "map_to_str": (map_to_str, _FEATURE),
    "map_values": (map_values, _FEATURE),
    "wrap_in_list": (wrap_in_list, _FEATURE),
    "unwrap_list": (unwrap_list, _FEATURE),
    "drop_values": (drop_values, ({"Feature"}, {"Feature"})),
    "keep_values": (keep_values, (set(), {"Feature"})),
    "fill_nulls": (fill_nulls, _FEATURE),
    "accept_non_null": (accept_non_null, _FEATURE),
    "rescale": (lambda: rescale(0.001), _FEATURE),
    "accept_unit_change": (accept_unit_change, _FEATURE),
    "fill_missing": (fill_missing, _SCHEMA),
    "accept_added_member": (accept_added_member, _SCHEMA),
    "drop_member_values": (drop_member_values, _SCHEMA),
    "keep_member_values": (keep_member_values, _SCHEMA),
    "accept_index_change": (accept_index_change, _SCHEMA),
    "drop_nonmember_annotations": (drop_nonmember_annotations, _SCHEMA),
    "accept_flexible_change": (accept_flexible_change, _SCHEMA),
    "accept_maximal_set": (accept_maximal_set, _SCHEMA),
    "accept_minimal_set": (accept_minimal_set, _SCHEMA),
    "accept_ordered_set": (accept_ordered_set, _SCHEMA),
    "accept_slot_change": (accept_slot_change, _SCHEMA),
    "merge_schema": (lambda: merge_schema("Schema:Other"), _SCHEMA),
    "reexport_sheets": (reexport_sheets, _EXPORTS),
    "keep_stale_exports": (keep_stale_exports, _EXPORTS),
    "revalidate_rows": (revalidate_rows, ({"Sheet"}, set())),
    "update_record": (update_record, ({"Record"}, set())),
    "drop_rows": (drop_rows, (set(), {"RecordType", "Sheet"})),
    "keep_validated": (keep_validated, (set(), {"Schema"})),
    "detach_from_schemas": (
        lambda: detach_from_schemas(["Schema:Foreign"]),
        (set(), {"Feature"}),
    ),
    "drop_record": (drop_record, (set(), {"Record"})),
}


def _allowed(objects: set[str], removals: set[str]) -> set[str]:
    """The targets of `_TARGETS` a helper validates on.

    A natural key stands in for an object too -- the engine looks declarations
    up by key, which is how a schema no plan manages is answered for.
    """
    return objects | {f"key:{kind}" for kind in objects | removals}


@pytest.mark.parametrize("target", list(_TARGETS))
@pytest.mark.parametrize("helper", list(_HELPERS))
def test_a_helper_validates_exactly_on_the_targets_it_answers(
    helper: str, target: str
) -> None:
    factory, kinds = _HELPERS[helper]

    def declare(ctx: PlanContext, objects: dict[str, Any]) -> None:
        ctx.migrate(_TARGETS[target](objects), factory())

    if target in _allowed(*kinds):
        _finalize(declare)
        return
    with pytest.raises(PlanDeclarationError, match=r" answers .*, not "):
        _finalize(declare)


@pytest.mark.parametrize("target", list(_TARGETS))
def test_a_hand_written_migration_naming_no_narrowing_is_unchecked(
    target: str,
) -> None:
    migration = DataMigration(name="noop", migrate=_noop)

    _finalize(lambda ctx, o: ctx.migrate(_TARGETS[target](o), migration))


def test_a_key_may_address_a_schema_no_plan_manages() -> None:
    # As a feature's detachment from a hand-made schema is answered for.
    _finalize(lambda ctx, o: ctx.migrate("Schema:HandMade", keep_stale_exports()))


def test_an_object_given_to_a_removal_says_to_name_its_key() -> None:
    with pytest.raises(PlanDeclarationError, match="declared by that object's natural"):
        _finalize(lambda ctx, o: ctx.migrate(o["Feature"], keep_values()))


def test_a_helper_serving_two_targets_names_both() -> None:
    with pytest.raises(PlanDeclarationError) as excinfo:
        _finalize(lambda ctx, o: ctx.migrate("Schema:Gone", drop_values()))

    assert str(excinfo.value) == (
        "plan 'm': drop_values() answers a Feature (a change to its dtype) or the "
        "natural key of a Feature (the values of one the plan deletes), not the "
        "natural key 'Schema:Gone'."
    )


def test_a_hand_written_migration_naming_a_narrowing_is_checked() -> None:
    migration = DataMigration(name="noop", migrate=_noop, narrowing=ROWS_REMOVAL)

    with pytest.raises(PlanDeclarationError, match=r"noop\(\) answers the natural"):
        _finalize(lambda ctx, o: ctx.migrate(o["Sheet"], migration))


def test_a_hand_written_migration_may_declare_its_targets() -> None:
    migration = DataMigration(
        name="custom step",
        migrate=_noop,
        targets=(DeclarationTarget("Schema", "a custom change"),),
    )

    _finalize(lambda ctx, o: ctx.migrate(o["Schema"], migration))
    with pytest.raises(PlanDeclarationError, match="^plan 'm': custom step answers"):
        _finalize(lambda ctx, o: ctx.migrate(o["Feature"], migration))


def test_a_target_must_be_a_plan_object_or_a_key() -> None:
    with pytest.raises(PlanDeclarationError, match="is declared for a int; it takes"):
        _finalize(lambda ctx, o: ctx.migrate(42, map_values()))  # type: ignore[arg-type]


def test_a_hand_written_migration_still_needs_a_natural_key() -> None:
    migration = DataMigration(name="noop", migrate=_noop)

    with pytest.raises(PlanDeclarationError, match="'Feat' is not a kind"):
        _finalize(lambda ctx, o: ctx.migrate("Feat:x", migration))


# --- contradictions -------------------------------------------------------------


@pytest.mark.parametrize(
    ("key", "migration"),
    [
        ("FeatureType:m_ft/Feature:m_a", keep_values),
        ("FeatureType:m_ft/Feature:m_a", lambda: detach_from_schemas(["Schema:X"])),
        ("Sheet:MSheet", drop_rows),
        ("RecordType:MVocab", drop_rows),
        ("SchemaType:m_st/Schema:MSchema", keep_validated),
        ("RecordType:MVocab/Record:m_r", drop_record),
    ],
)
def test_a_removal_of_a_declared_object_is_refused(
    key: str, migration: Callable[[], DataMigration]
) -> None:
    with pytest.raises(PlanDeclarationError, match="which the plan still declares"):
        _finalize(lambda ctx, o: ctx.migrate(key, migration()))


def test_a_drop_by_key_of_a_declared_feature_points_at_its_object() -> None:
    with pytest.raises(PlanDeclarationError) as excinfo:
        _finalize(
            lambda ctx, o: ctx.migrate("FeatureType:m_ft/Feature:m_a", drop_values())
        )

    assert str(excinfo.value).endswith(
        "To answer a change to it, pass the plan's Feature object."
    )


class _BaseExports(PlanExports):
    feature: Feature


@create_plan
def _base(ctx: PlanContext, exports: _BaseExports) -> None:
    exports.feature = Feature("b_x", dtype=LaminDType(str), type=FeatureType("b_ft"))


@create_plan
def _child(ctx: PlanContext, exports: None, base: Plan[_BaseExports] = _base) -> None:
    Schema(name="ChildSchema", features=[base.exports.feature])
    ctx.migrate("FeatureType:b_ft/Feature:b_x", drop_values())


def test_a_removal_of_an_object_a_prerequisite_declares_is_refused() -> None:
    _child.reset()

    with pytest.raises(
        PlanDeclarationError, match="which prerequisite plan '_base' still declares"
    ):
        _child.finalize()


# --- ctx.moved ------------------------------------------------------------------


@pytest.mark.parametrize("from_", ["m_a", "FeatureType:m_ft/Feature:m_a"])
def test_a_move_from_the_current_address_is_refused(from_: str) -> None:
    with pytest.raises(PlanDeclarationError) as excinfo:
        _finalize(lambda ctx, o: ctx.moved(o["Feature"], from_))

    assert str(excinfo.value).startswith(
        f"plan 'm': ctx.moved(Feature 'm_a', {from_!r}) names the object's current "
        "address."
    )


@pytest.mark.parametrize("from_", ["old_a", "FeatureType:old_ft/Feature:m_a"])
def test_a_move_from_a_previous_address_is_accepted(from_: str) -> None:
    _finalize(lambda ctx, o: ctx.moved(o["Feature"], from_))


def test_a_move_from_a_malformed_key_is_refused() -> None:
    with pytest.raises(PlanDeclarationError, match="names no previous address"):
        _finalize(lambda ctx, o: ctx.moved(o["Feature"], "FeatureType:ft/Schema:x"))


# --- natural keys ---------------------------------------------------------------


def test_a_natural_key_parses_into_its_segments() -> None:
    assert parse_natural_key("RecordType:Vocab/Record:old") == (
        ("RecordType", "Vocab"),
        ("Record", "old"),
    )
    assert parse_natural_key("Schema:#a1b2c3d4") == (("Schema", "#a1b2c3d4"),)


def test_a_name_may_hold_a_slash_or_a_colon() -> None:
    assert parse_natural_key("FeatureType:ft/Feature:a/b") == (
        ("FeatureType", "ft"),
        ("Feature", "a/b"),
    )
    assert parse_natural_key("Feature:ratio 1:2") == (("Feature", "ratio 1:2"),)


@pytest.mark.parametrize(
    ("key", "problem"),
    [
        ("", "does not start with 'Kind:name'"),
        ("ULabel:x", "'ULabel' is not a kind"),
        ("Feature:", "its Feature segment has no name"),
        ("RecordType:rt/Feature:x", "a Feature cannot sit below a RecordType"),
        ("Feature:x/Record:y", "a Record cannot sit below a Feature"),
    ],
)
def test_a_malformed_natural_key_says_what_is_wrong(key: str, problem: str) -> None:
    with pytest.raises(ValueError, match=problem):
        parse_natural_key(key)


def test_named_key_spells_a_key_as_the_engine_does() -> None:
    from lamindb_setup.evolve.engine import natural_key

    objects = _objects()
    for obj in objects.values():
        assert named_key(obj) == natural_key(obj)
    assert named_key(Schema(features=[objects["Feature"]])) is None
