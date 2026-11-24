"""Ready-made data migrations for the standard dtype-change scenarios.

A :class:`~lamindb_setup.evolve.authoring._migrations.DataMigration` is a bare callable: the
engine flips the feature's dtype and hands the migration a live feature to fix the
stored values up.  That is maximally flexible and maximally repetitive -- every
hand-written `str -> cat[Record]` migration re-implements the same five steps
(read the old rows, map the distinct values onto targets, write the new values,
delete the old rows, and remember that values live in *two* different stores).

This module packages those steps for the four scenarios that cover nearly every
real change:

============================  ==================================================
scenario                      helper
============================  ==================================================
`str` -> `cat[...]`       :func:`map_to_records` / :func:`relocate_to_records`
`cat[...]` -> `cat[...]`  :func:`remap_records` / :func:`relocate_records`
`cat[...]` -> `str`       :func:`map_to_str` / :func:`relocate_to_str`
scalar -> scalar              :func:`map_values` / :func:`rewrite_values`
`x` -> `list[x]`          :func:`wrap_in_list` / :func:`wrap_values`
`list[x]` -> `x`          :func:`unwrap_list` / :func:`unwrap_values`
drop every value              :func:`drop_values` / :func:`clear_values`
============================  ==================================================

`map_values(element_wise=True)` converts a list's elements (`list[str] ->
list[int]`), and `map_to_records(element_wise=True)` maps each element of a
list onto a record (`list[str] -> list[cat[...]]`).

Each scenario comes in two forms.  The `map_*` / `drop_*` **factories** return
a ready :class:`DataMigration` to hand straight to
:meth:`~lamindb_setup.evolve.PlanContext.migrate`; the `relocate_*` /
`rewrite_*` / `clear_*` **functions** take a
:class:`~lamindb_setup.evolve.authoring._migrations.MigrationContext` and can be called from
inside a hand-written migration that needs to do something extra.  The factories
are thin wrappers around the functions.

Resolving a value
-----------------

Every helper resolves each *distinct* stored value through the same chain:

1. an explicit `mapping` entry wins outright -- it is a rewrite table applied
   before any lookup, so it can also *merge* two values that would both otherwise
   resolve (`{"ASA": "aspirin"}`);
2. otherwise the value is looked up against the target (an existing record for a
   categorical target, a dtype coercion for a scalar one);
3. if that finds nothing, `transform` is called to rescue the value.  Its result
   is looked up *again* and must resolve -- a transform cannot invent a target.

Returning `None` from `mapping` or `transform` **drops** the annotation: the
stored value is deleted and nothing replaces it.  Raising from `transform`
aborts the apply, rolling back the dtype flip along with every value already
rewritten.  A value that reaches the end of the chain unresolved raises
:class:`~lamindb_setup.evolve.authoring._migrations.MigrationError`; nothing is ever coerced
or discarded silently.

Null and the feature's default
------------------------------

Two sentinels stand for a value the *feature* defines rather than the migration,
and either can be a `mapping` entry, a `transform` result, or the value a
member backfill fills a row with:

* :data:`~lamindb_setup.evolve.NULL` -- the value is null; and
* :data:`~lamindb_setup.evolve.DEFAULT` -- the value is the feature's
  `default_value`, falling back to null when it declares none, which is the
  order lamindb's own `DataFrameCurator.standardize()` fills a missing column
  in.

Both are refused for a feature that is not nullable and defines no default: a
null there is precisely the invalidation the block being lifted exists to
prevent.  Neither is looked up as a null -- there is nothing to look up -- while a
`DEFAULT` that resolves to a concrete value is checked against the target like
any other replacement.

A null is never *stored*.  lamindb has no such thing: `add_values` skips a
`None`, a categorical value is a foreign key, and both JSON stores are
`NOT NULL` columns -- an un-annotated row is how a null annotation looks, for
every dtype alike.  So a value rewritten to null has its stored row deleted, and
a member backfilled with null gets no row at all; the difference from a plain
drop is that the migration *answered* for it, which it says with
:meth:`~lamindb_setup.evolve.MemberMigrationContext.accept_null` so
the engine does not read the empty row as a backfill that missed.

Targets are never created
-------------------------

A helper only ever maps onto records that *already exist*.  Declare them in the
plan (or pre-create them out of band); declaration order does not matter, because
the apply runs every migration in its own phase, after it has materialized every
object the plan declares::

    @create_plan
    def plan(ctx: PlanContext, exports: None) -> None:
        import lamindb as ln

        rt = RecordType("Compound")
        aspirin = Record("aspirin", type=rt)
        ibuprofen = Record("ibuprofen", type=rt)
        treatment = Feature(
            "treatment", dtype=LaminDType(ln.Record, type__uid=rt.uid), type=ft
        )
        ctx.migrate(
            treatment,
            map_to_records(
                mapping={"ASA": "aspirin", "n/a": None},
                transform=str.lower,
            ),
        )

Which values are touched
------------------------

A helper rewrites exactly the set of values the engine verifies afterwards with
:func:`~lamindb_setup.evolve.engine._drift_data.feature_stranded_value_count`, so a
successful helper always satisfies that check:

* scalar values in **both** stores -- `RecordJson` (per-record) and
  `JsonValue` (shared across the artifacts and runs it annotates); and
* categorical values in the per-registry link tables that carry a `feature` and
  a relational `value`.

Values on *trashed* records are skipped, matching the engine's own accounting:
they are effectively deleted and are never re-validated.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..authoring import (
    CHILDREN_REMOVAL,
    DEFAULT,
    DETACH_MEMBER,
    DTYPE_CHANGE_TARGETS,
    FEATURE_REMOVAL_TARGETS,
    FLEXIBLE_CHANGE,
    INDEX_CHANGE,
    MAXIMAL_SET,
    MEMBER_ADDED,
    MEMBER_REMOVAL,
    MEMBER_REQUIRED,
    MERGE_SCHEMA,
    MINIMAL_SET,
    NULL,
    NULLABILITY,
    ORDERED_SET,
    RECORD_REMOVAL,
    RECORD_UPDATE,
    ROWS_REMOVAL,
    SHEET_SCHEMA,
    SLOT_CHANGE,
    STALE_EXPORTS,
    UNIT_CHANGE,
    VALIDATED_REMOVAL,
    DataMigration,
    DefaultFill,
    MigrationError,
    NullFill,
    WriteStrategy,
)
from ..core import DEFAULT_MODELS, LamindbModels
from ._drift_data import (
    _SCALAR_VALUE_RELATIONS,
    ValueLinks,
    _iter_categorical_managers,
    _type_scope_depths,
    admitted_label_keys,
    artifact_label_link_models,
    feature_inadmissible_link_count,
    feature_members,
    link_label_field,
    link_subject_field,
)
from ._inferred_schemas import _members

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

    from ..authoring import (
        FeatureRemovalContext,
        FlexibleChangeContext,
        IndexChangeContext,
        IndexNameConflict,
        MemberMigrationContext,
        MemberRemovalContext,
        MigrationContext,
        RecordRemovalContext,
        RecordTypeRemovalContext,
        RecordUpdateContext,
        SchemaRemovalContext,
        SheetExportContext,
    )

__all__ = [
    "DEFAULT",
    "NULL",
    "accept_added_member",
    "accept_flexible_change",
    "accept_index_change",
    "accept_non_null",
    "adopt_children",
    "backfill_members",
    "clear_member_values",
    "clear_nonmember_annotations",
    "clear_values",
    "drop_member_values",
    "drop_nonmember_annotations",
    "drop_record",
    "drop_rows",
    "drop_values",
    "fill_missing",
    "fill_nulls",
    "keep_member_values",
    "keep_rows",
    "keep_stale_exports",
    "keep_validated",
    "keep_values",
    "map_to_records",
    "map_to_str",
    "map_values",
    "move_rows",
    "reexport_sheets",
    "relocate_to_records",
    "relocate_to_str",
    "relocate_value_links",
    "revalidate_against",
    "rewrite_exports",
    "rewrite_values",
    "update_record",
]


class _Drop:
    """Sentinel for "resolve this value to nothing and delete the annotation"."""

    def __repr__(self) -> str:
        return "<drop>"


_DROP = _Drop()


class _Null:
    """Sentinel for "resolve this value to null".

    The *resolved* counterpart of :data:`~lamindb_setup.evolve.NULL`, which is
    what a plan author writes: the nullability check and the `DEFAULT` lookup
    have already happened by the time a value carries this.  Deleting the stored
    row is all it takes to write -- an un-annotated row is how lamindb spells a
    null -- so what distinguishes it from :data:`_DROP` is what it *means*: the
    value was answered for rather than discarded, which is why it is reported
    apart and why a member backfill may declare the row settled.
    """

    def __repr__(self) -> str:
        return "<null>"


_NULL = _Null()


# === null and default fills ==================================================


def _is_categorical(feature: Any) -> bool:
    """Whether `feature`'s live dtype stores its values as foreign keys."""
    dtype = getattr(feature, "dtype_as_str", None) or ""
    return dtype.startswith(("cat", "list[cat"))


def _resolve_fill(feature: Any, value: Any) -> Any:
    """Resolve :data:`NULL` / :data:`DEFAULT` against `feature`.

    Returns :data:`_NULL` for a value that is to become null, or the concrete
    replacement a `DEFAULT` names; anything else is passed through untouched.

    `DEFAULT` resolves to the feature's `default_value`, falling back to null
    when it declares none -- the same order lamindb's own
    `DataFrameCurator.standardize()` fills a missing column in.  Either
    sentinel is refused for a non-nullable feature, since a null there is
    exactly the invalidation the block being lifted exists to prevent.
    """
    if isinstance(value, DefaultFill):
        default = getattr(feature, "default_value", None)
        if default is not None:
            return default
        if not getattr(feature, "nullable", False):
            raise MigrationError(
                f"DEFAULT cannot fill {feature.name!r}: it declares no "
                "'default_value' and is not nullable. Set a default on the "
                "feature, make it nullable, or pass a concrete value."
            )
        return _NULL
    if isinstance(value, NullFill):
        if not getattr(feature, "nullable", False):
            raise MigrationError(
                f"NULL cannot fill {feature.name!r}: the feature is not "
                "nullable, so a null value would not validate. Declare it "
                "'nullable=True' in the plan, or pass a concrete value."
            )
        return _NULL
    return value


# === stored-value access =====================================================


@dataclass
class _StoredValue:
    """One stored value row together with the objects it annotates.

    `owners` is resolved eagerly at construction because the row is deleted
    before the replacement is written: a `JsonValue` reaches its artifacts and
    runs through many-to-many links that vanish with the row, so they must be
    captured while it still exists.  A `RecordJson` or categorical link row has
    exactly one owner; a `JsonValue` may have any number.
    """

    row: Any
    owners: tuple[Any, ...]
    value: Any


def _delete_all(
    stored: list[_StoredValue],
    *,
    batch_size: int | None = None,
    unlink_owners: Callable[[Any, list[Any]], None] | None = None,
) -> None:
    """Permanently remove every stored row, with one delete per table.

    Deleting row by row costs a query per annotation, which dominates a large
    relocation.  The rows of one table share a primary key column, so they go in
    a `pk__in` delete, batched by :data:`_DELETE_BATCH_SIZE` so a single `IN`
    clause stays within the backend's bind-parameter limit.  `permanent` skips
    the trash where the manager understands it; not every value table's manager
    does, and a delete on a link row is already permanent.  Neither call can
    reach the "already in
    trash, erase it?" prompt that
    :func:`~lamindb_setup.evolve.engine._trash.delete_object` exists to avoid: that
    prompt is only in the *record* delete, and a queryset delete without
    `permanent` falls through to Django's row removal for any model without a
    `branch` field, which is every model reached here.

    A `JsonValue` reaches its artifacts and runs through link tables whose
    foreign key back to it is `PROTECT`, so those links have to go first --
    Django refuses the delete outright otherwise.  They are cleared in one
    `jsonvalue_id__in` delete per link table rather than by walking each row's
    manager, which would cost two queries per value.  The owners were resolved
    eagerly when the row was read (see :class:`_StoredValue`), so nothing still
    needs them.

    `batch_size` and `unlink_owners` default to :data:`_DELETE_BATCH_SIZE`
    and :func:`_unlink_owners`; they are parameters so a test can drive the
    batching with a handful of rows instead of ten thousand.
    """
    unlink = _unlink_owners if unlink_owners is None else unlink_owners
    by_model: dict[Any, list[Any]] = {}
    for entry in stored:
        by_model.setdefault(type(entry.row), []).append(entry.row.pk)
    for model, pks in by_model.items():
        unlink(model, pks)
        for chunk in _in_clause_batches(pks, size=batch_size):
            queryset = model.objects.filter(pk__in=chunk)
            try:
                queryset.delete(permanent=True)
            except TypeError:
                queryset.delete()


# Chunk size for the `IN` clauses the deletes below build, mirroring
# `_drift_data._PREFETCH_VALUE_BATCH_SIZE`.  One delete per table is the whole
# point of `_delete_all` -- a query per annotation is what dominates a large
# relocation -- but a single `IN` holding every primary key trades that cost
# for a hard failure: SQLite caps a statement at 999 bound parameters by
# default and Postgres at 65535, and a feature with more annotations than that
# is exactly the "large relocation" this path exists for.
_DELETE_BATCH_SIZE = 10_000


def _in_clause_batches(
    pks: list[Any], *, size: int | None = None
) -> Iterator[list[Any]]:
    """Yield `pks` in chunks small enough for one `IN` clause."""
    size = _DELETE_BATCH_SIZE if size is None else size
    for start in range(0, len(pks), size):
        yield pks[start : start + size]


def _unlink_owners(model: Any, pks: list[Any], *, size: int | None = None) -> None:
    """Drop the link rows whose foreign key back to `model` would block a delete.

    A shared value table (`JsonValue`) is reached from the artifacts and runs
    it annotates through link tables whose foreign key back to it is `PROTECT`,
    so Django refuses to delete the value while any link survives.  They are
    removed here, one `<column>_id__in` delete per link table (batched by
    :data:`_DELETE_BATCH_SIZE`, like the delete itself) rather than two
    queries per value row.

    Only `PROTECT`ed relations are touched: a `CASCADE` one disappears with
    the row anyway, and deleting it separately would just cost a query.  A
    per-record row (`RecordJson`) and a categorical link row own their
    annotation outright and have no such relations, so this is a no-op for them.
    """
    from django.db.models.deletion import PROTECT

    meta = getattr(model, "_meta", None)
    if meta is None:  # pragma: no cover - every real value table is a model
        return
    for relation in meta.related_objects:
        if relation.on_delete is not PROTECT:
            continue
        for chunk in _in_clause_batches(pks, size=size):
            relation.related_model.objects.filter(
                **{f"{relation.field.name}_id__in": chunk}
            ).delete()


def _scalar_row_owners(row: Any) -> tuple[Any, ...]:
    """The objects a scalar value row annotates.

    `RecordJson` carries a single `record` foreign key.  `JsonValue` is a
    *shared* value: the same row annotates every artifact and run linked to it,
    so each must be rewritten before the row is deleted.
    """
    record = getattr(row, "record", None)
    if record is not None:
        return (record,)
    owners: list[Any] = []
    for relation in ("artifacts", "runs"):
        manager = getattr(row, relation, None)
        if manager is not None:
            owners.extend(manager.all())
    return tuple(owners)


def _link_row_owner(row: Any) -> Any:
    """The object a categorical link row annotates.

    A link table pairs the annotated object with the `feature` and the label;
    which foreign key is which is decided by
    :func:`~lamindb_setup.evolve.engine._drift_data.link_subject_field`, since the label
    is only called `value` when the annotated object is a `Record`.
    """
    subject = link_subject_field(type(row))
    if subject is None:
        raise MigrationError(
            f"cannot determine which object {type(row).__name__} annotates. "
            "Write the migration by hand for this link table."
        )
    return getattr(row, subject.name)


def _with_owners(queryset: Any) -> Any:
    """`queryset` with the objects each row annotates loaded eagerly.

    :func:`_scalar_row_owners` dereferences a relation per row, which is a query
    per row unless it is joined (`RecordJson.record`, a single foreign key) or
    prefetched (`JsonValue`'s artifacts and runs, many-to-many).
    """
    model = queryset.model
    concrete = {
        f.name
        for f in model._meta.get_fields()
        if getattr(f, "concrete", False) and getattr(f, "is_relation", False)
    }
    if "record" in concrete:
        return queryset.select_related("record")
    return queryset.prefetch_related(
        *(name for name in ("artifacts", "runs") if hasattr(model, name))
    )


def _iter_scalar_values(feature: Any) -> Iterator[_StoredValue]:
    """Yield every stored scalar value of `feature`, across both stores.

    Trashed rows are excluded with the same filters the engine's stranded-value
    accounting uses, so relocating everything yielded here always satisfies it.
    """
    for relation, trash_filter in _SCALAR_VALUE_RELATIONS:
        manager = getattr(feature, relation, None)
        if manager is None:
            continue
        queryset = manager.exclude(**trash_filter) if trash_filter else manager.all()
        for row in _with_owners(queryset):
            yield _StoredValue(row=row, owners=_scalar_row_owners(row), value=row.value)


def _iter_categorical_values(feature: Any) -> Iterator[_StoredValue]:
    """Yield every stored categorical value of `feature`.

    Enumerates the same per-registry link tables the engine's stranded-value
    accounting counts.  `value` is the *label record*, not a string.  The
    label and the annotated object are joined in, so a table costs one query
    rather than two per link.
    """
    for manager in _iter_categorical_managers(feature):
        label = link_label_field(manager.model)
        if label is None:  # pragma: no cover - rejected by the manager filter
            continue
        subject = link_subject_field(manager.model)
        joined = [label.name] if subject is None else [label.name, subject.name]
        for row in manager.select_related(*joined):
            yield _StoredValue(
                row=row,
                owners=(_link_row_owner(row),),
                value=getattr(row, label.name),
            )


def delete_stored_values(feature: Any) -> int:
    """Permanently delete every value `feature` stores, returning how many.

    Covers both scalar stores and every categorical link table -- the same rows
    the stranded-value accounting counts -- so what is left afterwards is
    nothing.  The caller owns the transaction.
    """
    stored = [*_iter_scalar_values(feature), *_iter_categorical_values(feature)]
    _delete_all(stored)
    return len(stored)


def _write(owner: Any, feature: Any, value: Any) -> None:
    """Annotate `owner` with `value`, letting lamindb pick the store.

    `add_values` dispatches on the feature's *current* dtype -- already flipped
    by the engine -- so a categorical target lands in the right link table and a
    scalar one in the scalar table.

    lamindb (>= 2.12) refuses `add_values` for a to-one feature on a
    `Record`, so there a scalar is written straight into `RecordJson`, as
    :func:`_annotate` does: `update_values`, lamindb's replacement, re-curates
    the whole row, and mid-apply another migrated feature of the row may still
    hold its stale value.  Only what lamindb has to place itself -- a feature
    that is the type's schema index (`Record.name`) or a categorical label it
    could not be linked to directly -- goes through `update_values`.
    """
    is_record = type(owner).__name__ == "Record"
    dtype = getattr(feature, "dtype_as_str", None) or ""
    if not is_record or dtype.startswith("list["):
        owner.features.add_values({feature.name: value})
        return
    index = DEFAULT_MODELS.type_schema_index(owner.type) if owner.type_id else None
    is_index = index is not None and index.uid == feature.uid
    if not _is_categorical(feature) and not is_index:
        feature.links_recordjson.model(
            record=owner, feature=feature, value=value
        ).save()
        return
    owner.features.update_values({feature.name: value})


# === batched categorical writes ==============================================


def _is_saved(obj: Any) -> bool:
    """Whether `obj` is a row that already exists in the database."""
    state = getattr(obj, "_state", None)
    return getattr(obj, "pk", None) is not None and not getattr(state, "adding", True)


def _plan_link_writes(
    feature: Any,
    writes: list[tuple[Any, Any]],
    *,
    models: LamindbModels | None = None,
) -> list[Any] | None:
    """The link rows expressing `writes`, or `None` to let lamindb write them.

    :func:`_write` goes through `add_values`, which re-resolves the feature,
    rebuilds a schema and runs a curator *per annotated object* -- work that is
    identical for every write of a single feature and that a relocation has
    already done when it resolved each distinct value to a target record.  For
    the shape a relocation actually produces -- an already-saved label record
    annotating an already-saved object -- that whole apparatus reduces to one row
    in the categorical link table, so the rows are built here and inserted in a
    single batch instead.

    `None` means "not that shape", and the caller falls back to `add_values`
    per write so that `map_values` / `relocate_to_str` keep behaving exactly
    as before.  It is returned for a scalar replacement, an unsaved object, a
    registry pair lamindb has no categorical link table for, or a feature that is
    the schema index of an owner's type -- there `add_values` writes the value
    into `Record.name` rather than into a link table.
    """
    if not writes:
        return None
    models = DEFAULT_MODELS if models is None else models
    feature_id = getattr(feature, "id", None)
    if feature_id is None:
        return None
    link_info: dict[tuple[Any, Any], tuple[Any, str, str]] = {}
    indexed_types: set[Any] = set()
    seen: set[tuple[Any, Any, Any]] = set()
    rows: list[Any] = []
    for owner, value in writes:
        if not _is_saved(owner) or not _is_saved(value):
            return None
        owner_model, value_model = type(owner), type(value)
        if owner_model.__name__ == "Record":
            type_id = getattr(owner, "type_id", None)
            if type_id is not None and type_id not in indexed_types:
                indexed_types.add(type_id)
                index = models.type_schema_index(owner.type)
                if index is not None and index.uid == getattr(feature, "uid", None):
                    return None
        key = (owner_model, value_model)
        if key not in link_info:
            link = models.categorical_link(
                owner_model,
                value_model,
                instance=getattr(owner._state, "db", None),
            )
            if link is None:
                return None
            link_info[key] = (link.model, link.owner_field, link.value_field)
        link_model, owner_field, value_field = link_info[key]
        # Two stored values of one object can resolve to the same target; the
        # link tables are unique on (object, feature, value), so emit one row.
        if (link_model, owner.pk, value.pk) in seen:
            continue
        seen.add((link_model, owner.pk, value.pk))
        rows.append(
            link_model(
                **{
                    f"{owner_field}_id": owner.pk,
                    "feature_id": feature_id,
                    f"{value_field}_id": value.pk,
                }
            )
        )
    return rows


def _save_links(rows: list[Any]) -> None:
    """Insert `rows` in bulk, skipping links that already exist.

    Conflicts are ignored rather than retried: the caller runs inside the apply
    transaction, where a failed statement would poison it, and an already-present
    link already expresses the annotation being written.
    """
    from lamindb.models.save import save

    save(rows, ignore_conflicts=True)


def _apply(
    context: MigrationContext,
    stored: list[_StoredValue],
    resolved: dict[Any, tuple[Any, str]],
) -> None:
    """Rewrite every stored value to its resolved replacement.

    The old rows are deleted *before* the replacements are written: a scalar
    target would otherwise collide with the row it replaces (`RecordJson` is
    unique per record and feature, `JsonValue` per feature and value hash).
    Both happen inside the apply transaction, so a later failure rolls the pair
    back together.

    Values are *resolved* once per distinct value but *counted* once per stored
    row, so the report says how many annotations changed rather than how many
    distinct strings were looked up.

    The writes are planned before anything is deleted, so a plan that turns out
    not to be expressible as link rows falls back to `add_values` without the
    old rows having been touched.

    A value resolved to null needs no write of its own: an un-annotated row is
    how lamindb spells a null, so deleting the stored row is the whole of it.
    """
    report = context.report
    rows: list[tuple[_StoredValue, Any]] = []
    for entry in stored:
        replacement, outcome = resolved[_value_key(entry.value)]
        setattr(report, outcome, getattr(report, outcome) + 1)
        rows.append((entry, replacement))
    _rewrite(context, rows)


@dataclass(frozen=True)
class _Many:
    """Several labels replacing one stored value: the elements of a list."""

    items: tuple[Any, ...]


def _rewrite(context: MigrationContext, rows: list[tuple[_StoredValue, Any]]) -> None:
    """Replace each stored value with its (already counted) replacement.

    See :func:`_apply`.  A :class:`_Many` replacement is written as one link
    per label, which is how lamindb stores a `list[cat[...]]` value.
    """
    report = context.report
    writes: list[tuple[Any, Any]] = []
    for entry, replacement in rows:
        if replacement is _DROP or replacement is _NULL:
            continue
        items = replacement.items if isinstance(replacement, _Many) else (replacement,)
        writes.extend((owner, item) for owner in entry.owners for item in items)
    links = _plan_link_writes(context.feature, writes)
    _delete_all([entry for entry, _ in rows])
    if links is None:
        report.write_strategy = WriteStrategy.ADD_VALUES
        for owner, replacement in writes:
            _write(owner, context.feature, replacement)
    else:
        report.write_strategy = WriteStrategy.BATCHED_LINKS
        _save_links(links)


# === value resolution ========================================================


def _value_key(value: Any) -> Any:
    """`value` as a dictionary key: a list -- a `list[...]` value -- as a tuple."""
    if isinstance(value, list):
        return tuple(_value_key(item) for item in value)
    return value


def _distinct(values: list[Any]) -> list[Any]:
    """The distinct entries of `values`, preserving order.

    A list compares by its elements (see :func:`_value_key`).  Raises a clear
    error for any other unhashable stored value (a JSON object) rather than
    letting the set construction fail opaquely.
    """
    seen: set[Any] = set()
    distinct: list[Any] = []
    try:
        for value in values:
            key = _value_key(value)
            if key not in seen:
                seen.add(key)
                distinct.append(value)
    except TypeError as exc:
        raise MigrationError(
            "cannot migrate a feature holding structured (unhashable) values "
            f"such as {values[len(distinct)]!r}; write the migration by hand."
        ) from exc
    return distinct


def _resolve_one(
    raw: Any,
    *,
    feature: Any,
    lookup: Callable[[Any], Any],
    mapping: Mapping[Any, Any] | None,
    transform: Callable[[Any], Any] | None,
    target: str,
) -> tuple[Any, str]:
    """Resolve one distinct stored value to `(replacement, outcome)`.

    `replacement` is :data:`_DROP` when the annotation should be deleted and
    :data:`_NULL` when it should become the feature's null; `outcome` names the
    :class:`~lamindb_setup.evolve.authoring._migrations.MigrationReport` counter to credit
    (the caller bumps it once per *row*, not once per distinct value).  See the
    module docstring for the chain; raises
    :class:`~lamindb_setup.evolve.authoring._migrations.MigrationError` when the value
    cannot be resolved, so nothing is discarded silently.

    `mapping` and `transform` may answer with :data:`~lamindb_setup.evolve.NULL`
    or :data:`~lamindb_setup.evolve.DEFAULT`, which are resolved against
    `feature` (see :func:`_resolve_fill`) *before* the lookup -- a null has
    nothing to look up, and a `default_value` is still checked against the
    target like any other replacement.
    """
    if mapping is not None and raw in mapping:
        replacement = _resolve_fill(feature, mapping[raw])
        if replacement is None:
            return _DROP, "dropped"
        if replacement is _NULL:
            return _NULL, "nulled"
        resolved = lookup(replacement)
        if resolved is None:
            raise MigrationError(
                f"mapping sends {raw!r} to {replacement!r}, which does not "
                f"resolve against {target}. Declare it in the plan or map to a "
                "value that exists."
            )
        return resolved, "mapped"

    resolved = lookup(raw)
    if resolved is not None:
        return resolved, "matched"

    if transform is None:
        raise MigrationError(
            f"stored value {raw!r} does not resolve against {target}. Declare a "
            "matching target in the plan, add a 'mapping' entry, or pass a "
            "'transform' to rewrite or drop it."
        )

    replacement = _resolve_fill(feature, transform(raw))
    if replacement is None:
        return _DROP, "dropped"
    if replacement is _NULL:
        return _NULL, "nulled"
    resolved = lookup(replacement)
    if resolved is None:
        raise MigrationError(
            f"transform rewrote {raw!r} to {replacement!r}, which still does not "
            f"resolve against {target}. A transform cannot create a target: "
            "return a value that exists, return NULL or DEFAULT to fill it in, "
            "return None to drop the annotation, or raise to abort."
        )
    return resolved, "transformed"


def _resolve_all(
    stored: list[_StoredValue],
    *,
    feature: Any,
    lookup: Callable[[Any], Any],
    mapping: Mapping[Any, Any] | None,
    transform: Callable[[Any], Any] | None,
    target: str,
) -> dict[Any, tuple[Any, str]]:
    """Resolve each *distinct* stored value once, returning the replacement map."""
    values = [entry.value for entry in stored]
    return {
        _value_key(raw): _resolve_one(
            raw,
            feature=feature,
            lookup=lookup,
            mapping=mapping,
            transform=transform,
            target=target,
        )
        for raw in _distinct(values)
    }


def _elements_of(value: Any) -> list[Any]:
    """A stored value's elements: a list's own, a single value as one."""
    return value if isinstance(value, list) else [value]


def _rewrite_element_wise(
    context: MigrationContext,
    stored: list[_StoredValue],
    *,
    lookup: Callable[[Any], Any],
    mapping: Mapping[Any, Any] | None,
    transform: Callable[[Any], Any] | None,
    target: str,
    as_labels: bool,
) -> None:
    """Rewrite each stored value element by element into a `list[...]` value.

    A single value is the one element of its list -- `x -> [x]`.  Each
    distinct element is resolved once through the chain of the module
    docstring and counted per occurrence; one resolving to `None` or
    :data:`~lamindb_setup.evolve.NULL` is left out of its list, and a value left
    with no element is dropped.  `as_labels` writes the elements as one link
    each (a `list[cat[...]]`) rather than as one JSON list.
    """
    elements = [item for entry in stored for item in _elements_of(entry.value)]
    resolved = {
        _value_key(raw): _resolve_one(
            raw,
            feature=context.feature,
            lookup=lookup,
            mapping=mapping,
            transform=transform,
            target=target,
        )
        for raw in _distinct(elements)
    }
    report = context.report
    rows: list[tuple[_StoredValue, Any]] = []
    for entry in stored:
        items: list[Any] = []
        for element in _elements_of(entry.value):
            replacement, outcome = resolved[_value_key(element)]
            setattr(report, outcome, getattr(report, outcome) + 1)
            if replacement is not _DROP and replacement is not _NULL:
                items.append(replacement)
        if not items:
            rows.append((entry, _DROP))
        else:
            rows.append((entry, _Many(tuple(items)) if as_labels else items))
    _rewrite(context, rows)


# === target lookups ==========================================================


def _default_field(registry: Any) -> str:
    """The field a registry is normally addressed by (`name`, `symbol`, ...)."""
    from ..authoring import DEFAULT_NAME_FIELD

    return DEFAULT_NAME_FIELD.get(
        registry.__name__, getattr(registry, "_name_field", "name") or "name"
    )


def _standardized(registry: Any, value: Any, field: str) -> Any:
    """`value` resolved through the registry's synonyms, or `None`.

    Only meaningful for registries that carry a synonym table (the bionty
    ontologies); anything else, or any failure, simply declines to standardize.
    """
    standardize = getattr(registry, "standardize", None)
    if standardize is None:
        return None
    try:
        result = standardize([value], field=field, mute=True)
    except Exception:
        return None
    if isinstance(result, str):
        return result
    if isinstance(result, list) and result:
        return result[0]
    return None


def _record_lookup(
    context: MigrationContext, *, field: str | None, standardize: bool
) -> Callable[[Any], Any]:
    """A lookup resolving a string to an existing record of the target registry.

    The registry and its filters come from the plan's declared dtype (see
    :class:`~lamindb_setup.evolve.authoring._migrations.MigrationContext`), so a
    `cat[Record[<uid>]]` target only ever matches records of that type or of
    one of its subtypes -- what lamindb validates such a value against (see
    :func:`_scoped_one`).
    """
    registry = context.target_registry
    if registry is None:
        raise MigrationError(
            "cannot map values onto records: the plan's target dtype "
            f"({context.desired_dtype!r}) does not resolve to a registry. Use a "
            "categorical dtype (e.g. LaminDType(ln.Record, type__uid=...)) or "
            "write the migration by hand."
        )
    lookup_field = field or _default_field(registry)
    filters = dict(context.cat_filters or {})
    type_uid = filters.pop("type__uid", None)
    depths = _type_scope_depths(registry, type_uid) if type_uid else None

    def lookup(value: Any) -> Any:
        match = _scoped_one(
            registry, {lookup_field: value, **filters}, lookup_field, value, depths
        )
        if match is not None or not standardize:
            return match
        replacement = _standardized(registry, value, lookup_field)
        if replacement is None or replacement == value:
            return None
        return _scoped_one(
            registry,
            {lookup_field: replacement, **filters},
            lookup_field,
            replacement,
            depths,
        )

    return lookup


def _one(registry: Any, query: dict[str, Any], field: str, value: Any) -> Any:
    """The single record matching `query`, or `None`; ambiguity is an error."""
    matches = list(registry.filter(**query)[:2])
    if len(matches) > 1:
        raise MigrationError(
            f"{registry.__name__}.{field} == {value!r} matches more than one "
            "record, so the value cannot be migrated unambiguously. Narrow the "
            "feature's dtype (e.g. with a type constraint) or deduplicate first."
        )
    return matches[0] if matches else None


def _scoped_one(
    registry: Any,
    query: dict[str, Any],
    field: str,
    value: Any,
    depths: dict[Any, int] | None,
) -> Any:
    """The record `query` names within a type's subtree, or `None`.

    `depths` (see :func:`~lamindb_setup.evolve.engine._drift_data._type_scope_depths`)
    scopes the lookup to a type and its subtypes, which is what lamindb
    validates a `Registry[<type>]` value against.  A name labelling records at
    several depths resolves to the topmost, as lamindb's curator resolves it; two
    at that depth are ambiguous.  Without `depths` the lookup is :func:`_one`.
    """
    if depths is None:
        return _one(registry, query, field, value)
    matches = list(registry.filter(**query, type_id__in=sorted(depths)))
    if not matches:
        return None
    top = min(depths[match.type_id] for match in matches)
    topmost = [match for match in matches if depths[match.type_id] == top]
    if len(topmost) > 1:
        raise MigrationError(
            f"{registry.__name__}.{field} == {value!r} matches more than one "
            f"record at the same depth below the dtype's type ({len(topmost)}), "
            "so the value cannot be migrated unambiguously. Narrow the feature's "
            "dtype to one of their types or deduplicate first."
        )
    return topmost[0]


def _to_int(value: Any) -> int:
    """`value` as an `int`, refusing anything that would lose information."""
    if isinstance(value, bool):
        raise ValueError("a boolean is not an integer annotation")
    result = int(value)
    if float(value) != result:
        # 42.7 -> 42 would silently discard the fraction; treat it as unmatched
        # so the plan author decides explicitly.
        raise ValueError("would truncate")
    return result


def _to_num(value: Any) -> int | float:
    """`value` as an `int` when exact, otherwise as a `float`."""
    try:
        return _to_int(value)
    except (TypeError, ValueError):
        return float(value)


def _to_bool(value: Any) -> bool:
    """`value` as a `bool`, refusing the truthiness of everything else."""
    if isinstance(value, bool):
        return value
    raise ValueError("not a boolean")


# How each scalar target dtype converts a stored value.  A conversion that
# raises marks the value *unmatched*, sending it down the mapping / transform
# chain -- which is the whole point, so these are deliberately strict: they
# refuse anything that would silently change the value (a truncating float, the
# truthiness of a non-boolean).  lamindb validates `path` and `url` as
# strings, so they convert as `str` does.
_SCALAR_CASTS: dict[str, Callable[[Any], Any]] = {
    "int": _to_int,
    "float": float,
    "num": _to_num,
    "bool": _to_bool,
    "str": str,
    "path": str,
    "url": str,
}


def _scalar_lookup(context: MigrationContext) -> Callable[[Any], Any]:
    """A lookup converting a value to the target scalar dtype, or `None`.

    lamindb's own `infer_convert_dtype_key_value` only *infers* a value's dtype
    -- it never converts one to a target -- so a string `"42"` would sail
    through an `int` narrowing and then fail curation.  These conversions are
    explicit instead, and strict: whatever will not convert cleanly becomes an
    unmatched value for `mapping` / `transform` to handle.
    """
    return _cast_lookup(context.desired_dtype)


def _cast_lookup(desired_dtype: str) -> Callable[[Any], Any]:
    """:func:`_scalar_lookup` for `desired_dtype`, a list's element dtype included."""
    from ..authoring import canonical_dtype_str
    from ._drift_data import _list_element_dtype

    dtype = canonical_dtype_str(desired_dtype) or desired_dtype
    cast = _SCALAR_CASTS.get(dtype)
    if cast is None:
        listed = (
            " Pass element_wise=True to convert a list element by element, or use "
            "wrap_in_list() / unwrap_list() to change its shape."
            if _list_element_dtype(desired_dtype) is not None
            else ""
        )
        raise MigrationError(
            f"no built-in conversion to dtype {desired_dtype!r}. Use "
            "map_to_records() for a categorical target, or write the migration "
            f"by hand. Supported here: {', '.join(sorted(_SCALAR_CASTS))}.{listed}"
        )

    def lookup(value: Any) -> Any:
        try:
            return cast(value)
        except (TypeError, ValueError):
            return None

    return lookup


def _label_text(value: Any, field: str | None) -> Any:
    """Render a label record as the string a `str` feature would store."""
    return getattr(value, field or _default_field(type(value)))


def _refuse_categorical_source(context: MigrationContext, helper: str) -> None:
    """Refuse a categorical source whose links the new dtype does not admit.

    `map_to_records` reads the *scalar* stores only, so on a `cat -> cat`
    change it would relocate nothing and report success while every link
    stayed under a label the feature no longer accepts.  A link the new dtype
    still admits -- `cat[A] -> cat[A|B]`, `cat[A] -> list[cat[A]]` -- needs no
    relocation and passes.
    """
    live = context.live_dtype or ""
    if not ("cat[" in live or live == "cat"):
        return
    inadmissible = feature_inadmissible_link_count(
        context.feature, context.desired_dtype
    )
    if not inadmissible:
        return
    raise MigrationError(
        f"{helper} relocates scalar values only, but {inadmissible} categorical "
        f"value(s) of {context.feature.name!r} are labels the new dtype "
        f"{context.desired_dtype!r} does not admit (it was {live!r}). Relocate "
        "them with remap_records()."
    )


# === imperative helpers ======================================================


def relocate_to_records(
    context: MigrationContext,
    /,
    *,
    mapping: Mapping[Any, Any] | None = None,
    transform: Callable[[Any], Any] | None = None,
    standardize: bool = False,
    field: str | None = None,
    element_wise: bool = False,
) -> None:
    """Relocate a feature's scalar values onto existing records (`str` -> `cat`).

    Reads every stored scalar value (both stores), resolves each distinct one to a
    record of the plan's target registry, then rewrites the annotations onto the
    categorical store.  See the module docstring for the resolution chain and for
    why targets must already exist.

    `field` overrides the field records are matched on (defaulting to the
    registry's name field); `standardize` additionally resolves synonyms before
    a value is considered unmatched.

    `element_wise` maps each element of a stored list onto a record, for a
    `list[cat[...]]` target (`list[str] -> list[cat[...]]`); a single value
    is mapped as a list of one.

    `mapping` / `transform` may answer with :data:`~lamindb_setup.evolve.NULL`
    or :data:`~lamindb_setup.evolve.DEFAULT` for a nullable feature, which leaves
    the row un-annotated -- lamindb's way of spelling a null -- rather than
    mapping it onto a record.
    """
    _refuse_categorical_source(context, "map_to_records")
    lookup = _record_lookup(context, field=field, standardize=standardize)
    stored = list(_iter_scalar_values(context.feature))
    registry = context.target_registry
    target = f"{registry.__name__} records"
    if element_wise:
        _require_list_target(context, "map_to_records(element_wise=True)")
        _rewrite_element_wise(
            context,
            stored,
            lookup=lookup,
            mapping=mapping,
            transform=transform,
            target=target,
            as_labels=True,
        )
        return
    resolved = _resolve_all(
        stored,
        feature=context.feature,
        lookup=lookup,
        mapping=mapping,
        transform=transform,
        target=target,
    )
    _apply(context, stored, resolved)


def relocate_records(
    context: MigrationContext,
    /,
    *,
    mapping: Mapping[Any, Any] | None = None,
    transform: Callable[[Any], Any] | None = None,
    match_on: str | None = None,
    standardize: bool = False,
) -> None:
    """Relocate a feature's categorical values onto the new categorical dtype.

    Reads every link the feature stores and keeps it when the new dtype already
    admits its label -- a union member that survives, a record below the new
    type, the same bionty record under another name field.  Every other label
    is resolved through the chain of the module docstring, with two twists: a
    `mapping` entry may be keyed by the old label's `uid` as well as by its
    name, and `transform` receives the old label *record*.  A name is looked
    up among the labels the new dtype admits, union member by union member,
    on `match_on` (by default the old label's name field read and the new
    dtype's field matched).  The links of the relocated labels are rewritten;
    the kept ones are not touched.

    A scalar source is :func:`relocate_to_records`'s, and turning a list into a
    single value is refused: admitting each label says nothing about how many a
    row links.
    """
    live = context.live_dtype or ""
    if not ("cat[" in live or live == "cat"):
        raise MigrationError(
            f"remap_records relocates categorical values, but {context.feature.name!r} "
            f"was {live!r}. Relocate its scalar values with map_to_records()."
        )
    if live.startswith("list[") and not context.desired_dtype.startswith("list["):
        raise MigrationError(
            f"remap_records cannot turn the list {live!r} of "
            f"{context.feature.name!r} into the single value "
            f"{context.desired_dtype!r}: a row may link several labels. Write the "
            "migration by hand."
        )
    stored = list(_iter_categorical_values(context.feature))
    labels = _distinct([entry.value for entry in stored])
    try:
        admitted = admitted_label_keys(labels, context.desired_dtype)
    except Exception as exc:
        raise MigrationError(
            f"cannot tell which labels {context.desired_dtype!r} admits: {exc}"
        ) from exc
    if admitted is None:
        raise MigrationError(
            f"remap_records needs a categorical target, but {context.desired_dtype!r} "
            "names no registry. Write the migration by hand."
        )
    lookup = _label_lookup(context, match_on=match_on, standardize=standardize)
    target = f"the labels of {context.desired_dtype!r}"
    resolved = {
        label: _resolve_label(
            label,
            feature=context.feature,
            admitted=(type(label), label.pk) in admitted,
            lookup=lookup,
            mapping=mapping,
            transform=transform,
            match_on=match_on,
            target=target,
        )
        for label in labels
    }
    moving = [entry for entry in stored if resolved[entry.value][1] != "kept"]
    context.report.kept += len(stored) - len(moving)
    _apply(context, moving, resolved)


def _label_lookup(
    context: MigrationContext, *, match_on: str | None, standardize: bool
) -> Callable[[Any], Any]:
    """A lookup resolving a name to a label the new categorical dtype admits.

    Each union member is tried in turn, as lamindb's curator does, within what
    it admits: its type's subtree (see :func:`_scoped_one`) and its filters.
    """
    targets: list[tuple[Any, str, dict[str, Any], dict[Any, int] | None]] = []
    for component in DEFAULT_MODELS.parse_dtype(context.desired_dtype) or []:
        registry = component["registry"]
        field = match_on or component.get("field_str") or _default_field(registry)
        filters: dict[str, Any] = {}
        if component.get("filter_str"):
            lookups = DEFAULT_MODELS.cat_filter_lookups(component["filter_str"])
            if lookups is None:
                raise MigrationError(
                    f"cannot read the filter {component['filter_str']!r} of "
                    f"{context.desired_dtype!r}."
                )
            filters.update(lookups)
        type_uid = component.get("type_uid")
        depths = _type_scope_depths(registry, type_uid) if type_uid else None
        targets.append((registry, field, filters, depths))

    def lookup(value: Any) -> Any:
        for registry, field, filters, depths in targets:
            match = _scoped_one(
                registry, {field: value, **filters}, field, value, depths
            )
            if match is None and standardize:
                replacement = _standardized(registry, value, field)
                if replacement is not None and replacement != value:
                    match = _scoped_one(
                        registry,
                        {field: replacement, **filters},
                        field,
                        replacement,
                        depths,
                    )
            if match is not None:
                return match
        return None

    return lookup


def _resolve_label(
    label: Any,
    *,
    feature: Any,
    admitted: bool,
    lookup: Callable[[Any], Any],
    mapping: Mapping[Any, Any] | None,
    transform: Callable[[Any], Any] | None,
    match_on: str | None,
    target: str,
) -> tuple[Any, str]:
    """Resolve one distinct old label to `(replacement, outcome)`.

    The categorical counterpart of :func:`_resolve_one`: a label the new dtype
    admits is `kept` as it is; otherwise `mapping` (by uid, then by name),
    the lookup and `transform` answer in turn, and nothing answering raises.
    """
    if admitted:
        return label, "kept"
    name = getattr(label, match_on or _default_field(type(label)), None)
    if mapping is not None:
        for key in (getattr(label, "uid", None), name):
            if key is not None and key in mapping:
                said = f"mapping sends {name!r}"
                return _replace(feature, mapping[key], lookup, said, target, "mapped")
    if name is not None and (match := lookup(name)) is not None:
        return match, "matched"
    if transform is None:
        raise MigrationError(
            f"label {name!r} ({type(label).__name__} {label.uid!r}) is not admitted "
            f"by {target}, and none of them is called {name!r}. Declare it in the "
            "plan, add a 'mapping' entry, or pass a 'transform' to rewrite or drop "
            "it."
        )
    said = f"transform rewrote {name!r}"
    return _replace(feature, transform(label), lookup, said, target, "transformed")


def _replace(
    feature: Any,
    answer: Any,
    lookup: Callable[[Any], Any],
    said: str,
    target: str,
    outcome: str,
) -> tuple[Any, str]:
    """What a `mapping` entry or a `transform` result resolves to.

    `None` drops the annotation, :data:`~lamindb_setup.evolve.NULL` /
    :data:`~lamindb_setup.evolve.DEFAULT` fill it in, and anything else is a name
    that has to resolve, reported as `outcome`.
    """
    replacement = _resolve_fill(feature, answer)
    if replacement is None:
        return _DROP, "dropped"
    if replacement is _NULL:
        return _NULL, "nulled"
    resolved = lookup(replacement)
    if resolved is None:
        raise MigrationError(
            f"{said} to {replacement!r}, which is not one of {target}. Declare it "
            "in the plan or answer with a label that exists."
        )
    return resolved, outcome


def relocate_to_str(
    context: MigrationContext,
    /,
    *,
    mapping: Mapping[Any, Any] | None = None,
    transform: Callable[[Any], Any] | None = None,
    field: str | None = None,
) -> None:
    """Relocate a feature's categorical values to strings (`cat` -> `str`).

    Renders each linked label as text (its name field, or `field`) and rewrites
    the annotation into the scalar store.  Every label renders, so no value is
    ever unmatched: `mapping` and `transform` are pure rewrites here, and
    `None` still drops the annotation, while
    :data:`~lamindb_setup.evolve.NULL` / :data:`~lamindb_setup.evolve.DEFAULT`
    null it out / substitute the feature's default.
    """
    stored = list(_iter_categorical_values(context.feature))
    # Resolve against the rendered text rather than the label object, so mapping
    # keys and transform arguments are the strings the plan author sees.
    rendered = [
        _StoredValue(
            row=entry.row, owners=entry.owners, value=_label_text(entry.value, field)
        )
        for entry in stored
    ]
    resolved = _resolve_all(
        rendered,
        feature=context.feature,
        lookup=lambda value: value,
        mapping=mapping,
        transform=transform,
        target="a string",
    )
    _apply(context, rendered, resolved)


def rewrite_values(
    context: MigrationContext,
    /,
    *,
    mapping: Mapping[Any, Any] | None = None,
    transform: Callable[[Any], Any] | None = None,
    element_wise: bool = False,
) -> None:
    """Rewrite a feature's scalar values for a narrowed scalar dtype.

    Each distinct value is coerced to the target dtype; one that will not coerce
    goes through `mapping` / `transform` exactly as elsewhere, so
    `transform=lambda _: None` drops the offending values,
    `transform=lambda _: 0` replaces them, and
    `transform=lambda _: NULL` nulls them out.

    `element_wise` coerces each element of a list to a `list[...]` target's
    element dtype instead (`list[str] -> list[int]`); a single value becomes
    a list of one, and an element answered with `None` is left out of its list.
    """
    if element_wise:
        element = _require_list_target(context, "map_values(element_wise=True)")
        _rewrite_element_wise(
            context,
            list(_iter_scalar_values(context.feature)),
            lookup=_cast_lookup(element),
            mapping=mapping,
            transform=transform,
            target=f"dtype {element!r}",
            as_labels=False,
        )
        return
    lookup = _scalar_lookup(context)
    stored = list(_iter_scalar_values(context.feature))
    resolved = _resolve_all(
        stored,
        feature=context.feature,
        lookup=lookup,
        mapping=mapping,
        transform=transform,
        target=f"dtype {context.desired_dtype!r}",
    )
    _apply(context, stored, resolved)


def _require_list_target(context: MigrationContext, helper: str) -> str:
    """The element dtype of the migration's `list[...]` target, or refuse."""
    from ._drift_data import _list_element_dtype

    element = _list_element_dtype(context.desired_dtype)
    if element is None:
        raise MigrationError(
            f"{helper} needs a list[...] dtype, but {context.feature.name!r} becomes "
            f"{context.desired_dtype!r}."
        )
    return element


def wrap_values(
    context: MigrationContext,
    /,
    *,
    mapping: Mapping[Any, Any] | None = None,
    transform: Callable[[Any], Any] | None = None,
) -> None:
    """Wrap each stored value in a list for a `list[...]` dtype (`x -> [x]`).

    Each value becomes the one element of its list, coerced to the element
    dtype exactly like :func:`rewrite_values` coerces a value (`str ->
    list[int]` turns `"42"` into `[42]`).  A categorical element dtype is
    :func:`relocate_to_records`'s.
    """
    from ._drift_data import _target_is_categorical

    element = _require_list_target(context, "wrap_in_list()")
    if _target_is_categorical(element):
        raise MigrationError(
            f"wrap_in_list() writes JSON lists, but {context.desired_dtype!r} links "
            "records. Use map_to_records(), which links a single value as a list "
            "of one."
        )
    rewrite_values(context, mapping=mapping, transform=transform, element_wise=True)


def unwrap_values(
    context: MigrationContext,
    /,
    *,
    pick: str | Callable[[list[Any]], Any] = "single",
    mapping: Mapping[Any, Any] | None = None,
    transform: Callable[[Any], Any] | None = None,
) -> None:
    """Unwrap each stored list to a single value for a scalar dtype (`[x] -> x`).

    `pick` chooses the element: `"single"` takes the only one and refuses a
    list holding more, `"first"` takes the first, and a callable receives the
    list and returns the element -- or `None` to drop the annotation,
    :data:`~lamindb_setup.evolve.NULL` / :data:`~lamindb_setup.evolve.DEFAULT` to
    fill it in.  An empty list is dropped.  The element is then coerced to the
    new dtype like :func:`rewrite_values` coerces a value, `mapping` and
    `transform` included.
    """
    from ._drift_data import _list_element_dtype, _target_is_categorical

    desired = context.desired_dtype
    if _list_element_dtype(desired) is not None or _target_is_categorical(desired):
        raise MigrationError(
            f"unwrap_list() writes a single JSON value, but {context.feature.name!r} "
            f"becomes {desired!r}."
        )
    stored = list(_iter_scalar_values(context.feature))
    picked = [(entry, _pick(entry.value, pick, context.feature)) for entry in stored]
    chosen = [value for _, value in picked if value is not _DROP and value is not _NULL]
    lookup = _cast_lookup(desired)
    resolved = {
        _value_key(raw): _resolve_one(
            raw,
            feature=context.feature,
            lookup=lookup,
            mapping=mapping,
            transform=transform,
            target=f"dtype {desired!r}",
        )
        for raw in _distinct(chosen)
    }
    report = context.report
    rows: list[tuple[_StoredValue, Any]] = []
    for entry, value in picked:
        if value is _DROP or value is _NULL:
            outcome = "dropped" if value is _DROP else "nulled"
            replacement = value
        else:
            replacement, outcome = resolved[_value_key(value)]
        setattr(report, outcome, getattr(report, outcome) + 1)
        rows.append((entry, replacement))
    _rewrite(context, rows)


def _pick(value: Any, pick: str | Callable[[list[Any]], Any], feature: Any) -> Any:
    """The element of a stored list `pick` chooses, :data:`_DROP` or :data:`_NULL`."""
    items = _elements_of(value)
    if callable(pick):
        answer = _resolve_fill(feature, pick(items))
        return _DROP if answer is None else answer
    if not items:
        return _DROP
    if pick == "first":
        return items[0]
    if pick != "single":
        raise MigrationError(
            f"unwrap_list() takes pick='single', pick='first' or a callable, not "
            f"{pick!r}."
        )
    if len(items) > 1:
        raise MigrationError(
            f"{feature.name!r} holds the list {value!r}; unwrap_list() with "
            "pick='single' refuses to choose one of its elements. Pass "
            "pick='first' or a callable choosing one."
        )
    return items[0]


def rescale_values(
    context: MigrationContext, /, *, factor: float | Callable[[Any], Any]
) -> None:
    """Rewrite every stored number of a feature for its new unit.

    `factor` multiplies each value (`0.001` turns `1500` mg into `1.5`
    g); a callable converts it instead (`lambda c: c * 9 / 5 + 32`).  Unlike
    `map_values`' `transform`, which only reaches a value that does not
    convert, this rewrites every one -- each already is a valid number.  The
    result is converted back to the feature's dtype, an `int` refusing a
    fraction, and a list is rescaled element by element.
    """
    from ._drift_data import _list_element_dtype

    dtype = context.desired_dtype
    element = _list_element_dtype(dtype) or dtype
    if (canonical := _canonical(element)) not in _NUMERIC_DTYPES:
        raise MigrationError(
            f"rescale() rewrites numbers, but {context.feature.name!r} is "
            f"{dtype!r}. Write the migration by hand."
        )
    convert = factor if callable(factor) else (lambda value: value * factor)
    cast = _SCALAR_CASTS[canonical]

    def rescaled(value: Any) -> Any:
        if isinstance(value, list):
            return [rescaled(item) for item in value]
        try:
            result = convert(value)
        except Exception as exc:
            raise MigrationError(
                f"rescale() cannot convert the stored value {value!r} of "
                f"{context.feature.name!r}: {exc}"
            ) from exc
        try:
            return cast(result)
        except (TypeError, ValueError) as exc:
            raise MigrationError(
                f"rescale() turns the stored value {value!r} of "
                f"{context.feature.name!r} into {result!r}, which is not a whole "
                f"{canonical}" + (" number" if canonical == "int" else "") + ". "
                "Rescale by a factor that keeps it one, or change the dtype too."
            ) from exc

    stored = list(_iter_scalar_values(context.feature))
    resolved = {
        _value_key(raw): (rescaled(raw), "transformed")
        for raw in _distinct([entry.value for entry in stored])
    }
    _apply(context, stored, resolved)


_NUMERIC_DTYPES = frozenset({"int", "float", "num"})


def _canonical(dtype: str) -> str:
    """`dtype` in its canonical spelling (`builtins.int` -> `int`)."""
    from ..authoring import canonical_dtype_str

    return canonical_dtype_str(dtype) or dtype


def clear_values(context: MigrationContext | FeatureRemovalContext, /) -> None:
    """Delete every value stored for the feature, scalar and categorical alike.

    The blunt instrument for a change no mapping can express: the feature keeps
    its identity and its new dtype but starts out unannotated.  Serves the
    feature's *deletion* too (see
    :class:`~lamindb_setup.evolve.authoring._migrations.FeatureRemovalContext`), where the
    point is that nothing is left keyed to a record about to be trashed.
    """
    stored = [
        *_iter_scalar_values(context.feature),
        *_iter_categorical_values(context.feature),
    ]
    _delete_all(stored)
    context.report.dropped += len(stored)


# === plan-facing factories ===================================================


def map_to_records(
    *,
    mapping: Mapping[Any, Any] | None = None,
    transform: Callable[[Any], Any] | None = None,
    standardize: bool = False,
    field: str | None = None,
    element_wise: bool = False,
    name: str = "map_to_records",
    reason: str = "",
) -> DataMigration:
    """A migration relocating scalar values onto existing records.

    The declarative form of :func:`relocate_to_records`, ready to hand to
    :meth:`~lamindb_setup.evolve.PlanContext.migrate`.  The records it maps
    onto must be declared in the plan (or already exist); the apply runs every
    migration after it has materialized the whole plan, so where in the plan file
    they are declared does not matter.
    """

    def migrate(context: MigrationContext) -> None:
        relocate_to_records(
            context,
            mapping=mapping,
            transform=transform,
            standardize=standardize,
            field=field,
            element_wise=element_wise,
        )

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "relocate scalar values onto existing records",
        targets=DTYPE_CHANGE_TARGETS,
    )


def remap_records(
    *,
    mapping: Mapping[Any, Any] | None = None,
    transform: Callable[[Any], Any] | None = None,
    match_on: str | None = None,
    standardize: bool = False,
    name: str = "remap_records",
    reason: str = "",
) -> DataMigration:
    """A migration relocating categorical values onto the new categorical dtype.

    The declarative form of :func:`relocate_records`: `cat[ULabel] ->
    cat[Record[<type>]]`, moving a vocabulary to another record type, a bionty
    source upgrade, dropping a union member.  The labels it maps onto must be
    declared in the plan (or already exist).
    """

    def migrate(context: MigrationContext) -> None:
        relocate_records(
            context,
            mapping=mapping,
            transform=transform,
            match_on=match_on,
            standardize=standardize,
        )

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "relocate categorical values onto the new dtype's labels",
        targets=DTYPE_CHANGE_TARGETS,
    )


def map_to_str(
    *,
    mapping: Mapping[Any, Any] | None = None,
    transform: Callable[[Any], Any] | None = None,
    field: str | None = None,
    name: str = "map_to_str",
    reason: str = "",
) -> DataMigration:
    """A migration rendering categorical values back to strings.

    The declarative form of :func:`relocate_to_str`.
    """

    def migrate(context: MigrationContext) -> None:
        relocate_to_str(context, mapping=mapping, transform=transform, field=field)

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "render categorical labels as strings",
        targets=DTYPE_CHANGE_TARGETS,
    )


def map_values(
    *,
    mapping: Mapping[Any, Any] | None = None,
    transform: Callable[[Any], Any] | None = None,
    element_wise: bool = False,
    name: str = "map_values",
    reason: str = "",
) -> DataMigration:
    """A migration rewriting scalar values for a narrowed scalar dtype.

    The declarative form of :func:`rewrite_values`.
    """

    def migrate(context: MigrationContext) -> None:
        rewrite_values(
            context, mapping=mapping, transform=transform, element_wise=element_wise
        )

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "rewrite scalar values for the new dtype",
        targets=DTYPE_CHANGE_TARGETS,
    )


def wrap_in_list(
    *,
    mapping: Mapping[Any, Any] | None = None,
    transform: Callable[[Any], Any] | None = None,
    name: str = "wrap_in_list",
    reason: str = "",
) -> DataMigration:
    """A migration wrapping each stored value in a list (`x -> [x]`).

    The declarative form of :func:`wrap_values`, for `str -> list[str]` and
    the like.
    """

    def migrate(context: MigrationContext) -> None:
        wrap_values(context, mapping=mapping, transform=transform)

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "wrap each stored value in a list",
        targets=DTYPE_CHANGE_TARGETS,
    )


def unwrap_list(
    pick: str | Callable[[list[Any]], Any] = "single",
    *,
    mapping: Mapping[Any, Any] | None = None,
    transform: Callable[[Any], Any] | None = None,
    name: str = "unwrap_list",
    reason: str = "",
) -> DataMigration:
    """A migration unwrapping each stored list to one value (`[x] -> x`).

    The declarative form of :func:`unwrap_values`, for `list[str] -> str` and
    the like.
    """

    def migrate(context: MigrationContext) -> None:
        unwrap_values(context, pick=pick, mapping=mapping, transform=transform)

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "unwrap each stored list to a single value",
        targets=DTYPE_CHANGE_TARGETS,
    )


def drop_values(
    *,
    name: str = "drop_values",
    reason: str = "",
) -> DataMigration:
    """A migration deleting every value stored for the feature.

    The declarative form of :func:`clear_values`.
    """

    def migrate(context: MigrationContext) -> None:
        clear_values(context)

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "drop every stored value",
        targets=DTYPE_CHANGE_TARGETS + FEATURE_REMOVAL_TARGETS,
    )


def drop_rows(*, name: str = "drop_rows", reason: str = "") -> DataMigration:
    """A migration trashing the records of a record type the plan deletes.

    Declared by natural key on a `RecordType` or `Sheet` the plan no longer
    declares, whose deletion is refused while it still has records::

        ctx.migrate("Sheet:Experiments", drop_rows())

    It moves every record typed by it to the trash -- what deleting the record
    type does to them anyway, now stated.  Sub-types are not rows: one the plan
    does not delete in the same apply still refuses the deletion.
    """

    def migrate(context: RecordTypeRemovalContext) -> None:
        rows = context.rows()
        context.report.dropped += rows.count()
        rows.update(branch_id=-1)

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "move its records to the trash with it",
        narrowing=ROWS_REMOVAL,
    )


def keep_validated(*, reason: str = "", name: str = "keep_validated") -> DataMigration:
    """A declaration vouching for the datasets a schema the plan deletes validated.

    Declared by natural key on a `Schema` the plan no longer declares, whose
    deletion is refused while artifacts or collections were validated against
    it::

        ctx.migrate("Schema:Legacy", keep_validated(reason="superseded by v2"))

    It writes nothing: the datasets keep pointing at the schema in the trash, as
    they did before the deletion was vetted, and the diff shows the `reason`.
    """

    def migrate(context: SchemaRemovalContext) -> None:
        """Nothing to rewrite: the declaration only vouches for the data."""

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "the datasets it validated keep pointing at it",
        keeps_values=True,
        narrowing=VALIDATED_REMOVAL,
    )


# === what a deleted type or schema leaves behind =======================


def _plan_kind(obj: Any) -> str | None:
    """The kind of a plan object, `None` for a natural key, a uid or `None`."""
    from ..core import Materializable

    return type(obj).__name__ if isinstance(obj, Materializable) else None


def _performed_by_the_apply(context: Any) -> None:
    """Nothing to run: the apply moves what the declaration names itself."""


def keep_rows(
    *, type: Any = None, reason: str = "", name: str = "keep_rows"
) -> DataMigration:
    """A declaration keeping the records of a record type the plan deletes.

    Declared by natural key on a `RecordType` or `Sheet` the plan no longer
    declares, whose deletion is refused while it still has records::

        ctx.migrate("Sheet:Old", keep_rows(reason="still referenced"))
        ctx.migrate("RecordType:Vocab", keep_rows(type=archive))

    Instead of going to the trash with it, every record is re-typed onto
    `type` -- a `RecordType` as a plan object, a natural key or a uid -- or
    left untyped when it is `None`.  Their feature values and names stay as
    they are.  A `Sheet` is refused as `type`: rows landing in a sheet must
    validate against its schema, which :func:`move_rows` checks.  So is a name
    already taken under `type`, and a feature filtering on the deleted type
    that holds these records as values, since they would no longer satisfy it.
    """
    if _plan_kind(type) == "Sheet":
        raise ValueError(
            "keep_rows(type=...) takes a RecordType, not a Sheet: rows that land "
            "in a sheet must validate against its schema, which move_rows(to=...) "
            "checks"
        )
    where = "untyped" if type is None else "under another record type"
    return DataMigration(
        name=name,
        migrate=_performed_by_the_apply,
        reason=reason or f"keep its records, {where}",
        narrowing=ROWS_REMOVAL,
        relocates_to=type,
        relocation="keep_rows",
    )


def move_rows(
    *,
    to: Any,
    reason: str = "",
    index_name_conflict: IndexNameConflict | None = None,
    name: str = "move_rows",
) -> DataMigration:
    """A declaration moving the records of a record type the plan deletes.

    Declared by natural key on a `RecordType` or `Sheet` the plan no longer
    declares::

        ctx.migrate("Sheet:Old", move_rows(to=new_sheet, reason="merged"))

    Every record is re-typed onto `to` -- a `Sheet` or `RecordType` as a
    plan object, a natural key or a uid -- instead of going to the trash.  Rows
    moving into a sheet are checked against its schema first, exactly as a
    sheet pointed at another schema is: curated, and each must carry every
    member the schema requires.  They keep their names, which a target sheet
    keyed by another index than the source's would have to re-key, so that is
    refused, and so is a name already taken there.  A row whose name differs
    from its value for the shared index is settled by `index_name_conflict`
    (`"keep_name"` or `"keep_feature"`), as for an index change.  A target
    sheet's exports go stale, so it also needs :func:`reexport_sheets` or
    :func:`keep_stale_exports`.
    """
    if to is None:
        raise ValueError("move_rows(to=...) needs the Sheet or RecordType to move to")
    if (kind := _plan_kind(to)) is not None and kind not in ("Sheet", "RecordType"):
        raise ValueError(f"move_rows(to=...) takes a Sheet or RecordType, not a {kind}")
    return DataMigration(
        name=name,
        migrate=_performed_by_the_apply,
        reason=reason or "move its records to another record type",
        narrowing=ROWS_REMOVAL,
        index_name_conflict=index_name_conflict,
        relocates_to=to,
        relocation="move_rows",
    )


def revalidate_against(
    *,
    schema: Any,
    reason: str = "",
    narrower: bool = False,
    name: str = "revalidate_against",
) -> DataMigration:
    """A declaration pointing the datasets a deleted schema validated at another.

    Declared by natural key on a `Schema` the plan no longer declares, whose
    deletion is refused while artifacts or collections were validated against
    it::

        ctx.migrate("Schema:Legacy", revalidate_against(schema=v2))

    Each artifact and collection -- and every artifact slot link -- naming it
    is pointed at `schema`, a plan object, a natural key or a uid, instead of
    being left on a trashed one.  Nothing is re-curated in place, so `schema`
    has to admit everything the deleted one did: the same members (in their
    order under `ordered_set`) plus optional ones, no set flag turned on,
    `flexible` not turned off under `maximal_set`, the same index, otype,
    itype and dtype -- and for a composite the same slots, each judged alike.
    `narrower=True` points them at one that admits less, which `reason`
    has to vouch for.  A sheet export stays with the deleted schema: the CSV
    was written against it.
    """
    if schema is None:
        raise ValueError("revalidate_against(schema=...) needs the Schema to use")
    if (kind := _plan_kind(schema)) is not None and kind != "Schema":
        raise ValueError(f"revalidate_against(schema=...) takes a Schema, not a {kind}")
    if narrower and not reason:
        raise ValueError(
            "revalidate_against(narrower=True) needs a reason: it vouches for "
            "datasets the schema would not admit"
        )
    return DataMigration(
        name=name,
        migrate=_performed_by_the_apply,
        reason=reason or "point its datasets at a schema admitting them all",
        narrowing=VALIDATED_REMOVAL,
        relocates_to=schema,
        relocation="revalidate_against",
        narrower=narrower,
    )


def adopt_children(
    *, to: Any = None, reason: str = "", name: str = "adopt_children"
) -> DataMigration:
    """A declaration re-parenting the children of a type the plan deletes.

    Declared by natural key on a `FeatureType`, `SchemaType`,
    `RecordType` or `Sheet` the plan no longer declares, whose deletion is
    refused while sub-types, features or schemas the plan does not delete are
    typed under it::

        ctx.migrate("FeatureType:legacy", adopt_children(to=current))

    Each is moved under `to` -- a type of the same kind, as a plan object, a
    natural key or a uid -- or to the root when it is `None`, with its uid and
    its own children.  A name already taken there is refused, and so is a child
    another plan manages.  A record type's records are rows, not children: they
    take :func:`drop_rows`, :func:`keep_rows` or :func:`move_rows`.
    """
    where = "the root" if to is None else "another type"
    return DataMigration(
        name=name,
        migrate=_performed_by_the_apply,
        reason=reason or f"move its children to {where}",
        narrowing=CHILDREN_REMOVAL,
        relocates_to=to,
        relocation="adopt_children",
    )


# === seeded records: opting into the plan's edits, dropping one ===============


def update_record(
    *,
    values: bool = True,
    description: bool = True,
    reference: bool = True,
    name: str = "update_record",
    reason: str = "",
) -> DataMigration:
    """A declaration opting a seeded record into the plan's edits.

    A plan seeds its records: it creates one the instance lacks and otherwise
    leaves it alone, so a corrected annotation or description never reaches an
    instance that already has the record.  Declared on the record, it writes the
    plan's version over the instance's::

        hepatocyte = Record("hepatocyte", type=cell_types, features={"germ_layer": "endoderm"})
        ctx.migrate(hepatocyte, update_record())

    `values` writes each feature value the plan declares that differs from
    the stored one, straight into its link table or `RecordJson` so the row is
    not re-curated against a schema mid-apply; `description` and
    `reference` (with `reference_type`) set those attributes.  Only what the
    plan declares is written: a feature it leaves out, or a `None`, keeps its
    stored value.  Once the instance matches the plan the declaration is a
    no-op, so it can stay in place.
    """
    flags = (("values", values), ("description", description), ("reference", reference))
    updates = tuple(aspect for aspect, enabled in flags if enabled)
    if not updates:
        raise ValueError(
            "update_record() has nothing to update: enable values, description "
            "or reference"
        )

    def migrate(context: RecordUpdateContext) -> None:
        record = context.record
        if context.fields:
            # Written as columns, so lamindb's save hooks do not re-validate the
            # record against its type mid-apply.
            type(record).objects.filter(pk=record.pk).update(**context.fields)
            for attribute, value in context.fields.items():
                setattr(record, attribute, value)
        labels: dict[Any, Any] = {}
        for feature, value in context.values.items():
            _clear_record_values(record, feature)
            _annotate(feature, record, value, labels=labels)
        context.report.updated += len(context.fields) + len(context.values)

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason
        or f"write the plan's {' and '.join(updates)} over the instance's",
        narrowing=RECORD_UPDATE,
        updates=updates,
    )


def _clear_record_values(record: Any, feature: Any) -> None:
    """Delete what `feature` stores on `record`, without re-curating it."""
    if not _is_categorical(feature):
        feature.links_recordjson.filter(record=record).delete()
        return
    for manager in _iter_categorical_managers(feature):
        subject = link_subject_field(manager.model)
        if subject is not None and subject.related_model is type(record):
            manager.filter(**{subject.name: record}).delete()


def drop_record(
    *,
    relocate_to: Any = None,
    name: str = "drop_record",
    reason: str = "",
) -> DataMigration:
    """A declaration trashing a seeded record the plan no longer declares.

    A plan never deletes a record it stops declaring: a vocabulary value's
    lifecycle is the instance's.  Declared by the record's natural key, it moves
    the record to the trash::

        ctx.migrate("RecordType:CellTypes/Record:hepatocite", drop_record())

    The removal is refused while other objects still hold the record as a
    categorical value -- a record's, an artifact's or a run's annotation.
    `relocate_to` -- a plan `Record`, a natural key or a uid -- moves each of
    them onto that record first, which every annotating feature's dtype has to
    admit and the plan must not drop as well.  It is refused, too, while records
    still name it as their parent: `relocate_to` moves annotations, not
    children.  Once the record is gone the declaration is a no-op.
    """

    def migrate(context: RecordRemovalContext) -> None:
        if context.relocate_to is not None:
            context.report.relocated += relocate_value_links(
                context.links(), context.relocate_to
            )

    default_reason = (
        "move its annotations onto another record and trash it"
        if relocate_to is not None
        else "trash it"
    )
    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or default_reason,
        narrowing=RECORD_REMOVAL,
        relocates_to=relocate_to,
    )


def relocate_value_links(links: Iterable[ValueLinks], target: Any) -> int:
    """Point each link row in `links` at `target` instead; return how many.

    A row whose annotated object already holds `target` for the same feature
    -- or, for a label attached without one (`artifact.records.add(...)`),
    without a feature as well -- would duplicate it, so it is deleted instead:
    either way the object ends up annotated with `target`, once.
    """
    from django.db.models import Exists, OuterRef

    moved = 0
    for link in links:
        if link.subject is not None:
            holding = link.model.objects.filter(
                **{
                    link.label.name: target,
                    link.subject.attname: OuterRef(link.subject.attname),
                }
            )
            # Split by hand: NULL never equals NULL, so the featureless rows
            # would never match their duplicate by feature_id.
            for featureless, same_feature in (
                (False, {"feature_id": OuterRef("feature_id")}),
                (True, {"feature__isnull": True}),
            ):
                duplicates = link.rows.filter(feature__isnull=featureless).filter(
                    Exists(holding.filter(**same_feature))
                )
                moved += duplicates.count()
                duplicates.delete()
        moved += link.rows.update(**{link.label.name: target})
    return moved


# Aux key marking a feature whose values were deliberately kept when it was
# deleted, so `repair stranded-values` can tell an accepted retention from the
# accident it exists to find.
_VALUES_RETAINED_AUX = "_valuesRetained"


def values_retained(feature: Any) -> bool:
    """Whether `feature` was deleted with :func:`keep_values`.

    Its values outliving it is then the declared intent, not an accident.  The
    mark is on the feature's `_aux`, so it survives the move to the trash.
    """
    aux = getattr(feature, "_aux", None) or {}
    return bool(aux.get(_VALUES_RETAINED_AUX))


def keep_values(
    *,
    name: str = "",
    reason: str = "",
) -> DataMigration:
    """A migration that deletes a feature and *keeps* its stored values.

    The non-destructive resolution for a feature the plan no longer declares, to
    hand to :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>`
    by natural key::

        ctx.migrate("FeatureType:ft/Feature:retired", keep_values())

    Nothing is written and nothing is deleted; the declaration states that the
    values outliving the feature is intended, which is what unblocks the
    deletion.  Be aware of what that leaves: the values stay keyed to a trashed
    record, so they are readable only by name and a later feature of the same
    name will not see them.

    The acceptance is recorded on the feature itself so it survives the deletion,
    which is how `repair stranded-values` knows not to report it.
    """

    def migrate(context: FeatureRemovalContext) -> None:
        feature = context.feature
        feature._aux = feature._aux or {}
        feature._aux[_VALUES_RETAINED_AUX] = 1
        feature.save()
        context.report.note("kept the stored values of the deleted feature")

    return DataMigration(
        name=name or "keep_values",
        migrate=migrate,
        reason=reason or "keep the values of a feature the plan deletes",
        keeps_values=True,
        targets=FEATURE_REMOVAL_TARGETS,
    )


# === schema members: backfilling a newly-required one ========================


def backfill_members(
    context: MemberMigrationContext,
    /,
    value: Any | Callable[[Any, Any], Any] = DEFAULT,
    *,
    feature: str | None = None,
) -> None:
    """Annotate every row that omits one of the schema's newly-required members.

    `value` is either the value to write for every such row, or a callable
    receiving `(feature, row)` and returning one -- returning `None` leaves
    the row alone, which then fails the apply, since the engine requires the
    backfill to cover every row.

    Two sentinels stand for a value the *feature* defines rather than the
    declaration, and either may equally be returned from the callable, so a
    backfill can decide row by row:

    * :data:`~lamindb_setup.evolve.DEFAULT` (the default) fills the row with the
      member's `default_value`, falling back to null when it declares none --
      the same order lamindb's own `DataFrameCurator.standardize()` fills a
      missing column in.  One declaration therefore covers members of different
      dtypes.
    * :data:`~lamindb_setup.evolve.NULL` fills the row with null.

    Both are refused for a member that is not nullable and defines no default,
    since a null there would not validate.  A null is not *written*: lamindb has
    no stored null -- `add_values` skips one, a categorical value is a foreign
    key, and the JSON stores are `NOT NULL` -- so an un-annotated row is what a
    null member looks like, whatever its dtype.  The row is instead declared
    settled with
    :meth:`~lamindb_setup.evolve.MemberMigrationContext.accept_null`,
    which is what exempts it from the engine's post-check; a row merely left
    alone (the callable returning `None`) still fails the apply.

    `feature` narrows this to the member of that name, so a schema narrowing
    several members can take one declaration per member (with a value of the
    right dtype for each) instead of one callable branching on the feature.  A
    name that is not among the newly-required members is a no-op, which is what
    keeps such a declaration idempotent once its member has been settled.

    The write deliberately bypasses `add_values`: that re-curates the *whole*
    row against its type's schema, which a backfill by definition cannot satisfy
    yet -- it is running precisely because the row does not, and a row missing
    two required members would have its first repair rejected for still missing
    the second.  One annotation is one row in the feature's value table, so it is
    written there directly.
    """
    resolve = value if callable(value) else (lambda member, row: value)
    labels: dict[Any, Any] = {}
    for member in context.features:
        if feature is not None and member.name != feature:
            continue
        writes = [
            (row, _resolve_fill(member, resolved))
            for row in context.missing(member)
            if (resolved := resolve(member, row)) is not None
        ]
        for row, resolved in writes:
            if resolved is _NULL:
                context.accept_null(member, row)
                context.report.nulled += 1
                continue
            _annotate(member, row, resolved, labels=labels)
            context.report.transformed += 1


def fill_missing(
    value: Any | Callable[[Any, Any], Any] = DEFAULT,
    *,
    feature: str | None = None,
    name: str = "",
    reason: str = "",
) -> DataMigration:
    """A migration giving every row that omits a newly-required member a value.

    The declarative form of :func:`backfill_members`, to hand to
    :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>` on the
    *schema* whose optional set the plan narrows.  Pass `feature` to scope it
    to one member and declare one per member::

        ctx.migrate(Experiment.schema, fill_missing("bla", feature="ngs_instrument"))
        ctx.migrate(Experiment.schema, fill_missing(-1, feature="replicate"))

    Omitting `value` fills every member with its own `default_value`, and
    :data:`~lamindb_setup.evolve.NULL` fills it with null -- which works for a
    record dtype as much as for a scalar one, since a null annotation is an
    absent one either way::

        ctx.migrate(Experiment.schema, fill_missing())  # each member's default
        ctx.migrate(Experiment.schema, fill_missing(NULL, feature="cell_line"))
    """

    def migrate(context: MemberMigrationContext) -> None:
        backfill_members(context, value, feature=feature)

    return DataMigration(
        name=name
        or ("fill_missing" if feature is None else f"fill_missing[{feature}]"),
        migrate=migrate,
        reason=reason
        or (
            "backfill rows that omitted a newly-required member"
            if feature is None
            else f"backfill rows that omitted {feature!r}"
        ),
        narrowing=MEMBER_REQUIRED,
    )


def fill_nulls(
    value: Any | Callable[[Any, Any], Any] = DEFAULT,
    *,
    name: str = "fill_nulls",
    reason: str = "",
) -> DataMigration:
    """A migration giving a value to every row that is null for a feature made non-nullable.

    Hand it to :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>` on
    the *feature* whose `nullable` the plan turns off::

        ctx.migrate(organism, fill_nulls("unknown"))

    A row that omits the feature is null for it: lamindb stores no null, and the
    row reads as one in every tabular view of its sheet, an export included.  So
    every row of a sheet whose schema declares the feature is backfilled where it
    omits it, like :func:`fill_missing` does for a member made required, and the
    apply fails if one is left.  `value` works as there -- a value, a
    `(feature, row) -> value` callable, or :data:`~lamindb_setup.evolve.DEFAULT`
    for the feature's `default_value` -- except that a null is refused: it is
    what the change forbids.

    Datasets validated against such a schema cannot be backfilled; see
    :func:`accept_non_null`.
    """

    def migrate(context: MemberMigrationContext) -> None:
        backfill_members(context, value)

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "backfill the rows that are null for it",
        narrowing=NULLABILITY,
    )


def rescale(
    factor: float | Callable[[Any], Any],
    *,
    name: str = "rescale",
    reason: str = "",
) -> DataMigration:
    """A migration rewriting a feature's numbers for the unit the plan gives it.

    Hand it to :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>` on
    the feature whose `unit` the plan changes::

        ctx.migrate(dose, rescale(0.001))  # was "mg", now "g"

    The declarative form of :func:`rescale_values`.
    """

    def migrate(context: MigrationContext) -> None:
        rescale_values(context, factor=factor)

    described = "a conversion" if callable(factor) else f"a factor of {factor!r}"
    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or f"rescale every value by {described}",
        narrowing=UNIT_CHANGE,
    )


def detach_from_schemas(
    schemas: Sequence[str], *, reason: str = "", name: str = "detach_from_schemas"
) -> DataMigration:
    """A declaration removing a deleted feature from schemas no plan manages.

    Deleting a feature is refused while a schema outside the plan still lists it
    as a member.  Declared by the feature's natural key, naming each such schema
    by uid or natural key, this removes the member from them before the feature
    is trashed::

        ctx.migrate(
            "FeatureType:ft/Feature:retired",
            detach_from_schemas(["Schema:HandMade"], reason="retired"),
        )

    A bare `Schema:<name>` names a schema under any schema type too, so one
    matching several schemas listing the feature is refused: name each by uid or
    full natural key.  Each schema loses its features in a single save and is
    named in the diff; it is rehashed with the composites slotting it, and one
    landing on another live schema's content absorbs it when it is an inferred
    schema, merges it when a :func:`merge_schema` declared on the schema names
    it, and is refused otherwise -- as `plan` previews.  Its sheet exports are
    vetted like a plan schema's.  A schema a plan manages is refused -- remove
    the member in that plan -- and so is one using the feature as its index.  A
    `maximal_set` one is judged on its configuration, as a plan schema
    dropping a member is: datasets still carrying the feature would have a
    member it no longer allows, so it is refused unless the deletion declares
    :func:`drop_values`.  It does nothing about the feature's stored values,
    which its deletion answers for.
    """
    if not schemas:
        raise ValueError("detach_from_schemas() needs the schemas to detach from")

    def migrate(context: Any) -> None:
        """Nothing to rewrite: the apply detaches the member itself."""

    listed = ", ".join(schemas)
    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or f"removed from {listed}",
        keeps_values=True,
        narrowing=DETACH_MEMBER,
        detaches=tuple(schemas),
    )


def merge_schema(
    other: str, *, reason: str = "", name: str = "merge_schema"
) -> DataMigration:
    """A declaration letting a plan schema take over another live schema's content.

    lamindb keeps one live schema per content hash, so an edit that gives a plan
    schema the content of another one is refused -- unless that other is an
    inferred schema nothing names.  Hand this to :meth:`ctx.migrate
    <lamindb_setup.evolve.PlanContext.migrate>` on the plan schema, naming the
    other by uid or natural key::

        ctx.migrate(schema, merge_schema("Schema:HandMade", reason="a duplicate"))

    The apply then points everything that references the other schema --
    artifacts, collections and sheets it validates, the composites slotting it,
    its links -- at the plan schema, and trashes it with its own members and
    slots intact.  A schema a plan manages -- one this plan has locked, or one
    a sibling plan applied alongside declares -- is not merged away.
    """

    def migrate(context: Any) -> None:
        """Nothing to rewrite: the apply merges the schemas when it saves."""

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or f"{other} holds the same content",
        keeps_values=True,
        narrowing=MERGE_SCHEMA,
        merges=other,
    )


def accept_maximal_set(
    *, reason: str = "", name: str = "accept_maximal_set"
) -> DataMigration:
    """A declaration vouching for the data a schema validated before `maximal_set`.

    Hand it to :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>` on
    a schema the plan turns `maximal_set` on for::

        ctx.migrate(schema, accept_maximal_set(reason="extra columns are dropped"))

    A dataset that carried a column the schema does not declare, or a sheet row
    holding a value for such a feature, would no longer validate.  This
    declaration is the plan author vouching for them: it writes nothing and
    shows up in the diff with its `reason`.
    """

    def migrate(context: Any) -> None:
        """Nothing to rewrite: the declaration only vouches for the data."""

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "the data it validated carries no other column",
        keeps_values=True,
        narrowing=MAXIMAL_SET,
    )


def accept_minimal_set(
    *, reason: str = "", name: str = "accept_minimal_set"
) -> DataMigration:
    """A declaration vouching for the datasets a schema validated before `minimal_set`.

    Hand it to :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>` on
    a schema the plan turns `minimal_set` on for::

        ctx.migrate(schema, accept_minimal_set(reason="every frame has all columns"))

    Every member not marked optional becomes required.  The rows of a sheet are
    checked and backfilled (see :func:`~lamindb_setup.evolve.fill_missing`), but
    an artifact or collection validated without one of those columns cannot be
    re-curated in place.  This declaration is the plan author vouching for them:
    it writes nothing and shows up in the diff with its `reason`.
    """

    def migrate(context: Any) -> None:
        """Nothing to rewrite: the declaration only vouches for the datasets."""

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "the datasets it validated carry every member",
        keeps_values=True,
        narrowing=MINIMAL_SET,
    )


def accept_ordered_set(
    *, reason: str = "", name: str = "accept_ordered_set"
) -> DataMigration:
    """A declaration vouching for the data a schema validated before `ordered_set`.

    Hand it to :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>` on
    a schema the plan turns `ordered_set` on for.  The column order of a
    dataset already validated is not reliably recorded, so it cannot be
    re-checked; this declaration vouches for it, writes nothing and shows up in
    the diff with its `reason`.
    """

    def migrate(context: Any) -> None:
        """Nothing to rewrite: the declaration only vouches for the data."""

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "the datasets it validated have their columns in order",
        keeps_values=True,
        narrowing=ORDERED_SET,
    )


def accept_slot_change(
    *, reason: str = "", name: str = "accept_slot_change"
) -> DataMigration:
    """A declaration vouching for the datasets a composite validated before its slots changed.

    Hand it to :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>` on
    a composite schema the plan gives a slot, or a component that admits less::

        ctx.migrate(anndata_schema, accept_slot_change(reason="every file has var"))

    A dataset validated against the composite was never checked against the new
    component, and cannot be re-checked in place, so the change is refused while
    such datasets exist.  This declaration is the plan author vouching for them:
    it writes nothing and shows up in the diff with its `reason`.
    """

    def migrate(context: Any) -> None:
        """Nothing to rewrite: the declaration only vouches for the datasets."""

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "the datasets it validated fit the new slots",
        keeps_values=True,
        narrowing=SLOT_CHANGE,
    )


def revalidate_rows(
    *, reason: str = "", name: str = "revalidate_rows"
) -> DataMigration:
    """A declaration asking the apply to check a sheet's rows against its new schema.

    Hand it to :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>` on
    a sheet the plan points at another schema -- or on a record type it promotes
    to a sheet -- that already has rows::

        ctx.migrate(samples, revalidate_rows())

    The apply curates every row against the new schema with lamindb's
    `DataFrameCurator`, checks that each carries a value for every member the
    new schema requires, and goes ahead only when all of them validate, naming
    what does not otherwise.  It rewrites nothing: rows that do not fit have to
    be fixed first.  `--allow-data-validated-changes` does the same for every
    sheet at once.  The sheet's exports are a separate question, answered by
    :func:`reexport_sheets` or :func:`keep_stale_exports` on the same sheet.
    """

    def migrate(context: Any) -> None:
        """Nothing to rewrite: the apply curates the rows itself."""

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "every row validates against the new schema",
        keeps_values=True,
        narrowing=SHEET_SCHEMA,
    )


def accept_unit_change(
    *, reason: str = "", name: str = "accept_unit_change"
) -> DataMigration:
    """A declaration vouching that a feature's values already are in its new unit.

    Hand it to :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>` on
    the feature whose `unit` the plan changes, when the old unit was only
    mislabelled or misspelled (`"mg "` -> `"mg"`)::

        ctx.migrate(dose, accept_unit_change(reason="a trailing space"))

    It writes nothing and shows up in the diff with its `reason`; to convert
    the values instead, declare :func:`rescale`.
    """

    def migrate(context: Any) -> None:
        """Nothing to rewrite: the declaration only vouches for the values."""

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "the stored values already are in the new unit",
        keeps_values=True,
        narrowing=UNIT_CHANGE,
    )


def accept_non_null(
    *, reason: str = "", name: str = "accept_non_null"
) -> DataMigration:
    """A declaration vouching that datasets validated with a feature hold no nulls for it.

    Hand it to :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>` on
    the *feature* whose `nullable` the plan turns off::

        ctx.migrate(organism, accept_non_null(reason="every obs frame is complete"))

    A dataset validated against a schema declaring the feature may have held
    nulls for it while it was nullable, and its cells cannot be re-checked in
    place, so the change is refused while such datasets exist.  This declaration
    is the plan author vouching for them: it writes nothing and shows up in the
    diff with its `reason`.  Rows of a sheet are backfilled instead (see
    :func:`fill_nulls`).
    """

    def migrate(context: Any) -> None:
        """Nothing to rewrite: the declaration only vouches for the data."""

    return DataMigration(
        name=name,
        migrate=migrate,
        reason=reason or "the datasets validated with it hold no nulls for it",
        keeps_values=True,
        narrowing=NULLABILITY,
    )


def _annotate(
    feature: Any, owner: Any, value: Any, *, labels: dict[Any, Any] | None = None
) -> None:
    """Write one annotation of `feature` on `owner` without re-curating it.

    A categorical value is a row in the `(owner registry, value registry)` link
    table, which :func:`_plan_link_writes` already knows how to build.  A plan
    declares such a backfill by *name* (`fill_missing("v1")`) rather than by
    record, so the name is resolved against the member's own dtype first (see
    :func:`_label_for`) -- the JSON store is not an alternative here, lamindb
    raising on a scalar row found for a categorical feature.  A `list[cat[...]]`
    member takes a sequence of names, resolved element by element into one link
    row each, which is how lamindb stores a multi-valued annotation.

    A scalar value on a `Record` is a `RecordJson` row, reached through the
    feature's own reverse manager rather than an import path.

    Anything else -- a scalar on an `Artifact` or `Collection`, whose scalar
    values live in the shared, many-to-many `JsonValue` -- falls back to
    `add_values`, which may reject the write for the very schema the backfill is
    repairing; such a row is better annotated by a hand-written migration.

    `labels` memoizes the names resolved so far, for a caller annotating many
    rows with the same few values.
    """
    writes = [(owner, label) for label in _labels_for(feature, value, labels=labels)]
    rows = _plan_link_writes(feature, writes)
    if rows:
        _save_links(rows)
        return
    if type(owner).__name__ == "Record" and not _is_categorical(feature):
        feature.links_recordjson.model(
            record=owner, feature=feature, value=_jsonable(value)
        ).save()
        return
    _write(owner, feature, value)


def _jsonable(value: Any) -> Any:
    """`value` as lamindb stores it in a JSON column: a date as its ISO string."""
    if isinstance(value, datetime.date):  # a datetime is a date, too
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    return value


def _labels_for(
    feature: Any, value: Any, *, labels: dict[Any, Any] | None = None
) -> list[Any]:
    """`value` as the list of link targets a categorical write needs.

    A scalar feature is left alone -- it is written as a value, not as a link --
    and so is a value that is already a saved record.  Everything else is a name
    (or, for `list[cat[...]]`, a sequence of them) to resolve against the
    feature's own dtype.
    """
    if not _is_categorical(feature):
        return [value]
    values = list(value) if isinstance(value, (list, tuple, set)) else [value]
    return [
        item if _is_saved(item) else _label_for(feature, item, labels=labels)
        for item in values
    ]


def _label_for(
    feature: Any, value: Any, *, labels: dict[Any, Any] | None = None
) -> Any:
    """The existing record `value` names in `feature`'s categorical dtype.

    Resolved from the *live* dtype rather than from a plan-declared one, because
    a member backfill has no plan feature to read a target off: the change it
    answers for is the schema's, and the member may not even be in the plan.
    Each union component is tried in turn, narrowed to the labels it admits --
    its type's subtree (see :func:`_scoped_one`) and its filters; nothing is
    ever created, so a name with no record raises.  `labels` (when given)
    remembers each resolved name, keyed by feature.
    """
    from lamindb.models.feature import parse_dtype

    key = (feature.pk, value)
    try:
        if labels is not None and key in labels:
            return labels[key]
    except TypeError:  # an unhashable value is resolved, just not remembered
        labels = None
    dtype = feature.dtype_as_str
    for component in parse_dtype(dtype):
        registry = component["registry"]
        field = component["field_str"] or _default_field(registry)
        filters: dict[str, Any] = {}
        if component.get("filter_str"):
            lookups = DEFAULT_MODELS.cat_filter_lookups(component["filter_str"])
            if lookups is None:
                continue
            filters.update(lookups)
        type_uid = component.get("type_uid")
        depths = _type_scope_depths(registry, type_uid) if type_uid else None
        match = _scoped_one(registry, {field: value, **filters}, field, value, depths)
        if match is not None:
            if labels is not None:
                labels[key] = match
            return match
    raise MigrationError(
        f"cannot backfill {feature.name!r} with {value!r}: no record of "
        f"{dtype} carries that value. Declare it in the plan, or backfill with "
        "a value that exists."
    )


# === schema members: clearing a removed one ==================================


def _unlink_shared(row: Any, owner: Any) -> None:
    """Detach `owner` from a shared value `row` without deleting the row.

    Only `JsonValue` is shared -- the same row annotates every artifact and run
    linked to it -- so a drop scoped to some of them must remove the links rather
    than the row, which the others still need.
    """
    for relation in ("artifacts", "runs"):
        manager = getattr(row, relation, None)
        if manager is not None and manager.filter(pk=owner.pk).exists():
            manager.remove(owner)
            return
    raise MigrationError(
        f"cannot detach {type(owner).__name__} from a shared "
        f"{type(row).__name__} row. Write the migration by hand for this store."
    )


def clear_member_values(
    context: MemberRemovalContext,
    /,
    *,
    feature: str | None = None,
) -> None:
    """Delete the values a schema's removed members hold on the rows it validates.

    `feature` narrows this to the member of that name, so a schema dropping
    several members can take one declaration per member.  A name that is not
    among the removed members is a no-op, which is what keeps such a declaration
    idempotent once its member is gone.

    Only the rows *this* schema validates are touched (see
    :meth:`~lamindb_setup.evolve.MemberRemovalContext.carrying`).  The
    same feature may hold values on rows some *other* schema validates, and those
    are that schema's business -- deleting them here would invalidate it.

    A value row annotating several objects at once (`JsonValue`, shared across
    the artifacts and runs it is linked to) is only *unlinked* from the objects in
    scope; the row survives for the rest.  Every other store holds one annotation
    per row, so there the row itself goes.
    """
    for member in context.features:
        if feature is not None and member.name != feature:
            continue
        scope = {(type(row).__name__, row.pk) for row in context.carrying(member)}
        if not scope:
            continue
        doomed: list[_StoredValue] = []
        for stored in (
            *_iter_scalar_values(member),
            *_iter_categorical_values(member),
        ):
            hits = [
                owner
                for owner in stored.owners
                if (type(owner).__name__, owner.pk) in scope
            ]
            if not hits:
                continue
            if len(hits) == len(stored.owners):
                doomed.append(stored)
                continue
            for owner in hits:
                _unlink_shared(stored.row, owner)
            context.report.dropped += len(hits)
        _delete_all(doomed)
        context.report.dropped += len(doomed)


def drop_member_values(
    *,
    feature: str | None = None,
    name: str = "",
    reason: str = "",
) -> DataMigration:
    """A migration deleting the values a removed schema member still holds.

    The declarative form of :func:`clear_member_values`, to hand to
    :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>` on the
    *schema* the plan removes the member from::

        ctx.migrate(Experiment.schema, drop_member_values(feature="ngs_instrument"))

    This is destructive and irreversible: the value rows have no trash state.  Use
    :func:`keep_member_values` to retire a member without touching the data.
    """

    def migrate(context: MemberRemovalContext) -> None:
        clear_member_values(context, feature=feature)

    return DataMigration(
        name=name
        or (
            "drop_member_values"
            if feature is None
            else f"drop_member_values[{feature}]"
        ),
        migrate=migrate,
        reason=reason
        or (
            "delete the values of members the schema no longer declares"
            if feature is None
            else f"delete the values of {feature!r}"
        ),
        narrowing=MEMBER_REMOVAL,
    )


def keep_member_values(
    *,
    feature: str | None = None,
    name: str = "",
    reason: str = "",
) -> DataMigration:
    """A migration that retires a schema member and *keeps* its values.

    The non-destructive counterpart of :func:`drop_member_values`: the schema
    stops declaring the member, and the values already stored for it stay on the
    rows as undeclared extra data.  Nothing is written and nothing is deleted --
    the declaration exists to state that the leftover values are intended, which
    is what unblocks the removal::

        ctx.migrate(Experiment.schema, keep_member_values(feature="replicate"))

    Refused for a `maximal_set` schema: that is exactly the kind that does not
    tolerate a member it does not declare, so keeping the values there would
    invalidate the very rows carrying them.  Use :func:`drop_member_values`.
    """

    def migrate(context: MemberRemovalContext) -> None:
        for member in context.features:
            if feature is not None and member.name != feature:
                continue
            context.keep(member)
            context.report.note(f"kept the stored values of {member.name!r}")

    return DataMigration(
        name=name
        or (
            "keep_member_values"
            if feature is None
            else f"keep_member_values[{feature}]"
        ),
        migrate=migrate,
        reason=reason
        or (
            "keep the values of members the schema no longer declares"
            if feature is None
            else f"keep the values of {feature!r}"
        ),
        keeps_values=True,
        narrowing=MEMBER_REMOVAL,
    )


# === stale sheet exports =====================================================


def rewrite_exports(
    context: SheetExportContext,
    /,
    *,
    sheet: str | None = None,
) -> None:
    """Write a new version of every stale export, from the sheet's current rows.

    The imperative twin of :func:`reexport_sheets`, for a hand-written
    `DataMigration` that has something to do before or after.  `sheet`
    narrows it to the exports of one sheet, by name.

    Runs *after* the apply transaction committed and writes onto
    `context.branch`, so the new versions only become visible when the branch
    is merged (see :class:`~lamindb_setup.evolve.authoring._migrations.SheetExportContext`).
    """
    for export in context.exports:
        if sheet is not None and getattr(export.sheet, "name", None) != sheet:
            continue
        artifact = context.write(export)
        if getattr(artifact, "pk", None) == getattr(export.artifact, "pk", None):
            # lamindb deduplicates by content hash: the rows still render to the
            # file already stored, so there is no new version to report.
            context.report.matched += 1
            context.report.note(f"{export.label} already up to date")
        elif any(new.pk == artifact.pk for _old, new in context.promotions):
            # The rows render what a superseded version already holds, so the
            # family is re-pointed at it rather than growing a duplicate.
            context.report.mapped += 1
            context.report.note(f"{export.label} restored to {artifact.uid}")
        else:
            context.report.transformed += 1
            context.report.note(f"{export.label} re-exported as {artifact.uid}")


def reexport_sheets(
    *,
    sheet: str | None = None,
    name: str = "",
    reason: str = "",
) -> DataMigration:
    """A migration re-exporting the sheet snapshots a schema change invalidated.

    Hand it to :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>`
    on the *schema* whose change invalidates them::

        ctx.migrate(BioSampleSheetSchema.schema, reexport_sheets())

    or on a *sheet* the plan points at another schema, whose exports were
    written against the old one; the new version is written against the new.

    Each stale export gains a new version written from the sheet's rows *after*
    the rest of the apply -- including every other declared migration -- has run
    and committed, so the new file reflects the migrated data.  `sheet` narrows
    the declaration to one sheet by name, for a schema several sheets use.

    Nothing is created for an export whose data did not actually change: lamindb
    deduplicates by content hash, so the re-export resolves to the version
    already stored.  If it instead resolves to a *superseded* version -- the rows
    render exactly what an older export holds, which a schema change and its
    revert produce -- the family is re-pointed at that version rather than
    growing a duplicate, since `(hash, key, storage)` is a database unique
    constraint.  Either way the head ends up describing the schema as the plan
    declares it.

    Unlike the other migrations, this one runs *outside* the apply transaction
    (a file write cannot be rolled back with it) and onto a dedicated branch that
    is merged into the instance only once every declared migration succeeded.
    """

    def migrate(context: SheetExportContext) -> None:
        rewrite_exports(context, sheet=sheet)

    return DataMigration(
        name=name
        or ("reexport_sheets" if sheet is None else f"reexport_sheets[{sheet}]"),
        migrate=migrate,
        reason=reason
        or (
            "write a new version of each invalidated sheet export"
            if sheet is None
            else f"write a new version of {sheet!r}'s invalidated exports"
        ),
        narrowing=STALE_EXPORTS,
    )


def keep_stale_exports(
    *,
    sheet: str | None = None,
    name: str = "",
    reason: str = "",
) -> DataMigration:
    """A migration that leaves an invalidated sheet export as it is.

    The non-destructive counterpart of :func:`reexport_sheets`: the schema
    changes and the exported snapshot keeps describing it as it was.  Nothing is
    written -- the declaration exists to state that the stale export is intended,
    which is what unblocks the change::

        ctx.migrate(BioSampleSheetSchema.schema, keep_stale_exports())

    Declared on a sheet the plan points at another schema, it keeps the exports
    written against the old one.

    Reach for it when the export is a deliberate historical record rather than a
    live view of the sheet.  Note that the artifact keeps pointing at the changed
    schema through its `schema` foreign key, so it will read as validated
    against a schema it no longer satisfies.
    """

    def migrate(context: SheetExportContext) -> None:
        for export in context.exports:
            if sheet is not None and getattr(export.sheet, "name", None) != sheet:
                continue
            context.report.note(f"{export.label} left stale deliberately")

    return DataMigration(
        name=name
        or ("keep_stale_exports" if sheet is None else f"keep_stale_exports[{sheet}]"),
        migrate=migrate,
        reason=reason
        or (
            "keep the invalidated sheet exports as they are"
            if sheet is None
            else f"keep {sheet!r}'s invalidated exports as they are"
        ),
        keeps_values=True,
        narrowing=STALE_EXPORTS,
    )


# === index changes ===========================================================


def accept_index_change(
    *,
    reason: str = "",
    index_name_conflict: IndexNameConflict | None = None,
    name: str = "",
) -> DataMigration:
    """A migration accepting a change to a schema's `index` despite validated data.

    Hand it to :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>`
    on the *schema* whose `index=` the plan adds, changes or removes::

        ctx.migrate(
            visium_hd_obs,
            accept_index_change(reason="every table already uses the barcode as obs index"),
        )

    Changing the index changes how every dataset the schema already validated
    would be curated now, and that cannot be re-checked in place, so the change is
    refused while such data exists.  This declaration is the plan author vouching
    that the data conforms: it writes nothing beyond the index itself, and shows up
    in the `plan` / `apply` output with its `reason`.  A schema that has
    validated nothing needs no declaration -- `--allow-data-validated-changes`
    covers it.

    `index_name_conflict` settles the one case lamindb cannot decide alone.  A
    sheet using the schema stores its index value on `Record.name`, so when a
    row already has a `Record.name` *and* a different value for the new index
    feature, one of them has to win: `"keep_name"` keeps the name,
    `"keep_feature"` takes the feature's value.  The other one is discarded.
    Without a choice the apply refuses and names the rows -- it never prompts.
    """

    def migrate(context: IndexChangeContext) -> None:
        """Nothing to rewrite: the engine has already switched the index."""

    return DataMigration(
        name=name or "accept_index_change",
        migrate=migrate,
        reason=reason or "the data the schema validated conforms to the new index",
        keeps_values=True,
        narrowing=INDEX_CHANGE,
        index_name_conflict=index_name_conflict,
    )


def accept_added_member(*, reason: str = "", name: str = "") -> DataMigration:
    """A declaration vouching for datasets a schema validated before it gained a member.

    Hand it to :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>`
    on the *schema* the plan adds a required member to::

        ctx.migrate(
            obs_schema,
            accept_added_member(reason="every obs frame already carries 'donor'"),
        )

    A member that is not optional must be present in everything the schema
    validates.  Sheet rows can be checked and backfilled (see
    :func:`~lamindb_setup.evolve.fill_missing`), but an artifact or collection
    validated without the new column cannot be re-curated in place, so adding the
    member is refused while such datasets exist.  This declaration is the plan
    author vouching for them: it writes nothing, and shows up in the `plan` /
    `apply` output with its `reason`.  A schema that validated nothing needs
    no declaration at all.
    """

    def migrate(context: Any) -> None:
        """Nothing to rewrite: the declaration only vouches for the data."""

    return DataMigration(
        name=name or "accept_added_member",
        migrate=migrate,
        reason=reason or "the datasets the schema validated carry the new member",
        keeps_values=True,
        narrowing=MEMBER_ADDED,
    )


# === flexible: what the flexible match annotated ============================


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}{'' if count == 1 else 's'}"


def _member_columns_schema(recorded: Any, dropped: set[Any]) -> Any | None:
    """The schema lamindb records for `recorded`'s columns less `dropped`.

    Built the way lamindb's `annotate_artifact` builds a recorded schema -- the
    remaining columns with `recorded`'s own index and set flags -- so lamindb
    hands back the live schema already holding that content, or creates one.
    `None` when no column is left besides the index.
    """
    index = recorded.index
    remaining = [
        member
        for member in _members(recorded)
        if member.pk not in dropped and (index is None or member.pk != index.pk)
    ]
    if not remaining:
        return None
    schema = type(recorded)(
        features=remaining,
        itype=recorded.itype,
        index=index,
        minimal_set=recorded.minimal_set,
        maximal_set=recorded.maximal_set,
        coerce=recorded.coerce,
        ordered_set=recorded.ordered_set,
    )
    return schema.save() if schema._state.adding else schema


def _rerecord_columns(entries: list[Any]) -> None:
    """Point each entry's link at its recorded columns without the non-members.

    Entries recording the same schema and dropping the same features share one
    target, so each distinct content is looked up (or created) once.
    """
    groups: dict[tuple[Any, frozenset[Any]], list[Any]] = {}
    for entry in entries:
        dropped = frozenset(feature.pk for feature in entry.features)
        groups.setdefault((entry.recorded.pk, dropped), []).append(entry)
    for (_recorded, dropped), group in groups.items():
        target = _member_columns_schema(group[0].recorded, set(dropped))
        model = type(group[0].link)
        using = group[0].link._state.db
        for chunk in _in_clause_batches([entry.link.pk for entry in group]):
            rows = model._base_manager.using(using).filter(pk__in=chunk)
            if target is None:
                rows.delete()
            else:
                rows.update(schema_id=target.pk)


def _unclaimed_label_features(entries: list[Any]) -> dict[Any, set[Any]]:
    """The non-member features whose label links each artifact loses, by artifact pk.

    A feature an artifact still records in another slot, or that a schema it is
    validated against declares -- its own, or one of its components such as an
    `__external__` slot -- was annotated by something other than the flexible
    match, so its labels stay.  Read after :func:`_rerecord_columns`, so the slot
    being cleaned no longer claims what it drops.
    """
    first = entries[0]
    using = first.link._state.db
    link_model = type(first.link)
    artifact_model = type(first.artifact)
    component_model = first.recorded.links_component.model
    artifact_ids = sorted({entry.artifact.pk for entry in entries})
    recorded: dict[Any, set[Any]] = {}
    validating: dict[Any, Any] = {}
    for chunk in _in_clause_batches(artifact_ids):
        for artifact_id, schema_id in (
            link_model._base_manager.using(using)
            .filter(artifact_id__in=chunk)
            .values_list("artifact_id", "schema_id")
        ):
            recorded.setdefault(artifact_id, set()).add(schema_id)
        validating.update(
            artifact_model._base_manager.using(using)
            .filter(pk__in=chunk)
            .values_list("pk", "schema_id")
        )
    composites = sorted({pk for pk in validating.values() if pk is not None})
    components: dict[Any, set[Any]] = {}
    for chunk in _in_clause_batches(composites):
        for composite_id, component_id in (
            component_model._base_manager.using(using)
            .filter(composite_id__in=chunk)
            .values_list("composite_id", "component_id")
        ):
            components.setdefault(composite_id, set()).add(component_id)
    claiming: dict[Any, set[Any]] = {}
    for artifact_id in artifact_ids:
        schemas = set(recorded.get(artifact_id, ()))
        validator = validating.get(artifact_id)
        if validator is not None:
            schemas |= {validator, *components.get(validator, ())}
        claiming[artifact_id] = schemas
    members = feature_members(
        type(first.recorded), set().union(*claiming.values()), using
    )
    unclaimed: dict[Any, set[Any]] = {}
    for entry in entries:
        claimed = {f for s in claiming[entry.artifact.pk] for f in members.get(s, ())}
        lost = {feature.pk for feature in entry.features} - claimed
        if lost:
            unclaimed.setdefault(entry.artifact.pk, set()).update(lost)
    return unclaimed


def _delete_label_links(
    unclaimed: dict[Any, set[Any]], artifact_model: Any, using: Any
) -> int:
    """Delete the label links `unclaimed` names; returns how many went.

    One read and one batched delete per categorical link table: the rows are
    narrowed to the artifacts and features involved in SQL, and paired up in
    Python, since each artifact loses its own set of features.
    """
    if not unclaimed:
        return 0
    feature_ids = sorted(set().union(*unclaimed.values()))
    deleted = 0
    for model, subject in artifact_label_link_models(artifact_model):
        doomed: list[Any] = []
        for chunk in _in_clause_batches(sorted(unclaimed)):
            rows = (
                model._base_manager.using(using)
                .filter(
                    **{f"{subject.attname}__in": chunk, "feature_id__in": feature_ids}
                )
                .values_list("pk", subject.attname, "feature_id")
            )
            doomed.extend(
                pk
                for pk, artifact_id, feature_id in rows
                if feature_id in unclaimed.get(artifact_id, ())
            )
        for chunk in _in_clause_batches(doomed):
            model._base_manager.using(using).filter(pk__in=chunk).delete()
        deleted += len(doomed)
    return deleted


def clear_nonmember_annotations(context: FlexibleChangeContext, /) -> None:
    """Remove what the flexible match annotated on the artifacts a schema curated.

    Brings every artifact
    :meth:`~lamindb_setup.evolve.FlexibleChangeContext.nonmember_columns`
    lists to what curating it against the schema with `flexible` off records:

    * the slot's recorded columns are re-pointed to a schema of the member columns
      alone -- the live one lamindb keeps for that content, or a new one, built as
      lamindb's own `annotate_artifact` builds it.  A slot left with no column
      loses its record.  The schema it recorded before is not edited, since other
      artifacts may record it too.
    * the label links of the non-member features are deleted, unless the artifact
      still records the feature in another slot or a schema it is validated
      against declares it: that annotation is not the flexible match's.

    Scalar values are left alone: curation writes none for a slot's columns, so
    any stored there were annotated some other way.
    """
    entries = context.nonmember_columns()
    if not entries:
        return
    first = entries[0]
    _rerecord_columns(entries)
    unclaimed = _unclaimed_label_features(entries)
    deleted = _delete_label_links(unclaimed, type(first.artifact), first.link._state.db)
    names = sorted({feature.name for entry in entries for feature in entry.features})
    artifacts = len({entry.artifact.pk for entry in entries})
    context.report.note(
        f"recorded the columns of {_plural(artifacts, 'artifact')} without "
        + ", ".join(repr(name) for name in names)
    )
    if deleted:
        context.report.note(f"deleted {_plural(deleted, 'label link')}")


def drop_nonmember_annotations(*, name: str = "", reason: str = "") -> DataMigration:
    """A migration removing what the flexible match annotated, as `flexible` goes off.

    The declarative form of :func:`clear_nonmember_annotations`, to hand to
    :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>` on the
    *schema* whose `flexible=True` the plan turns off::

        ctx.migrate(obs_schema, drop_nonmember_annotations())

    Without it the annotations stay, and the diff names the artifacts carrying
    them.  This is destructive: label links have no trash state.  On a
    `maximal_set` schema it is also the plan author's word that a dataset still
    carrying such a column may stop validating, which lifts the block there just
    as :func:`accept_flexible_change` does.
    """
    return DataMigration(
        name=name or "drop_nonmember_annotations",
        migrate=clear_nonmember_annotations,
        reason=reason
        or "remove what the flexible match annotated for non-member columns",
        narrowing=FLEXIBLE_CHANGE,
    )


def accept_flexible_change(*, reason: str = "", name: str = "") -> DataMigration:
    """A migration accepting that a schema's `flexible` goes off despite validated data.

    Hand it to :meth:`ctx.migrate <lamindb_setup.evolve.PlanContext.migrate>`
    on the *schema* whose `flexible=True` the plan turns off::

        ctx.migrate(
            var_schema,
            accept_flexible_change(reason="every var frame only carries member columns"),
        )

    Only a `maximal_set=True` schema needs it.  There, turning `flexible` off
    shrinks the admitted columns to the members, so a dataset that carried a
    column the flexible match accepted would no longer validate -- and that cannot
    be re-checked in place, so the change is refused while such datasets may
    exist.  This declaration is the plan author vouching for them.  Under
    `maximal_set=False` the change is applied without one, and a schema that has
    validated no dataset needs none either: `--allow-data-validated-changes`
    covers it.

    It writes nothing: what the flexible match annotated stays on the artifacts.
    Declare :func:`drop_nonmember_annotations` instead to remove it.
    """

    def migrate(context: FlexibleChangeContext) -> None:
        """Nothing to rewrite: the engine has already switched the flag."""

    return DataMigration(
        name=name or "accept_flexible_change",
        migrate=migrate,
        reason=reason
        or "the datasets the schema validated conform without the flexible match",
        keeps_values=True,
        narrowing=FLEXIBLE_CHANGE,
    )
