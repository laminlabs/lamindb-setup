"""Data-aware overrides of the drift taxonomy.

The pure taxonomy in :mod:`lamindb_setup.evolve.engine._drift` refuses a change whenever
it *could* invalidate an already-validated record.  That refusal is deliberately
conservative: it never looks at the data.  When the operator explicitly opts in
(`--allow-data-validated-changes`) a blocked change may still be safe for a
particular feature because either

1. the feature has no stored values at all -- nothing can be invalidated -- or
2. every stored value already validates against the new dtype.

This module answers those two data questions against a *live* LaminDB feature.
It is imported lazily (it touches lamindb) and only consulted once the pure
taxonomy has already returned :attr:`~lamindb_setup.evolve.engine._drift.Verdict.BLOCKED`.

A third question has the same shape but is asked of a *schema*: making an
optional member required is refused because a row that omitted it would no
longer validate, which is safe exactly when no such row exists (see
:func:`schema_member_omissions`).  Its mirror image is *removing* a member
outright: there the rows that already carry a value are the ones whose
annotation the schema would stop accounting for, so they are what the removal
has to answer for (see :func:`schema_member_usage`).

Turning a schema's `flexible` flag off leaves the same kind of thing behind
without removing a member: while it was on, curation also validated and
annotated every column whose name matched a registered feature, and recorded
it among the artifact's columns.  :func:`schema_nonmember_usage` counts what is
left of that.

A `list[...]` dtype is validated element by element, and a value of the
other shape -- a single value under a list dtype, a list under a single-valued
one -- never validates: lamindb's dtype check refuses it, so a change of shape
has to rewrite the values (see :func:`_misshapen_value_count`).

Only *scalar* (non-categorical) values are introspected: they live in the
`JsonValue` / `RecordJson` tables, each keyed by a `feature` foreign key.
Categorical values are spread across many per-registry link tables
(`links_artifact*` / `links_record*`); proving those empty or revalidating
them label by label against a scalar dtype is not done here, so
:func:`feature_has_any_value` presumes a categorical feature has values.  Two
questions do look at the links: whether a categorical feature holds any value at
all (:func:`feature_has_any_stored_value`), and whether every link is a label
another categorical dtype admits (:func:`feature_inadmissible_link_count`) --
which is what proves a `cat -> cat` change, and what a migration answers to
after it ran.

The *target* dtype, by contrast, may be categorical: a `<scalar> -> cat[...]`
change (e.g. `str -> cat[ULabel]`) is refused by the pure taxonomy, but the
scalar values already stored can be checked against the target registry's labels.
Each stored value validates when it is a label in *any* member registry of the
target union, within what that member admits: a `Record[<type>]` member admits
the type's whole subtree, and a filter (`source__uid=...`) narrows it further,
as lamindb's curator validates.  The check stays conservative (keeps the block)
for a bare `cat` target or a non-default name field.  Such a value still has
to be *moved* -- a categorical dtype reads link rows, not the scalar store -- so
the check says whether a migration relocating the values would find a label for
each, not that the dtype may be flipped in place.

The value queries deliberately span **every space**, including any a plan or
operation would otherwise exclude (`--exclude-space`): a value in an excluded
space is just as real and would be invalidated by the change, so it must count.
LaminDB's reverse-relation managers are not space-scoped, so a plain
`feature.values` query already sees all spaces; this module relies on that and
never narrows the query to a single space.

*Trashed* records are the one exclusion: a `RecordJson` value on a record
lamindb has moved to the trash (`branch_id == -1`) is effectively deleted and
would not be re-validated, so it cannot be invalidated by a dtype change and is
dropped from every query here.  Archived records (`branch_id == 0`) and all
other branches still count.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, NamedTuple

from ..authoring import parse_dtype
from ..core import DEFAULT_MODELS, LamindbModels

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterable, Iterator

__all__ = [
    "MemberOmissions",
    "NonMemberRows",
    "MemberUsage",
    "NonMemberColumns",
    "NonMemberUsage",
    "SchemaUsage",
    "ValueLinks",
    "SheetIndexConflicts",
    "SheetRowsCheck",
    "ValueFailures",
    "admitted_label_keys",
    "artifact_label_link_models",
    "categorical_value_count",
    "feature_has_any_categorical_value",
    "feature_has_any_stored_value",
    "feature_has_any_value",
    "feature_inadmissible_link_count",
    "feature_members",
    "feature_stranded_value_count",
    "feature_value_count",
    "feature_value_failures",
    "feature_values_all_validate",
    "prefetch_categorical_presence",
    "prefetch_scalar_values",
    "prefetch_schema_usage_presence",
    "record_value_links",
    "schema_member_conflicts",
    "schema_member_omissions",
    "schema_member_usage",
    "schema_nonmember_columns",
    "schema_nonmember_rows",
    "schema_nonmember_usage",
    "schema_rows_carrying_member",
    "schema_rows_omitting_member",
    "schema_validated_usage",
    "rows_index_name_conflicts",
    "sheet_index_name_conflicts",
    "sheet_member_omissions",
    "sheet_rows",
    "sheet_rows_validate",
    "type_subtree_link_count",
]

# How many distinct offending values :class:`ValueFailures` keeps as examples.
# Enough to recognise a pattern ("they are all free-text notes") without turning
# a drift message into a data dump; the *distinct* count reports the rest.
_MAX_FAILURE_EXAMPLES = 5

# Chunk size for the `feature_id__in` batches in :func:`prefetch_scalar_values`
# and :func:`prefetch_categorical_presence`, bounding the size of a single `IN`
# clause on very large plans.
_PREFETCH_VALUE_BATCH_SIZE = 10_000

# Reverse relations on a `Feature` that hold *scalar* (non-categorical) values,
# each paired with the kwargs that drop *trashed* rows (records lamindb has moved
# to the trash, `branch_id == -1`): their values are effectively deleted and
# would not be re-validated, so a dtype change cannot invalidate them and they
# must not count.  `JsonValue` (`feature.values`) is a shared value annotating
# artifacts / runs, not a per-record row, so it carries no record branch to filter
# on (empty kwargs).  `RecordJson` (`feature.links_recordjson`) links to a
# `record` whose `branch_id` we can filter.  Only the trash is dropped:
# archived records (`branch_id == 0`) and every space are still counted, since
# these managers are not space-scoped -- no live value is missed.
_SCALAR_VALUE_RELATIONS: tuple[tuple[str, dict[str, Any]], ...] = (
    ("values", {}),
    ("links_recordjson", {"record__branch_id": -1}),
)

# Foreign keys on a link table that are never the label it carries: the feature
# the link expresses, plus the bookkeeping columns lamindb's `TracksRun` /
# `BaseSQLRecord` mixins contribute.
_LINK_BOOKKEEPING_FIELDS = frozenset(
    {"feature", "branch", "space", "run", "created_by", "created_on"}
)


def _scalar_value_queryset(existing: Any, relation: str, trash_filter: dict[str, Any]):
    """The relation's queryset on `existing`, minus trashed rows, or `None`.

    `None` when the relation manager is absent.  `trash_filter` (when
    non-empty) excludes rows whose linked record is trashed.
    """
    manager = getattr(existing, relation, None)
    if manager is None:
        return None
    return manager.exclude(**trash_filter) if trash_filter else manager.all()


def _is_categorical_feature(existing: Any) -> bool:
    """Whether `existing`'s live dtype is (or nests) a categorical union."""
    dtype = getattr(existing, "dtype_as_str", None) or ""
    return dtype == "cat" or "cat[" in dtype


def _concrete_relations(model: Any) -> list[Any]:
    """The foreign keys declared on `model` itself."""
    return [
        field
        for field in model._meta.get_fields()
        if getattr(field, "concrete", False) and getattr(field, "is_relation", False)
    ]


def link_subject_field(model: Any) -> Any | None:
    """The foreign key naming the object a link row annotates, or `None`.

    lamindb names a link table `<host registry><label registry>` and its host
    foreign key after the host registry -- `ArtifactULabel.artifact`,
    `RecordRecord.record` -- which is the only thing telling the annotated
    object apart from the label it carries: the label field is called `value`
    for a `Record` host but is named after its own registry for every other one
    (`ArtifactULabel.ulabel`, `ArtifactRecord.record`).

    `None` when no single foreign key matches the model's own name, so a link
    table shaped differently is left to a hand-written migration rather than
    guessed at.
    """
    name = model.__name__
    subjects = [
        field
        for field in _concrete_relations(model)
        if field.name not in ("feature", "value")
        and name.startswith(field.related_model.__name__)
    ]
    return subjects[0] if len(subjects) == 1 else None


def _link_label_field(model: Any, subject: Any) -> Any | None:
    labels = [
        field
        for field in _concrete_relations(model)
        if field.name not in _LINK_BOOKKEEPING_FIELDS and field.name != subject.name
    ]
    return labels[0] if len(labels) == 1 else None


def link_label_field(model: Any) -> Any | None:
    """The foreign key holding the *label* of a categorical link row, or `None`.

    Whatever foreign key is left once the feature, the annotated object (see
    :func:`link_subject_field`) and the bookkeeping columns lamindb's
    `TracksRun` / `BaseSQLRecord` mixins contribute are taken out.  `None`
    when that is not exactly one field.
    """
    subject = link_subject_field(model)
    if subject is None:
        return None
    return _link_label_field(model, subject)


def _is_categorical_link_model(model: Any) -> bool:
    """Whether `model` is a per-registry *categorical* link table on `Feature`.

    A categorical value is a foreign-key link in such a table (`RecordRecord` /
    `RecordULabel` / `ArtifactRecord` / ...), each carrying a `feature` FK,
    a foreign key to the annotated object and one to the label.  A *relational*
    label is what distinguishes them from the scalar tables (`JsonValue` /
    `RecordJson`), whose value is a JSON column and which are handled
    elsewhere.
    """
    if not any(field.name == "feature" for field in model._meta.get_fields()):
        return False
    return link_label_field(model) is not None


def _categorical_link_models(existing: Any) -> list[Any]:
    """The model class of every categorical link table reachable from `existing`.

    Raises on any introspection failure so callers can choose their own
    conservative default.
    """
    return [
        relation.related_model
        for relation in existing._meta.related_objects
        if _is_categorical_link_model(relation.related_model)
    ]


def _iter_categorical_managers(existing: Any) -> Iterator[Any]:
    """Yield the reverse-relation manager of each *categorical* link table.

    Keeps the relations :func:`_is_categorical_link_model` accepts and yields their
    managers.  It raises on any introspection failure so callers can choose their
    own conservative default.
    """
    for relation in existing._meta.related_objects:
        if not _is_categorical_link_model(relation.related_model):
            continue
        manager = getattr(existing, relation.get_accessor_name(), None)
        if manager is None:
            continue
        yield manager


class ValueLinks(NamedTuple):
    """The rows of one link table that hold a record as their categorical value.

    `label` is the foreign key holding the value, `subject` the one naming
    the annotated object (`None` for a table shaped differently), and `rows`
    the live rows.
    """

    model: Any
    label: Any
    subject: Any
    rows: Any


def _annotates_with(registry: Any, model: Any) -> bool:
    """Whether `registry`'s records are the labels in link table `model`.

    lamindb declares a labelling relation on the label's registry --
    `Record.artifacts`, the artifacts a record annotates -- so a relation
    through `model` declared on another registry makes `registry`'s records
    the annotated side: `Project.records` and `Reference.records` (through
    `ProjectRecord` / `ReferenceRecord`) are the records a project or
    reference annotates, the reverse of what the table names suggest.  A table no
    relation runs through is taken at its name.
    """
    declared_here = declared_elsewhere = False
    for field in registry._meta.get_fields():
        if not field.many_to_many:
            continue
        if field.concrete:
            declared_here |= field.remote_field.through is model
        else:
            declared_elsewhere |= field.through is model
    return declared_here or not declared_elsewhere


def record_value_links(record: Any) -> list[ValueLinks]:
    """The link rows naming `record` as their categorical value, per link table.

    Every table whose *label* is `record`'s registry -- another record's value
    (`RecordRecord.value`), an artifact's, a run's, a collection's -- rather
    than the tables whose rows annotate `record` itself, a project's or a
    reference's label included (see :func:`_annotates_with`).  A row annotating
    an object in the trash is left out: nothing reads it any more.
    """
    links: list[ValueLinks] = []
    for relation in type(record)._meta.related_objects:
        model = relation.related_model
        if not _is_categorical_link_model(model):
            continue
        label = link_label_field(model)
        if label is None or label.name != relation.field.name:
            continue
        if not _annotates_with(type(record), model):
            continue
        subject = link_subject_field(model)
        rows = model.objects.filter(**{label.name: record})
        if subject is not None and any(
            field.name == "branch" for field in subject.related_model._meta.get_fields()
        ):
            rows = rows.exclude(**{f"{subject.name}__branch_id": -1})
        links.append(ValueLinks(model, label, subject, rows))
    return links


def _feature_ids_with_links(models: list[Any], pks: list[Any], using: Any) -> set[Any]:
    """The subset of `pks` that own a row in any of `models`.

    The per-table queries are combined into a single `UNION` so the whole answer
    costs one round-trip rather than one per link table -- lamindb declares over
    twenty of them, and against a remote instance that difference is the entire
    cost of the check.  A backend that refuses the compound select falls back to
    one query per table, which is slower but yields the same set.
    """
    querysets = [
        model._default_manager.using(using)
        .filter(feature_id__in=pks)
        .values_list("feature_id", flat=True)
        for model in models
    ]
    if not querysets:
        return set()
    try:
        combined = (
            querysets[0].union(*querysets[1:]) if len(querysets) > 1 else querysets[0]
        )
        return set(combined)
    except Exception:
        found: set[Any] = set()
        for queryset in querysets:
            found.update(queryset)
        return found


def prefetch_categorical_presence(features: list[Any]) -> dict[Any, bool]:
    """Bulk-answer :func:`feature_has_any_categorical_value` for many features.

    Returns a mapping of feature `pk` -> whether that feature owns any stored
    categorical link row.  A missing key means "ask per-object" to the caller,
    which stays conservative.

    Replaces the per-feature presence walk in
    :func:`feature_has_any_categorical_value` -- one `exists` per feature *per
    link table*, and lamindb declares over twenty of them -- with a single
    `feature_id__in` union across every link table, chunked to bound the `IN`
    clause.  A plan whose features change dtype to `str` then costs a constant
    number of round-trips instead of `features x tables`.

    The queries go through each model's default manager, matching the reverse
    relation managers they replace; trashed rows are *not* excluded, exactly as in
    the per-object path (counting them only keeps a change blocked, the safe
    direction for an in-place widening).

    Any introspection or query failure disables the prefetch entirely (returns
    `{}`): every feature then falls back to its per-object walk, which is
    conservative on error, so a batch failure never reports a feature as free of
    categorical values.
    """
    if not features:
        return {}
    result: dict[Any, bool] = {feature.pk: False for feature in features}
    pks = list(result)
    sample = features[0]
    try:
        models = _categorical_link_models(sample)
        using = sample._state.db
        for start in range(0, len(pks), _PREFETCH_VALUE_BATCH_SIZE):
            chunk = pks[start : start + _PREFETCH_VALUE_BATCH_SIZE]
            for feature_id in _feature_ids_with_links(models, chunk, using):
                if feature_id in result:
                    result[feature_id] = True
    except Exception:
        # A batched query failure -> disable the prefetch; the per-object path
        # (conservative on error) takes over for every feature.
        return {}
    return result


def feature_has_any_categorical_value(
    existing: Any, *, has_values: bool | None = None
) -> bool:
    """Whether the feature holds any stored *categorical* value to relocate.

    A categorical value is a foreign-key link in a per-registry link table
    (`RecordRecord` / `RecordULabel` / `ArtifactRecord` / ...), each carrying
    a `feature` FK and a *relational* `value`.  This walks the feature's reverse
    relations, keeps the ones whose `value` is a relation (distinguishing them
    from the scalar `JsonValue` / `RecordJson` tables, whose `value` is a JSON
    column), and reports whether any holds a row.

    Used to decide whether a `cat[...] -> str` widening is relocation-free: those
    values live outside the scalar tables an in-place dtype flip touches, so the
    change is only truly safe when none are stored.

    Conservative by design: returns `True` whenever presence cannot be
    established (an unintrospectable relation or a failing query), so a caller may
    only treat the change as relocation-free when this returns `False`.  A scalar
    feature has no such link rows and returns `False`.  Trashed rows are *not*
    excluded here -- counting them only keeps the change blocked, the safe
    direction for an in-place widening.

    `has_values` optionally supplies the already-prefetched answer for this
    feature (see :func:`prefetch_categorical_presence`); when given it is returned
    as-is with no query.  When `None` the link tables are walked per-object.
    """
    if has_values is not None:
        return has_values
    try:
        for manager in _iter_categorical_managers(existing):
            if manager.exists():
                return True
    except Exception:
        # Cannot introspect or query a relation -> stay conservative.
        return True
    return False


def _target_is_categorical(new_dtype: str) -> bool:
    """Whether `new_dtype` is a categorical target (`cat`/`cat[...]`/registry)."""
    dtype = (new_dtype or "").strip()
    if dtype == "cat" or "cat[" in dtype:
        return True
    return _resolve_registry_path(dtype) is not None


def _raw_scalar_value_count(existing: Any) -> int | None:
    """Count the feature's stored scalar rows, ignoring its live dtype.

    Unlike :func:`feature_value_count`, this does *not* short-circuit on a
    categorical feature: after a migration flips the dtype to a categorical, any
    surviving scalar row is exactly what we want to count.  Trashed rows are
    excluded as elsewhere.  Returns `None` when a query fails, so the caller
    stays conservative.
    """
    total = 0
    for relation, trash_filter in _SCALAR_VALUE_RELATIONS:
        queryset = _scalar_value_queryset(existing, relation, trash_filter)
        if queryset is None:
            continue
        try:
            total += queryset.count()
        except Exception:
            return None
    return total


def _categorical_value_count(existing: Any) -> int | None:
    """Count the feature's stored categorical link rows.

    Sums the rows across every per-registry categorical link table (see
    :func:`_iter_categorical_managers`).  Returns `None` when introspection or a
    query fails, so the caller stays conservative.
    """
    total = 0
    try:
        for manager in _iter_categorical_managers(existing):
            total += manager.count()
    except Exception:
        return None
    return total


def feature_inadmissible_link_count(
    existing: Any, dtype: str, *, models: LamindbModels | None = None
) -> int | None:
    """Count the feature's categorical links whose label `dtype` does not admit.

    A categorical value is a link to a label record, so whether it validates
    against a categorical `dtype` is a question about that record: it must
    belong to one of the dtype's registries and satisfy that component's
    filters.  A `Record[<type>]` (or `ULabel[<type>]`) component admits the
    type's whole subtree, as lamindb's curator does; a filter payload
    (`source__uid=...`, `is_type=True`) narrows it further.  A link in the
    table of a registry the dtype does not name is never admitted.

    Labels are looked up across every branch: a link to a trashed or archived
    label inside the admitted subtree is where it belongs, whatever became of
    the label since.

    Returns `None` when `dtype` names no categorical registry or the links
    cannot be introspected, so a caller stays conservative.
    """
    models = DEFAULT_MODELS if models is None else models
    components = models.parse_dtype(dtype)
    if not components:
        return None
    by_registry: dict[Any, list[dict[str, Any]]] = {}
    for component in components:
        by_registry.setdefault(component["registry"], []).append(component)
    total = 0
    try:
        for manager in _iter_categorical_managers(existing):
            label = link_label_field(manager.model)
            if label is None:  # pragma: no cover - rejected by the manager filter
                continue
            registry = label.related_model
            if registry not in by_registry:
                total += manager.count()
                continue
            admitted = _admitted_labels(registry, by_registry[registry], models)
            if admitted is not None:
                total += manager.exclude(**{f"{label.name}__in": admitted}).count()
    except Exception:
        return None
    return total


def admitted_label_keys(
    labels: Iterable[Any], dtype: str, *, models: LamindbModels | None = None
) -> set[tuple[Any, Any]] | None:
    """The `(registry, pk)` of each of `labels` the categorical `dtype` admits.

    A label is admitted when it belongs to one of the dtype's registries and
    sits within what that component names -- a type's whole subtree, its
    filters -- exactly as :func:`feature_inadmissible_link_count` judges a link.
    `None` when `dtype` names no categorical registry; raises when a filter
    cannot be read.
    """
    models = DEFAULT_MODELS if models is None else models
    components = models.parse_dtype(dtype)
    if not components:
        return None
    by_registry: dict[Any, list[dict[str, Any]]] = {}
    for component in components:
        by_registry.setdefault(component["registry"], []).append(component)
    wanted: dict[Any, set[Any]] = {}
    for label in labels:
        wanted.setdefault(type(label), set()).add(label.pk)
    admitted: set[tuple[Any, Any]] = set()
    for registry, pks in wanted.items():
        if registry not in by_registry:
            continue
        scope = _admitted_labels(registry, by_registry[registry], models)
        if scope is not None:
            pks = set(scope.filter(pk__in=list(pks)).values_list("pk", flat=True))
        admitted |= {(registry, pk) for pk in pks}
    return admitted


def _admitted_labels(
    registry: Any, components: list[dict[str, Any]], models: LamindbModels
) -> Any | None:
    """The `registry` labels `components` admit, or `None` for all of them."""
    admitted = None
    for component in components:
        labels = _component_labels(registry, component, models)
        if labels is None:
            return None
        admitted = labels if admitted is None else admitted | labels
    return admitted


def _component_labels(
    registry: Any, component: dict[str, Any], models: LamindbModels
) -> Any | None:
    """The labels one parsed dtype component admits, or `None` when unfiltered.

    Queried through the plain `objects` manager, which -- unlike lamindb's
    `Registry.filter` -- adds no main-branch filter.  Raises when the filter
    payload cannot be read, for :func:`feature_inadmissible_link_count` to stay
    conservative.
    """
    type_uid = component.get("type_uid")
    filter_str = component.get("filter_str")
    if not type_uid and not filter_str:
        return None
    labels = registry.objects.all()
    if type_uid:
        type_record = registry.objects.filter(uid=type_uid).first()
        subtree = set() if type_record is None else _type_subtree_ids(type_record)
        labels = labels.filter(id__in=subtree)
    if filter_str:
        lookups = models.cat_filter_lookups(filter_str)
        if lookups is None:
            raise ValueError(f"cannot read the categorical filter {filter_str!r}")
        labels = labels.filter(**lookups)
    return labels


def type_subtree_link_count(existing: Any, moved: Any) -> int | None:
    """Count the feature's links to the record type `moved` or to anything below it.

    What a `cat[Record[<ancestor>]]` feature stops admitting when `moved`
    leaves the ancestor's subtree: `moved` itself, its subtypes and every
    record of any of them.  Subtypes are walked on every branch, as in
    :func:`feature_inadmissible_link_count` -- a link to a trashed label below a
    type still sits below it.  `None` when the links cannot be introspected.
    """
    from django.db.models import Q

    registry = type(moved)
    try:
        type_ids = {moved.id} | _subtype_ids(moved)
        total = 0
        for manager in _iter_categorical_managers(existing):
            label = link_label_field(manager.model)
            if label is None or label.related_model is not registry:
                continue
            below = Q(**{f"{label.name}__type_id__in": type_ids})
            total += manager.filter(below | Q(**{label.attname: moved.id})).count()
    except Exception:
        return None
    return total


def _subtype_ids(type_record: Any) -> set[Any]:
    """The ids of every type below `type_record`, on every branch."""
    model = type(type_record)
    seen: set[Any] = set()
    frontier = {type_record.id}
    while frontier:
        found = set(
            model.objects.filter(type_id__in=frontier, is_type=True).values_list(
                "id", flat=True
            )
        )
        frontier = found - seen - {type_record.id}
        seen |= frontier
    return seen


def _type_subtree_ids(type_record: Any) -> set[Any]:
    """The ids of every record below `type_record`, on every branch.

    lamindb validates a `cat[Record[<type>]]` value subtype-inclusively
    (`query_records`): a record whose type is the type itself or any subtype
    of it.  The type record is not one of its own values.
    """
    model = type(type_record)
    seen: set[Any] = set()
    frontier = {type_record.id}
    while frontier:
        found = set(
            model.objects.filter(type_id__in=frontier).values_list("id", flat=True)
        )
        frontier = found - seen - {type_record.id}
        seen |= frontier
    return seen


def feature_stranded_value_count(existing: Any, new_dtype: str) -> int | None:
    """Count stored values `new_dtype` does not read, or does not admit.

    A data migration flips the feature's dtype and *relocates* its stored values
    into the store the new dtype uses; afterwards no value should remain where
    the new dtype does not reach it:

    * a *categorical* `new_dtype` keeps its values as foreign-key links in the
      per-registry link tables, so any surviving *scalar* row (`JsonValue` /
      `RecordJson`) is stranded -- and so is a link whose label the new dtype
      does not admit: one in another registry's table, or one outside the type
      or filters it names (see :func:`feature_inadmissible_link_count`), which
      a `cat -> cat` change leaves behind when nothing relocates it; and
    * a *scalar* `new_dtype` keeps its values in the scalar tables, so any
      surviving *categorical* link row is stranded -- and so is a scalar row of
      the other shape: a single value under a `list[...]` dtype, a list under
      a single-valued one, which lamindb's dtype check refuses.

    Returns the number of stranded rows -- `0` means the migration relocated
    every value -- or `None` when presence cannot be established (an
    unintrospectable relation or a failing query), so a caller stays conservative.

    Used to *prove* a declared migration succeeded: apply runs the migration and
    then asserts this is `0` (in APPLY, raising to roll back otherwise; in a
    PLAN dry-run, reporting the count).  A scalar value is not re-validated
    against `new_dtype` value by value -- the migration writes it through
    lamindb, which rejects an invalid value at write time, so a dry-run that runs
    the migration already surfaces such a failure directly.
    """
    if not _target_is_categorical(new_dtype):
        linked = _categorical_value_count(existing)
        misshapen = _misshapen_value_count(existing, new_dtype)
        if linked is None or misshapen is None:
            return None
        return linked + misshapen
    scalar = _raw_scalar_value_count(existing)
    if scalar is None:
        return None
    inadmissible = feature_inadmissible_link_count(existing, new_dtype)
    if inadmissible is None:
        # Unparseable as lamindb's categorical (a bare `cat`, a registry path):
        # only the scalar store can be judged.
        return scalar
    return scalar + inadmissible


def _misshapen_value_count(existing: Any, new_dtype: str) -> int | None:
    """Count the stored scalar values whose shape `new_dtype` does not read.

    A `list[...]` dtype reads a list, any other scalar dtype a single value.
    `None` when the values cannot be read.
    """
    wants_list = _list_element_dtype(new_dtype) is not None
    try:
        return sum(
            1
            for value in _iter_scalar_values(existing)
            if isinstance(value, list) != wants_list
        )
    except Exception:
        return None


def prefetch_scalar_values(features: list[Any]) -> dict[int, list[Any]]:
    """Bulk-load the stored scalar values of many features in a few queries.

    Returns a mapping of feature `pk` -> the list of its stored scalar values
    (trashed rows excluded), for the *non-categorical* features in `features`.
    Categorical features are omitted (their values are never introspected, see
    the module docstring), so a missing key means "query per-object" to the
    caller -- which stays conservative for a categorical source.

    Replaces the per-feature relation queries in :func:`_iter_scalar_values`
    (one `exists` / `count` / `values_list` per feature per table) with a
    handful of batched reads: one `feature_id__in` query per value table (the
    `JsonValue` / `RecordJson` relations), chunked to bound the `IN`
    clause.  A plan with many drifting features then costs a constant number of
    round-trips instead of scaling with the feature count.

    The queries go through each model's default manager (`Model.objects`),
    which -- like the reverse relation managers this replaces -- is not
    space-scoped, so values in every space are seen; trashed rows are dropped
    with the same `record__branch_id` exclusion as the per-object path.

    Any query failure disables the prefetch entirely (returns `{}`): every
    feature then falls back to its per-object query, which is conservative on
    error, so a batch failure never silently reports a feature as value-free.
    """
    scalar = [feature for feature in features if not _is_categorical_feature(feature)]
    if not scalar:
        return {}
    result: dict[int, list[Any]] = {feature.pk: [] for feature in scalar}
    pks = list(result)
    sample = scalar[0]
    try:
        for relation, trash_filter in _SCALAR_VALUE_RELATIONS:
            manager = getattr(sample, relation, None)
            if manager is None:
                continue
            model = manager.model
            for start in range(0, len(pks), _PREFETCH_VALUE_BATCH_SIZE):
                chunk = pks[start : start + _PREFETCH_VALUE_BATCH_SIZE]
                queryset = model.objects.filter(feature_id__in=chunk)
                if trash_filter:
                    queryset = queryset.exclude(**trash_filter)
                for feature_id, value in queryset.values_list("feature_id", "value"):
                    bucket = result.get(feature_id)
                    if bucket is not None:
                        bucket.append(value)
    except Exception:
        # A batched query failure -> disable the prefetch; the per-object path
        # (conservative on error) takes over for every feature.
        return {}
    return result


def feature_has_any_value(existing: Any, *, values: list[Any] | None = None) -> bool:
    """Whether the feature has any stored (non-trashed) value.

    Conservative by design: returns `True` whenever emptiness cannot be
    established, so a caller may only override a block when this returns
    `False`.  A categorical feature always returns `True` -- its values are
    not introspected here.  Values on trashed records (`branch_id == -1`) are
    ignored, so a feature whose only values sit on trashed records reads as empty.

    `values` optionally supplies the feature's already-prefetched scalar values
    (see :func:`prefetch_scalar_values`); when given, emptiness is decided in
    memory with no query.  When `None` the relations are queried per-object.
    """
    if _is_categorical_feature(existing):
        return True
    if values is not None:
        return bool(values)
    return _has_any_scalar_value(existing)


def feature_has_any_stored_value(
    existing: Any, *, has_links: bool | None = None
) -> bool:
    """Whether any value is still stored for the feature, in either store.

    Unlike :func:`feature_has_any_value`, which presumes a categorical feature
    holds values because it never looks, this checks the rows themselves, so it
    can tell that a categorical feature is empty -- what deciding to delete a
    feature, proving a migration cleared it beforehand, or lifting a block on an
    empty categorical feature needs.  It counts the scalar rows whatever the live
    dtype (trashed records excluded, as everywhere else) and every categorical
    link row (trashed ones included, see :func:`feature_has_any_categorical_value`).

    `has_links` optionally supplies the prefetched categorical presence (see
    :func:`prefetch_categorical_presence`).

    Conservative by design: returns `True` whenever emptiness cannot be
    established.
    """
    return _has_any_scalar_value(existing) or feature_has_any_categorical_value(
        existing, has_values=has_links
    )


def categorical_value_count(existing: Any) -> int | None:
    """How many categorical link rows the feature owns, or `None` when unknown."""
    return _categorical_value_count(existing)


def _has_any_scalar_value(existing: Any) -> bool:
    """Whether a non-trashed scalar row is stored for `existing`, whatever its dtype.

    Returns `True` when a query fails, so the caller stays conservative.
    """
    for relation, trash_filter in _SCALAR_VALUE_RELATIONS:
        queryset = _scalar_value_queryset(existing, relation, trash_filter)
        if queryset is None:
            continue
        try:
            if queryset.exists():
                return True
        except Exception:
            # Any query failure -> stay conservative and assume values exist.
            return True
    return False


def feature_value_count(existing: Any, *, values: list[Any] | None = None) -> int:
    """Count the stored scalar values recorded for `existing`.

    Sums the `JsonValue` / `RecordJson` rows keyed to the feature across
    every space, excluding rows on trashed records (`branch_id == -1`).  A
    categorical feature returns `0` -- its values are not introspected here --
    so callers must only rely on this count once :func:`feature_has_any_value`
    has established the feature is scalar.

    `values` optionally supplies the feature's already-prefetched scalar values
    (see :func:`prefetch_scalar_values`); when given, the count is their length
    with no query.  When `None` the relations are counted per-object.
    """
    if _is_categorical_feature(existing):
        return 0
    if values is not None:
        return len(values)
    total = 0
    for relation, trash_filter in _SCALAR_VALUE_RELATIONS:
        queryset = _scalar_value_queryset(existing, relation, trash_filter)
        if queryset is None:
            continue
        try:
            total += queryset.count()
        except Exception:
            # A query failure must not crash the report; skip this relation.
            continue
    return total


def feature_values_all_validate(
    existing: Any,
    new_dtype: str,
    *,
    values: list[Any] | None = None,
    cat_cache: dict[Any, Any] | None = None,
    models: LamindbModels | None = None,
) -> bool:
    """Whether every stored scalar value validates against `new_dtype`.

    Returns `False` for a categorical *source* feature or an unsupported target
    dtype so the block stays in force.  A feature with no scalar values validates
    vacuously, though that case is handled up front by
    :func:`feature_has_any_value`.

    `new_dtype` may be a numeric scalar (`int` / `float`) or a categorical
    union (`cat[A|B]`); for a categorical target each stored value must be a
    label in one of the member registries.

    `values` / `cat_cache` are the batching hooks: `values` supplies the
    prefetched scalar values (skipping the per-object fetch), and `cat_cache`
    memoizes categorical label lookups across features (see
    :func:`_classify_scalar_values`).
    """
    counts = _classify_scalar_values(
        existing, new_dtype, values=values, cat_cache=cat_cache, models=models
    )
    return counts is not None and counts.failing == 0


class ValueFailures(NamedTuple):
    """How a feature's stored scalar values fare against a candidate dtype.

    `failing` of `total` stored values do not validate; those failures collapse
    to `distinct` different values, of which `examples` holds the first few (at
    most :data:`_MAX_FAILURE_EXAMPLES`) in the order they were stored.

    The counts alone say *how much* data blocks a change; the examples say *what*
    blocks it, which is what an operator needs to decide between fixing the data,
    declaring a migration, or dropping the change.
    """

    failing: int
    total: int
    distinct: int
    examples: list[Any]


def feature_value_failures(
    existing: Any,
    new_dtype: str,
    *,
    values: list[Any] | None = None,
    cat_cache: dict[Any, Any] | None = None,
    models: LamindbModels | None = None,
) -> ValueFailures | None:
    """How `existing`'s stored scalar values fare against `new_dtype`.

    Returns a :class:`ValueFailures` -- the failing / total counts plus the
    distinct offending values and a few examples of them -- or `None` when the
    values cannot be introspected (a categorical *source* feature or a target
    dtype with no per-value checker), so the caller can stay silent rather than
    report a misleading count.

    Complements :func:`feature_values_all_validate`: it quantifies and
    illustrates *why* a change is data-unsafe instead of only answering whether
    every value validates.

    `values` / `cat_cache` are the batching hooks; see
    :func:`feature_values_all_validate`.
    """
    return _classify_scalar_values(
        existing, new_dtype, values=values, cat_cache=cat_cache, models=models
    )


def _classify_scalar_values(
    existing: Any,
    new_dtype: str,
    *,
    values: list[Any] | None = None,
    cat_cache: dict[Any, Any] | None = None,
    models: LamindbModels | None = None,
) -> ValueFailures | None:
    """Score `existing`'s stored scalar values against `new_dtype`.

    The shared core of :func:`feature_values_all_validate` and
    :func:`feature_value_failures`.  Returns `None` when the values cannot be
    introspected -- a categorical *source* feature or a target dtype with no
    validator -- so both callers can stay conservative.

    Uses `values` when given -- the feature's prefetched scalar values (see
    :func:`prefetch_scalar_values`) -- otherwise fetches them once per object.
    The *distinct* set is validated in bulk (see :func:`_valid_value_set`): a
    categorical target is checked with a single `field__in` query per member
    registry rather than one query per value, and `cat_cache` (when supplied)
    memoizes those lookups across features, so each distinct value is queried at
    most once per registry for the whole run.
    """
    if _is_categorical_feature(existing):
        return None
    if values is None:
        values = list(_iter_scalar_values(existing))
    is_valid = _value_validator(values, new_dtype, cat_cache=cat_cache, models=models)
    if is_valid is None:
        return None
    failing = 0
    seen: set[Any] = set()
    examples: list[Any] = []
    for value in values:
        if is_valid(value):
            continue
        failing += 1
        key = _dedupe_key(value)
        if key in seen:
            continue
        seen.add(key)
        if len(examples) < _MAX_FAILURE_EXAMPLES:
            examples.append(value)
    return ValueFailures(failing, len(values), len(seen), examples)


def _dedupe_key(value: Any) -> Any:
    """A hashable identity for `value`, usable as a set member.

    Unhashable stored values (e.g. a list held in a JSON column) cannot go into a
    set directly, so they are keyed by their `repr`; the tag distinguishes such
    a key from a stored string that happens to look like one.
    """
    try:
        hash(value)
    except TypeError:
        return ("repr", repr(value))
    return ("value", value)


def _iter_scalar_values(existing: Any) -> Iterator[Any]:
    """Yield every stored scalar value recorded for `existing`.

    Excludes values on trashed records (`branch_id == -1`), matching
    :func:`feature_has_any_value` and :func:`feature_value_count`.
    """
    for relation, trash_filter in _SCALAR_VALUE_RELATIONS:
        queryset = _scalar_value_queryset(existing, relation, trash_filter)
        if queryset is None:
            continue
        yield from queryset.values_list("value", flat=True)


def _value_validator(
    values: list[Any],
    new_dtype: str,
    *,
    cat_cache: dict[Any, Any] | None = None,
    models: LamindbModels | None = None,
) -> Callable[[Any], bool] | None:
    """A predicate telling whether one of `values` validates against `new_dtype`.

    A `list[...]` dtype validates a list element by element against its
    element dtype.  A value of the other shape -- a scalar under a list dtype, a
    list under a scalar one -- never validates: lamindb's dtype check refuses
    it, and nothing rewrites it in place.  `None` when the dtype has no
    validator (see :func:`_valid_value_set`).
    """
    element = _list_element_dtype(new_dtype)
    if element is None:
        valid = _valid_value_set(values, new_dtype, cat_cache=cat_cache, models=models)
        if valid is None:
            return None
        labels: set[Any] = valid
        return lambda value: _is_valid(value, labels)
    items = [item for value in values if isinstance(value, list) for item in value]
    valid = _valid_value_set(items, element, cat_cache=cat_cache, models=models)
    if valid is None:
        return None
    elements: set[Any] = valid
    return lambda value: (
        isinstance(value, list) and all(_is_valid(item, elements) for item in value)
    )


def _list_element_dtype(dtype: str | None) -> str | None:
    """The element dtype of a `list[...]` dtype, or `None` for any other."""
    text = (dtype or "").strip()
    if text.startswith("list[") and text.endswith("]"):
        return text[5:-1]
    return None


def _valid_value_set(
    values: list[Any],
    new_dtype: str,
    *,
    cat_cache: dict[Any, Any] | None = None,
    models: LamindbModels | None = None,
) -> set[Any] | None:
    """The subset of `values` that validate against `new_dtype`, or `None`.

    `None` when the target is unsupported -- a dtype that neither parses nor
    resolves to a scalar / categorical validator -- so the caller keeps the block.

    Only the *distinct, hashable* values are considered: a stored value may itself
    be a list (an unhashable JSON scalar) which can never be a categorical label
    or a numeric / string scalar, so it is dropped here and treated as invalid by
    :func:`_is_valid`.

    A simple scalar target validates purely: numeric (`int` / `float`) by
    parsing, and the string-family dtypes (`str` / `path` / `url`) by an
    `isinstance(str)` check, since lamindb validates all three as strings.  A
    categorical target validates each distinct value against its registry's labels
    in bulk: one `field__in` query per member registry and scope (see
    :func:`_cat_lookups`) instead of a query per value.  `cat_cache` (when
    supplied) memoizes those label lookups across features.  Every other target --
    and any categorical target with a non-default name field -- returns `None`
    so the change stays blocked.
    """
    parsed = parse_dtype(new_dtype)
    if parsed is None:
        return None
    distinct = _distinct_hashable(values)
    if parsed[0] == "simple":
        checker = _SCALAR_CHECKERS.get(parsed[1])
        if checker is not None:
            return {value for value in distinct if checker(value)}
        # `serialize_config` encodes a single-registry categorical as a dotted
        # class path (e.g. `"lamindb.models.ulabel.ULabel"`), which parses as a
        # simple token; route any unrecognised simple token to the cat lookup,
        # which resolves such a path (and returns `None` for a genuine unknown).
    elif parsed[0] != "cat":
        return None
    return _valid_cat_value_set(distinct, new_dtype, cat_cache=cat_cache, models=models)


def _is_valid(value: Any, valid: set[Any]) -> bool:
    """Whether `value` is in the pre-computed `valid` set.

    Guards the membership test against an unhashable `value` (e.g. a list stored
    as a JSON scalar), which cannot be a member of `valid` and so is invalid.
    """
    try:
        return value in valid
    except TypeError:
        return False


def _distinct_hashable(values: list[Any]) -> list[Any]:
    """The distinct, hashable members of `values` (order-preserving).

    Unhashable values (e.g. a list stored as a JSON scalar) are skipped: they can
    never be a categorical label or a scalar and are handled as invalid by
    :func:`_is_valid`.
    """
    seen: set[Any] = set()
    distinct: list[Any] = []
    for value in values:
        try:
            if value in seen:
                continue
            seen.add(value)
        except TypeError:
            continue
        distinct.append(value)
    return distinct


def _valid_cat_value_set(
    values: list[Any],
    new_dtype: str,
    *,
    cat_cache: dict[Any, Any] | None = None,
    models: LamindbModels | None = None,
) -> set[Any] | None:
    """The subset of `values` that are labels in the categorical `new_dtype`.

    Resolves the target registries from either dtype encoding (see
    :func:`_cat_lookups`).  Stays conservative (returns `None`) for a bare
    `cat` or any component with a non-default name field.  A value validates
    when it is a label in *any* member registry of the union, among the labels
    that member admits; each member is queried with the values not yet matched,
    and `cat_cache` (when supplied) memoizes which values have already been
    checked against each `(registry, field, scope)` so a value queried for one
    feature is not re-queried for the next (see :func:`_cat_labels_for`).
    """
    lookups = _cat_lookups(new_dtype, models=models, cat_cache=cat_cache)
    if lookups is None:
        return None
    valid: set[Any] = set()
    remaining = set(values)
    for registry, field, scope in lookups:
        if not remaining:
            break
        matched = _cat_labels_for(registry, field, remaining, cat_cache, scope=scope)
        valid |= matched
        remaining -= matched
    return valid


def _cat_labels_for(
    registry: Any,
    field: str,
    wanted: set[Any],
    cat_cache: dict[Any, Any] | None,
    *,
    scope: dict[str, Any] | None = None,
) -> set[Any]:
    """The members of `wanted` that are existing `registry` labels on `field`.

    `scope` narrows the labels to those a dtype component admits (see
    :func:`_component_scope`).  Without `cat_cache` this is a single
    `field__in` query over `wanted`.  With it, results are memoized across
    features by `(registry, field, scope)`: only the values not yet checked are
    queried, so each distinct value costs at most one round-trip per lookup for
    the whole run -- turning the per-feature categorical lookups into an
    amortized batch.

    The query goes through lamindb's `Registry.filter`, so -- as in the
    curator -- a label on a non-default branch (trashed, archived) is not one.

    A query failure keeps the block for the values it would have covered: on
    error they are neither marked checked nor added to the valid set, so a later
    feature may retry them and the change stays blocked in the meantime.
    """
    scope = scope or {}
    if cat_cache is None:
        try:
            found = set(
                registry.filter(**scope, **{f"{field}__in": list(wanted)}).values_list(
                    field, flat=True
                )
            )
        except Exception:
            return set()
        return {value for value in wanted if value in found}

    key = (id(registry), field, _scope_key(scope))
    entry = cat_cache.get(key)
    if entry is None:
        entry = {"checked": set(), "valid": set()}
        cat_cache[key] = entry
    checked: set[Any] = entry["checked"]
    known_valid: set[Any] = entry["valid"]
    to_query = {value for value in wanted if value not in checked}
    if to_query:
        try:
            found = set(
                registry.filter(
                    **scope, **{f"{field}__in": list(to_query)}
                ).values_list(field, flat=True)
            )
        except Exception:
            # Leave `to_query` unchecked so a later feature may retry them;
            # only previously-known-valid values count for this feature.
            return {value for value in wanted if value in known_valid}
        checked |= to_query
        known_valid |= {value for value in to_query if value in found}
    return {value for value in wanted if value in known_valid}


def _cat_lookups(
    new_dtype: str,
    *,
    models: LamindbModels | None = None,
    cat_cache: dict[Any, Any] | None = None,
) -> list[tuple[Any, str, dict[str, Any]]] | None:
    """Resolve a categorical target to `(registry, field, scope)` lookups, or `None`.

    Handles both dtype encodings the codebase produces: lamindb's canonical
    `cat[...]` string and `serialize_config`'s single-registry dotted class
    path (`"lamindb.models.ulabel.ULabel"`).  `scope` holds the filter
    keywords narrowing a component to the labels it admits (see
    :func:`_component_scope`).  Returns `None` to keep the block for a bare
    `cat`, a non-default-field target, a filter that cannot be read, or
    anything that does not resolve to a categorical registry.
    """
    registry = _resolve_registry_path(new_dtype)
    if registry is not None:
        # `serialize_config` form: a plain registry, always its default field
        # (any cat_filters are carried in a separate config field, not here).
        return [(registry, _default_name_field(registry), {})]
    models = DEFAULT_MODELS if models is None else models
    # `None` means unparseable, or a registry that is not configured for this
    # instance: either way we cannot say what the dtype validates against, so
    # stay blocked.
    components = models.parse_dtype(new_dtype)
    if not components:
        # Bare `cat` (no registry) or a non-categorical token.
        return None
    lookups: list[tuple[Any, str, dict[str, Any]]] = []
    for component in components:
        if component.get("list"):
            # `list[cat[...]]` is a shape change handled by the pure taxonomy.
            return None
        registry = component["registry"]
        field_str = component["field_str"]
        if field_str != _default_name_field(registry):
            return None
        scope = _component_scope(registry, component, models, cat_cache=cat_cache)
        if scope is None:
            return None
        lookups.append((registry, field_str, scope))
    return lookups


def _component_scope(
    registry: Any,
    component: dict[str, Any],
    models: LamindbModels,
    *,
    cat_cache: dict[Any, Any] | None = None,
) -> dict[str, Any] | None:
    """The filter keywords narrowing `registry` to the labels `component` admits.

    A `Registry[<type>]` component admits a record of the type or of any of
    its subtypes (see :func:`_type_scope_depths`), and a filter payload
    (`source__uid=...`) narrows that further, as lamindb's curator validates.
    Empty for an unfiltered component; `None` when the filter cannot be read
    or the subtree cannot be walked, so the caller keeps the block.
    """
    scope: dict[str, Any] = {}
    type_uid = component.get("type_uid")
    if type_uid:
        try:
            depths = _cached_type_scope(registry, type_uid, cat_cache)
        except Exception:
            return None
        scope["type_id__in"] = sorted(depths)
    filter_str = component.get("filter_str")
    if filter_str:
        lookups = models.cat_filter_lookups(filter_str)
        if lookups is None:
            return None
        scope.update(lookups)
    return scope


def _cached_type_scope(
    registry: Any, type_uid: str, cat_cache: dict[Any, Any] | None
) -> dict[Any, int]:
    """:func:`_type_scope_depths`, walked once per run when `cat_cache` is given."""
    if cat_cache is None:
        return _type_scope_depths(registry, type_uid)
    key = ("type_scope", id(registry), type_uid)
    if key not in cat_cache:
        cat_cache[key] = _type_scope_depths(registry, type_uid)
    return cat_cache[key]


def _type_scope_depths(registry: Any, type_uid: str) -> dict[Any, int]:
    """The types whose records a `Registry[<type_uid>]` value may be, by depth.

    lamindb validates such a value subtype-inclusively (`query_records`): a
    record whose type is the type itself or any type below it, walking the
    default branches only -- which `Registry.filter` applies.  Maps the id of
    the type and of each subtype to how far below the type its records sit
    (`1` for the type's own records): when one name labels records at several
    depths, lamindb takes the topmost.  Empty when no record carries
    `type_uid`, so nothing is admitted.
    """
    type_record = registry.objects.filter(uid=type_uid).first()
    if type_record is None:
        return {}
    depths = {type_record.id: 1}
    frontier = {type_record.id}
    depth = 1
    while frontier:
        depth += 1
        found = set(
            registry.filter(type_id__in=frontier, is_type=True).values_list(
                "id", flat=True
            )
        )
        frontier = found - depths.keys()
        depths.update(dict.fromkeys(frontier, depth))
    return depths


def _scope_key(scope: dict[str, Any]) -> tuple[Any, ...]:
    """A hashable spelling of a lookup `scope`, to memoize label lookups by."""
    return tuple(
        sorted(
            (name, tuple(value) if isinstance(value, list) else value)
            for name, value in scope.items()
        )
    )


def _default_name_field(registry: Any) -> str:
    """The field a plain `cat[Registry]` validates against (mirrors lamindb)."""
    return getattr(registry, "_name_field", None) or "name"


def _resolve_registry_path(text: str) -> Any | None:
    """Resolve a `serialize_config` dotted registry class path to its class.

    Returns `None` for anything that is not a `lamindb` / `bionty` /
    `pertdb` registry class path (canonical `cat[...]` strings, plain scalar
    tokens, builtins, etc.), so the caller can fall back to lamindb's parser.
    """
    text = text.strip()
    if "[" in text or "|" in text or "." not in text:
        return None
    module_name, _, class_name = text.rpartition(".")
    if not module_name.startswith(("lamindb.models", "bionty.models", "pertdb")):
        return None
    try:
        import importlib

        module = importlib.import_module(module_name)
        return getattr(module, class_name)
    except Exception:
        return None


# === Schema members: who validated against them ===============================

# Where the rows a `Schema` validated live, keyed by the schema's reverse
# relation naming them and valued by the query path from that row registry back
# to the schema.
#
# An `Artifact` / `Collection` carries its validating schema directly.  A
# `Record` does not: its `schema` foreign key is only meaningful on a *type*
# (the sheet definition, `is_type=True`), so the validated rows are the records
# *of* that type -- one hop further, and with the sheet definitions themselves
# excluded since they carry no feature values of their own.
#
# `Schema.artifacts` (the many-to-many feature-set link) is deliberately
# absent: it records which artifacts carry the schema as a component of their
# own, not which rows the schema validated.
_SCHEMA_ROW_SOURCES: dict[str, str] = {
    "records": "type__schema_id__in",
    "validated_artifacts": "schema_id__in",
    "validated_collections": "schema_id__in",
}


class SheetRowsCheck(NamedTuple):
    """How a sheet's rows fare against a schema it would be pointed at.

    `rows` were checked, and `failure` is lamindb's account of what does not
    validate -- `None` when every row does.
    """

    rows: int
    failure: str | None


class _Rollback(Exception):
    """Unwinds the savepoint a check runs in, carrying its result out."""


def sheet_rows(sheet: Any) -> Any:
    """The rows of `sheet` a schema it is pointed at has to admit.

    Every data record of the sheet that is not in the trash -- archived ones and
    those on another branch included, as everywhere else in this module -- and
    no record type nested below it, which lamindb does not validate as a row.
    The one set :func:`sheet_rows_validate` and :func:`sheet_member_omissions`
    both judge, so a count of it is what they speak for.
    """
    return sheet.records.exclude(branch_id=-1).exclude(is_type=True)


def sheet_rows_validate(sheet: Any, schema: Any) -> SheetRowsCheck | None:
    """Whether every row of `sheet` validates against `schema`.

    Asked before pointing a sheet at another schema: the rows were validated
    against the old one, and the new one would judge every row written from now
    on.  The :func:`sheet_rows` are exported the way lamindb exports a sheet and
    curated with `DataFrameCurator` -- lamindb's own verdict, in one batch.
    Not `Record.to_dataframe`, which exports the default branches only: an
    archived row would be counted but never curated.  The check runs in a
    savepoint that is always rolled back, since curating may save the public
    ontology records a value names.

    The curator judges columns, and a blank column passes for a nullable member
    even when the member is required; :func:`sheet_member_omissions` is the
    per-row half.

    `None` when the rows cannot be exported or curated.
    """
    from django.db import transaction

    try:
        with transaction.atomic():
            raise _Rollback(_curate_sheet_rows(sheet, schema))
    except _Rollback as outcome:
        return outcome.args[0]
    except Exception:
        return None


def _curate_sheet_rows(sheet: Any, schema: Any) -> SheetRowsCheck | None:
    """The body of :func:`sheet_rows_validate`, inside its savepoint."""
    from lamindb.curators import DataFrameCurator
    from lamindb.errors import ValidationError

    try:
        frame = sheet_rows(sheet).to_dataframe(
            include="features", is_run_input=False, limit=None
        )
    except Exception:
        return None
    if not len(frame):
        return SheetRowsCheck(rows=0, failure=None)
    internal = [c for c in frame.columns if str(c).startswith("__lamindb_record_")]
    frame = frame.drop(columns=internal)
    try:
        DataFrameCurator(frame, schema).validate()
    except ValidationError as exc:
        return SheetRowsCheck(rows=len(frame), failure=_compact(str(exc)))
    except Exception:
        return None
    return SheetRowsCheck(rows=len(frame), failure=None)


def _compact(message: str, *, limit: int = 300) -> str:
    """`message` on one line, cut at `limit` characters."""
    text = " ".join(message.split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


class MemberOmissions(NamedTuple):
    """How the rows a schema validates fare against making a member required.

    `omitting` of `total` rows carry no value for the member, and `examples`
    names the first few of them (at most :data:`_MAX_FAILURE_EXAMPLES`).

    `total == 0` means the schema validated nothing at all, which is the other
    way an optional-to-required narrowing can be data-safe.
    """

    total: int
    omitting: int
    examples: list[str]


class MemberUsage(NamedTuple):
    """How the rows a schema validates fare against *removing* a member.

    `carrying` of `total` rows hold a value for the member, and `examples`
    names the first few of them (at most :data:`_MAX_FAILURE_EXAMPLES`).

    `carrying == 0` means the removal strands nothing: either the schema
    validated no rows at all, or none of them ever carried the member.
    """

    total: int
    carrying: int
    examples: list[str]


def _schema_closure(schema: Any) -> list[Any]:
    """`schema` plus every composite schema that embeds it, transitively.

    A sheet schema is usually reached indirectly: an experiment schema declares
    it as a *component*, and the rows validated against the composite are just as
    invalidated by making one of the component's members required.  Walking
    `composites` (the reverse of `Schema.components`) collects every schema
    whose rows have to be counted.
    """
    seen: dict[Any, Any] = {schema.pk: schema}
    frontier = [schema]
    while frontier:
        composites = getattr(frontier.pop(), "composites", None)
        if composites is None:
            continue
        for composite in composites.all():
            if composite.pk not in seen:
                seen[composite.pk] = composite
                frontier.append(composite)
    return list(seen.values())


def _annotation_lookups(row_model: Any) -> list[str]:
    """Query paths from `row_model` to the tables holding its feature values.

    A row's annotations live in per-registry link tables named after the row
    registry (`RecordJson` / `RecordRecord` / `ArtifactULabel` / ...), each
    carrying a `feature` foreign key plus one back to the row.  Two rules pick
    exactly those out of the reverse relations:

    * the link table's name starts with the row registry's -- which excludes the
      mirror tables where the row is the *value* rather than the subject
      (`RunRecord`, `ProjectRecord`, `ArtifactRecord` seen from `Record`);
      and
    * the relation is reached through the *subject* foreign key (`record` on
      `Record`), not through a self-referential `value` one, which is what
      keeps `RecordRecord` from being counted twice.

    Artifact scalar values are the one annotation that is not a per-registry link
    table: they live in the shared `JsonValue`, linked many-to-many.  Any
    many-to-many whose target carries a `feature` foreign key is included for
    that reason.

    Returns the query path of each, usable as `exclude(<path>__feature_id=...)`.
    """
    prefix = row_model.__name__
    subject = prefix.lower()
    lookups: list[str] = []
    for relation in row_model._meta.related_objects:
        link_model = relation.related_model
        if not link_model.__name__.startswith(prefix):
            continue
        if relation.field.name != subject:
            continue
        if not any(field.name == "feature" for field in link_model._meta.get_fields()):
            continue
        lookups.append(relation.get_accessor_name())
    lookups.extend(
        field.name
        for field in row_model._meta.get_fields()
        if getattr(field, "many_to_many", False)
        and not field.auto_created
        and any(
            related.name == "feature"
            for related in field.related_model._meta.get_fields()
        )
    )
    return lookups


def _row_label(row: Any) -> str:
    """A short human identifier for a validated row."""
    for attr in ("name", "key"):
        value = getattr(row, attr, None)
        if value:
            return str(value)
    return f"uid {getattr(row, 'uid', '?')!r}"


def schema_rows_omitting_member(
    schema: Any, feature: Any, *, registries: Collection[str] | None = None
) -> list[Any]:
    """The rows `schema` validates that carry no value for `feature`.

    The rows a migration has to backfill before the member can be made required;
    see :func:`schema_member_omissions` for how they are found, and for
    `registries`.  Raises whatever the underlying queries raise -- a migration
    runs inside the apply transaction, so a failure here must roll the apply back
    rather than be swallowed.
    """
    rows: list[Any] = []
    schema_ids = [entry.pk for entry in _schema_closure(schema)]
    for row_model, lookup in _schema_row_sources(schema, registries):
        rows.extend(_omitting_queryset(row_model, lookup, schema_ids, feature))
    return rows


def schema_member_omissions(
    schema: Any,
    feature: Any,
    *,
    exempt: dict[str, set[Any]] | None = None,
    registries: Collection[str] | None = None,
) -> MemberOmissions | None:
    """How many rows `schema` validates carry no value for `feature`.

    Answers the data question behind an optional-to-required narrowing: the
    taxonomy refuses one because a row that omitted the member would no longer
    validate, and this counts the rows that actually did.  `omitting == 0`
    proves the narrowing safe (either the schema validated nothing, or every row
    already carries a value); anything else quantifies and illustrates the block.

    Counted across every registry a schema validates rows of -- `Record` (the
    sheet case), `Artifact` and `Collection`, see
    :data:`_SCHEMA_ROW_SOURCES` -- for `schema` *and* every composite that
    embeds it (see :func:`_schema_closure`).  Trashed rows (`branch_id == -1`)
    are excluded, matching the rest of this module: they would not be
    re-validated, so they cannot be invalidated.

    `exempt` maps a row registry's name onto the primary keys of rows a
    migration declared as deliberately null (see
    :meth:`~lamindb_setup.evolve.authoring._migrations.MemberMigrationContext.accept_null`).
    Those stay in `total` -- they are rows the schema validates -- but do not
    count as omitting: lamindb has no stored null, so a null-filled row is
    indistinguishable from a skipped one until the migration says which it is.

    The exempted set is subtracted in Python rather than as a `pk__in`: it
    holds one entry per nulled row and nulling writes nothing, so it is exactly
    the set that grows without bound on a bulk backfill -- and an `IN` list
    wide enough would trip the backend's bind-parameter limit, which the
    `except` below would then report as an unprovable backfill rather than as
    the query failure it is.  Only the *omitting* rows are streamed, which after
    a successful backfill is the exempted set itself.

    `registries` limits the count to the named row registries (`"Record"`
    for the rows of a sheet); all three by default.

    Returns `None` when the rows cannot be introspected (an unintrospectable
    relation or a failing query) so the caller stays conservative and keeps the
    block.
    """
    try:
        schema_ids = [entry.pk for entry in _schema_closure(schema)]
        total = 0
        omitting = 0
        examples: list[str] = []
        for row_model, lookup in _schema_row_sources(schema, registries):
            rows = _schema_row_queryset(row_model, lookup, schema_ids)
            missing = _omitting_queryset(row_model, lookup, schema_ids, feature)
            total += rows.count()
            accepted = (exempt or {}).get(row_model.__name__)
            if not accepted:
                omitting += missing.count()
                unanswered = missing[: _MAX_FAILURE_EXAMPLES - len(examples)]
            else:
                unanswered = []
                for row in missing.iterator():
                    if row.pk in accepted:
                        continue
                    omitting += 1
                    if len(unanswered) < _MAX_FAILURE_EXAMPLES:
                        unanswered.append(row)
            if len(examples) < _MAX_FAILURE_EXAMPLES:
                examples.extend(_row_label(row) for row in unanswered)
                del examples[_MAX_FAILURE_EXAMPLES:]
        return MemberOmissions(total=total, omitting=omitting, examples=examples)
    except Exception:
        # Cannot introspect or query a relation -> stay conservative.
        return None


def sheet_member_omissions(sheet: Any, feature: Any | None) -> MemberOmissions | None:
    """How many of the :func:`sheet_rows` of `sheet` carry no value for `feature`.

    The per-row half of pointing a sheet at a schema that requires `feature`
    (see :func:`sheet_rows_validate`).  lamindb refuses a row joining a sheet
    without a value for a required member, and making a member required in place
    is judged the same way (see :func:`schema_member_omissions`); its curator
    only sees a column, which the export fills with blanks.

    `feature` is `None` for one the apply has yet to create, which no row
    carries.  Returns `None` when the rows cannot be queried.
    """
    try:
        rows = sheet_rows(sheet)
        missing = rows
        if feature is not None:
            for annotation in _annotation_lookups(rows.model):
                missing = missing.exclude(**{f"{annotation}__feature_id": feature.pk})
        examples = [
            _row_label(row) for row in missing.order_by("pk")[:_MAX_FAILURE_EXAMPLES]
        ]
        return MemberOmissions(
            total=rows.count(), omitting=missing.count(), examples=examples
        )
    except Exception:
        return None


def _schema_row_sources(
    schema: Any, registries: Collection[str] | None = None
) -> list[tuple[Any, str]]:
    """`(row registry, query path back to the schema)` for each source of rows.

    `registries` keeps only the row registries of those names.
    """
    return [
        (relation.related_model, lookup)
        for relation in schema._meta.related_objects
        if (lookup := _SCHEMA_ROW_SOURCES.get(relation.get_accessor_name())) is not None
        and (registries is None or relation.related_model.__name__ in registries)
    ]


def _schema_row_queryset(row_model: Any, lookup: str, schema_ids: list[Any]):
    """Every live row of `row_model` validated against one of `schema_ids`.

    `is_type` rows are dropped where the registry has the flag: a sheet
    definition is not one of the rows its own schema validates.
    """
    queryset = row_model.objects.filter(**{lookup: schema_ids}).exclude(branch_id=-1)
    if any(field.name == "is_type" for field in row_model._meta.get_fields()):
        queryset = queryset.exclude(is_type=True)
    return queryset


def _omitting_queryset(
    row_model: Any, lookup: str, schema_ids: list[Any], feature: Any
):
    """Those rows again, minus the ones carrying a value for `feature`.

    One `NOT IN` sub-select per annotation table (see
    :func:`_annotation_lookups`), each keyed on the table's indexed `feature_id`
    and therefore cheap; the whole answer is a single query per row registry.

    Deliberately knows nothing about a migration's accepted nulls: this is also
    what :func:`schema_rows_omitting_member` hands a backfill to iterate, and a
    backfill has to see every row that lacks a value, including the ones a
    previous run of it nulled.  The exemption is a post-check concern and lives
    in :func:`schema_member_omissions`.
    """
    queryset = _schema_row_queryset(row_model, lookup, schema_ids)
    for annotation in _annotation_lookups(row_model):
        queryset = queryset.exclude(**{f"{annotation}__feature_id": feature.pk})
    return queryset


def _carrying_queryset(
    row_model: Any, lookup: str, schema_ids: list[Any], feature: Any
):
    """The rows that *do* carry a value for `feature`.

    Deliberately expressed as the complement of :func:`_omitting_queryset` rather
    than as an `OR` across the annotation tables: the two counts are compared
    against the same total (and, for a drop, against zero afterwards), so they
    have to partition the rows exactly.  Deriving one from the other makes that
    true by construction, and avoids the row duplication a multi-relation `OR`
    join would need a `DISTINCT` to undo.
    """
    return _schema_row_queryset(row_model, lookup, schema_ids).exclude(
        pk__in=_omitting_queryset(row_model, lookup, schema_ids, feature).values("pk")
    )


def schema_rows_carrying_member(schema: Any, feature: Any) -> list[Any]:
    """The rows `schema` validates that carry a value for `feature`.

    The rows a migration has to clear before the member can be removed; see
    :func:`schema_member_usage` for how they are found.  Raises whatever the
    underlying queries raise -- a migration runs inside the apply transaction, so
    a failure here must roll the apply back rather than be swallowed.
    """
    rows: list[Any] = []
    schema_ids = [entry.pk for entry in _schema_closure(schema)]
    for row_model, lookup in _schema_row_sources(schema):
        rows.extend(_carrying_queryset(row_model, lookup, schema_ids, feature))
    return rows


def schema_member_usage(schema: Any, feature: Any) -> MemberUsage | None:
    """How many rows `schema` validates carry a value for `feature`.

    Answers the data question behind removing a member: the member stops being
    part of what the schema declares, but the values already stored for it do not
    go anywhere, so `carrying == 0` means the removal strands nothing and
    anything else quantifies and illustrates what it would leave behind.

    Counted over the same rows as :func:`schema_member_omissions` -- of which it
    is the exact complement -- so `carrying + omitting == total`.

    Returns `None` when the rows cannot be introspected (an unintrospectable
    relation or a failing query) so the caller stays conservative.
    """
    try:
        schema_ids = [entry.pk for entry in _schema_closure(schema)]
        total = 0
        carrying = 0
        examples: list[str] = []
        for row_model, lookup in _schema_row_sources(schema):
            rows = _schema_row_queryset(row_model, lookup, schema_ids)
            present = _carrying_queryset(row_model, lookup, schema_ids, feature)
            total += rows.count()
            carrying += present.count()
            if len(examples) < _MAX_FAILURE_EXAMPLES:
                examples.extend(
                    _row_label(row)
                    for row in present[: _MAX_FAILURE_EXAMPLES - len(examples)]
                )
        return MemberUsage(total=total, carrying=carrying, examples=examples)
    except Exception:
        # Cannot introspect or query a relation -> stay conservative.
        return None


class NonMemberRows(NamedTuple):
    """The sheet rows a schema validates that hold a value for a non-member feature.

    `rows` of them do, `examples` names the first few.
    """

    rows: int
    examples: list[str]


def schema_nonmember_rows(schema: Any) -> NonMemberRows | None:
    """How many sheet rows `schema` validates hold a value for a feature it lacks.

    What turning `maximal_set` on would refuse in a row -- a feature the schema
    does not declare -- counted over the sheets using the schema or a composite
    embedding it.  Datasets have no such count: lamindb records only the columns
    a schema that is not `flexible` declares, so one carrying another column
    leaves no trace.  `None` when the rows cannot be read.
    """
    from django.db.models import Q

    try:
        schema_ids = [entry.pk for entry in _schema_closure(schema)]
        feature_model = type(schema)._meta.get_field("features").related_model
        nonmembers = feature_model._base_manager.exclude(
            pk__in=schema.features.values("pk")
        ).values("pk")
        count = 0
        examples: list[str] = []
        for row_model, lookup in _schema_row_sources(schema):
            if row_model.__name__ != "Record":
                continue
            rows = _schema_row_queryset(row_model, lookup, schema_ids)
            holding = Q()
            for annotation in _annotation_lookups(row_model):
                holding |= Q(**{f"{annotation}__feature_id__in": nonmembers})
            if not holding:
                continue
            present = rows.filter(pk__in=rows.filter(holding).values("pk"))
            count += present.count()
            examples.extend(
                _row_label(row)
                for row in present[: max(0, _MAX_FAILURE_EXAMPLES - len(examples))]
            )
        return NonMemberRows(rows=count, examples=examples)
    except Exception:
        return None


def schema_member_conflicts(schema: Any, feature: Any) -> list[str] | None:
    """Other live schemas that still declare `feature` and validate rows in scope.

    Scoping a drop to "the rows this schema validates" does not make those values
    exclusively this schema's: a row can be validated by several schemas at once
    (a sheet schema embedded in two experiment schemas, say), and one of them may
    still declare the member.  Deleting the value would satisfy this schema and
    invalidate that one, so the drop is refused and the other schema named.

    Only *direct* membership counts -- a composite that merely embeds `schema`
    does not declare the feature itself, and its rows are already part of this
    schema's own scope (see :func:`_schema_closure`).

    Returns the offending schema names (possibly empty), or `None` when the
    question cannot be answered, which the caller must treat as unproven.
    """
    try:
        schema_ids = [entry.pk for entry in _schema_closure(schema)]
        in_scope: dict[Any, set[Any]] = {}
        for row_model, lookup in _schema_row_sources(schema):
            in_scope[row_model] = set(
                _carrying_queryset(row_model, lookup, schema_ids, feature).values_list(
                    "pk", flat=True
                )
            )
        if not any(in_scope.values()):
            return []
        others = (
            type(schema)
            .objects.filter(features=feature)
            .exclude(pk__in=schema_ids)
            .exclude(branch_id=-1)
            .distinct()
        )
        conflicts: set[str] = set()
        for other in others:
            other_ids = [entry.pk for entry in _schema_closure(other)]
            for row_model, lookup in _schema_row_sources(other):
                rows = in_scope.get(row_model)
                if not rows:
                    continue
                if (
                    _schema_row_queryset(row_model, lookup, other_ids)
                    .filter(pk__in=rows)
                    .exists()
                ):
                    conflicts.add(_row_label(other))
                    break
        return sorted(conflicts)
    except Exception:
        # Cannot introspect or query a relation -> the caller keeps the block.
        return None


class SchemaUsage(NamedTuple):
    """What a schema has validated, for a change that cannot be re-checked in place.

    Counted over the schema and every composite that embeds it (see
    :func:`_schema_closure`): `artifacts` and `collections` validated against
    one of them, and the `rows` of the sheets that use one of them.
    `examples` labels the first few artifacts and collections (at most
    :data:`_MAX_FAILURE_EXAMPLES`), and `sheets` names every sheet with rows.
    """

    artifacts: int
    collections: int
    rows: int
    examples: list[str]
    sheets: list[str]

    @property
    def total(self) -> int:
        """Everything the schema validated, across the three registries."""
        return self.artifacts + self.collections + self.rows


def schema_validated_usage(
    schema: Any, *, exclude_sheet_exports: bool = False
) -> SchemaUsage | None:
    """Everything `schema` -- directly or through a composite -- has validated.

    Answers the data question behind changing a schema's `index`.  Unlike a
    member narrowing there is no per-row test for it: the index decides how a
    dataset is curated as a whole, so the only proof that the change invalidates
    nothing is that nothing was validated.  The same three sources a member
    narrowing counts (see :data:`_SCHEMA_ROW_SOURCES`), over the same composite
    closure, with trashed rows excluded.

    `exclude_sheet_exports` leaves out the artifacts a sheet was exported to,
    for a change the sheet-export narrowing already answers for them.

    Returns `None` when the rows cannot be introspected, so the caller keeps the
    block.
    """
    try:
        schema_ids = [entry.pk for entry in _schema_closure(schema)]
        counts = {"Artifact": 0, "Collection": 0, "Record": 0}
        examples: list[str] = []
        sheets: list[str] = []
        for row_model, lookup in _schema_row_sources(schema):
            rows = _schema_row_queryset(row_model, lookup, schema_ids)
            name = row_model.__name__
            if name == "Artifact" and exclude_sheet_exports:
                from ._sheet_exports import export_marker

                rows = rows.exclude(export_marker())
            counts[name] = counts.get(name, 0) + rows.count()
            if name == "Record":
                sheets = sorted(
                    {
                        str(sheet)
                        for sheet in rows.order_by()
                        .values_list("type__name", flat=True)
                        .distinct()
                        if sheet
                    }
                )
            elif len(examples) < _MAX_FAILURE_EXAMPLES:
                examples.extend(
                    _row_label(row)
                    for row in rows[: _MAX_FAILURE_EXAMPLES - len(examples)]
                )
        return SchemaUsage(
            artifacts=counts["Artifact"],
            collections=counts["Collection"],
            rows=counts["Record"],
            examples=examples,
            sheets=sheets,
        )
    except Exception:
        # Cannot introspect or query a relation -> stay conservative.
        return None


def prefetch_schema_usage_presence(schemas: list[Any]) -> dict[Any, bool]:
    """Bulk-answer whether each schema validated anything at all.

    Returns `schema pk -> bool`: whether any live sheet row, artifact or
    collection was validated against the schema or a composite embedding it,
    transitively -- the question :func:`schema_validated_usage` answers per
    schema, reduced to whether its total is zero.  That is all most callers need
    to know, since a schema that validated nothing takes any change, so a plan
    touching many schemas asks it once: one query per level of composite nesting
    and a single `UNION` across the three row registries.

    A missing key means "ask per schema"; `{}` when the batch fails, so a
    caller falls back to :func:`schema_validated_usage`, which is conservative.
    """
    if not schemas:
        return {}
    try:
        sample = schemas[0]
        using = sample._state.db
        links = type(sample).components.through._default_manager.using(using)
        parents: dict[Any, set[Any]] = {}
        seen = {schema.pk for schema in schemas}
        frontier = set(seen)
        while frontier:
            found: set[Any] = set()
            for chunk in _in_chunks(frontier):
                for component, composite in links.filter(
                    component_id__in=chunk
                ).values_list("component_id", "composite_id"):
                    parents.setdefault(component, set()).add(composite)
                    found.add(composite)
            frontier = found - seen
            seen |= frontier
        used = _schema_ids_with_rows(sample, seen, using)
    except Exception:
        return {}

    def closure(pk: Any) -> set[Any]:
        ids = {pk}
        stack = [pk]
        while stack:
            for parent in parents.get(stack.pop(), ()):
                if parent not in ids:
                    ids.add(parent)
                    stack.append(parent)
        return ids

    return {schema.pk: bool(closure(schema.pk) & used) for schema in schemas}


def _schema_ids_with_rows(sample: Any, schema_ids: set[Any], using: Any) -> set[Any]:
    """The subset of `schema_ids` some live row, artifact or collection validates.

    The same sources and exclusions as :func:`schema_validated_usage`, combined
    into one `UNION`; a backend that refuses the compound select falls back to
    one query per registry.
    """
    used: set[Any] = set()
    for chunk in _in_chunks(schema_ids):
        querysets = [
            _schema_row_queryset(row_model, lookup, chunk)
            .using(using)
            .order_by()
            .values_list(lookup.removesuffix("__in"), flat=True)
            for row_model, lookup in _schema_row_sources(sample)
        ]
        if not querysets:
            continue
        try:
            used.update(querysets[0].union(*querysets[1:]))
        except Exception:
            for queryset in querysets:
                used.update(queryset)
    return used


class NonMemberColumns(NamedTuple):
    """Columns an artifact's curation recorded for a schema that it does not declare.

    Curating an artifact records, per slot, a schema of the columns it matched
    (`ArtifactSchema`; see :mod:`._schema_collisions`).  For a `flexible`
    schema those include every column whose name matched a registered feature, so
    the recorded schema lists members the validating schema does not declare.

    `link` is the artifact's `ArtifactSchema` row for the slot the schema
    validated -- `columns` when it validated the dataframe itself, else the slot
    a composite gives it -- and `recorded` the schema that row points at.
    `features` are the members of `recorded` the schema does not declare.
    """

    artifact: Any
    slot: str
    link: Any
    recorded: Any
    features: list[Any]


class NonMemberUsage(NamedTuple):
    """How many artifacts recorded columns a schema does not declare, and which.

    `features` names those features and `examples` the first few artifacts
    (at most :data:`_MAX_FAILURE_EXAMPLES`).
    """

    artifacts: int
    features: list[str]
    examples: list[str]


def _in_chunks(values: Iterable[Any]) -> Iterator[list[Any]]:
    """`values` in chunks small enough for one `IN` clause."""
    items = sorted(set(values))
    for start in range(0, len(items), _PREFETCH_VALUE_BATCH_SIZE):
        yield items[start : start + _PREFETCH_VALUE_BATCH_SIZE]


def feature_members(
    model: Any, schema_ids: Iterable[Any], using: Any
) -> dict[Any, list[Any]]:
    """The feature ids each of `schema_ids` links as members, in link order.

    Read through the member link table, so a schema with another `itype` (genes,
    say) has none.  `model` is the live `Schema` class.
    """
    through = model._meta.get_field("features").through
    members: dict[Any, list[Any]] = {}
    for chunk in _in_chunks(schema_ids):
        rows = (
            through._base_manager.using(using)
            .filter(schema_id__in=chunk)
            .order_by("id")
            .values_list("schema_id", "feature_id")
        )
        for schema_id, feature_id in rows:
            members.setdefault(schema_id, []).append(feature_id)
    return members


def _recorded_slots(schema: Any) -> list[tuple[Any, str]]:
    """`(validating schema pk, slot)` for every way `schema` records columns.

    A schema validating a dataframe itself records its columns under `columns`;
    as a component of a composite, under the slot the composite gives it.  Only
    a composite slotting it directly counts: lamindb names an artifact's slots
    after the composite the artifact was validated against.
    """
    from ._sheet_exports import COLUMNS_SLOT

    slots: list[tuple[Any, str]] = [(schema.pk, COLUMNS_SLOT)]
    for composite_id, slot in schema.links_composite.values_list(
        "composite_id", "slot"
    ):
        if slot:
            slots.append((composite_id, slot))
    return slots


def _nonmember_links(schema: Any) -> list[tuple[Any, Any, str, list[Any]]]:
    """`(link pk, artifact pk, slot, non-member feature ids)` per offending slot.

    Every live artifact's `ArtifactSchema` row for a slot `schema` validated
    whose recorded schema lists features `schema` does not declare.  A row
    recording `schema` itself lists nothing else by definition.
    """
    from django.db.models import Q

    links = schema._links_artifact.model
    using = schema._state.db
    condition = Q()
    for validating_id, slot in _recorded_slots(schema):
        condition |= Q(artifact__schema_id=validating_id, slot=slot)
    rows = list(
        links._base_manager.using(using)
        .filter(condition)
        .exclude(artifact__branch_id=-1)
        .exclude(schema_id=schema.pk)
        .order_by("artifact_id", "slot")
        .values_list("pk", "artifact_id", "slot", "schema_id")
    )
    if not rows:
        return []
    members = feature_members(
        type(schema), [schema.pk, *(row[3] for row in rows)], using
    )
    declared = set(members.get(schema.pk, []))
    found: list[tuple[Any, Any, str, list[Any]]] = []
    for pk, artifact_id, slot, recorded_id in rows:
        extra = [f for f in members.get(recorded_id, []) if f not in declared]
        if extra:
            found.append((pk, artifact_id, slot, extra))
    return found


def schema_nonmember_columns(schema: Any) -> list[NonMemberColumns]:
    """Every artifact slot `schema` curated that recorded columns it does not declare.

    What turning `flexible` off leaves behind (see :class:`NonMemberColumns`).
    Raises whatever the underlying queries raise -- a migration runs inside the
    apply transaction, so a failure here must roll the apply back rather than be
    swallowed.
    """
    found = _nonmember_links(schema)
    if not found:
        return []
    using = schema._state.db
    links = schema._links_artifact.model
    by_pk: dict[Any, Any] = {}
    for chunk in _in_chunks(row[0] for row in found):
        for link in (
            links._base_manager.using(using)
            .filter(pk__in=chunk)
            .select_related("artifact", "schema")
        ):
            by_pk[link.pk] = link
    feature_model = type(schema)._meta.get_field("features").related_model
    features: dict[Any, Any] = {}
    for chunk in _in_chunks(f for row in found for f in row[3]):
        for feature in feature_model._base_manager.using(using).filter(pk__in=chunk):
            features[feature.pk] = feature
    return [
        NonMemberColumns(
            artifact=link.artifact,
            slot=slot,
            link=link,
            recorded=link.schema,
            features=[features[f] for f in extra if f in features],
        )
        for pk, _artifact_id, slot, extra in found
        if (link := by_pk.get(pk)) is not None
    ]


def schema_nonmember_usage(schema: Any) -> NonMemberUsage | None:
    """How many artifacts `schema` curated recorded columns it does not declare.

    The cheap, read-only summary of :func:`schema_nonmember_columns` that a diff
    reports.  Returns `None` when the rows cannot be introspected.
    """
    try:
        found = _nonmember_links(schema)
        if not found:
            return NonMemberUsage(artifacts=0, features=[], examples=[])
        using = schema._state.db
        artifact_ids = sorted({row[1] for row in found})
        artifact_model = schema._links_artifact.model._meta.get_field(
            "artifact"
        ).related_model
        examples = [
            _row_label(artifact)
            for artifact in artifact_model._base_manager.using(using)
            .filter(pk__in=artifact_ids[:_MAX_FAILURE_EXAMPLES])
            .order_by("pk")
        ]
        feature_model = type(schema)._meta.get_field("features").related_model
        names: set[str] = set()
        for chunk in _in_chunks(f for row in found for f in row[3]):
            names.update(
                feature_model._base_manager.using(using)
                .filter(pk__in=chunk)
                .values_list("name", flat=True)
            )
        return NonMemberUsage(
            artifacts=len(artifact_ids), features=sorted(names), examples=examples
        )
    except Exception:
        # Cannot introspect or query a relation -> report nothing.
        return None


def artifact_label_link_models(artifact_model: Any) -> list[tuple[Any, Any]]:
    """`(link model, subject field)` of every categorical link table on artifacts.

    The tables a categorical annotation of an artifact lives in
    (`ArtifactULabel`, `ArtifactRecord`, a bionty `ArtifactCellType`, ...):
    those :func:`_is_categorical_link_model` accepts whose annotated object (see
    :func:`link_subject_field`) is the artifact.
    """
    found: dict[Any, Any] = {}
    for relation in artifact_model._meta.related_objects:
        model = relation.related_model
        if model in found or not _is_categorical_link_model(model):
            continue
        subject = link_subject_field(model)
        if subject is not None and subject.related_model is artifact_model:
            found[model] = subject
    return list(found.items())


class SheetIndexConflicts(NamedTuple):
    """The sheet rows lamindb cannot re-key on its own when an index is added.

    `sheets` names the sheets that use the schema -- lamindb re-keys their rows
    whenever the index changes -- and `conflicts` counts the rows that already
    have a `Record.name` *and* a different value for the new index feature,
    which is the case lamindb would otherwise settle by prompting on stdin.
    `examples` describes the first few of them.
    """

    sheets: list[str]
    conflicts: int
    examples: list[str]


def sheet_index_name_conflicts(
    schema: Any,
    feature: Any,
    *,
    replaces_index: bool = False,
    models: LamindbModels = DEFAULT_MODELS,
) -> SheetIndexConflicts | None:
    """The rows of `schema`'s sheets whose name disagrees with `feature`'s value.

    Mirrors what lamindb's `migrate_record_sheet_index_on_schema_save` does when
    a schema that had no index gains `feature` as one: every row's value for the
    feature moves onto `Record.name`, and a row whose existing name differs from
    that value is a conflict only `index_name_conflict=` -- or a prompt -- can
    settle.  It reads exactly the rows lamindb will touch, trashed ones included,
    because that is what the save then acts on.

    A schema that *had* an index (`replaces_index`) never conflicts: lamindb
    first moves every name back onto the old index feature and clears it, so
    there is no name left to disagree with.  Only the sheets are listed then.

    Returns `None` when the rows cannot be read, or hold a value lamindb would
    refuse as an index, so the caller keeps the block.
    """
    try:
        record_model = schema.records.model
        sheets = list(
            record_model.objects.filter(is_type=True, schema_id=schema.pk).values_list(
                "id", "name"
            )
        )
        if not sheets:
            return SheetIndexConflicts(sheets=[], conflicts=0, examples=[])
        sheet_names = {pk: str(name or pk) for pk, name in sheets}
        if replaces_index:
            return SheetIndexConflicts(
                sheets=sorted(sheet_names.values()), conflicts=0, examples=[]
            )
        rows = record_model.objects.filter(is_type=False, type_id__in=list(sheet_names))
        links = (
            feature.links_recordjson.model.objects.filter(
                record_id__in=rows.values("id"), feature_id=feature.pk
            )
            .order_by("record_id")
            .values_list("record__name", "record__type_id", "value")
        )
        conflicts = 0
        examples: list[str] = []
        for name, type_id, raw in links:
            value = models.index_value_as_record_name(raw, feature)
            if value is None or not name or name == value:
                continue
            conflicts += 1
            if len(examples) < _MAX_FAILURE_EXAMPLES:
                # The sheet is only worth naming when there is more than one.
                where = (
                    f"sheet {sheet_names.get(type_id, type_id)!r}, "
                    if len(sheet_names) > 1
                    else ""
                )
                examples.append(f"{where}row {name!r}: {feature.name}={value!r}")
        return SheetIndexConflicts(
            sheets=sorted(sheet_names.values()),
            conflicts=conflicts,
            examples=examples,
        )
    except Exception:
        # Cannot read the rows, or one holds a value lamindb would refuse as an
        # index -> the caller keeps the block.
        return None


def rows_index_name_conflicts(
    rows: Any, feature: Any, *, models: LamindbModels = DEFAULT_MODELS
) -> dict[Any, tuple[str, str]] | None:
    """The `rows` whose `Record.name` differs from their value for `feature`.

    The case :func:`sheet_index_name_conflicts` looks for, over the rows a
    `move_rows` moves into a sheet keyed by `feature`: each keeps its name,
    which the sheet reads as its index value, so a row also holding a different
    value for the feature has to be settled by `index_name_conflict`.  Keyed
    by the row's pk, with its `(name, value)`.  `None` when they cannot be
    read, or one holds a value lamindb would refuse as an index.
    """
    try:
        links = (
            feature.links_recordjson.model.objects.filter(
                record_id__in=rows.values("id"), feature_id=feature.pk
            )
            .order_by("record_id")
            .values_list("record_id", "record__name", "value")
        )
        found: dict[Any, tuple[str, str]] = {}
        for pk, name, raw in links:
            value = models.index_value_as_record_name(raw, feature)
            if value is None or not name or name == value:
                continue
            found[pk] = (name, value)
        return found
    except Exception:
        return None


def _validates_int(value: Any) -> bool:
    """Whether `value` is representable as an `int` without loss."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return value.is_integer()
    if isinstance(value, str):
        try:
            int(value.strip())
        except (TypeError, ValueError):
            return False
        return True
    return False


def _validates_float(value: Any) -> bool:
    """Whether `value` parses as a `float`."""
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str):
        try:
            float(value.strip())
        except (TypeError, ValueError):
            return False
        return True
    return False


def _validates_str(value: Any) -> bool:
    """Whether `value` is a string.

    lamindb validates `str`, `path` and `url` identically -- all three
    require `pandas.api.types.is_string_dtype` (see
    `lamindb.base.dtypes`); `url` / `path` carry no format constraint and
    are "validated as str, but specially treated in the UI".  So a stored string
    already satisfies a `str -> url` / `str -> path` (or the reverse)
    narrowing, and this checker treats any string value as valid.
    """
    return isinstance(value, str)


_SCALAR_CHECKERS: dict[str, Callable[[Any], bool]] = {
    "int": _validates_int,
    "float": _validates_float,
    "str": _validates_str,
    "path": _validates_str,
    "url": _validates_str,
}
