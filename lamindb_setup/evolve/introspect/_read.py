"""Read the declarative objects of a connected LaminDB instance.

This module performs the only LaminDB-touching step of introspection: it reads
feature types, features, schema types, (named) schemas, record types and sheets
into plain, lamindb-free dataclasses.  Everything downstream (reverse mapping
and rendering) operates on these dataclasses so it can be unit-tested without an
instance.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

__all__ = [
    "FeatureInfo",
    "FeatureTypeInfo",
    "InstanceContents",
    "ObjectMeta",
    "RecordTypeInfo",
    "SchemaInfo",
    "SchemaTypeInfo",
    "read_instance",
]


@dataclass
class ObjectMeta:
    """Lightweight metadata (name, creator, space) for an object in the instance.

    Captured for every live object across the whole instance — regardless of the
    read scope — so a consumer can annotate a reference that points outside the
    read scope (e.g. a feature living in an excluded space) with its creator and
    space.
    """

    name: str | None = None
    created_by: str | None = None
    space_uid: str | None = None


@dataclass
class FeatureTypeInfo:
    """A feature type (`ln.Feature` with `is_type=True`)."""

    uid: str
    name: str
    type_uid: str | None
    space_uid: str | None = None
    type_name: str | None = None
    created_by: str | None = None
    created_at: str | None = None


@dataclass
class FeatureInfo:
    """A feature (`ln.Feature` with `is_type=False`)."""

    uid: str
    name: str
    type_uid: str | None
    dtype_str: str | None
    synonyms: list[str] = field(default_factory=list)
    unit: str | None = None
    description: str | None = None
    nullable: bool | None = None
    default_value: Any | None = None
    coerce: bool | None = None
    space_uid: str | None = None
    type_name: str | None = None
    created_by: str | None = None
    created_at: str | None = None


@dataclass
class SchemaTypeInfo:
    """A schema type (`ln.Schema` with `is_type=True`).

    `itype` / `feature_uids` / `slots` describe the content the schema type
    carries.  A schema type normally has none — it only groups schemas — but
    lamindb does not forbid it, and a schema type that does carry content is a
    schema mislabeled as a type (see :mod:`~lamindb_setup.evolve.maintenance._sheet_schemas`).
    """

    uid: str
    name: str
    description: str | None
    type_uid: str | None
    itype: str | None = None
    feature_uids: list[str] = field(default_factory=list)
    slots: dict[str, str] = field(default_factory=dict)
    space_uid: str | None = None
    type_name: str | None = None
    created_by: str | None = None
    created_at: str | None = None


@dataclass
class SchemaInfo:
    """A schema (`ln.Schema` with `is_type=False`).

    `name` is `None` for unnamed schemas (which LaminDB addresses by content
    hash); these are only reconstructed when referenced as a composite schema's
    slot component.
    """

    uid: str
    name: str | None
    description: str | None
    type_uid: str | None
    itype: str | None
    otype: str | None
    schema_dtype: str | None
    flexible: bool | None
    coerce: bool | None
    minimal_set: bool
    ordered_set: bool
    maximal_set: bool
    feature_uids: list[str] = field(default_factory=list)
    optional_feature_uids: list[str] = field(default_factory=list)
    index_feature_uid: str | None = None
    slots: dict[str, str] = field(default_factory=dict)
    formats: dict[str, Any] | None = None
    space_uid: str | None = None
    type_name: str | None = None
    created_by: str | None = None
    created_at: str | None = None


@dataclass
class RecordTypeInfo:
    """A record type or sheet (`ln.Record` with `is_type=True`).

    `schema_uid` is set when the record type is a sheet (constrained by a
    schema); it is `None` for a plain record type.
    """

    uid: str
    name: str
    description: str | None
    type_uid: str | None
    schema_uid: str | None
    space_uid: str | None = None
    single_space_uid: str | None = None
    is_experiment_sheet: bool = False
    exclude_children: bool = False
    type_name: str | None = None
    created_by: str | None = None


@dataclass
class InstanceContents:
    """All declarative objects read from an instance, keyed by uid where useful."""

    feature_types: list[FeatureTypeInfo] = field(default_factory=list)
    features: list[FeatureInfo] = field(default_factory=list)
    schema_types: list[SchemaTypeInfo] = field(default_factory=list)
    schemas: list[SchemaInfo] = field(default_factory=list)
    record_types: list[RecordTypeInfo] = field(default_factory=list)
    deleted_uids: set[str] = field(default_factory=set)
    """Uids of trashed objects (`branch_id == -1`) across the read registries.

    Used by the reverse step to flag out-of-scope references that point at a
    deleted object rather than one that is merely outside the read scope.
    """
    deleted_names: dict[str, str] = field(default_factory=dict)
    """Map of trashed object uid to name, for objects that have a name.

    Lets a consumer (e.g. prune) show the name of a deleted reference target
    rather than only its uid.
    """
    deleted_record_uids: set[str] = field(default_factory=set)
    """Uids of trashed records (`ln.Record` with `branch_id == -1`).

    Lets a consumer (e.g. repair dtypes) tell a record dtype that references a
    trashed record type apart from one that references a record type that does
    not exist at all.
    """
    deleted_record_names: dict[str, str] = field(default_factory=dict)
    """Map of trashed record uid to name, for trashed records that have a name."""
    object_meta: dict[str, ObjectMeta] = field(default_factory=dict)
    """Metadata for every live object in the instance, keyed by uid.

    Populated across the whole instance, ignoring the read scope, so consumers
    can tell a reference to a live object that merely lies outside the scope (and
    annotate it with its creator and space) apart from a reference to an object
    that no longer exists at all.
    """


def _type_uid(obj: Any) -> str | None:
    parent = getattr(obj, "type", None)
    return str(parent.uid) if parent is not None else None


def _type_name(obj: Any) -> str | None:
    parent = getattr(obj, "type", None)
    return getattr(parent, "name", None) if parent is not None else None


def _created_by(obj: Any) -> str | None:
    user = getattr(obj, "created_by", None)
    if user is None:
        return None
    return getattr(user, "handle", None) or getattr(user, "name", None)


def _space_uid(obj: Any) -> str | None:
    space = getattr(obj, "space", None)
    return str(space.uid) if space is not None else None


def _created_at(obj: Any) -> str | None:
    value = getattr(obj, "created_at", None)
    if value is None:
        return None
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return str(isoformat())
    return str(value)


def _synonyms(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [s for s in value.split("|") if s]
    return [str(s) for s in value]


def _dtype_str(feature: Any) -> str | None:
    value = getattr(feature, "dtype_as_str", None)
    if value is None:
        value = getattr(feature, "_dtype_str", None)
    return value


def _identity_track(sequence: Sequence[Any], description: str) -> Iterable[Any]:
    """Default tracker: iterate `sequence` without any progress display."""
    return sequence


# Maximum number of rows fetched per database round-trip.  Every registry read
# and every related-row bulk read is paginated in chunks of this size so a single
# query never has to materialise an unbounded result set (or build an unbounded
# `IN (...)` clause that some database backends reject).
_BATCH_SIZE = 10_000

# Foreign keys read for every declarative object via the `_type_*`,
# `_created_by` and `_space_uid` helpers.  Eagerly joining them with
# `select_related` turns what would otherwise be four extra queries per object
# into a single joined query per batch.
_FK_RELATED = ("type", "created_by", "space")


def _chunked(items: Sequence[Any], size: int) -> Iterable[list[Any]]:
    """Yield successive `size`-length chunks of `items`."""
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def _fetch_batched(qs: Any, size: int, key: Callable[[Any], Any]) -> Iterable[Any]:
    """Yield rows of `qs` (which must be ordered by `uid`) in 10k batches.

    Pages with `uid`-keyset pagination (`uid > last_uid` plus a `LIMIT`)
    rather than :meth:`QuerySet.iterator`: lamindb wraps every statement to
    inject an auth token, which is incompatible with the server-side (named)
    cursors that `QuerySet.iterator(chunk_size=...)` opens on PostgreSQL.
    `key` extracts the `uid` from a yielded row (the row itself for model
    instances, the first column for a `values_list` tuple).
    """
    last_uid: Any = None
    while True:
        page = qs if last_uid is None else qs.filter(uid__gt=last_uid)
        chunk = list(page[:size])
        if not chunk:
            return
        yield from chunk
        if len(chunk) < size:
            return
        last_uid = key(chunk[-1])


def _read_slot_map(ln: Any, schema_ids: Sequence[int]) -> dict[int, dict[str, str]]:
    """Bulk-read composite-schema slots for all `schema_ids` in 10k batches.

    Returns a map of composite schema id to `{slot: component_uid}`, fetched
    with a single joined query per batch instead of two queries per schema (as
    the per-instance `Schema.slots` property would incur).
    """
    result: dict[int, dict[str, str]] = {}
    if not schema_ids:
        return result
    through = ln.Schema.components.through
    for chunk in _chunked(schema_ids, _BATCH_SIZE):
        for composite_id, slot, component_uid in through.objects.filter(
            composite_id__in=chunk
        ).values_list("composite_id", "slot", "component__uid"):
            result.setdefault(int(composite_id), {})[slot] = str(component_uid)
    return result


def _read_member_map(ln: Any, schema_ids: Sequence[int]) -> dict[int, list[str]]:
    """Bulk-read schema feature members for all `schema_ids` in 10k batches.

    Returns a map of schema id to an ordered list of member feature uids, fetched
    with a single joined query per batch instead of the two queries per schema
    that the per-instance `Schema.members` property would incur.  Members are
    ordered by their through-row id to match `Schema.members` exactly.
    """
    result: dict[int, list[str]] = {}
    if not schema_ids:
        return result
    through = ln.Schema.features.through
    for chunk in _chunked(schema_ids, _BATCH_SIZE):
        for schema_id, feature_uid in (
            through.objects.filter(schema_id__in=chunk)
            .order_by("id")
            .values_list("schema_id", "feature__uid")
        ):
            result.setdefault(int(schema_id), []).append(str(feature_uid))
    return result


def _drop_excluded_children(contents: InstanceContents) -> None:
    """Drop record types nested under a `_excludeChildrenFromPlans` root.

    A record type is dropped when any ancestor in its `type` parent chain is
    flagged with `exclude_children`; the flagged roots themselves are kept
    (they are declared and tracked by a plan).  Walking the chain rather than
    only the direct parent covers arbitrarily deep nesting, and a `seen` guard
    keeps a cyclic (or self-referential) chain from looping forever.
    """
    roots = {rt.uid for rt in contents.record_types if rt.exclude_children}
    if not roots:
        return
    parent_of = {rt.uid: rt.type_uid for rt in contents.record_types}

    def _under_root(uid: str) -> bool:
        seen: set[str] = set()
        parent = parent_of.get(uid)
        while parent is not None and parent not in seen:
            if parent in roots:
                return True
            seen.add(parent)
            parent = parent_of.get(parent)
        return False

    contents.record_types = [
        rt for rt in contents.record_types if not _under_root(rt.uid)
    ]


def read_instance(
    *,
    space_uids: Sequence[str] | None = None,
    exclude_space_uids: Sequence[str] | None = None,
    exclude_uids: set[str] | None = None,
    exclude_child_record_types: bool = False,
    track: Callable[[Sequence[Any], str], Iterable[Any]] | None = None,
) -> InstanceContents:
    """Read the declarative objects of the currently connected instance.

    `space_uids` optionally restricts the read to one or more spaces.
    `exclude_space_uids` optionally excludes one or more spaces; exclusion is
    applied after any inclusion filter.  `exclude_uids` optionally drops the
    objects with those uids from the returned contents (used by drift detection
    to remove objects already managed by other plans).  `exclude_child_record_types`
    optionally drops every record type whose `type` parent chain leads to a
    record type flagged with `_excludeChildrenFromPlans` (its runtime-created
    children); the flagged root itself is kept.  The closure is computed over the
    full read *before* `exclude_uids` is applied, so a child is still excluded
    when its flagged root is itself an already-managed (excluded) object.  All
    schemas are returned, including unnamed (dataset / slot) schemas; the reverse
    step decides which unnamed schemas to reconstruct (those referenced as a
    composite schema's slot component) and counts the rest as skipped.

    `track` optionally wraps each read loop's sequence (signature
    `(sequence, description) -> iterable`) so callers can render a progress bar
    per object kind; it defaults to iterating without any progress display.
    """
    tracker = track if track is not None else _identity_track

    include = list(space_uids) if space_uids else []
    exclude = list(exclude_space_uids) if exclude_space_uids else []

    import lamindb as ln

    def _scoped(
        registry: Any, *, is_type: bool, related: Sequence[str] = _FK_RELATED
    ) -> list[Any]:
        qs = registry.filter(is_type=is_type)
        if include:
            qs = qs.filter(space__uid__in=include)
        if exclude:
            qs = qs.exclude(space__uid__in=exclude)
        if related:
            qs = qs.select_related(*related)
        # Order by uid in the database (deterministic, matches the previous
        # in-Python `sorted(..., key=str(uid))`) and read the rows in 10k
        # keyset-paginated batches so a single query never materialises an
        # unbounded result set.
        qs = qs.order_by("uid")
        return list(_fetch_batched(qs, _BATCH_SIZE, lambda obj: obj.uid))

    contents = InstanceContents()

    for ft in tracker(_scoped(ln.Feature, is_type=True), "Reading feature types"):
        contents.feature_types.append(
            FeatureTypeInfo(
                uid=str(ft.uid),
                name=ft.name,
                type_uid=_type_uid(ft),
                space_uid=_space_uid(ft),
                type_name=_type_name(ft),
                created_by=_created_by(ft),
                created_at=_created_at(ft),
            )
        )

    for f in tracker(_scoped(ln.Feature, is_type=False), "Reading features"):
        contents.features.append(
            FeatureInfo(
                uid=str(f.uid),
                name=f.name,
                type_uid=_type_uid(f),
                dtype_str=_dtype_str(f),
                synonyms=_synonyms(getattr(f, "synonyms", None)),
                unit=getattr(f, "unit", None),
                description=getattr(f, "description", None),
                nullable=getattr(f, "nullable", None),
                default_value=getattr(f, "default_value", None),
                coerce=getattr(f, "coerce", None),
                space_uid=_space_uid(f),
                type_name=_type_name(f),
                created_by=_created_by(f),
                created_at=_created_at(f),
            )
        )

    schema_types = _scoped(ln.Schema, is_type=True)
    schemas = _scoped(ln.Schema, is_type=False)
    all_schema_ids = [s.id for s in (*schema_types, *schemas)]
    slot_map = _read_slot_map(ln, all_schema_ids)
    member_map = _read_member_map(ln, all_schema_ids)

    for st in tracker(schema_types, "Reading schema types"):
        contents.schema_types.append(
            SchemaTypeInfo(
                uid=str(st.uid),
                name=st.name,
                description=getattr(st, "description", None),
                type_uid=_type_uid(st),
                itype=getattr(st, "itype", None),
                feature_uids=member_map.get(st.id, []),
                slots=slot_map.get(st.id, {}),
                space_uid=_space_uid(st),
                type_name=_type_name(st),
                created_by=_created_by(st),
                created_at=_created_at(st),
            )
        )

    for s in tracker(schemas, "Reading schemas"):
        # `Composite` schemas have no feature members (their structure lives in
        # `slots`); `Feature`/untyped schemas read their members from the
        # bulk-loaded map.  Any other itype (e.g. a Gene/Protein feature set) is
        # rare here, so fall back to the per-instance `members` property.
        if s.itype == "Composite":
            feature_uids: list[str] = []
        elif s.itype in (None, "Feature"):
            feature_uids = member_map.get(s.id, [])
        else:
            feature_uids = [str(m.uid) for m in s.members.all()]
        # The optional members live in `Schema._aux`, not on the member links,
        # so they have to be read separately or the reconstructed plan would
        # declare every member required.  `get_uids` reads the loaded aux and
        # costs no query; keep the member order for a stable rendering.
        optional_uids = set(s.optionals.get_uids())
        zarr = (getattr(s, "_aux", None) or {}).get("zarr")
        contents.schemas.append(
            SchemaInfo(
                uid=str(s.uid),
                name=s.name or None,
                description=getattr(s, "description", None),
                type_uid=_type_uid(s),
                itype=getattr(s, "itype", None),
                otype=getattr(s, "otype", None),
                schema_dtype=getattr(s, "_dtype_str", None),
                flexible=getattr(s, "flexible", None),
                coerce=getattr(s, "coerce", None),
                minimal_set=bool(getattr(s, "minimal_set", True)),
                ordered_set=bool(getattr(s, "ordered_set", False)),
                maximal_set=bool(getattr(s, "maximal_set", False)),
                feature_uids=feature_uids,
                optional_feature_uids=[
                    uid for uid in feature_uids if uid in optional_uids
                ],
                index_feature_uid=getattr(s, "_index_feature_uid", None),
                slots=slot_map.get(s.id, {}),
                formats={"zarr": zarr} if zarr else None,
                space_uid=_space_uid(s),
                type_name=_type_name(s),
                created_by=_created_by(s),
                created_at=_created_at(s),
            )
        )

    for rt in tracker(
        _scoped(ln.Record, is_type=True, related=(*_FK_RELATED, "schema")),
        "Reading record types",
    ):
        schema = getattr(rt, "schema", None)
        aux = getattr(rt, "_aux", None) or {}
        ss = aux.get("ss")
        if ss is None:
            single_space_uid = None
        elif ss == 1:
            # `ss == 1` enforces the record type's own space; the plan model
            # only expresses an exact space, so resolve it to that uid.
            single_space_uid = _space_uid(rt)
        else:
            single_space_uid = str(ss)
        contents.record_types.append(
            RecordTypeInfo(
                uid=str(rt.uid),
                name=rt.name,
                description=getattr(rt, "description", None),
                type_uid=_type_uid(rt),
                schema_uid=str(schema.uid) if schema is not None else None,
                space_uid=_space_uid(rt),
                single_space_uid=single_space_uid,
                is_experiment_sheet=bool(aux.get("isExperimentSheet")),
                exclude_children=bool(aux.get("_excludeChildrenFromPlans")),
                type_name=_type_name(rt),
                created_by=_created_by(rt),
            )
        )

    # Trashed objects (branch_id == -1) are hidden by the default queryset; read
    # their uids (and names) so consumers can tell a deleted reference apart from
    # one that is merely outside the read scope, and name the deleted target.
    deleted: set[str] = set()
    deleted_names: dict[str, str] = {}
    for registry in (ln.Feature, ln.Schema, ln.Record):
        rows = registry.filter(branch_id=-1).order_by("uid").values_list("uid", "name")
        for uid, name in _fetch_batched(rows, _BATCH_SIZE, lambda row: row[0]):
            deleted.add(str(uid))
            if name is not None:
                deleted_names[str(uid)] = str(name)
            if registry is ln.Record:
                contents.deleted_record_uids.add(str(uid))
                if name is not None:
                    contents.deleted_record_names[str(uid)] = str(name)
    contents.deleted_uids = deleted
    contents.deleted_names = deleted_names

    # Metadata for every live object across the whole instance (ignoring the read
    # scope), so consumers can annotate references that point outside the scope
    # with the target's creator and space, and tell such references apart from
    # references to objects that no longer exist at all.
    meta: dict[str, ObjectMeta] = {}
    for registry in (ln.Feature, ln.Schema, ln.Record):
        rows = (
            registry.filter()
            .order_by("uid")
            .values_list("uid", "name", "created_by__handle", "space__uid")
        )
        for uid, name, handle, space_uid in _fetch_batched(
            rows, _BATCH_SIZE, lambda row: row[0]
        ):
            meta[str(uid)] = ObjectMeta(
                name=name,
                created_by=str(handle) if handle is not None else None,
                space_uid=str(space_uid) if space_uid is not None else None,
            )
    contents.object_meta = meta

    if exclude_child_record_types:
        _drop_excluded_children(contents)

    if exclude_uids:
        contents.feature_types = [
            i for i in contents.feature_types if i.uid not in exclude_uids
        ]
        contents.features = [i for i in contents.features if i.uid not in exclude_uids]
        contents.schema_types = [
            i for i in contents.schema_types if i.uid not in exclude_uids
        ]
        contents.schemas = [i for i in contents.schemas if i.uid not in exclude_uids]
        contents.record_types = [
            i for i in contents.record_types if i.uid not in exclude_uids
        ]

    return contents
