"""Reverse-map instance objects into render-ready plan object specs.

Turns the :class:`~lamindb_setup.evolve.introspect._read.InstanceContents` read
from an instance into an ordered list of :class:`ObjectSpec` that the renderer
emits as plan source.  Reconstruction is strict: any object that cannot be
faithfully reconstructed (unsupported dtype, categorical schema-level dtype,
reference to an out-of-scope object, ...) is recorded as an error and excluded,
and every object that transitively depends on it is excluded too.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._read import (
        FeatureInfo,
        FeatureTypeInfo,
        InstanceContents,
        ObjectMeta,
        RecordTypeInfo,
        SchemaInfo,
        SchemaTypeInfo,
    )

__all__ = [
    "DtypeInfo",
    "ExternalRef",
    "ObjectSpec",
    "ReconstructedPlan",
    "build_plan",
]

# Simple lamindb dtype tokens -> the Python expression rendered into the plan.
# `num` / `path` / `url` are lamindb's string dtypes: they round-trip
# through `LaminDType("num" | "path" | "url")` and must not degrade to `str`.
_SIMPLE_DTYPES: dict[str, str] = {
    "int": "int",
    "float": "float",
    "num": '"num"',
    "bool": "bool",
    "str": "str",
    "dict": "dict",
    "date": "datetime.date",
    "datetime": "datetime.datetime",
    "path": '"path"',
    "url": '"url"',
}

# Ordering of kinds for deterministic, readable output.
_KIND_RANK: dict[str, int] = {
    "FeatureType": 0,
    "Feature": 1,
    "SchemaType": 2,
    "Schema": 3,
    "RecordType": 4,
    "Sheet": 5,
}

# The uid of lamindb's built-in "all" space (`lamindb_setup` creates it as the
# first space of every instance), where a record lands unless it names another.
ALL_SPACE_UID = "a" * 12

_VAR_PREFIX: dict[str, str] = {
    "FeatureType": "ft",
    "Feature": "f",
    "SchemaType": "st",
    "Schema": "s",
    "RecordType": "rt",
    "Sheet": "sheet",
}

# Core lamindb registries that can be used as a categorical dtype.  They are all
# exposed directly on the `ln` namespace, so they render as `ln.<Name>`.
_CORE_REGISTRIES: frozenset[str] = frozenset(
    {
        "Record",
        "ULabel",
        "Artifact",
        "Transform",
        "Run",
        "Project",
        "Reference",
        "User",
        "Collection",
    }
)

# Categorical filters we can reconstruct and render on a feature dtype.
_SUPPORTED_FILTERS: frozenset[str] = frozenset(
    {"schema__uid", "source__uid", "is_type"}
)


class _ReconstructError(Exception):
    """Internal: an object could not be reconstructed (carries a message).

    `out_of_scope_uid` is set when the failure is a reference to an object that
    is not in scope, so callers can map that target uid back to an owning plan
    and suggest exporting it.
    """

    def __init__(self, message: str, *, out_of_scope_uid: str | None = None) -> None:
        super().__init__(message)
        self.out_of_scope_uid = out_of_scope_uid


class _EmptyObject(_ReconstructError):
    """Internal: an object is empty and should be cleaned up, not reconstructed.

    Raised for objects that exist in the instance but cannot be expressed as a
    valid declarative object (e.g. a composite schema with no slots and no
    members).  Unlike a plain :class:`_ReconstructError`, this is surfaced as a
    warning rather than a hard error: the object is excluded from the plan and
    the user is advised to delete it from the instance.
    """


@dataclass
class DtypeInfo:
    """A reversed feature dtype, ready to render as a `LaminDType(...)` call."""

    is_list: bool = False
    simple_py: str | None = None
    registry_expr: str | None = None
    field_name: str | None = None
    type_uid: str | None = None
    filters: dict[str, str] = field(default_factory=dict)
    union_members: list[tuple[str, str | None]] = field(default_factory=list)
    """For a union categorical (`cat[A|B]`): `(registry_expr, field_name)` per member."""
    needs_bionty: bool = False
    needs_pertdb: bool = False
    needs_lamindb: bool = False
    needs_sourceuid: bool = False
    needs_datetime: bool = False


@dataclass
class ObjectSpec:
    """A single reconstructed object, ready for rendering."""

    uid: str
    kind: str
    name: str | None
    deps: set[str] = field(default_factory=set)
    payload: dict[str, Any] = field(default_factory=dict)
    varname: str = ""


@dataclass(frozen=True)
class ExternalRef:
    """A reference to an object that another plan owns and exports.

    Lets the renderer emit a cross-plan import and a `Plan[Exports]` parameter
    so a drift plan can reference an already-tracked object via the owning plan's
    exports (`<param>.exports.<field>`) instead of recreating it.
    """

    uid: str
    module: str
    """Importable module name of the owning plan file (its file stem)."""
    plan_var: str
    """The plan variable to import and pass as the parameter default."""
    exports_cls: str
    """Name of the owning plan's :class:`PlanExports` subclass."""
    field: str
    """The export field on the owning plan that holds the referenced object."""


@dataclass
class ReconstructedPlan:
    """The result of reconstructing an instance: ordered specs plus diagnostics."""

    specs: list[ObjectSpec] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    skipped_unnamed_schemas: int = 0
    external_refs: dict[str, ExternalRef] = field(default_factory=dict)
    out_of_scope_uids: dict[str, int] = field(default_factory=dict)
    """Out-of-scope reference targets mapped to how many objects referenced them."""
    default_space_uid: str | None = None
    """The space every object shares, rendered once as `ctx.default_space_uid`."""
    needs_lamindtype: bool = False
    needs_spaceuid: bool = False
    needs_sourceuid: bool = False
    needs_bionty: bool = False
    needs_pertdb: bool = False
    needs_lamindb: bool = False
    needs_datetime: bool = False


def _registry_expr(registry_str: str) -> tuple[str, bool, bool, bool]:
    """Map a parsed registry string to (expr, needs_bionty, needs_pertdb, needs_lamindb).

    Core lamindb registries are exposed directly on the `ln` namespace and so
    render without a module prefix (e.g. `ln.Transform`); `bionty` and
    `pertdb` registries are prefixed with their module (`bt.models.*` /
    `pt.models.*`).
    """
    if registry_str == "SQLRecord":
        return "ln.models.SQLRecord", False, False, True
    if registry_str.startswith("bionty."):
        return f"bt.models.{registry_str.split('.', 1)[1]}", True, False, False
    if registry_str.startswith("pertdb."):
        return f"pt.models.{registry_str.split('.', 1)[1]}", False, True, False
    if registry_str in _CORE_REGISTRIES:
        return f"ln.{registry_str}", False, False, True
    raise _ReconstructError(f"unsupported categorical registry {registry_str!r}")


def _parse_filters(filter_str: str, dtype_str: str) -> dict[str, str]:
    """Read the `key=value` payload of a categorical dtype's bracket.

    Delegated to lamindb, which is the only thing that writes these strings and
    which has changed how it quotes them (up to 2.9 every value was quoted, from
    2.10 only those containing a comma are).  Its parser reads both, so plans
    reverse-engineered from an instance written by either version keep their
    filters.
    """
    from lamindb.models.feature import parse_filter_string

    if not filter_str:
        return {}
    try:
        parsed = parse_filter_string(filter_str)
    except ValueError as exc:
        raise _ReconstructError(
            f"unparsable categorical filters in dtype {dtype_str!r}: {exc}"
        ) from exc
    return {key: value for key, (_, _, value) in parsed.items()}


def _reverse_dtype(dtype_str: str | None) -> DtypeInfo:
    """Reverse a lamindb dtype string into a :class:`DtypeInfo`.

    Raises :class:`_ReconstructError` for anything that cannot be reconstructed
    exactly (unknown simple dtypes, unsupported registries, or a union that
    carries filters or a typed subtype -- which lamindb itself forbids).
    """
    if not dtype_str:
        raise _ReconstructError("feature has no dtype")

    inner = dtype_str
    is_list = False
    if inner.startswith("list[") and inner.endswith("]"):
        is_list = True
        inner = inner[len("list[") : -1]

    if inner in _SIMPLE_DTYPES:
        expr = _SIMPLE_DTYPES[inner]
        return DtypeInfo(
            is_list=is_list,
            simple_py=expr,
            needs_datetime=expr.startswith("datetime."),
        )

    if inner.startswith("cat[") and inner.endswith("]"):
        try:
            from lamindb.models.feature import parse_dtype

            components = parse_dtype(inner)
        except Exception as exc:
            # `parse_dtype` validates each registry against the *connected*
            # instance's configured modules, so a dtype referencing a module that
            # is not loaded (e.g. `cat[bionty.Gene]` on a bionty-less instance)
            # raises a lamindb `ValidationError`.  Treat any such failure as a
            # dtype we cannot reconstruct rather than letting it crash the whole
            # read.
            raise _ReconstructError(
                f"unresolvable categorical dtype {dtype_str!r}: {exc}"
            ) from exc
        if not components:
            # A bare `cat[]` with no members: nothing to reconstruct.
            raise _ReconstructError(f"unsupported dtype: {dtype_str!r}")
        if len(components) > 1:
            return _reverse_union_cat(dtype_str, components, is_list)
        comp = components[0]
        registry_expr, needs_bionty, needs_pertdb, needs_lamindb = _registry_expr(
            comp["registry_str"]
        )
        # `parse_dtype` always resolves a field; drop it when it is the
        # registry's default name field so `cat[Transform]` round-trips as
        # `cat[Transform]` rather than `cat[Transform.key]`.
        registry_cls = comp.get("registry")
        default_field = (
            getattr(registry_cls, "_name_field", None) or "name"
            if registry_cls is not None
            else "name"
        )
        field_str = comp.get("field_str") or None
        if field_str == default_field:
            field_str = None
        filters = _parse_filters(comp.get("filter_str", ""), dtype_str)
        type_uid = comp.get("type_uid")
        if "type__uid" in filters:
            type_uid = type_uid or filters.pop("type__uid")
        if field_str is not None and type_uid is not None:
            raise _ReconstructError(
                f"field dtype with a type uid is not supported: {dtype_str!r}"
            )
        if registry_expr == "ln.Record" and type_uid is None:
            raise _ReconstructError(
                f"{registry_expr} dtype requires a type uid: {dtype_str!r}"
            )
        return DtypeInfo(
            is_list=is_list,
            registry_expr=registry_expr,
            field_name=field_str,
            type_uid=type_uid,
            filters=filters,
            needs_bionty=needs_bionty,
            needs_pertdb=needs_pertdb,
            needs_lamindb=needs_lamindb,
            needs_sourceuid="source__uid" in filters,
        )

    raise _ReconstructError(f"unsupported dtype: {dtype_str!r}")


def _reverse_union_cat(
    dtype_str: str, components: list[dict[str, Any]], is_list: bool
) -> DtypeInfo:
    """Reverse a union categorical (`cat[A|B]`) into a :class:`DtypeInfo`.

    lamindb forbids `cat_filters` and typed `Record`/`ULabel` subtypes on
    a union, so every member is a plain registry (optionally with a field
    selector).  A component carrying a filter or type uid is therefore not
    reconstructable and is refused.
    """
    members: list[tuple[str, str | None]] = []
    needs_bionty = needs_pertdb = needs_lamindb = False
    for comp in components:
        if comp.get("type_uid") or comp.get("filter_str"):
            raise _ReconstructError(
                "union dtypes with filters or typed subtypes are not "
                f"supported: {dtype_str!r}"
            )
        registry_expr, comp_bionty, comp_pertdb, comp_lamindb = _registry_expr(
            comp["registry_str"]
        )
        needs_bionty = needs_bionty or comp_bionty
        needs_pertdb = needs_pertdb or comp_pertdb
        needs_lamindb = needs_lamindb or comp_lamindb
        # Drop a field that is the registry's default name field so the member
        # round-trips as `Registry` rather than `Registry.name`.
        registry_cls = comp.get("registry")
        default_field = (
            getattr(registry_cls, "_name_field", None) or "name"
            if registry_cls is not None
            else "name"
        )
        field_str = comp.get("field_str") or None
        if field_str == default_field:
            field_str = None
        members.append((registry_expr, field_str))
    return DtypeInfo(
        is_list=is_list,
        union_members=members,
        needs_bionty=needs_bionty,
        needs_pertdb=needs_pertdb,
        needs_lamindb=needs_lamindb,
    )


def _target_label(
    uid: str,
    deleted: frozenset[str] | set[str],
    object_meta: dict[str, ObjectMeta] | None,
) -> str:
    """Describe an out-of-scope reference target by uid, creator and space.

    Notes that the target is `deleted` when it is trashed, and appends its
    creator and space when known (i.e. when it lives elsewhere in the instance).
    """
    parts = [f"uid {uid!r}"]
    if uid in deleted:
        parts.append("deleted")
    meta = (object_meta or {}).get(uid)
    if meta is not None:
        if meta.created_by is not None:
            parts.append(f"created_by {meta.created_by!r}")
        if meta.space_uid is not None:
            parts.append(f"space {meta.space_uid!r}")
    return ", ".join(parts)


def _require_in_scope(
    uid: str | None,
    in_scope: set[str],
    what: str,
    deleted: frozenset[str] | set[str] = frozenset(),
    object_meta: dict[str, ObjectMeta] | None = None,
) -> set[str]:
    """Return `{uid}` if it is in scope, raising otherwise (empty if `None`).

    When the uid is out of scope, the error names the target by uid and, when
    known, annotates it as trashed and/or with its creator and space.
    """
    if uid is None:
        return set()
    if uid not in in_scope:
        raise _ReconstructError(
            f"references out-of-scope {what} ({_target_label(uid, deleted, object_meta)})",
            out_of_scope_uid=uid,
        )
    return {uid}


def _build_feature_type(
    info: FeatureTypeInfo,
    in_scope: set[str],
    deleted: frozenset[str] | set[str],
    object_meta: dict[str, ObjectMeta] | None = None,
) -> ObjectSpec:
    deps = _require_in_scope(
        info.type_uid, in_scope, "feature type", deleted, object_meta
    )
    return ObjectSpec(
        uid=info.uid,
        kind="FeatureType",
        name=info.name,
        deps=deps,
        payload={"type_uid": info.type_uid, "space_uid": info.space_uid},
    )


def _build_feature(
    info: FeatureInfo,
    in_scope: set[str],
    deleted: frozenset[str] | set[str],
    object_meta: dict[str, ObjectMeta] | None = None,
) -> ObjectSpec:
    dtype = _reverse_dtype(info.dtype_str)
    deps = _require_in_scope(
        info.type_uid, in_scope, "feature type", deleted, object_meta
    )
    deps |= _require_in_scope(
        dtype.type_uid,
        in_scope,
        "record type referenced in dtype",
        deleted,
        object_meta,
    )
    schema_filter_uid = dtype.filters.get("schema__uid")
    deps |= _require_in_scope(
        schema_filter_uid,
        in_scope,
        "schema referenced in dtype",
        deleted,
        object_meta,
    )
    for key in dtype.filters:
        if key not in _SUPPORTED_FILTERS:
            raise _ReconstructError(f"unsupported categorical filter {key!r}")
    return ObjectSpec(
        uid=info.uid,
        kind="Feature",
        name=info.name,
        deps=deps,
        payload={
            "type_uid": info.type_uid,
            "dtype": dtype,
            "synonyms": info.synonyms,
            "unit": info.unit,
            "description": info.description,
            "nullable": info.nullable,
            "default_value": info.default_value,
            "coerce": info.coerce,
            "space_uid": info.space_uid,
        },
    )


def _build_schema_type(
    info: SchemaTypeInfo,
    in_scope: set[str],
    deleted: frozenset[str] | set[str],
    object_meta: dict[str, ObjectMeta] | None = None,
) -> ObjectSpec:
    deps = _require_in_scope(
        info.type_uid, in_scope, "schema type", deleted, object_meta
    )
    return ObjectSpec(
        uid=info.uid,
        kind="SchemaType",
        name=info.name,
        deps=deps,
        payload={
            "type_uid": info.type_uid,
            "description": info.description,
            "space_uid": info.space_uid,
        },
    )


def _reverse_itype(itype_str: str) -> tuple[str, bool, bool, bool]:
    """Reverse a schema `itype` string into `(expr, needs_bionty, needs_pertdb, needs_lamindb)`.

    Handles both the registry-class form (e.g. `"bionty.Gene"` ->
    `bt.models.Gene`) and the field form (e.g. `"bionty.Gene.ensembl_gene_id"`
    -> `bt.models.Gene.ensembl_gene_id`), for core lamindb as well as the
    `bionty.` and `pertdb.` plugin registries.
    """
    parts = itype_str.split(".")
    if itype_str.startswith(("bionty.", "pertdb.")):
        registry_str = ".".join(parts[:2])
        field = parts[2] if len(parts) > 2 else None
        if len(parts) > 3:
            raise _ReconstructError(f"unsupported itype: {itype_str!r}")
    else:
        registry_str = parts[0]
        field = parts[1] if len(parts) > 1 else None
        if len(parts) > 2:
            raise _ReconstructError(f"unsupported itype: {itype_str!r}")
    expr, needs_bionty, needs_pertdb, needs_lamindb = _registry_expr(registry_str)
    if field is not None:
        expr = f"{expr}.{field}"
    return expr, needs_bionty, needs_pertdb, needs_lamindb


def _build_schema(
    info: SchemaInfo,
    in_scope: set[str],
    deleted: frozenset[str] | set[str],
    object_meta: dict[str, ObjectMeta] | None = None,
) -> ObjectSpec:
    schema_dtype: DtypeInfo | None = None
    if info.schema_dtype:
        schema_dtype = _reverse_dtype(info.schema_dtype)
        if schema_dtype.registry_expr is not None:
            raise _ReconstructError("categorical schema-level dtype is not supported")

    itype_expr: str | None = None
    itype_needs_bionty = False
    itype_needs_pertdb = False
    itype_needs_lamindb = False
    # `Composite` is not a registry: it is the itype lamindb assigns to a
    # composite schema, whose structure is reconstructed from its `slots`.
    if info.itype not in (None, "Feature", "Composite"):
        (
            itype_expr,
            itype_needs_bionty,
            itype_needs_pertdb,
            itype_needs_lamindb,
        ) = _reverse_itype(info.itype)
    elif info.itype == "Feature" and not info.feature_uids and not info.slots:
        # A feature schema with no explicit members (e.g.
        # `ln.Schema(itype=ln.Feature)`, often flexible): the `Feature`
        # itype is normally implied by the schema's features, but here there
        # are none, so emit `itype=ln.Feature` explicitly. Without it the
        # reconstructed `Schema(features=[], ...)` would be rejected for
        # having neither features, slots, nor an itype.
        itype_expr = "ln.Feature"
        itype_needs_lamindb = True

    # A schema addresses its data through features, slots, or an itype; if it has
    # none of them there is nothing to reconstruct and `Schema(features=[], ...)`
    # would be rejected at plan-definition time.  Such an object (e.g. a composite
    # schema whose slots were removed, leaving `itype='Composite'` with no
    # members) is empty and should be cleaned up rather than reconstructed.
    if not info.feature_uids and not info.slots and itype_expr is None:
        detail = (
            f"itype is {info.itype!r} but it has no slots and no members"
            if info.itype == "Composite"
            else "it has no features, no slots, and no itype"
        )
        raise _EmptyObject(
            f"empty schema ({detail}); it cannot be reconstructed as a valid "
            "Schema. Consider deleting it from the instance."
        )

    deps = _require_in_scope(
        info.type_uid, in_scope, "schema type", deleted, object_meta
    )
    for feature_uid in info.feature_uids:
        deps |= _require_in_scope(
            feature_uid, in_scope, "feature", deleted, object_meta
        )
    deps |= _require_in_scope(
        info.index_feature_uid, in_scope, "index feature", deleted, object_meta
    )
    slots: dict[str, str] = {}
    for slot_key, slot_schema_uid in info.slots.items():
        deps |= _require_in_scope(
            slot_schema_uid, in_scope, "slot schema", deleted, object_meta
        )
        slots[slot_key] = slot_schema_uid
    return ObjectSpec(
        uid=info.uid,
        kind="Schema",
        name=info.name,
        deps=deps,
        payload={
            "type_uid": info.type_uid,
            "description": info.description,
            "otype": info.otype,
            "flexible": info.flexible,
            "coerce": info.coerce,
            "minimal_set": info.minimal_set,
            "ordered_set": info.ordered_set,
            "maximal_set": info.maximal_set,
            "feature_uids": info.feature_uids,
            "optional_feature_uids": info.optional_feature_uids,
            "index_feature_uid": info.index_feature_uid,
            "slots": slots,
            "formats": info.formats,
            "dtype": schema_dtype,
            "itype_expr": itype_expr,
            "itype_needs_bionty": itype_needs_bionty,
            "itype_needs_pertdb": itype_needs_pertdb,
            "itype_needs_lamindb": itype_needs_lamindb,
            "space_uid": info.space_uid,
        },
    )


def _build_record_type(
    info: RecordTypeInfo,
    in_scope: set[str],
    deleted: frozenset[str] | set[str],
    object_meta: dict[str, ObjectMeta] | None = None,
    *,
    schema_type_uids: frozenset[str] = frozenset(),
) -> ObjectSpec:
    deps = _require_in_scope(
        info.type_uid, in_scope, "record type", deleted, object_meta
    )
    if info.schema_uid is not None:
        # lamindb's `Record.schema` foreign key accepts a schema type, but a
        # schema type declares no features and so constrains nothing; `Sheet`
        # only models a record type constrained by a real schema.
        if info.schema_uid in schema_type_uids:
            raise _ReconstructError(
                f"its schema ({_target_label(info.schema_uid, deleted, object_meta)}) "
                "is a schema type ('is_type=True'), which declares no features "
                "and cannot constrain a sheet; repair the instance with "
                "'lamin evolve repair sheet-schemas'"
            )
        deps |= _require_in_scope(
            info.schema_uid, in_scope, "sheet schema", deleted, object_meta
        )
        return ObjectSpec(
            uid=info.uid,
            kind="Sheet",
            name=info.name,
            deps=deps,
            payload={
                "type_uid": info.type_uid,
                "description": info.description,
                "schema_uid": info.schema_uid,
                "space_uid": info.space_uid,
                "single_space_uid": info.single_space_uid,
                "is_experiment_sheet": info.is_experiment_sheet,
                "exclude_children_from_plans": info.exclude_children,
            },
        )
    return ObjectSpec(
        uid=info.uid,
        kind="RecordType",
        name=info.name,
        deps=deps,
        payload={
            "type_uid": info.type_uid,
            "description": info.description,
            "space_uid": info.space_uid,
            "single_space_uid": info.single_space_uid,
            "exclude_children_from_plans": info.exclude_children,
        },
    )


def _slug(name: str) -> str:
    slug = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()
    return slug or "x"


def _spec_slug(spec: ObjectSpec) -> str:
    """Return the slug used for `spec`'s variable name.

    Named objects use their name; an unnamed schema (a composite slot component)
    derives a readable hint from its `otype` or index `itype` instead.
    """
    if spec.name:
        return _slug(spec.name)
    otype = spec.payload.get("otype")
    if otype:
        return _slug(str(otype))
    itype_expr = spec.payload.get("itype_expr")
    if itype_expr:
        parts = itype_expr.split(".")
        for part in reversed(parts):
            if part[:1].isupper():
                return _slug(part)
        return _slug(parts[-1])
    return "schema"


def _assign_varnames(specs: list[ObjectSpec]) -> None:
    used: set[str] = set()
    for spec in specs:
        base = f"{_VAR_PREFIX[spec.kind]}_{_spec_slug(spec)}"
        candidate = base
        counter = 1
        while candidate in used:
            counter += 1
            candidate = f"{base}_{counter}"
        used.add(candidate)
        spec.varname = candidate


def _toposort(specs: dict[str, ObjectSpec]) -> list[ObjectSpec]:
    """Return `specs` ordered so every object precedes those that depend on it."""
    indegree = dict.fromkeys(specs, 0)
    dependents: dict[str, list[str]] = {uid: [] for uid in specs}
    for uid, spec in specs.items():
        for dep in spec.deps:
            # External (cross-plan) deps are not nodes in this graph; skip them.
            if dep not in specs:
                continue
            indegree[uid] += 1
            dependents[dep].append(uid)

    def _key(uid: str) -> tuple[int, str, str]:
        spec = specs[uid]
        return (_KIND_RANK[spec.kind], spec.name or "", spec.uid)

    ready = sorted((uid for uid, deg in indegree.items() if deg == 0), key=_key)
    order: list[ObjectSpec] = []
    while ready:
        uid = ready.pop(0)
        order.append(specs[uid])
        for dependent in dependents[uid]:
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                ready.append(dependent)
        ready.sort(key=_key)
    if len(order) != len(specs):
        # A cycle should be impossible for these object kinds; fail loudly.
        raise RuntimeError("cyclic dependency detected while ordering objects")
    return order


def _info_kind(info: Any) -> str:
    """Return the human-readable object kind for a read `info` dataclass."""
    kind = type(info).__name__[:-4]  # strip the trailing `Info`
    if kind == "RecordType" and getattr(info, "schema_uid", None) is not None:
        return "Sheet"
    return kind


def _object_label(info: Any) -> str:
    """Describe a read object by kind, name, uid and (when known) creator/space."""
    parts = [f"uid {info.uid!r}"]
    created_by = getattr(info, "created_by", None)
    if created_by is not None:
        parts.append(f"created_by {created_by!r}")
    space_uid = getattr(info, "space_uid", None)
    if space_uid is not None:
        parts.append(f"space {space_uid!r}")
    return f"{_info_kind(info)} {info.name!r} ({', '.join(parts)})"


def _hoist_spaces(plan: ReconstructedPlan) -> None:
    """Leave the "all" space unstated, and state a space every object shares once.

    A plan that names no space creates its objects in lamindb's "all" space, so
    an object found there needs no `space_uid`.  When every object sits in the
    same other space, it becomes the plan's `ctx.default_space_uid` rather
    than being repeated on each object.
    """
    for spec in plan.specs:
        if spec.payload.get("space_uid") == ALL_SPACE_UID:
            spec.payload["space_uid"] = None
    spaces = {spec.payload.get("space_uid") for spec in plan.specs}
    if len(spaces) == 1 and None not in spaces:
        plan.default_space_uid = spaces.pop()
        for spec in plan.specs:
            spec.payload["space_uid"] = None


def build_plan(
    contents: InstanceContents,
    external_refs: dict[str, ExternalRef] | None = None,
) -> ReconstructedPlan:
    """Reconstruct `contents` into an ordered, render-ready plan.

    `external_refs` maps the uid of an out-of-scope object that another plan
    owns and exports to the :class:`ExternalRef` describing how to reference it.
    Such uids are treated as in scope (a reference to them does not fail) and the
    renderer emits a cross-plan import for the ones that end up referenced.
    """
    externals = external_refs or {}
    external_uids = set(externals)
    in_scope: set[str] = set(external_uids)
    for group in (
        contents.feature_types,
        contents.features,
        contents.schema_types,
        contents.schemas,
        contents.record_types,
    ):
        in_scope.update(info.uid for info in group)

    build_record_type = partial(
        _build_record_type,
        schema_type_uids=frozenset(info.uid for info in contents.schema_types),
    )
    builders: list[tuple[Any, Any]] = [
        *((info, _build_feature_type) for info in contents.feature_types),
        *((info, _build_feature) for info in contents.features),
        *((info, _build_schema_type) for info in contents.schema_types),
        *((info, _build_schema) for info in contents.schemas),
        *((info, build_record_type) for info in contents.record_types),
    ]

    # A short label per uid (`Kind 'name' (uid 'X', created_by '...', space
    # '...')`) so cascade errors can name the exact dependency that failed to
    # reconstruct.
    labels: dict[str, str] = {info.uid: _object_label(info) for info, _ in builders}

    specs: dict[str, ObjectSpec] = {}
    errors: dict[str, str] = {}
    warnings: dict[str, str] = {}
    out_of_scope_uids: dict[str, int] = {}
    for info, builder in builders:
        try:
            spec = builder(info, in_scope, contents.deleted_uids, contents.object_meta)
        except _EmptyObject as exc:
            warnings[info.uid] = f"{labels[info.uid]}: {exc}"
            continue
        except _ReconstructError as exc:
            errors[info.uid] = f"{labels[info.uid]}: {exc}"
            if exc.out_of_scope_uid is not None:
                out_of_scope_uids[exc.out_of_scope_uid] = (
                    out_of_scope_uids.get(exc.out_of_scope_uid, 0) + 1
                )
            continue
        specs[spec.uid] = spec

    # Cascade: drop any object that depends on a failed/excluded object.  An
    # external (cross-plan) dependency is satisfied by an import, so it never
    # triggers a cascade drop.
    changed = True
    while changed:
        changed = False
        for uid, spec in list(specs.items()):
            missing = [
                dep
                for dep in spec.deps
                if dep not in specs and dep not in external_uids
            ]
            if missing:
                missing_labels = ", ".join(
                    labels.get(dep, f"uid {dep!r}") for dep in sorted(missing)
                )
                errors[uid] = (
                    f"{labels.get(uid, f'{spec.kind} {spec.name!r} (uid {uid!r})')}: "
                    f"depends on {missing_labels} which could not be reconstructed"
                )
                del specs[uid]
                changed = True

    # Unnamed schemas cannot be addressed declaratively by name; keep only those
    # referenced as a composite schema's slot component, and count the rest as
    # skipped.
    referenced: set[str] = set()
    for spec in specs.values():
        referenced |= spec.deps
    skipped_unnamed_schemas = 0
    for uid, spec in list(specs.items()):
        if spec.kind == "Schema" and spec.name is None and uid not in referenced:
            del specs[uid]
            skipped_unnamed_schemas += 1

    ordered = _toposort(specs)
    _assign_varnames(ordered)

    # Keep only the external references actually used by a surviving spec, so the
    # renderer imports exactly the owning plans it needs.
    used_external: dict[str, ExternalRef] = {}
    for spec in ordered:
        for dep in spec.deps:
            if dep in externals:
                used_external[dep] = externals[dep]

    plan = ReconstructedPlan(
        specs=ordered,
        errors=[errors[uid] for uid in sorted(errors)],
        warnings=[warnings[uid] for uid in sorted(warnings)],
        skipped_unnamed_schemas=skipped_unnamed_schemas,
        external_refs=used_external,
        out_of_scope_uids=out_of_scope_uids,
    )
    _hoist_spaces(plan)
    if plan.default_space_uid is not None:
        plan.needs_spaceuid = True
    for spec in ordered:
        if spec.payload.get("space_uid") is not None:
            plan.needs_spaceuid = True
        if spec.payload.get("single_space_uid") is not None:
            plan.needs_spaceuid = True
        if spec.kind == "Feature":
            dtype: DtypeInfo = spec.payload["dtype"]
            plan.needs_lamindtype = True
            plan.needs_bionty |= dtype.needs_bionty
            plan.needs_pertdb |= dtype.needs_pertdb
            plan.needs_lamindb |= dtype.needs_lamindb
            plan.needs_sourceuid |= dtype.needs_sourceuid
            plan.needs_datetime |= dtype.needs_datetime
        elif spec.kind == "Schema":
            plan.needs_bionty |= spec.payload["itype_needs_bionty"]
            plan.needs_pertdb |= spec.payload["itype_needs_pertdb"]
            plan.needs_lamindb |= spec.payload["itype_needs_lamindb"]
            schema_dtype: DtypeInfo | None = spec.payload["dtype"]
            if schema_dtype is not None:
                plan.needs_lamindtype = True
                plan.needs_datetime |= schema_dtype.needs_datetime
    return plan
