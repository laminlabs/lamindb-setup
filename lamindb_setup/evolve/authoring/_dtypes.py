# ruff: noqa: TC003
"""Typed, lamindb-independent wrapper around lamindb's Dtype system.

Three annotation-friendly building blocks describe a feature's data type:

* :class:`DType` -- a simple, non-categorical dtype (`DType[int]`, `DType[url]`).
* :class:`ListDType` -- a list of a simple dtype (`ListDType[int]`) or of a
  categorical (`ListDType[CatDType[Record, RecordCat(type__uid=...)]]`).
* :data:`CatDType` -- a categorical dtype tied to a registry, carrying per-registry
  filter metadata (`CatDType[lamindb.models.Record, RecordCat(type__uid="123")]`).

`CatDType` is :data:`typing.Annotated`, so the filter object is ordinary runtime
metadata (exactly like `Annotated` is used with dataclasses) and still type-checks.

All three forms are used **only as type annotations** and are resolved back into a
concrete lamindb dtype + `cat_filters` dict via :func:`get_dtype`.  None of them is
meant to be instantiated.
"""

from __future__ import annotations

from typing import Any, Generic

from typing_extensions import TypeVar

from ..core import Promise

#: The Python type represented by a feature dtype.
T = TypeVar("T")

import datetime
import importlib
import pathlib
import types
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Never, get_args, get_origin, overload

# `CatDType` is `Annotated`: the first argument is the registry type, the
# second is a `*Cat` metadata instance.  Aliasing `Annotated` is what lets a
# *value* (`RecordCat(type__uid="123")`) sit inside `[...]` and still type-check.
# needed for mypy to understand that `CatDType[Record, RecordCat(...)]` is a valid type expression.
from typing import Annotated as CatDType

__all__ = [
    "num",
    "path",
    "url",
    "DType",
    "ListDType",
    "CatDType",
    "LaminDType",
    "ResolvedDType",
    "get_dtype",
    "inline_cat_filters",
    "DEFAULT_NAME_FIELD",
    # categorical filter metadata
    "RecordCat",
    "ArtifactCat",
    "ULabelCat",
    "DangerousCat",
    "UnionCat",
    # per-registry aliases -- lamindb
    "CollectionCat",
    "FeatureCat",
    "ProjectCat",
    "ReferenceCat",
    "RunCat",
    "SchemaCat",
    "StorageCat",
    "TransformCat",
    "UserCat",
    # per-registry aliases -- bionty
    "CellLineCat",
    "CellMarkerCat",
    "CellTypeCat",
    "DevelopmentalStageCat",
    "DiseaseCat",
    "EthnicityCat",
    "ExperimentalFactorCat",
    "GeneCat",
    "OrganismCat",
    "PathwayCat",
    "PhenotypeCat",
    "ProteinCat",
    "SourceCat",
    "TissueCat",
    # per-registry aliases -- pertdb
    "BiologicCat",
    "CombinationPerturbationCat",
    "CompoundCat",
    "CompoundPerturbationCat",
    "EnvironmentalPerturbationCat",
    "GeneticPerturbationCat",
    "PerturbationTargetCat",
]


# ---------------------------------------------------------------------------
# lamindb's string dtypes exposed as distinct, annotatable types.
#
# `num` is lamindb's `int | float` convenience dtype; `path`/`url` are
# validated as `str` but rendered specially in the UI.  Each is a plain
# subclass of the Python type it is validated as, which keeps `DType[url]` a
# valid type expression; the runtime maps the class -- by identity, through
# :data:`_STRING_DTYPE_MARKERS` -- to the `"num"`/`"path"`/`"url"`
# string, wherever a dtype is accepted.
# ---------------------------------------------------------------------------


class num(float): ...


class path(str): ...


class url(str): ...


_STRING_DTYPE_MARKERS: dict[type, str] = {num: "num", path: "path", url: "url"}


def _string_dtype_marker(dtype: Any) -> str | None:
    """The dtype string of a `num`/`path`/`url` marker class, else `None`."""
    return _STRING_DTYPE_MARKERS.get(dtype) if isinstance(dtype, type) else None


_SIMPLE_TYPES: tuple[type, ...] = (
    int,
    float,
    bool,
    str,
    datetime.date,
    datetime.datetime,
    dict,
    pathlib.Path,
)


def _to_lamin_dtype(dtype: Any) -> Any:
    """Normalise a Python type / string-dtype marker for lamindb.

    Returns either a Python type that lamindb's `serialize_dtype` accepts, or an
    already-canonical dtype string (`"num"` / `"path"` / `"url"`).
    """
    # `num` / `path` / `url` marker classes -> their string dtype.
    if (marker := _string_dtype_marker(dtype)) is not None:
        return marker
    # `pathlib.Path` is the "path" dtype (lamindb has no Path type object).
    if dtype is pathlib.Path:
        return "path"
    if dtype in _SIMPLE_TYPES:
        return dtype
    # `dict[str, str]` and friends collapse to `dict`.
    if get_origin(dtype) is dict:
        return dict
    raise ValueError(f"Unsupported simple dtype: {dtype!r}")


def _is_cat_member(member: Any) -> bool:
    """Whether `member` is a categorical union member (a registry or field).

    A union categorical is built only from lamindb registry classes (or their
    field attributes); a union of plain Python types (`int | str`) is not a
    categorical and must stay unsupported.
    """
    if isinstance(member, type):
        if any(
            base.__name__ in ("BaseSQLRecord", "BioRecord", "SQLRecord", "Record")
            for base in getattr(member, "__mro__", ())
        ):
            return True
        return getattr(member, "__module__", "").startswith(
            ("lamindb.models", "bionty.models", "pertdb")
        )
    return member.__class__.__name__ in ("DeferredAttribute", "FieldAttr")


# ---------------------------------------------------------------------------
# FQN-based dtype specs.
#
# A categorical dtype references a lamindb / bionty / pertdb *class* (or a field
# attribute bound to one).  Holding that class object is unsafe: switching the
# connected instance in one process tears Django down and rebuilds every model
# class, so a class captured against the previous instance is stale for the next
# one and fails lamindb's `isinstance(dtype, Registry)` metaclass check.
#
# Instead of the class object we keep a *spec* -- a small tagged tuple that names
# the class by its fully-qualified name (module + qualname) -- and re-import it
# on demand.  Deriving the spec only reads `__module__` / `__qualname__` /
# `field.name`, which are plain strings valid even on a stale class; resolving
# it consults `sys.modules` at call time, so it always yields the *currently*
# connected instance's class.  Non-lamindb values (builtin types, the `"num"` /
# `"path"` / `"url"` dtype strings) never go stale and are stored verbatim.
#
#   ("raw", value)                     builtin type or dtype string, stored as-is
#   ("class", module, qualname)        a registry class
#   ("field", module, qualname, name)  a registry field attribute
#   ("union", (spec, ...))             a union categorical (cat[A|B])
#   ("list", spec)                     a list[...] wrapper
# ---------------------------------------------------------------------------


def _is_lamin_registry(cls: type) -> bool:
    """Whether `cls` is a lamindb/bionty/pertdb registry class (may be stale)."""
    if getattr(cls, "__module__", "").startswith(
        ("lamindb.models", "bionty.models", "pertdb")
    ):
        return True
    return any(
        base.__name__ in ("BaseSQLRecord", "BioRecord", "SQLRecord", "Record")
        for base in getattr(cls, "__mro__", ())
    )


def _to_dtype_spec(value: Any) -> tuple[str, Any]:
    """Capture `value` as an FQN-based, class-object-free dtype spec.

    Always a `(tag, payload)` pair, so every spec has the same shape; the tag
    decides how :func:`_resolve_dtype_spec` reads the payload back.
    """
    if isinstance(value, types.GenericAlias) and get_origin(value) is list:
        (inner,) = get_args(value)
        return ("list", _to_dtype_spec(inner))
    if isinstance(value, (list, tuple)):
        return ("union", tuple(_to_dtype_spec(member) for member in value))
    if isinstance(value, type):
        if _is_lamin_registry(value):
            return ("class", (value.__module__, value.__qualname__))
        return ("raw", value)
    if value.__class__.__name__ in ("DeferredAttribute", "FieldAttr"):
        field = getattr(value, "field", None)
        model = getattr(field, "model", None)
        name = getattr(field, "name", None)
        if model is not None and name is not None:
            return ("field", (model.__module__, model.__qualname__, name))
    return ("raw", value)


def _import_qualname(module: str, qualname: str) -> Any:
    """Import `module` and walk `qualname` to the referenced object."""
    obj: Any = importlib.import_module(module)
    for part in qualname.split("."):
        obj = getattr(obj, part)
    return obj


def _resolve_dtype_spec(spec: tuple[str, Any]) -> Any:
    """Resolve a dtype spec back into a live object against the current instance."""
    tag, payload = spec
    if tag == "raw":
        return payload
    if tag == "class":
        module, qualname = payload
        return _import_qualname(module, qualname)
    if tag == "field":
        module, qualname, name = payload
        return getattr(_import_qualname(module, qualname), name)
    if tag == "union":
        return [_resolve_dtype_spec(member) for member in payload]
    if tag == "list":
        return types.GenericAlias(list, (_resolve_dtype_spec(payload),))
    raise ValueError(f"unknown dtype spec: {spec!r}")


def _dtype_str(dtype: Any) -> str:
    """lamindb's canonical string form of a dtype spec.

    Mirrors lamindb's own conversion in `Feature.__init__`: the string dtypes
    (`"num"` / `"path"` / `"url"`) are canonical already, and so is a list
    of one (`list[num]`), which `serialize_dtype` would refuse; everything
    else is handed to :func:`lamindb.models.feature.serialize_dtype`.
    """
    if isinstance(dtype, str):
        return dtype
    if isinstance(dtype, types.GenericAlias) and get_origin(dtype) is list:
        (inner,) = get_args(dtype)
        if isinstance(inner, str):
            return f"list[{inner}]"
    from lamindb.models.feature import serialize_dtype

    return serialize_dtype(dtype)


# ---------------------------------------------------------------------------
# Default categorical name field per registry.
#
# The value is the field lamindb matches a `cat[Registry]` feature against,
# resolved via lamindb's own `get_name_field` (so registries without an
# explicit `_name_field` fall back to their `name` column, etc.).  Keyed by
# registry class name (unique across lamindb, bionty and pertdb).
# ---------------------------------------------------------------------------
DEFAULT_NAME_FIELD: dict[str, str] = {
    # --- lamindb ---
    "Artifact": "key",
    "Collection": "key",
    "Feature": "name",
    "Project": "name",
    "Record": "name",
    "Reference": "name",
    "Run": "started_at",
    "Schema": "name",
    "Storage": "root",
    "Transform": "key",
    "ULabel": "name",
    "User": "handle",
    # --- bionty ---
    "CellLine": "name",
    "CellMarker": "name",
    "CellType": "name",
    "DevelopmentalStage": "name",
    "Disease": "name",
    "Ethnicity": "name",
    "ExperimentalFactor": "name",
    "Gene": "symbol",
    "Organism": "name",
    "Pathway": "name",
    "Phenotype": "name",
    "Protein": "name",
    "Source": "name",
    "Tissue": "name",
    # --- pertdb ---
    "Biologic": "name",
    "CombinationPerturbation": "name",
    "Compound": "name",
    "CompoundPerturbation": "name",
    "EnvironmentalPerturbation": "name",
    "GeneticPerturbation": "name",
    "PerturbationTarget": "name",
}


# ---------------------------------------------------------------------------
# Simple / list dtypes.
# ---------------------------------------------------------------------------
class DType(Generic[T]):
    """A simple, non-categorical dtype (annotation only, e.g. `x: DType[int]`).

    Resolve it into a concrete lamindb dtype via :func:`get_dtype`.
    """

    def __new__(cls, *args: Any, **kwargs: Any) -> Never:
        raise TypeError(
            "DType is an annotation-only marker; use get_dtype() to extract the dtype"
        )


class ListDType(Generic[T]):
    """A list dtype (annotation only, e.g. `x: ListDType[int]`).

    The inner type is a simple dtype (`ListDType[int]`) or a categorical
    (`ListDType[CatDType[Record, RecordCat(type__uid=...)]]`); a categorical's
    filters are folded into the dtype string by :func:`inline_cat_filters` when
    the feature is staged.  Resolve it into a concrete lamindb dtype via
    :func:`get_dtype`.
    """

    def __new__(cls, *args: Any, **kwargs: Any) -> Never:
        raise TypeError(
            "ListDType is an annotation-only marker; use get_dtype() to extract the dtype"
        )


# ---------------------------------------------------------------------------
# Categorical filter metadata (à la `Annotated` + dataclasses).
#
# Each registry exposes only the categorical filters that lamindb accepts for it.
# There are only a handful of distinct filter *shapes*; the two shared by many
# registries are private (`_Cat` / `_BioCat`) and exposed via one public
# `<Registry>Cat` alias each, while the single-registry shapes
# (`RecordCat` / `ArtifactCat` / `ULabelCat`) keep their public name.
# `name_field` is an optional selector; when `None`, :func:`get_dtype` fills it
# from :data:`DEFAULT_NAME_FIELD`.  `DangerousCat` accepts arbitrary filters.
#
# A uid-bearing filter (`type__uid` / `schema__uid` / `source__uid`) accepts
# either a literal Base62 uid string or a `.uid` *promise* pointing at another
# object created by the same plan (e.g. `some_record_type.uid`).  The promise is
# carried verbatim through :func:`get_dtype` into the feature's `cat_filters` and
# resolved to a concrete uid during apply (see `Feature.stage`), so a declarative
# schema can express a cross-object categorical relation without hand-building a
# :class:`LaminDType`.
# ---------------------------------------------------------------------------


# A categorical uid filter: a literal uid string, or a `.uid` promise to another
# plan-managed object (resolved to a concrete uid during apply).
_UidFilter = str | Promise[Any, Any]


class _CatFilters:
    """Base for categorical filter metadata carried on a :data:`CatDType`."""

    def as_cat_filters(self) -> dict[str, Any]:
        """The non-`None` filters as a plain dict (`name_field` is excluded)."""
        field_names: dict[str, Any] = getattr(self, "__dataclass_fields__", {})
        return {
            name: value
            for name in field_names
            if name != "name_field" and (value := getattr(self, name)) is not None
        }


# --- distinct filter shapes ------------------------------------------------
@dataclass(frozen=True, kw_only=True)
class _Cat(_CatFilters):
    """Plain shape: a `name_field` selector only (most lamindb registries)."""

    name_field: str | None = None


@dataclass(frozen=True, kw_only=True)
class _BioCat(_CatFilters):
    """Bionty/pertdb shape: `name_field` + `source__uid`."""

    name_field: str | None = None
    source__uid: _UidFilter | None = None


@dataclass(frozen=True, kw_only=True)
class RecordCat(_CatFilters):
    """Filters for `cat[Record[...]]` features.

    `type__uid` selects the record type the categorical points at; pass either a
    literal uid string or a `.uid` promise to a plan-managed `RecordType` /
    `Sheet` (e.g. `RecordCat(type__uid=my_record_type.uid, is_type=True)`).
    """

    type__uid: _UidFilter
    name_field: str | None = None
    schema__uid: _UidFilter | None = None
    is_type: bool | None = None


@dataclass(frozen=True, kw_only=True)
class ArtifactCat(_CatFilters):
    """Filters for `cat[Artifact]` features."""

    name_field: str | None = None
    schema__uid: _UidFilter | None = None


@dataclass(frozen=True, kw_only=True)
class ULabelCat(_CatFilters):
    """Filters for `cat[ULabel]` features."""

    name_field: str | None = None
    type__uid: _UidFilter | None = None


class DangerousCat(_CatFilters):
    """Escape hatch: accepts arbitrary keyword filters.

    `name_field` stays the target-field selector and is not emitted as a
    filter; every other (non-`None`) keyword is passed to lamindb verbatim.
    Prefer a typed `*Cat` class where one exists.
    """

    def __init__(self, *, name_field: str | None = None, **cat_filters: Any) -> None:
        self.name_field = name_field
        self._cat_filters = cat_filters

    def as_cat_filters(self) -> dict[str, Any]:
        return {k: v for k, v in self._cat_filters.items() if v is not None}


class UnionCat(_CatFilters):
    """Metadata marking a *union* categorical (`CatDType[A | B, UnionCat()]`).

    lamindb serialises a union of registries to `cat[A|B]` and refuses
    `cat_filters` on it (a union may not carry per-member filters, and it may
    not include a `Record`/`ULabel` *subtype*).  `UnionCat` therefore
    carries **no** filters -- only optional per-member field selectors via
    `fields`, a mapping of union member (a registry class) to the field
    lamindb should match against.  A member absent from `fields` uses its
    default name field (:data:`DEFAULT_NAME_FIELD`)::

        CatDType[ULabel | Record, UnionCat()]                       # cat[ULabel|Record]
        CatDType[CellType | CellLine, UnionCat(fields={CellType: "uid"})]
    """

    def __init__(self, *, fields: dict[type, str] | None = None) -> None:
        self.fields: dict[type, str] = dict(fields or {})

    def as_cat_filters(self) -> dict[str, Any]:
        # A union categorical never carries cat_filters (lamindb rejects them).
        return {}


# --- per-registry aliases --------------------------------------------------
# Aliases of the shared shapes -- one public name per registry.
# Plain lamindb registries (`name_field` only).
CollectionCat = _Cat
FeatureCat = _Cat
ProjectCat = _Cat
ReferenceCat = _Cat
RunCat = _Cat
SchemaCat = _Cat
StorageCat = _Cat
TransformCat = _Cat
UserCat = _Cat

# bionty registries (`name_field` + `source__uid`).
CellLineCat = _BioCat
CellMarkerCat = _BioCat
CellTypeCat = _BioCat
DevelopmentalStageCat = _BioCat
DiseaseCat = _BioCat
EthnicityCat = _BioCat
ExperimentalFactorCat = _BioCat
GeneCat = _BioCat
OrganismCat = _BioCat
PathwayCat = _BioCat
PhenotypeCat = _BioCat
ProteinCat = _BioCat
SourceCat = _BioCat
TissueCat = _BioCat

# pertdb registries (also `BioRecord` subclasses).
BiologicCat = _BioCat
CombinationPerturbationCat = _BioCat
CompoundCat = _BioCat
CompoundPerturbationCat = _BioCat
EnvironmentalPerturbationCat = _BioCat
GeneticPerturbationCat = _BioCat
PerturbationTargetCat = _BioCat


# ---------------------------------------------------------------------------
# Resolution back into concrete lamindb dtype + cat_filters.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ResolvedDType:
    """lamindb's canonical dtype string + `cat_filters` for an annotation."""

    dtype: str
    cat_filters: dict[str, Any] | None

    @property
    def dtype_str(self) -> str:
        """lamindb's canonical dtype string (already resolved; excludes cat_filters).

        Mirrors :attr:`LaminDType.dtype_str` so both dtype wrappers expose the same
        interface to :class:`~lamindb_setup.evolve.authoring._features.Feature`.
        """
        return self.dtype


def _resolve_union_cat(union: Any, metadata: tuple[Any, ...]) -> ResolvedDType:
    """Resolve a union categorical (`CatDType[A | B, UnionCat(...)]`).

    Each union member resolves to its registry (or `registry.field` when a
    per-member selector is given in :class:`UnionCat`); the whole list is
    stringified through lamindb's `serialize_dtype` into `cat[A|B]`.  A
    union carries no `cat_filters` -- lamindb forbids them.
    """
    members = [m for m in get_args(union) if m is not type(None)]
    if len(members) < 2:
        raise ValueError(
            f"A union categorical needs at least two registry members, got: {union!r}"
        )
    marker = next((m for m in metadata if isinstance(m, UnionCat)), None)
    if marker is None:
        raise ValueError(
            "A union categorical (CatDType[A | B, ...]) requires a UnionCat() "
            "marker; a per-registry *Cat is not allowed on a union because "
            "lamindb forbids cat_filters on union dtypes."
        )
    specs: list[Any] = []
    for member in members:
        name_field = marker.fields.get(member)
        if name_field is None:
            name_field = DEFAULT_NAME_FIELD.get(getattr(member, "__name__", ""))
        specs.append(getattr(member, name_field) if name_field is not None else member)
    return ResolvedDType(dtype=_dtype_str(specs), cat_filters=None)


def get_dtype(annotation: Any) -> ResolvedDType:
    """Resolve a dtype annotation into lamindb's canonical dtype string.

    Handles :class:`DType`/:class:`ListDType` generic aliases, :data:`CatDType`
    (`Annotated`), and bare Python simple/list types.  For a categorical
    annotation the `name_field` selector picks the registry field (defaulting to
    :data:`DEFAULT_NAME_FIELD`, else the bare registry).  The resulting dtype spec
    is stringified through lamindb's own `serialize_dtype` mechanism.
    """
    # `CatDType[...]` == `Annotated[Registry, *CatFilters(...)]`.
    if hasattr(annotation, "__metadata__"):
        base = get_args(annotation)[0]
        if isinstance(base, types.UnionType):
            # `None` is an optional marker, not a registry member.
            non_none = tuple(m for m in get_args(base) if m is not type(None))
            # `CatDType[A | B, UnionCat(...)]` -- a union of registries (cat[A|B]).
            if len(non_none) >= 2:
                return _resolve_union_cat(base, annotation.__metadata__)
            if not non_none:
                raise ValueError(
                    f"A categorical annotation needs a registry member, got: {base!r}"
                )
            # `CatDType[A | None, ACat(...)]` -- an optional single categorical:
            # drop the `None` and resolve the sole member as a plain cat.
            base = non_none[0]
        if any(isinstance(item, UnionCat) for item in annotation.__metadata__):
            raise ValueError(
                "UnionCat() requires a union of at least two registry members."
            )
        filt = next(
            (m for m in annotation.__metadata__ if isinstance(m, _CatFilters)), None
        )
        if filt is None:
            raise ValueError(
                "CatDType requires a *Cat metadata argument, "
                f"got: {annotation.__metadata__!r}"
            )
        name_field = getattr(filt, "name_field", None)
        if name_field is None:
            name_field = DEFAULT_NAME_FIELD.get(getattr(base, "__name__", ""))
        dtype_obj = getattr(base, name_field) if name_field is not None else base
        return ResolvedDType(
            dtype=_dtype_str(dtype_obj), cat_filters=filt.as_cat_filters() or None
        )

    # `DType[...]` / `ListDType[...]` generic aliases and bare `list[...]`.
    origin = get_origin(annotation)
    if origin is DType:
        (arg,) = get_args(annotation)
        return ResolvedDType(dtype=_dtype_str(_to_lamin_dtype(arg)), cat_filters=None)
    if origin is ListDType or origin is list:
        (arg,) = get_args(annotation)
        if hasattr(arg, "__metadata__"):
            # `ListDType[CatDType[Registry, RegistryCat(...)]]` -- a list of a
            # categorical.  Its filters ride along on the `ResolvedDType` and are
            # folded into the dtype string at staging time (see
            # :func:`inline_cat_filters`), because lamindb's `cat_filters=`
            # keyword refuses any nested dtype.
            inner_cat = get_dtype(arg)
            return ResolvedDType(
                dtype=f"list[{inner_cat.dtype}]", cat_filters=inner_cat.cat_filters
            )
        inner = _dtype_str(_to_lamin_dtype(arg))
        return ResolvedDType(dtype=f"list[{inner}]", cat_filters=None)

    # Bare simple Python type.
    return ResolvedDType(
        dtype=_dtype_str(_to_lamin_dtype(annotation)), cat_filters=None
    )


def inline_cat_filters(dtype_str: str, cat_filters: dict[str, Any]) -> str:
    """Fold `cat_filters` into a `list[cat[...]]` dtype string.

    lamindb's `Feature(..., cat_filters=...)` keyword rejects every nested
    dtype (`cat_filters are incompatible with nested dtypes`), so a *list* of a
    filtered categorical cannot be declared through it.  Its dtype string
    *grammar* does support the filters inline, though --
    `list[cat[Record[<type-uid>]]]`, `list[cat[bionty.CellType[source__uid='x']]]`
    -- and `parse_dtype` reads them back with `list: True`.  This mirrors the
    serialization lamindb applies to a scalar categorical, so passing the result
    as the feature's `dtype` yields exactly the string lamindb would have
    stored had it accepted the keyword.

    Raises `ValueError` when `dtype_str` is not a list of a single
    categorical (a union carries no filters -- lamindb forbids them).
    """
    prefix, suffix = "list[cat[", "]]"
    if not (dtype_str.startswith(prefix) and dtype_str.endswith(suffix)):
        raise ValueError(
            "cat_filters can only be inlined into a list of a categorical, "
            f"got: {dtype_str!r}"
        )
    registry = dtype_str[len(prefix) : -len(suffix)]
    if "|" in registry:
        raise ValueError(
            f"cat_filters are incompatible with union dtypes: {dtype_str!r}"
        )
    filters = dict(cat_filters)
    parts: list[str] = []
    if registry in ("Record", "ULabel") and "type__uid" in filters:
        # lamindb's typed shorthand for a Record/ULabel subtype: `cat[Record[<uid>]]`.
        parts.append(str(filters.pop("type__uid")))
    parts.extend(f"{key}='{value}'" for key, value in filters.items())
    return f"{prefix}{registry}[{', '.join(parts)}]{suffix}"


def resolved_dtype_str(dtype_str: str, cat_filters: Mapping[str, Any] | None) -> str:
    """The full dtype string lamindb stores for `dtype_str` filtered by `cat_filters`.

    :attr:`LaminDType.dtype_str` is the *bare* string (`cat[Record]`); the
    filters a feature declares ride alongside it and only reach the string when
    lamindb builds a `Feature` from them.  Updating an existing feature's dtype
    in place needs that full string without building one: lamindb's constructor
    re-loads the existing record by name and refuses a dtype it does not already
    have.  This computes it purely, from the bare string and the *resolved*
    filters (every promise already a concrete value).

    A list target is inlined by :func:`inline_cat_filters`, exactly as
    :meth:`~lamindb_setup.evolve.authoring._features.Feature.stage` does when it
    creates one.  A scalar target mirrors the serialization lamindb's own
    `Feature(..., cat_filters=...)` applies: a record value becomes its
    `<key>__uid`, `type` is folded into `type__uid`, a `Record` /
    `ULabel` subtype takes the `cat[Record[<uid>]]` shorthand, and a value is
    quoted only when it contains a comma.

    Raises `ValueError` for a dtype the filters cannot be added to (a union, a
    non-categorical, or one already carrying filters -- a bare string is
    expected) and for the filter values lamindb would reject.
    """
    if not cat_filters:
        return dtype_str
    if dtype_str.startswith("list["):
        return inline_cat_filters(dtype_str, dict(cat_filters))
    if "|" in dtype_str:
        raise ValueError(
            f"cat_filters are incompatible with union dtypes: {dtype_str!r}"
        )
    if not (dtype_str.startswith("cat[") and dtype_str.endswith("]")):
        raise ValueError(f"cat_filters require a categorical dtype, got: {dtype_str!r}")
    if "]]" in dtype_str:
        raise ValueError(
            f"cat_filters cannot be added to an already filtered dtype: {dtype_str!r}"
        )

    filters: dict[str, Any] = {}
    for key, value in cat_filters.items():
        if (
            value is None
            or (isinstance(value, str) and not value.strip())
            or (not isinstance(value, bool) and not value)
        ):
            raise ValueError(f"Empty value in filter {key}")
        if getattr(value, "_meta", None) is not None and hasattr(value, "uid"):
            # A record filters by its uid, which is what the string can hold.
            filters[f"{key}__uid"] = value.uid
        else:
            filters[key] = value
    if "type" in filters:
        if "type__uid" in filters and str(filters["type"]) != str(filters["type__uid"]):
            raise ValueError(
                "Conflicting type selectors in cat_filters: 'type' and 'type__uid'"
            )
        filters["type__uid"] = filters.pop("type")

    shorthand: str | None = None
    if dtype_str in ("cat[Record]", "cat[ULabel]") and "type__uid" in filters:
        shorthand = str(filters.pop("type__uid"))
    parts = [] if shorthand is None else [shorthand]
    parts.extend(
        f"{key}={_format_cat_filter_value(value)}" for key, value in filters.items()
    )
    return f"{dtype_str[:-1]}[{', '.join(parts)}]]"


def _format_cat_filter_value(value: Any) -> str:
    """Render a scalar filter value the way lamindb does: quoted only around a comma."""
    if isinstance(value, (bool, int, float)):
        return str(value)
    text = str(value)
    if "," not in text:
        return text
    if '"' not in text:
        return f'"{text}"'
    if "'" not in text:
        return f"'{text}'"
    raise ValueError(
        f"cat_filters value {text!r} contains a comma and both quote characters, "
        "so it cannot be serialized into a dtype string"
    )


# ---------------------------------------------------------------------------
# Backwards-compatible object-based wrapper.
# ---------------------------------------------------------------------------


class LaminDType(Generic[T]):
    """Legacy dtype wrapper exposing an object `dtype` + `cat_filters` dict.

    Retained for existing `Feature(dtype=LaminDType(...))` call sites and the
    introspection codegen.  New code should prefer the annotation forms
    (:class:`DType` / :class:`ListDType` / :data:`CatDType`) resolved with
    :func:`get_dtype`.  Use :attr:`dtype_str` for lamindb's canonical string.
    """

    # The generic `type[T]` overload comes first so any class object -- including
    # `Any`-tainted lamindb registry classes such as `ln.Record` -- binds `T`
    # to that class.  The string forms are pure `Literal[...]` overloads (disjoint
    # from `type[T]`) that pin `T` via the `self` annotation.  Keeping the
    # literal strings out of the `type[T]` parameter avoids collapsing `T` to
    # `Never` when an `Any`-typed argument also satisfies a `str` branch.
    @overload
    def __init__(self, dtype: type[T], /, **cat_filters: Any) -> None: ...
    @overload
    def __init__(
        self: LaminDType[num], dtype: Literal["num"], /, **cat_filters: Any
    ) -> None: ...
    @overload
    def __init__(
        self: LaminDType[path], dtype: Literal["path"], /, **cat_filters: Any
    ) -> None: ...
    @overload
    def __init__(
        self: LaminDType[url], dtype: Literal["url"], /, **cat_filters: Any
    ) -> None: ...
    @overload
    def __init__(
        self: LaminDType[Any], dtype: list[Any] | tuple[Any, ...], /
    ) -> None: ...

    def __init__(self, dtype: Any, /, **cat_filters: Any) -> None:
        self.cat_filters: dict[str, Any] | None
        value: Any
        # `LaminDType(num)` is the same dtype as `LaminDType("num")`.
        dtype = _string_dtype_marker(dtype) or dtype
        if dtype in (
            int,
            float,
            bool,
            str,
            datetime.date,
            datetime.datetime,
            dict,
            pathlib.Path,
        ):
            value = dtype
            if cat_filters:
                raise ValueError(
                    "Categorical filters are not applicable for basic Python data types."
                )
            self.cat_filters = None
        elif (
            isinstance(dtype, types.UnionType) and set(get_args(dtype)) == {int, float}
        ) or dtype == "num":
            value = "num"
            if cat_filters:
                raise ValueError(
                    "Categorical filters are not applicable for the 'num' "
                    "(int | float) data type."
                )
            self.cat_filters = None
        elif (
            isinstance(dtype, (list, tuple)) or isinstance(dtype, types.UnionType)
        ) and all(
            _is_cat_member(m)
            for m in (get_args(dtype) if isinstance(dtype, types.UnionType) else dtype)
        ):
            # A union categorical: a list/tuple of registries (or registry
            # fields) -- `[Record, Gene]` -- or a `Record | ULabel` union.
            # lamindb serialises it to `cat[A|B]` and forbids `cat_filters`.
            members = (
                list(get_args(dtype))
                if isinstance(dtype, types.UnionType)
                else list(dtype)
            )
            if len(members) < 2:
                raise ValueError(
                    "A union categorical needs at least two registry members, "
                    f"got: {dtype!r}"
                )
            if cat_filters:
                raise ValueError(
                    "Categorical filters are incompatible with union dtypes."
                )
            value = members
            self.cat_filters = None
        elif dtype in ("path", "url"):
            value = dtype
            if cat_filters:
                raise ValueError(
                    f"Categorical filters are not applicable for the {dtype!r} data type."
                )
            self.cat_filters = None
        elif (
            (
                not isinstance(dtype, type)
                and dtype.__class__.__name__ == "DeferredAttribute"
            )
            or (
                isinstance(dtype, type)
                and dtype.__module__.startswith("lamindb.models")
                and dtype.__name__ in ("Artifact", "ULabel")
            )
            or (
                isinstance(dtype, type)
                and any(base.__name__ == "BioRecord" for base in dtype.__mro__)
            )
        ):
            value = dtype
            self.cat_filters = dict(cat_filters) or None
        elif (
            isinstance(dtype, type)
            and dtype.__module__.startswith("lamindb.models")
            and dtype.__name__ == "Record"
        ):
            value = dtype
            self.cat_filters = {
                "type__uid": cat_filters.pop("type__uid"),
                **cat_filters,
            }
        elif isinstance(dtype, type) and any(
            base.__name__ == "BaseSQLRecord" for base in dtype.__mro__
        ):
            value = dtype
            if cat_filters:
                raise ValueError(
                    "Categorical filters are blocked on purpose for this registry."
                )
            self.cat_filters = None
        elif get_origin(dtype) is list:
            args = get_args(dtype)
            if len(args) != 1:
                raise ValueError(
                    "List data types must have exactly one inner data type, "
                    f"got: {dtype}"
                )
            (inner,) = args
            inner_dt = LaminDType(inner, **cat_filters)
            value = types.GenericAlias(list, (inner_dt.dtype,))
            self.cat_filters = inner_dt.cat_filters
        else:
            raise ValueError(f"Unsupported dtype: {dtype}")

        # Store the dtype as an FQN-based spec rather than the live class object,
        # so no stale lamindb class survives an in-process instance reconnect.
        self._spec = _to_dtype_spec(value)

    @property
    def dtype(self) -> Any:
        """The underlying dtype, re-resolved against the connected instance.

        Categorical dtypes store the registry's qualified name. Reading this
        property resolves the class against the current connection.
        """
        return _resolve_dtype_spec(self._spec)

    @property
    def dtype_str(self) -> str:
        """lamindb's canonical dtype string for this dtype (excludes cat_filters)."""
        return _dtype_str(self.dtype)


def require_dtype_wrapper(dtype: object) -> None:
    """Refuse a `dtype=` that is not a :class:`LaminDType` or :class:`ResolvedDType`.

    A bare class (`Feature("x", dtype=num)`) used to be stored as given, so it
    passed `validate` and reached the lock -- recorded by its module path --
    before staging failed at `apply` reading its `dtype_str`.  Refusing it
    here makes it fail where it is written.
    """
    if isinstance(dtype, (LaminDType, ResolvedDType)):
        return
    wrapped = dtype.__name__ if isinstance(dtype, type) else repr(dtype)
    raise TypeError(
        f"dtype must be a LaminDType or ResolvedDType, got {dtype!r}; "
        f"wrap it: dtype=LaminDType({wrapped})"
    )


if TYPE_CHECKING:

    def _check_dtype_examples() -> None:
        import bionty.models
        import lamindb.models

        # Executable specification: these annotations mirror the supported syntax and
        # are type-checked by `ty` so the grammar cannot silently regress.
        _s0: DType[int]
        _s1: DType[float]
        _s2: DType[bool]
        _s3: DType[str]
        _s4: DType[datetime.date]
        _s5: DType[datetime.datetime]
        _s6: DType[dict[str, str]]
        _s7: DType[pathlib.Path]
        _s8: DType[url]
        _s9: DType[num]
        _s10: DType[path]
        _s11: ListDType[int]
        _c0: CatDType[
            lamindb.models.Record,
            RecordCat(type__uid="123", schema__uid="456", is_type=True),
        ]
        _c1: CatDType[lamindb.models.Artifact, ArtifactCat(schema__uid="123")]
        _c2: CatDType[
            bionty.models.Organism,
            OrganismCat(name_field="name", source__uid="123"),
        ]
        _c3: CatDType[
            lamindb.models.Project,
            DangerousCat(some_future_filter="x"),
        ]
        # union categoricals (cat[A|B]) -- no cat_filters, optional field selectors.
        _u0: CatDType[lamindb.models.ULabel | lamindb.models.Record, UnionCat()]
        _u1: CatDType[
            bionty.models.CellType | bionty.models.CellLine,
            UnionCat(fields={bionty.models.CellType: "uid"}),
        ]
        # a list of a categorical (list[cat[...]]); its filters are inlined into the
        # dtype string at staging time (lamindb's cat_filters= rejects nested dtypes).
        _lc0: ListDType[CatDType[lamindb.models.Record, RecordCat(type__uid="123")]]
        _lc1: ListDType[
            CatDType[bionty.models.Organism, OrganismCat(source__uid="123")]
        ]

        _d0 = LaminDType(int)
        _d1 = LaminDType("num")
        _d2 = LaminDType(num)
        _d3 = LaminDType([lamindb.models.Record, bionty.models.Gene])

        _num_type: type[num] = num


# === Canonical dtype strings =================================================
#
# Comparing two serialized dtypes means comparing the *strings* a lock, a
# remote snapshot or a plan produced for them, and the codebase emits several
# encodings of the same type.  Reducing them to one canonical form is a fact
# about dtypes, so it lives here rather than in the drift taxonomy that was
# its first caller.
#
# Deliberately absent from `__all__`: this is machinery the lock, the apply
# engine and the drift taxonomy share, not part of the vocabulary a plan file
# writes with.


# `serialize_config` renders builtin scalar types via their import path; map
# them back to lamindb's canonical dtype tokens before classifying.
_BUILTIN_SCALARS: dict[str, str] = {
    "builtins.int": "int",
    "builtins.float": "float",
    "builtins.str": "str",
    "builtins.bool": "bool",
    "builtins.bytes": "bytes",
}


def canonical_dtype_str(value: str | None) -> str | None:
    """Return a canonical, comparison-stable form of a serialized dtype string.

    Collapses the encodings the codebase produces -- the canonical lamindb
    strings (`int` / `cat[ULabel]` / `list[int]`), the `builtins.int` /
    quoted-`'num'` forms emitted by :func:`~lamindb_setup.evolve.engine._lock.serialize_config`,
    and the legacy registry-class-path form (`bionty.models.Gene`) -- and reduces
    a categorical to its bare registry token(s) with the module prefix, a
    *default* name field, and any inline `[...]` filter payload dropped.  So
    `cat[bionty.Gene]`, `cat[bionty.Gene.symbol]` and `bionty.models.Gene`
    all render identically (`cat[Gene]`), and `cat[Record]` matches
    `cat[Record[<uid>]]`, while a *non-default* field
    (`cat[bionty.Gene.ensembl_gene_id]`) stays distinct.  `None` and any
    unparseable / non-dtype string are returned unchanged (quotes stripped).
    """
    parsed = parse_dtype(value)
    if parsed is None:
        return _normalize_dtype_str(value) if isinstance(value, str) else value
    return _render_parsed(parsed)


def _render_parsed(parsed: tuple) -> str:
    """Render a parsed dtype tuple back into its canonical string form."""
    tag = parsed[0]
    if tag == "list":
        return f"list[{_render_parsed(parsed[1])}]"
    if tag == "cat":
        return "cat[" + "|".join(sorted(parsed[1])) + "]"
    return parsed[1]


def _canonical_registry_path(text: str) -> str | None:
    """Reduce a registry reference to a canonical token, or `None` if not one.

    Accepts a dotted registry path with an optional trailing name field --
    `bionty.Gene`, `bionty.models.Gene`, `bionty.Gene.symbol`, `ULabel` --
    and returns the bare registry class name, dropping the module prefix and a
    *default* name field (:data:`~lamindb_setup.evolve.DEFAULT_NAME_FIELD`).
    A non-default field is kept (`bionty.Gene.ensembl_gene_id` ->
    `Gene.ensembl_gene_id`) because it is a genuinely different match.  Returns
    `None` when `text` names no known registry, so a plain scalar dtype
    (`int` / `builtins.int`) is left untouched.
    """
    segments = text.split(".")
    # The registry class is the last CamelCase segment; module segments precede
    # it and a name field (if any) follows it, both lowercase/snake_case.
    reg_idx = next(
        (i for i in range(len(segments) - 1, -1, -1) if segments[i][:1].isupper()),
        None,
    )
    if reg_idx is None:
        return None
    registry = segments[reg_idx]
    if registry not in DEFAULT_NAME_FIELD:
        return None
    field = ".".join(segments[reg_idx + 1 :])
    if not field or field == DEFAULT_NAME_FIELD[registry]:
        return registry
    return f"{registry}.{field}"


def _canonical_cat_component(component: str) -> str:
    """Canonicalize one `cat[...]` member down to its bare registry token.

    Drops the module prefix and a *default* name field (via
    :func:`_canonical_registry_path`) and the inline `[...]` filter payload that
    lamindb bakes into a categorical's stored dtype string (a Record `type__uid`,
    a bionty `source__uid`, etc.).  A plan carries those filters separately in
    `cat_filters` -- compared as their own config field -- so the dtype string is
    compared only at the registry level: `cat[Record]` and `cat[Record[<uid>]]`
    are the same registry.  An unrecognized registry is returned unchanged (its
    filter payload, if any, is kept).
    """
    bracket = component.find("[")
    head = component if bracket == -1 else component[:bracket]
    canonical = _canonical_registry_path(head)
    if canonical is not None:
        return canonical
    return component


def parse_dtype(value: object) -> tuple | None:
    """Parse a dtype string into a comparable structure, or `None` if it is not one.

    `("simple", "str")`, `("cat", frozenset of canonical components)` or
    `("list", <inner>)`.  Two dtypes that only differ in how they are spelled
    -- component order, a field left at its default, a legacy bare registry
    path -- parse to the same value, which is what the drift taxonomy compares.
    """
    if not isinstance(value, str):
        return None
    text = _normalize_dtype_str(value)
    if text.startswith("list[") and text.endswith("]"):
        inner = parse_dtype(text[5:-1])
        return None if inner is None else ("list", inner)
    if text.startswith("cat[") and text.endswith("]"):
        components = _split_top_level(text[4:-1], "|")
        return (
            "cat",
            frozenset(
                _canonical_cat_component(c.strip()) for c in components if c.strip()
            ),
        )
    # A bare registry class path is a legacy encoding of a single categorical
    # (a pre-canonicalization lock stored e.g. `bionty.models.Gene`).
    registry = _canonical_registry_path(text)
    if registry is not None:
        return ("cat", frozenset({registry}))
    return ("simple", text)


def dtype_filter_terms(value: object) -> dict[str, frozenset[str]]:
    """The inline filters of each categorical component of a dtype string.

    :func:`parse_dtype` (and so :func:`canonical_dtype_str`) drops the `[...]`
    payload lamindb bakes into a categorical's stored string -- a `Record`
    type's uid, a bionty `source__uid`, `is_type=True` -- so a bare plan
    dtype compares equal to the instance's filtered one.  This is the other
    half: per canonical component, the set of `key=value` terms narrowing it.
    The `cat[Record[<uid>]]` shorthand reads as `type__uid=<uid>` and quotes
    around a value are dropped, so the spellings lamindb and
    :func:`resolved_dtype_str` produce for the same filters compare equal,
    whatever their order.

    A component without filters maps to an empty set, a non-categorical dtype
    (or a non-string) to an empty mapping.
    """
    if not isinstance(value, str):
        return {}
    text = _normalize_dtype_str(value)
    while text.startswith("list[") and text.endswith("]"):
        text = text[5:-1].strip()
    if not (text.startswith("cat[") and text.endswith("]")):
        registry = _canonical_registry_path(text)
        return {} if registry is None else {registry: frozenset()}
    terms: dict[str, frozenset[str]] = {}
    for component in _split_top_level(text[4:-1], "|"):
        component = component.strip()
        if component:
            terms[_canonical_cat_component(component)] = _filter_terms(component)
    return terms


def dtypes_match(left: str | None, right: str | None) -> bool:
    """Whether two dtype strings name the same dtype, filters included.

    :func:`canonical_dtype_str` equality alone treats `cat[Record[A]]` and
    `cat[Record[B]]` as one dtype; this also compares their
    :func:`dtype_filter_terms`.
    """
    return canonical_dtype_str(left) == canonical_dtype_str(
        right
    ) and dtype_filter_terms(left) == dtype_filter_terms(right)


def _filter_terms(component: str) -> frozenset[str]:
    """The normalized `key=value` filter terms of one `cat[...]` component."""
    bracket = component.find("[")
    if bracket == -1:
        return frozenset()
    depth = 0
    close = len(component)
    for index in range(bracket, len(component)):
        if component[index] == "[":
            depth += 1
        elif component[index] == "]":
            depth -= 1
            if depth == 0:
                close = index
                break
    terms: set[str] = set()
    for part in _split_filter_payload(component[bracket + 1 : close]):
        if "=" not in part:
            # lamindb's typed shorthand: `Record[<uid>]` is `type__uid=<uid>`.
            terms.add(f"type__uid={part}")
            continue
        key, raw = part.split("=", 1)
        terms.add(f"{key.strip()}={_unquote(raw.strip())}")
    return frozenset(terms)


def _split_filter_payload(payload: str) -> list[str]:
    """Split a filter payload on the commas outside quotes and brackets."""
    parts: list[str] = []
    buf: list[str] = []
    quote: str | None = None
    depth = 0
    for char in payload:
        if quote is not None:
            if char == quote:
                quote = None
        elif char in "'\"":
            quote = char
        elif char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
            continue
        buf.append(char)
    parts.append("".join(buf).strip())
    return [part for part in parts if part]


def _unquote(text: str) -> str:
    """`text` without one pair of matching surrounding quotes."""
    if len(text) >= 2 and text[0] == text[-1] and text[0] in "'\"":
        return text[1:-1]
    return text


def _normalize_dtype_str(text: str) -> str:
    text = text.strip()
    if len(text) >= 2 and text[0] == text[-1] == "'":
        # `serialize_config` stores the string dtypes ('num'/'path'/'url') as a
        # Python repr, i.e. wrapped in single quotes; unwrap them.
        text = text[1:-1]
    return _BUILTIN_SCALARS.get(text, text)


def _split_top_level(text: str, sep: str) -> list[str]:
    """Split `text` on `sep`, ignoring separators nested inside `[...]`."""
    parts: list[str] = []
    depth = 0
    buf: list[str] = []
    for char in text:
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
        if char == sep and depth == 0:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(char)
    parts.append("".join(buf))
    return parts
