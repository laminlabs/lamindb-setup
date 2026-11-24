# ruff: noqa: B010
"""Declarative, pydantic-style authoring surface for schemas.

Instead of building :class:`Feature` and :class:`Schema` objects by hand, a
schema can be declared as a class whose annotated fields become features::

    class MyExperiment(SchemaModel):
        schema_config = SchemaConfig(
            name="MyExperimentSchema",
            type=SchemaType("MyExperimentSchema"),
            default_feature_type=FeatureType("MyExperimentFeature"),
            index="sample_id",
        )

        sample_id: Feature[str]
        organism: Feature[str] = feature(description="Species of the sample")
        measured_at: Feature[datetime.date]

The metaclass reads the annotations at class-creation time and emits ordinary
:class:`Feature` and :class:`Schema` objects, so the resulting
`MyExperiment.schema` works with the plan, summary, apply, lock, and
`PlanExports` machinery unchanged.  Because a plan runs its body while a plan
is active, declaring a :class:`SchemaModel` subclass inside `run()` registers
the emitted objects with that plan automatically.

The annotation decides nullability: `Feature[<dtype>]` reads as "a <dtype>"
and emits a *non-nullable* feature (`nullable=False`), while
`Feature[<dtype> | None]` reads as "a <dtype> or nothing" and emits a
*nullable* one.  The outer union `Feature[...] | None` (equivalently
`Optional[Feature[...]]`) is a different axis: it marks the field *optional*
-- an optional schema member, i.e. not required by the schema.  The two
compose, and an explicit `feature(nullable=...)` / `feature(optional=...)`
still wins over the corresponding annotation::

    sample_id: Feature[str]                       # required, not nullable
    organism: Feature[str | None]                 # nullable feature
    replicate: Feature[int] | None                # optional feature
    note: Feature[str | None] | None              # optional and nullable
    site: Feature[int] | None = feature(optional=False)  # not optional
    legacy: Feature[str] = feature(nullable=True)        # nullable anyway

Feature descriptions can be provided with a numpy-style `Features` section in
the class docstring instead of (or alongside) `feature(description=...)`.
Each entry's name must match a declared field, and its indented body becomes
that feature's `description`; an explicit `feature(description=...)` still
wins, and a reused :class:`Feature` keeps its own description::

    class MyExperiment(SchemaModel):
        \"\"\"A single experiment.

        Features
        --------
        sample_id : Feature[str]
            The unique sample identifier.
        organism
            Species of the sample.
        \"\"\"

        schema_config = SchemaConfig(name="MyExperimentSchema")
        sample_id: Feature[str]
        organism: Feature[str]

Each field is exposed as the emitted :class:`Feature` on the class itself, so
`MyExperiment.sample_id` *is* the `Feature` object.  This makes features
shareable: assign an existing `Feature` to a field to reuse the very same
object in another schema, either a standalone feature or one declared by
another model::

    sample_id = Feature("sample_id", dtype=LaminDType(str))

    class A(SchemaModel):
        schema_config = SchemaConfig(name="A")
        sample_id: Feature[str] = sample_id

    class B(SchemaModel):
        schema_config = SchemaConfig(name="B")
        sample_id: Feature[str] = A.sample_id  # the exact same Feature object

A reused feature keeps its own settings (name, dtype, type, space); the field
name must match the feature's name and the annotated dtype must match its dtype.

Related features that share a single :class:`FeatureType` (but not a schema
shape) can instead be declared with :class:`FeatureGroup`, whose `group_config`
presets the feature type for every field::

    class ExperimentFeatures(FeatureGroup):
        group_config = GroupConfig(feature_type="experiment_feature")

        sample_id: Feature[str]
        organism: Feature[str] = feature(description="Species")

Each field is exposed as its emitted :class:`Feature` and
`ExperimentFeatures.feature_type` is the emitted :class:`FeatureType`; no
:class:`Schema` is built.  The features are reusable in a :class:`SchemaModel`.

"""

from __future__ import annotations

import inspect
import sys
import types
from dataclasses import dataclass
from typing import Any, ClassVar, TypedDict, Union, get_args, get_origin

from ..core import Promise, SpaceUID
from ._dtypes import (
    DType,
    LaminDType,
    ListDType,
    ResolvedDType,
    canonical_dtype_str,
    get_dtype,
    require_dtype_wrapper,
)
from ._features import Feature, FeatureType
from ._schemas import Schema, SchemaType

__all__ = [
    "SchemaModel",
    "SchemaConfig",
    "FeatureGroup",
    "GroupConfig",
    "feature",
]


class SchemaConfig(TypedDict, total=False):
    """Schema-level configuration for a :class:`SchemaModel`.

    Mirrors the way pydantic's `model_config` configures a model.  All keys
    are optional.  `default_feature_type` and `default_feature_space` are
    applied to fields that do not set their own via :func:`feature`.
    """

    name: str | None
    type: SchemaType | None
    description: str | None
    index: str | None
    default_feature_type: FeatureType | None
    default_feature_space: SpaceUID | None
    coerce: bool | None
    minimal_set: bool
    ordered_set: bool
    maximal_set: bool
    space_uid: SpaceUID | None


_ALLOWED_CONFIG_KEYS = frozenset(SchemaConfig.__annotations__)


class GroupConfig(TypedDict, total=False):
    """Configuration for a :class:`FeatureGroup`.

    A :class:`FeatureGroup` declares a set of features that all share a single
    :class:`FeatureType`.  `feature_type` is the group's type (a
    :class:`FeatureType`, or a `str` from which one is constructed) and is
    applied to every field that does not set its own via :func:`feature`.
    `default_feature_space` is likewise applied to fields lacking their own.
    """

    feature_type: FeatureType | str
    default_feature_space: SpaceUID | None


_ALLOWED_GROUP_CONFIG_KEYS = frozenset(GroupConfig.__annotations__)


@dataclass(frozen=True)
class _FeatureOverride:
    """Per-field overrides collected from a :func:`feature` call."""

    name: str | None = None
    dtype: LaminDType | ResolvedDType | None = None
    type: FeatureType | None = None
    synonyms: list[str] | None = None
    unit: str | None = None
    description: str | None = None
    nullable: bool | None = None
    optional: bool | None = None
    default_value: Any | None = None
    coerce: bool | None = None
    space_uid: SpaceUID | None = None


def feature(
    *,
    name: str | None = None,
    dtype: LaminDType | ResolvedDType | None = None,
    type: FeatureType | None = None,
    synonyms: list[str] | None = None,
    unit: str | None = None,
    description: str | None = None,
    nullable: bool | None = None,
    optional: bool | None = None,
    default_value: Any | None = None,
    coerce: bool | None = None,
    space_uid: SpaceUID | None = None,
) -> Any:
    """Declare per-field feature options on a :class:`SchemaModel` field.

    Assign the result to a `Feature[...]`-annotated field to override its
    name, dtype, feature type, or any other :class:`Feature` setting::

        organism: Feature[str] = feature(description="Species", nullable=True)

    Returns `Any` so it can be assigned to a `Feature[T]`-annotated field
    without a type error, mirroring pydantic's `Field()`.
    """
    if dtype is not None:
        require_dtype_wrapper(dtype)
    return _FeatureOverride(
        name=name,
        dtype=dtype,
        type=type,
        synonyms=synonyms,
        unit=unit,
        description=description,
        nullable=nullable,
        optional=optional,
        default_value=default_value,
        coerce=coerce,
        space_uid=space_uid,
    )


def _resolve_annotation(
    annotation: Any, globalns: dict[str, Any], localns: dict[str, Any]
) -> Any:
    """Resolve a possibly-stringized annotation to a runtime object.

    Under `from __future__ import annotations` every annotation is a string;
    it is evaluated against the frame in which the class is defined so that
    both module-level names (`Feature`, `datetime`) and function-local
    names (a locally-imported `lamindb as ln`) resolve.
    """
    if isinstance(annotation, str):
        return eval(annotation, globalns, localns)
    return annotation


def _unwrap_optional(hint: Any) -> tuple[Any, bool]:
    """Split a `X | None` union into its single non-`None` member and a flag.

    Returns `(inner_hint, True)` when `hint` is a union of exactly one
    non-`None` member and `None` (equivalently `Optional[X]`); otherwise
    `(hint, False)`.  A union with more than one non-`None` member is left
    untouched so the caller can reject it.

    Used twice while parsing a field annotation: on the whole annotation, where
    `Feature[...] | None` marks the feature *optional* (schema membership),
    and on the `Feature[<inner>]` argument, where `Feature[X | None]` marks
    the feature *nullable*.
    """
    if get_origin(hint) in (Union, types.UnionType):
        args = get_args(hint)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1 and len(non_none) != len(args):
            return non_none[0], True
    return hint, False


def _resolve_field_dtype(arg: Any) -> LaminDType | ResolvedDType:
    """Resolve a `Feature[<inner>]` argument into a dtype wrapper.

    The new annotation forms (:class:`~lamindb_setup.evolve.DType`,
    :class:`~lamindb_setup.evolve.ListDType` and
    :data:`~lamindb_setup.evolve.CatDType`) are resolved through
    :func:`~lamindb_setup.evolve.get_dtype`; every other (bare) type is
    handed to the legacy :class:`LaminDType` so existing schemas are unaffected.
    """
    if hasattr(arg, "__metadata__") or get_origin(arg) in (DType, ListDType):
        return get_dtype(arg)
    if get_origin(arg) is list:
        (inner,) = get_args(arg)
        if hasattr(inner, "__metadata__"):
            # `list[CatDType[...]]` -- spelled without the ListDType wrapper.
            return get_dtype(arg)
    return LaminDType(arg)


def _cat_filter_value(value: Any) -> Any:
    """Return a comparison-safe representation of a categorical filter value."""
    if isinstance(value, Promise):
        return ("promise", id(value.get_arg()))
    if isinstance(value, dict):
        return tuple(
            (key, _cat_filter_value(item)) for key, item in sorted(value.items())
        )
    if isinstance(value, (list, tuple)):
        return tuple(_cat_filter_value(item) for item in value)
    return value


def _dtype_signature(dtype: LaminDType | ResolvedDType) -> tuple[str | None, Any]:
    """Return the canonical dtype and categorical filters used for matching."""
    filters = getattr(dtype, "cat_filters", None) or {}
    return (
        canonical_dtype_str(dtype.dtype_str),
        tuple(
            (key, _cat_filter_value(value)) for key, value in sorted(filters.items())
        ),
    )


def _parse_feature_descriptions(doc: str | None) -> dict[str, str]:
    """Parse a numpy-style `Features` section into `name -> description`.

    A declarative class can document its fields with a numpy-style `Features`
    section in its docstring; the description text of each entry becomes the
    corresponding feature's `description` (unless overridden by an explicit
    `feature(description=...)`)::

        class Experiment(SchemaModel):
            \"\"\"An experiment.

            Features
            --------
            sample_id : Feature[str]
                The unique sample identifier.
            organism
                Species of the sample.
            \"\"\"

            sample_id: Feature[str]
            organism: Feature[str]

    Returns a mapping of field name to description; entries with no description
    body are omitted.  Comma-separated names on one entry line share the same
    description.  Returns an empty mapping when there is no `Features` section.
    """
    if not doc:
        return {}

    lines = inspect.cleandoc(doc).splitlines()
    n = len(lines)

    def _is_underline(idx: int) -> bool:
        return (
            0 <= idx < n
            and bool(lines[idx].strip())
            and set(lines[idx].strip()) == {"-"}
        )

    # Locate the `Features` header (a line `Features` underlined by dashes).
    start = None
    for i in range(n - 1):
        if lines[i].strip() == "Features" and _is_underline(i + 1):
            start = i + 2
            break
    if start is None:
        return {}

    descriptions: dict[str, str] = {}
    current_names: list[str] = []
    current_desc: list[str] = []

    def _flush() -> None:
        if not current_names:
            return
        body = current_desc[:]
        while body and not body[0].strip():
            body.pop(0)
        while body and not body[-1].strip():
            body.pop()
        text = " ".join(line.strip() for line in body if line.strip())
        if text:
            for name in current_names:
                descriptions[name] = text

    i = start
    while i < n:
        line = lines[i]
        # A following section header (`Header` underlined by dashes) ends the
        # `Features` section.
        if line.strip() and not line[:1].isspace() and _is_underline(i + 1):
            break
        if line[:1].isspace() or not line.strip():
            # Indented text (a description) or a blank line within the section.
            current_desc.append(line)
            i += 1
            continue
        # A new entry line at the section's base indentation: `name : type`
        # or just `name` (optionally several comma-separated names).
        _flush()
        name_part = line.split(":", 1)[0]
        current_names = [name.strip() for name in name_part.split(",") if name.strip()]
        current_desc = []
        i += 1
    _flush()

    return descriptions


def _build_features(
    kind: str,
    name: str,
    namespace: dict[str, Any],
    globalns: dict[str, Any],
    localns: dict[str, Any],
    default_type: FeatureType | None,
    default_space: SpaceUID | None,
) -> tuple[dict[str, Feature[Any]], list[str]]:
    """Turn a declarative class body's annotations into emitted features.

    Shared by :class:`SchemaModelMeta` and :class:`FeatureGroupMeta`.  Reads
    `Feature[...]` annotations (skipping `ClassVar`), applies any
    :func:`feature` overrides or reused :class:`Feature` objects, and returns a
    mapping of field name to emitted :class:`Feature` in declaration order plus
    the list of field names marked *optional*.  `default_type` /
    `default_space` are applied to fields that do not set their own; a reused
    :class:`Feature` keeps its own settings.  `kind` names the declaring
    surface (e.g. `"SchemaModel"`) for error messages.

    `Feature[X]` emits a non-nullable feature and `Feature[X | None]` a
    nullable one (`Feature.nullable`); the outer `Feature[...] | None` marks
    the field *optional* (schema membership).  An explicit
    `feature(nullable=...)` / `feature(optional=...)` wins over the
    corresponding annotation shorthand.

    A numpy-style `Features` section in the class docstring supplies feature
    descriptions; an explicit `feature(description=...)` wins over it, and a
    reused :class:`Feature` keeps its own description.
    """
    raw_annotations = namespace.get("__annotations__", {})
    descriptions = _parse_feature_descriptions(namespace.get("__doc__"))
    features: dict[str, Feature[Any]] = {}
    optional_fields: list[str] = []
    seen_names: dict[str, str] = {}

    for attr, raw in raw_annotations.items():
        hint = _resolve_annotation(raw, globalns, localns)
        origin = get_origin(hint)

        if origin is ClassVar or hint is ClassVar:
            continue

        # `Feature[...] | None` (the outer union) marks the field optional.
        hint, annotation_optional = _unwrap_optional(hint)
        origin = get_origin(hint)

        if origin is not Feature:
            raise TypeError(
                f"{kind} {name!r}: field {attr!r} must be annotated as "
                f"Feature[...] (or ClassVar[...] to opt out), got {raw!r}."
            )

        args = get_args(hint)
        if len(args) != 1:
            raise TypeError(
                f"{kind} {name!r}: field {attr!r} must be annotated as "
                f"Feature[<dtype>] with exactly one type argument."
            )
        # `Feature[X | None]` (the inner union) marks the feature nullable.
        inner, annotation_nullable = _unwrap_optional(args[0])
        generic_dtype = _resolve_field_dtype(inner)

        assigned = namespace.get(attr)

        # Reuse an existing Feature object verbatim, so it can be shared
        # across schemas (a single node in the dependency DAG).
        if isinstance(assigned, Feature):
            reused = assigned
            if _dtype_signature(reused.dtype) != _dtype_signature(generic_dtype):
                raise ValueError(
                    f"{kind} {name!r}: field {attr!r} declares "
                    f"Feature[{args[0]!r}] but the assigned feature "
                    f"{reused.name!r} has dtype {reused.dtype.dtype_str!r}; "
                    "the base dtypes must match, including categorical filters."
                )
            if reused.name != attr:
                raise ValueError(
                    f"{kind} {name!r}: field {attr!r} reuses feature "
                    f"{reused.name!r}; the field name must match the feature "
                    f"name."
                )
            if reused.name in seen_names:
                raise ValueError(
                    f"{kind} {name!r}: duplicate feature name "
                    f"{reused.name!r} declared by fields "
                    f"{seen_names[reused.name]!r} and {attr!r}."
                )
            seen_names[reused.name] = attr
            features[attr] = reused
            if annotation_optional:
                optional_fields.append(attr)
            continue

        override = assigned
        if override is not None and not isinstance(override, _FeatureOverride):
            raise TypeError(
                f"{kind} {name!r}: field {attr!r} may only be assigned "
                f"feature(...) or an existing Feature; got {override!r}."
            )

        dtype = generic_dtype
        if override is not None and override.dtype is not None:
            if _dtype_signature(override.dtype) != _dtype_signature(generic_dtype):
                raise ValueError(
                    f"{kind} {name!r}: field {attr!r} declares "
                    f"Feature[{args[0]!r}] but feature(dtype=...) resolves to "
                    f"{override.dtype.dtype_str!r}; the base dtypes must match, "
                    "including categorical filters."
                )
            dtype = override.dtype

        feat_name = override.name if override and override.name else attr
        if feat_name in seen_names:
            raise ValueError(
                f"{kind} {name!r}: duplicate feature name {feat_name!r} "
                f"declared by fields {seen_names[feat_name]!r} and {attr!r}."
            )
        seen_names[feat_name] = attr

        feat_type = (
            override.type if override and override.type is not None else default_type
        )
        feat_space = (
            override.space_uid
            if override and override.space_uid is not None
            else default_space
        )

        # The annotation is the source of truth for nullability: `Feature[X]`
        # says "an X", so the feature is *not* nullable, and `Feature[X | None]`
        # says "an X or nothing", so it is.  Leaving it unset instead would hand
        # the decision to lamindb, which fills an unset `nullable` with `True`
        # -- making a plain `Feature[X]` and a `Feature[X | None]` produce the
        # very same feature and the annotation shorthand a no-op.  An explicit
        # feature(nullable=...) still wins.
        nullable = (
            override.nullable
            if override and override.nullable is not None
            else annotation_nullable
        )

        # `Feature[...] | None` marks the field optional; an explicit
        # feature(optional=...) still wins.
        optional = (
            override.optional if override and override.optional is not None else None
        )
        if optional is None and annotation_optional:
            optional = True
        if optional:
            optional_fields.append(attr)

        # A numpy-style `Features` docstring entry supplies the description;
        # an explicit feature(description=...) still wins.
        description = (
            override.description
            if override and override.description is not None
            else descriptions.get(attr)
        )

        features[attr] = Feature(
            feat_name,
            dtype=dtype,
            type=feat_type,
            synonyms=override.synonyms if override else None,
            unit=override.unit if override else None,
            description=description,
            nullable=nullable,
            default_value=override.default_value if override else None,
            coerce=override.coerce if override else None,
            space_uid=feat_space,
        )

    if not features:
        raise ValueError(
            f"{kind} {name!r} must declare at least one Feature[...] field."
        )

    # A `Features` docstring entry that names no declared field is almost
    # certainly a typo; flag it rather than silently dropping the description.
    unknown_described = [d for d in descriptions if d not in features]
    if unknown_described:
        raise ValueError(
            f"{kind} {name!r}: the Features docstring section documents "
            f"{', '.join(repr(d) for d in unknown_described)}, which do not "
            f"match any declared field ({', '.join(features)})."
        )

    # A bare Feature value without a Feature[...] annotation would silently
    # be dropped; flag it so the field is declared explicitly.
    for key, value in namespace.items():
        if isinstance(value, Feature) and key not in raw_annotations:
            raise TypeError(
                f"{kind} {name!r}: attribute {key!r} is a Feature but is "
                f"not annotated as Feature[...]; add an annotation to declare "
                f"it as a field."
            )

    return features, optional_fields


class SchemaModelMeta(type):
    """Metaclass that turns a declarative class body into a :class:`Schema`."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> type:
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        base_cls = globals().get("SchemaModel")
        if base_cls is None:
            # Defining the `SchemaModel` base class itself; nothing to build.
            return cls

        model_bases = [b for b in bases if isinstance(b, SchemaModelMeta)]
        if not model_bases:
            return cls
        if any(b is not base_cls for b in model_bases):
            raise NotImplementedError(
                "Subclassing a SchemaModel subclass is not yet supported "
                "(SchemaModel field inheritance is on the TODO list)."
            )

        # The frame that executes the `class` statement (e.g. a plan's
        # `run()` body); used to resolve function-local annotation names.
        frame = sys._getframe(1)
        globalns = frame.f_globals
        localns = frame.f_locals

        config = mcs._read_config(name, namespace)
        default_type = config.get("default_feature_type")
        default_space = config.get("default_feature_space")

        if "schema" in namespace.get("__annotations__", {}):
            raise ValueError(
                f"SchemaModel {name!r}: 'schema' is a reserved attribute "
                "and may not be used as a field name."
            )

        features, optional_fields = _build_features(
            "SchemaModel",
            name,
            namespace,
            globalns,
            localns,
            default_type,
            default_space,
        )

        index_attr = config.get("index")
        index_feature: Feature[Any] | None = None
        if index_attr is not None:
            if index_attr not in features:
                raise ValueError(
                    f"SchemaModel {name!r}: index {index_attr!r} does not match any "
                    f"declared field ({', '.join(features)})."
                )
            if index_attr in optional_fields:
                raise ValueError(
                    f"SchemaModel {name!r}: index {index_attr!r} may not be "
                    f"optional; remove the '| None' (or feature(optional=...)) "
                    f"from the index field."
                )
            index_feature = features[index_attr]

        schema_kwargs: dict[str, Any] = {
            "features": list(features.values()),
            "index": index_feature,
            "optionals": [features[f] for f in optional_fields],
        }
        if config.get("name") is not None:
            schema_kwargs["name"] = config["name"]
        if config.get("type") is not None:
            schema_kwargs["type"] = config["type"]
        if config.get("description") is not None:
            schema_kwargs["description"] = config["description"]
        if config.get("space_uid") is not None:
            schema_kwargs["space_uid"] = config["space_uid"]
        if config.get("coerce") is not None:
            schema_kwargs["coerce"] = config["coerce"]
        if "minimal_set" in config:
            schema_kwargs["minimal_set"] = config["minimal_set"]
        if "ordered_set" in config:
            schema_kwargs["ordered_set"] = config["ordered_set"]
        if "maximal_set" in config:
            schema_kwargs["maximal_set"] = config["maximal_set"]

        schema = Schema(**schema_kwargs)

        # Expose each field as its emitted Feature and attach the built schema.
        for attr, feat in features.items():
            setattr(cls, attr, feat)
        setattr(cls, "schema", schema)
        return cls

    @staticmethod
    def _read_config(name: str, namespace: dict[str, Any]) -> SchemaConfig:
        """Read and validate the `schema_config` from a class body."""
        config = namespace.get("schema_config", {})
        if not isinstance(config, dict):
            raise TypeError(
                f"SchemaModel {name!r}: schema_config must be a SchemaConfig "
                f"(dict), got {type(config).__name__}."
            )
        unknown = set(config) - _ALLOWED_CONFIG_KEYS
        if unknown:
            raise TypeError(
                f"SchemaModel {name!r}: unknown schema_config keys "
                f"{sorted(map(str, unknown))}; allowed keys are {sorted(_ALLOWED_CONFIG_KEYS)}."
            )
        return config  # type: ignore[return-value]


class SchemaModel(metaclass=SchemaModelMeta):
    """Declarative base class for schemas (see the module docstring)."""

    schema: ClassVar[Schema]


class FeatureGroupMeta(type):
    """Metaclass that turns a declarative class body into a :class:`FeatureType`.

    Unlike :class:`SchemaModelMeta`, no :class:`Schema` is emitted: the class is
    a *feature-type namespace* whose annotated `Feature[...]` fields become
    features typed by the group's single :class:`FeatureType`.
    """

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> type:
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        base_cls = globals().get("FeatureGroup")
        if base_cls is None:
            # Defining the `FeatureGroup` base class itself; nothing to build.
            return cls

        group_bases = [b for b in bases if isinstance(b, FeatureGroupMeta)]
        if not group_bases:
            return cls
        if any(b is not base_cls for b in group_bases):
            raise NotImplementedError(
                "Subclassing a FeatureGroup subclass is not yet supported "
                "(FeatureGroup field inheritance is on the TODO list)."
            )

        # The frame that executes the `class` statement (e.g. a plan's
        # `run()` body); used to resolve function-local annotation names.
        frame = sys._getframe(1)
        globalns = frame.f_globals
        localns = frame.f_locals

        config = mcs._read_config(name, namespace)
        default_space = config.get("default_feature_space")

        if "feature_type" in namespace.get("__annotations__", {}):
            raise ValueError(
                f"FeatureGroup {name!r}: 'feature_type' is a reserved attribute "
                f"and may not be used as a field name."
            )

        # Construct the group's FeatureType first, so it is recorded as a
        # dependency of every feature that references it via `type=`.
        raw_type = config["feature_type"]
        feature_type = FeatureType(raw_type) if isinstance(raw_type, str) else raw_type

        features, optional_fields = _build_features(
            "FeatureGroup",
            name,
            namespace,
            globalns,
            localns,
            feature_type,
            default_space,
        )

        # `optional` is a schema-membership property; a FeatureGroup emits no
        # schema, so there is nothing for a field to be optional in.
        if optional_fields:
            raise ValueError(
                f"FeatureGroup {name!r}: field(s) "
                f"{', '.join(repr(f) for f in optional_fields)} are marked "
                f"optional (via 'Feature[...] | None' or feature(optional=...)), "
                f"but optionality only applies to a SchemaModel's schema, not a "
                f"FeatureGroup. Use 'Feature[<dtype> | None]' for a nullable "
                f"feature instead."
            )

        # Expose each field as its emitted Feature and attach the group's type.
        for attr, feat in features.items():
            setattr(cls, attr, feat)
        setattr(cls, "feature_type", feature_type)
        setattr(cls, "_features", features)
        return cls

    @staticmethod
    def _read_config(name: str, namespace: dict[str, Any]) -> GroupConfig:
        """Read and validate the `group_config` from a class body."""
        config = namespace.get("group_config", {})
        if not isinstance(config, dict):
            raise TypeError(
                f"FeatureGroup {name!r}: group_config must be a GroupConfig "
                f"(dict), got {type(config).__name__}."
            )
        unknown = set(config) - _ALLOWED_GROUP_CONFIG_KEYS
        if unknown:
            raise TypeError(
                f"FeatureGroup {name!r}: unknown group_config keys "
                f"{sorted(map(str, unknown))}; allowed keys are "
                f"{sorted(_ALLOWED_GROUP_CONFIG_KEYS)}."
            )
        if config.get("feature_type") is None:
            raise ValueError(
                f"FeatureGroup {name!r}: group_config must set 'feature_type' "
                f"(a FeatureType or a str)."
            )
        return config  # type: ignore[return-value]


class FeatureGroup(metaclass=FeatureGroupMeta):
    """Declarative base class for a group of features sharing a feature type.

    Annotate each field as `Feature[<dtype>]`; every field becomes a
    :class:`Feature` typed by the group's :class:`FeatureType` (set via
    `group_config`).  No :class:`Schema` is emitted -- use a
    :class:`SchemaModel` for that.  Declaring the class inside a plan body
    registers the emitted :class:`FeatureType` and features with the active
    plan::

        class ExperimentFeatures(FeatureGroup):
            group_config = GroupConfig(feature_type="experiment_feature")

            sample_id: Feature[str]
            organism: Feature[str] = feature(description="Species")

    Each field is exposed as its emitted :class:`Feature` on the class
    (`ExperimentFeatures.sample_id`), and `ExperimentFeatures.feature_type`
    is the emitted :class:`FeatureType`.  These features are reusable: assign
    one to a :class:`SchemaModel` field to share the exact same object.
    """

    feature_type: ClassVar[FeatureType]
    _features: ClassVar[dict[str, Feature[Any]]]
