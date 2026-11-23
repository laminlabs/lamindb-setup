# Source behavior retained; host-repository lint exceptions.
# ruff: noqa: TC001
"""Thin lamindb-independent wrapper around lamindb's Schema system, with better type checking and error messages."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal, Self

from ..core import (
    Materializable,
    MaterializeContext,
    Promise,
    SchemaUID,
    SpaceUID,
)
from ._dtypes import _resolve_dtype_spec, _to_dtype_spec
from ._features import Feature, LaminDType

if TYPE_CHECKING:
    import lamindb
    import lamindb as ln


__all__ = [
    "SchemaType",
    "Schema",
]

# The object types allowed for composite schemas in schema plans.
# NOTE: we need to extend these eventually when we support more internal file formats with custom curators
OTypes = Literal[
    "DataFrame",
    "AnnData",
    "MuData",
    "SpatialData",
]


# Slots addressing a table: a DataFrame, or an AnnData's `obs`/`var`.
_TABULAR_SLOTS = frozenset({"obs", "var", "var.T"})

# The slot validating the metadata passed as `Artifact(..., features=...)`
# rather than the dataset's content; every curator accepts it.
EXTERNAL_SLOT = "__external__"

# The slot forms each curator in `lamindb.curators.core` resolves, quoted here
# for error messages.  `tests/evolve/authoring/test_schemas_slot_keys.py` drives the real
# curators to keep this in step with lamindb as it is upgraded.
_SLOT_FORMS: dict[str, tuple[str, ...]] = {
    "DataFrame": ("attrs", "attrs:<key>[:<key>...]", EXTERNAL_SLOT),
    "AnnData": ("obs", "var", "var.T", "uns", "uns:<key>[:<key>...]", EXTERNAL_SLOT),
    "MuData": (
        "obs",
        "var",
        "var.T",
        "uns",
        "uns:<key>[:<key>...]",
        "<modality>:obs",
        "<modality>:var",
        "<modality>:var.T",
        "<modality>:uns",
        "<modality>:uns:<key>[:<key>...]",
        EXTERNAL_SLOT,
    ),
    "SpatialData": (
        "attrs",
        "attrs:<key>[:<key>...]",
        "tables:<table>:obs",
        "tables:<table>:var",
        "tables:<table>:var.T",
        EXTERNAL_SLOT,
    ),
}


def _is_dict_path(key: str, root: str) -> bool:
    """Whether `key` walks the `root` dict: `root` or `root:<key>[:<key>...]`."""
    head, sep, rest = key.partition(":")
    if head != root:
        return False
    return not sep or all(rest.split(":"))


def _is_valid_slot_key(otype: OTypes, key: str) -> bool:
    if key == EXTERNAL_SLOT:
        return otype in _SLOT_FORMS
    if otype == "DataFrame":
        return _is_dict_path(key, "attrs")
    if otype == "AnnData":
        return key in _TABULAR_SLOTS or _is_dict_path(key, "uns")
    if otype == "MuData":
        # unprefixed slots address the MuData object itself, as in lamindb's own
        # example (https://docs.lamin.ai/curate#mudata), which pairs a global
        # `obs` with per-modality `rna:obs`.
        if key in _TABULAR_SLOTS or _is_dict_path(key, "uns"):
            return True
        modality, sep, rest = key.partition(":")
        if not sep or not modality:
            return False
        return rest in _TABULAR_SLOTS or _is_dict_path(rest, "uns")
    if otype == "SpatialData":
        if _is_dict_path(key, "attrs"):
            return True
        # A bare `<key>` is also read off `sdata.attrs`, but only as a
        # deprecated fallback that lamindb warns about; rejecting it keeps a
        # typo'd slot from silently passing as an attrs lookup.
        parts = key.split(":")
        return (
            len(parts) == 3
            and parts[0] == "tables"
            and bool(parts[1])
            and parts[2] in _TABULAR_SLOTS
        )
    return False


def validate_slot_keys(otype: OTypes, slots: dict[str, Schema]) -> None:
    """Validate that `slots` keys are accepted by the curator for `otype`.

    Static slot names are checked exactly; dynamic segments (modality names,
    table keys, and nested `uns`/`attrs` dict paths) are accepted as-is.
    """
    if not isinstance(slots, dict):
        raise ValueError("Slots must be a dictionary.")

    for key in slots:
        if not _is_valid_slot_key(otype, key):
            forms = ", ".join(repr(f) for f in _SLOT_FORMS.get(otype, ()))
            raise ValueError(
                f"Slot key {key!r} is not valid for otype {otype!r}. "
                f"Expected one of: {forms}."
            )


# lamindb's format constraints (`ln.Schema.formats`): per format, the otype
# whose curator enforces it and the checks it knows.  lamindb silently ignores
# an unknown check, so a typo would disable the check it meant to enable.
_FORMATS: dict[str, tuple[str, frozenset[str]]] = {
    "zarr": (
        "SpatialData",
        frozenset({"zarr_format", "chunk_shape", "multiscales", "layers"}),
    ),
}


def _normalize_formats(
    name: str | None, otype: str | None, formats: dict[str, Any] | None
) -> dict[str, Any] | None:
    """Check format and check names; return the values as canonical JSON."""
    if not formats:
        return None
    for fmt, spec in formats.items():
        if fmt not in _FORMATS:
            raise ValueError(
                f"Schema {name!r}: unknown format {fmt!r}, expected one of "
                f"{sorted(_FORMATS)}."
            )
        fmt_otype, checks = _FORMATS[fmt]
        if otype != fmt_otype:
            raise ValueError(
                f"Schema {name!r}: formats[{fmt!r}] is only enforced for "
                f"otype={fmt_otype!r}, got otype={otype!r}."
            )
        if unknown := set(spec) - checks:
            raise ValueError(
                f"Schema {name!r}: unknown check(s) {sorted(unknown)} in "
                f"formats[{fmt!r}], expected one of {sorted(checks)}."
            )
    # lamindb reads `_aux` back as JSON, so a plan's tuples must become lists
    # to compare equal to it
    return json.loads(json.dumps(formats))


class SchemaType(Materializable):
    """Declare a type that groups related schemas.

    A schema type classifies schemas; it does not define validation rules.
    Use `type` for a parent type and `space_uid` to select its space.
    """

    def __init__(
        self,
        name: str,
        *,
        description: str | None = None,
        type: SchemaType | None = None,
        space_uid: SpaceUID | None = None,
    ) -> None:
        self.name = name
        self.description = description
        self.type = type
        self.space_uid = space_uid

    def __enter__(self) -> SchemaType:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None

    def get(self, ctx: MaterializeContext) -> ln.Schema | None:
        """Read-only lookup of the materialized schema type."""
        import lamindb as ln

        return ctx.find_typed(ln.Schema, self, name=self.name, is_type=True)

    def stage(self, ctx: MaterializeContext) -> ln.Schema:
        """Build the unsaved LaminDB Schema for this schema type."""
        import lamindb as ln

        parent_type = self.type.materialize(ctx) if self.type else None
        return ln.Schema(
            name=self.name, description=self.description, is_type=True, type=parent_type
        )

    @property
    def uid(self) -> Promise[SchemaUID, Self]:
        """Get a promise for the UID of the materialized schema type."""
        return Promise(
            self,
            lambda s, ctx: SchemaUID(s.materialize(ctx).uid),
            label=f"{type(self).__name__}({self.name!r}).uid",
        )


class Schema(Materializable):
    """Declare validation rules for a dataset or a sheet's records.

    Args:
        name: Schema name within its type and space.
        features: Member features; pass an empty list for a composite or dynamic schema.
        index: Feature that identifies rows. It cannot be optional.
        optionals: Members that may be absent, using objects from `features`.
        description: Human-readable description.
        type: Optional schema type.
        otype: Dataset object type for a composite schema.
        slots: Child schemas keyed by dataset component, such as `obs` and `var`.
        formats: Storage-format constraints checked during curation.
        itype: Registry or registry field for dynamic members.
        dtype: Shared dtype for dynamic members.
        flexible: Include additional features of the same `itype` in validation and annotation.
        coerce: Whether validation may coerce values to the dtype.
        minimal_set: Require all non-optional members.
        ordered_set: Require the declared member order.
        maximal_set: Reject members outside the schema.
        space_uid: Target space; otherwise use the plan's default.
    """

    def __init__(
        self,
        name: str | None = None,
        *,
        features: list[Feature],
        index: Feature | None = None,
        optionals: list[Feature] | None = None,
        # general schema settings
        description: str | None = None,
        type: SchemaType | None = None,
        # composite schema settings
        otype: OTypes | None = None,
        slots: dict[str, Schema] | None = None,
        # format constraints checked on curation, e.g. {"zarr": {...}}
        formats: dict[str, dict[str, Any]] | None = None,
        # dynamic schema settings
        itype: type[lamindb.models.SQLRecord]
        | lamindb.base.types.FieldAttr
        | None = None,
        dtype: LaminDType | None = None,
        flexible: bool | None = None,
        coerce: bool | None = None,
        minimal_set: bool = True,
        ordered_set: bool = False,
        maximal_set: bool = False,
        space_uid: SpaceUID | None = None,
    ) -> None:
        # at least one of features, slots, or itype must be provided;
        # pass an empty features list to opt out of features
        if not features and not slots and itype is None:
            raise ValueError("Please pass features or slots or itype.")
        # composite schemas require an otype to define their structure -- except
        # an external-only one, which constrains no dataset content and so
        # validates the external metadata of any artifact (lamindb >= 2.12)
        external_only = (
            slots is not None
            and set(slots) == {EXTERNAL_SLOT}
            and not features
            and index is None
            and itype is None
        )
        if slots and otype is None and not external_only:
            raise ValueError(
                "Please pass otype != None for composite schemas. Only a schema "
                f"whose sole slot is {EXTERNAL_SLOT!r}, with no features, index "
                "or itype, may omit it."
            )
        # validate the slot keys against the curator rules for the otype
        if slots and otype is not None:
            validate_slot_keys(otype, slots)
        # `stage` marks optional members while walking `features`, and drops
        # the index from that walk because lamindb re-inserts it itself.  An
        # optional outside that set is therefore silently discarded on save,
        # while the plan keeps declaring it -- so every apply re-reports it as a
        # missing optional and the plan never converges.  Reject it up front.
        if optionals:
            member_ids = {id(f) for f in features}
            stray = sorted(f.name for f in optionals if id(f) not in member_ids)
            if stray:
                raise ValueError(
                    f"Schema {name!r}: optionals {stray} are not among its "
                    "features; an optional must be one of the Feature objects "
                    "passed to 'features'."
                )
            if index is not None and any(f is index for f in optionals):
                raise ValueError(
                    f"Schema {name!r}: the index feature {index.name!r} cannot "
                    "be optional."
                )

        self.features = features
        self.name = name
        self.description = description
        self.type = type
        # `itype` (a registry class or field) and `dtype` (a categorical
        # registry via `LaminDType`) are stored without a live class object:
        # the itype is captured as an FQN-based spec and the LaminDType already
        # resolves its class on demand, so neither goes stale across an
        # in-process instance reconnect.  Both are re-resolved at `stage` time.
        self._itype_spec = _to_dtype_spec(itype) if itype is not None else None
        self.otype = otype
        self._dtype_wrapper = dtype
        self.flexible = flexible
        self.coerce = coerce
        self.minimal_set = minimal_set
        self.ordered_set = ordered_set
        self.maximal_set = maximal_set
        self.slots = slots
        self.formats = _normalize_formats(name, otype, formats)
        self.index = index
        self.optionals = optionals or []
        self.space_uid = space_uid

    @property
    def itype(self) -> Any:
        """The schema's `itype`, re-resolved against the connected instance."""
        if self._itype_spec is None:
            return None
        return _resolve_dtype_spec(self._itype_spec)

    @property
    def dtype(self) -> Any:
        """The schema's `dtype`, re-resolved against the connected instance."""
        if self._dtype_wrapper is None:
            return None
        return self._dtype_wrapper.dtype

    def get(self, ctx: MaterializeContext) -> ln.Schema | None:
        """Read-only lookup of the materialized schema.

        Named schemas are matched by name; unnamed schemas are matched only when
        a uid is recorded in the lock (or pinned by another instance's lock),
        since LaminDB addresses them by content hash and dedups them on save.
        """
        import lamindb as ln

        if (
            self.name is None
            and ctx.locked_uid_of(self) is None
            and ctx.pinned_uid_of(self) is None
        ):
            return None
        return ctx.find_typed(ln.Schema, self, name=self.name, is_type=False)

    def stage(self, ctx: MaterializeContext) -> ln.Schema:
        """Build the unsaved LaminDB Schema for this schema."""
        import lamindb as ln

        schema_type = self.type.materialize(ctx) if self.type else None
        # lamindb re-inserts the index feature into the schema's feature list
        # (Schema.__init__ does `features.insert(0, index)`), so passing the
        # index in `features` as well would link it twice and violate the
        # SchemaFeature unique constraint. Drop it here and let lamindb add it.
        staged_features = (
            [f for f in self.features if f is not self.index]
            if self.index is not None
            else self.features
        )
        optional_ids = {id(f) for f in self.optionals}
        materialized_features = [
            (
                f.materialize(ctx).with_config(optional=True)
                if id(f) in optional_ids
                else f.materialize(ctx)
            )
            for f in staged_features
        ]
        materialized_index = self.index.materialize(ctx) if self.index else None
        materialized_slots: dict[str, ln.Schema] | None = (
            {k: v.materialize(ctx) for k, v in self.slots.items()}
            if self.slots
            else None
        )

        kwargs: dict[str, Any] = {}
        if self.name is not None:
            kwargs["name"] = self.name
        if self.description is not None:
            kwargs["description"] = self.description
        # see Feature.stage: omitting `type` widens lamindb's dedup to every
        # type context, an explicit None keeps it at the untyped ones.
        kwargs["type"] = schema_type
        if self.itype is not None:
            kwargs["itype"] = self.itype
        if self.dtype is not None:
            kwargs["dtype"] = self.dtype
        if self.otype is not None:
            kwargs["otype"] = self.otype
        if self.flexible is not None:
            kwargs["flexible"] = self.flexible
        if self.coerce is not None:
            kwargs["coerce"] = self.coerce
        if materialized_index is not None:
            kwargs["index"] = materialized_index
        if materialized_slots:
            kwargs["slots"] = materialized_slots
        kwargs["minimal_set"] = self.minimal_set
        kwargs["ordered_set"] = self.ordered_set
        kwargs["maximal_set"] = self.maximal_set

        schema = ln.Schema(
            materialized_features,
            **kwargs,
        )
        if self.formats:
            schema.formats.zarr = self.formats.get("zarr")
        return schema

    @property
    def uid(self) -> Promise[SchemaUID, Self]:
        """Get a promise for the UID of the materialized schema."""
        return Promise(
            self,
            lambda s, ctx: SchemaUID(s.materialize(ctx).uid),
            label=f"{type(self).__name__}({self.name!r}).uid",
        )
