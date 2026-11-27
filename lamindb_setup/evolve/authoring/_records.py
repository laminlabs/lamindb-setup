"""Thin lamindb-independent wrapper around lamindb's Record system, with better type checking and error messages."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self

from ..core import (
    Materializable,
    MaterializeContext,
    Promise,
    RecordUID,
    SpaceUID,
)
from ._features import Feature
from ._schemas import Schema

if TYPE_CHECKING:
    import lamindb as ln


__all__ = [
    "RecordType",
    "Sheet",
    "Record",
]


def _is_str_dtype(dtype: Any) -> bool:
    """Return whether `dtype` is lamindb's plain `str` dtype.

    Checked without importing lamindb: :class:`~.dtypes.ResolvedDType` exposes the
    canonical dtype string (`"str"`) while :class:`~.dtypes.LaminDType` exposes
    the underlying Python type (`str`).
    """
    inner = getattr(dtype, "dtype", dtype)
    return inner is str or inner == "str"


def _describe_dtype(dtype: Any) -> str:
    """Compact, lamindb-free description of a dtype for error messages."""
    inner = getattr(dtype, "dtype", dtype)
    return getattr(inner, "__name__", None) or str(inner)


class RecordType(Materializable):
    """Declare a type that classifies records (`is_type=True`).

    Use `type` for a parent type and `space_uid` for the type's own space.
    `single_space_uid` restricts its records to one space. To also constrain
    their feature values, declare a :class:`Sheet`.
    """

    def __init__(
        self,
        name: str,
        *,
        description: str | None = None,
        type: RecordType | None = None,
        space_uid: SpaceUID | None = None,
        single_space_uid: SpaceUID | None = None,
        exclude_children_from_plans: bool = False,
    ) -> None:
        self.name = name
        self.description = description
        self.type = type
        self.space_uid = space_uid
        self.single_space_uid = single_space_uid
        self.exclude_children_from_plans = exclude_children_from_plans

    def __enter__(self) -> RecordType:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None

    def get(self, ctx: MaterializeContext) -> ln.Record | None:
        """Read-only lookup of the materialized record type."""
        import lamindb as ln

        return ctx.find_typed(ln.Record, self, name=self.name, is_type=True)

    def stage(self, ctx: MaterializeContext) -> ln.Record:
        """Build the unsaved LaminDB Record for this record type."""
        import lamindb as ln

        parent_type = self.type.materialize(ctx) if self.type else None
        return ln.Record(
            name=self.name,
            description=self.description,
            is_type=True,
            type=parent_type,
        )

    def _post_save(self, ctx: MaterializeContext, record: ln.Record) -> None:
        """Apply settings that require a saved record (`is_type=True` only).

        `single_space` is stored in `_aux["ss"]` and `isExperimentSheet` in
        `_aux`; both require the record (and its space) to be persisted first,
        so they are applied here rather than in :meth:`stage`.  The
        `_excludeChildrenFromPlans` aux flag is a definite desired state: it is
        set to `1` when `exclude_children_from_plans` is on and removed
        entirely when it is off.
        """
        changed = False
        if self.single_space_uid is not None:
            record.settings.single_space = ctx.resolve_space_uid(self.single_space_uid)
            changed = True
        if getattr(self, "is_experiment_sheet", False):
            record._aux = record._aux or {}
            record._aux["isExperimentSheet"] = True
            changed = True
        if self.exclude_children_from_plans:
            record._aux = record._aux or {}
            if record._aux.get("_excludeChildrenFromPlans") != 1:
                record._aux["_excludeChildrenFromPlans"] = 1
                changed = True
        else:
            aux = getattr(record, "_aux", None)
            if aux and "_excludeChildrenFromPlans" in aux:
                del aux["_excludeChildrenFromPlans"]
                changed = True
        if changed:
            record.save()

    @property
    def uid(self) -> Promise[RecordUID, Self]:
        """Get a promise for the UID of the materialized record type."""
        return Promise(
            self,
            lambda s, ctx: RecordUID(s.materialize(ctx).uid),
            label=f"{type(self).__name__}({self.name!r}).uid",
        )


class Sheet(RecordType):
    """Declare a record type whose records must satisfy `schema`.

    The schema must be a :class:`Schema`, not a schema type. Its index, if set,
    must have dtype `str` because index values are stored in `Record.name`.
    Other settings follow :class:`RecordType`.
    """

    def __init__(
        self,
        name: str,
        *,
        schema: Schema,
        description: str | None = None,
        type: RecordType | None = None,
        space_uid: SpaceUID | None = None,
        single_space_uid: SpaceUID | None = None,
        is_experiment_sheet: bool = False,
        exclude_children_from_plans: bool = False,
    ) -> None:
        super().__init__(
            name,
            description=description,
            type=type,
            space_uid=space_uid,
            single_space_uid=single_space_uid,
            exclude_children_from_plans=exclude_children_from_plans,
        )
        # `SchemaType` is also a valid `Record.schema` target for lamindb, but
        # a schema type declares no features, so it constrains nothing.
        if not isinstance(schema, Schema):
            raise TypeError(
                f"Sheet {name!r}: 'schema' must be a Schema, got "
                f"{schema.__class__.__name__}. A sheet is constrained by a "
                "schema; a schema type only groups schemas and declares no "
                "features."
            )
        # A record sheet stores its index feature's value on `Record.name`, so
        # lamindb rejects a non-str index when the record type is saved.  Other
        # schemas (DataFrame/AnnData curation) may index on any dtype.
        index = schema.index
        if index is not None and not _is_str_dtype(index.dtype):
            raise ValueError(
                f"Sheet {name!r}: schema index feature {index.name!r} must have "
                f"dtype 'str' because a record sheet stores it on Record.name, "
                f"got {_describe_dtype(index.dtype)!r}."
            )
        self.schema = schema
        self.is_experiment_sheet = is_experiment_sheet

    def stage(self, ctx: MaterializeContext) -> ln.Record:
        """Build the unsaved LaminDB Record for this sheet, with its schema."""
        import lamindb as ln

        parent_type = self.type.materialize(ctx) if self.type else None
        return ln.Record(
            name=self.name,
            description=self.description,
            is_type=True,
            type=parent_type,
            schema=self.schema.materialize(ctx),
        )


class Record(Materializable):
    """Seed a named record, optionally with a type and feature values.

    `features` maps feature declarations or names to values. Existing records
    are left unchanged when the plan is reapplied, and removing a declaration
    does not delete the record. Declare an explicit data migration to update
    or remove it.
    """

    def __init__(
        self,
        name: str | None = None,
        *,
        type: RecordType | None = None,
        features: dict[str | Feature, Any] | None = None,
        description: str | None = None,
        reference: str | None = None,
        reference_type: str | None = None,
        space_uid: SpaceUID | None = None,
    ) -> None:
        self.name = name
        self.type = type
        self.features = features
        self.description = description
        self.reference = reference
        self.reference_type = reference_type
        self.space_uid = space_uid

    def get(self, ctx: MaterializeContext) -> ln.Record | None:
        """Read-only lookup of the materialized record.

        Records are seeded. The lookup tries the UID from this instance's lock
        or another instance's lock, then matches by name and parent type.
        Finalizing the plan checks that a name was provided.
        """
        import lamindb as ln

        return ctx.find_typed(ln.Record, self, name=self.name, is_type=False)

    def stage(self, ctx: MaterializeContext) -> ln.Record:
        """Build the unsaved LaminDB Record for this record."""
        import lamindb as ln

        record_type = self.type.materialize(ctx) if self.type else None
        materialized_features: dict[str | ln.Feature, Any] | None = (
            {
                (k.materialize(ctx) if isinstance(k, Feature) else k): v
                for k, v in self.features.items()
            }
            if self.features
            else None
        )

        kwargs: dict[str, Any] = {}
        if self.name is not None:
            kwargs["name"] = self.name
        # see Feature.stage: omitting `type` widens lamindb's dedup to every
        # type context, an explicit None keeps it at the untyped ones.
        kwargs["type"] = record_type
        if materialized_features is not None:
            kwargs["features"] = materialized_features
        if self.description is not None:
            kwargs["description"] = self.description
        if self.reference is not None:
            kwargs["reference"] = self.reference
        if self.reference_type is not None:
            kwargs["reference_type"] = self.reference_type

        return ln.Record(**kwargs)

    @property
    def uid(self) -> Promise[RecordUID, Self]:
        """Get a promise for the UID of the materialized record."""
        return Promise(
            self,
            lambda s, ctx: RecordUID(s.materialize(ctx).uid),
            label=f"{type(self).__name__}({self.name!r}).uid",
        )
