"""Thin lamindb-independent wrapper around lamindb's Feature system, with better type checking and error messages."""

from __future__ import annotations

from typing import Any, Generic

from typing_extensions import TypeVar

T = TypeVar("T")

import logging
import re
from contextlib import contextmanager
from typing import TYPE_CHECKING, Self

from ..core import (
    FeatureUID,
    Materializable,
    MaterializeContext,
    Promise,
    SpaceUID,
)
from ._dtypes import (
    LaminDType,
    ResolvedDType,
    require_dtype_wrapper,
    resolved_dtype_str,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    import lamindb as ln


__all__ = [
    "FeatureType",
    "Feature",
]


# What `ln.Feature(dtype=<str>)` logs for every dtype other than "num", "path"
# and "url" (`lamindb.models.feature.process_init_feature_arguments`).
_DTYPE_STRING_HINT = re.compile(
    r"rather than passing a string '.*' to dtype, consider passing a Python object"
)


class _DTypeStringHintFilter(logging.Filter):
    """Drop lamindb's dtype-string hint and let every other record through."""

    def filter(self, record: logging.LogRecord) -> bool:
        return _DTYPE_STRING_HINT.fullmatch(record.getMessage().strip()) is None


@contextmanager
def _without_dtype_string_hint() -> Iterator[None]:
    """Silence lamindb's "pass a Python object" hint for the duration of the block.

    `Feature.stage` hands lamindb the dtype as a string because that is the one
    form every dtype has -- a categorical's `cat[Record[<uid>]]` has no Python
    object short of loading the record -- and the string path is the one that
    validates it (`parse_dtype(..., check_exists=True)`).  The hint names a
    string the plan author never wrote, once per feature, so it is noise; any
    other message lamindb logs inside the block is still printed.
    """
    from lamindb_setup import logger

    hint_filter = _DTypeStringHintFilter()
    logger.addFilter(hint_filter)
    try:
        yield
    finally:
        logger.removeFilter(hint_filter)


class FeatureType(Materializable):
    """Declare a type that groups related features.

    Args:
        name: Type name within its parent type and space.
        type: Optional parent feature type.
        space_uid: Target space; otherwise use the plan's default.
    """

    def __init__(
        self,
        name: str,
        *,
        type: FeatureType | None = None,
        space_uid: SpaceUID | None = None,
    ) -> None:
        self.name = name
        self.type = type
        self.space_uid = space_uid

    def __enter__(self) -> FeatureType:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None

    def get(self, ctx: MaterializeContext) -> ln.Feature | None:
        """Read-only lookup of the materialized feature type."""
        import lamindb as ln

        return ctx.find_typed(ln.Feature, self, name=self.name, is_type=True)

    def stage(self, ctx: MaterializeContext) -> ln.Feature:
        """Build the unsaved LaminDB Feature for this feature type."""
        import lamindb as ln

        parent_type = self.type.materialize(ctx) if self.type else None
        _dt: Any = None  # HACK: typing hack because Feature types don't have a dtype
        return ln.Feature(name=self.name, dtype=_dt, is_type=True, type=parent_type)

    @property
    def uid(self) -> Promise[FeatureUID, Self]:
        """Get a promise for the UID of the materialized feature type."""
        return Promise(
            self,
            lambda s, ctx: FeatureUID(s.materialize(ctx).uid),
            label=f"{type(self).__name__}({self.name!r}).uid",
        )


class Feature(Materializable, Generic[T]):
    """Declare a feature and its validation settings.

    Creating a feature inside a running plan registers it without saving it.
    Use `LaminDType(float)`, for example, to declare numeric measurements.

    Args:
        name: Feature name within its type and space.
        dtype: A `LaminDType` or resolved dtype specification.
        type: Optional feature type.
        synonyms: Alternative names.
        unit: Unit of measurement.
        description: Human-readable description.
        nullable: Whether values may be null. `None` leaves the setting unspecified.
        default_value: Default used during validation.
        coerce: Whether validation may coerce values to the dtype.
        space_uid: Target space; otherwise use the plan's default.
    """

    def __init__(
        self,
        name: str,
        *,
        dtype: LaminDType[T] | ResolvedDType,
        type: FeatureType | None = None,
        synonyms: list[str] | None = None,
        unit: str | None = None,
        description: str | None = None,
        nullable: bool | None = None,
        default_value: Any | None = None,
        coerce: bool | None = None,
        space_uid: SpaceUID | None = None,
    ) -> None:
        require_dtype_wrapper(dtype)
        self.name = name
        self.dtype = dtype
        self.type = type
        self.synonyms = synonyms or []
        self.unit = unit
        self.description = description
        self.nullable = nullable
        self.default_value = default_value
        self.coerce = coerce
        self.space_uid = space_uid

    def get(self, ctx: MaterializeContext) -> ln.Feature | None:
        """Read-only lookup of the materialized feature."""
        import lamindb as ln

        return ctx.find_typed(ln.Feature, self, name=self.name, is_type=False)

    def stage(self, ctx: MaterializeContext) -> ln.Feature:
        """Build the unsaved LaminDB Feature for this feature."""
        import lamindb as ln

        feature_type = self.type.materialize(ctx) if self.type else None
        dtype: Any = self.dtype.dtype_str
        cat_filters: Any = self.dtype.cat_filters
        if cat_filters:
            cat_filters = {
                key: (value.resolve(ctx) if isinstance(value, Promise) else value)
                for key, value in cat_filters.items()
            }
        if cat_filters and dtype.startswith("list["):
            # lamindb refuses `cat_filters=` on a nested dtype, but accepts the
            # same filters written into the dtype string itself.
            dtype = resolved_dtype_str(dtype, cat_filters)
            cat_filters = None
        synonyms = "|".join(self.synonyms) or None

        kwargs: dict[str, Any] = {"name": self.name, "dtype": dtype}
        # always passed, even as None: lamindb dedups a new record against every
        # type context when `type` is omitted, and only against the untyped
        # ones when it is explicitly None -- which is what an untyped Feature is.
        kwargs["type"] = feature_type
        if cat_filters:
            kwargs["cat_filters"] = cat_filters
        if self.description is not None:
            kwargs["description"] = self.description
        if self.nullable is not None:
            kwargs["nullable"] = self.nullable
        if self.default_value is not None:
            kwargs["default_value"] = self.default_value
        if self.coerce is not None:
            kwargs["coerce"] = self.coerce

        with _without_dtype_string_hint():
            feature = ln.Feature(**kwargs)
        # `synonyms` and `unit` are documented constructor arguments (and are
        # named in lamindb's own "only ... are valid keyword arguments" error),
        # but `ln.Feature.__init__` never consumes them, so passing either
        # raises `FieldValidationError`.  Both are plain model fields, so
        # assigning them to the unsaved record persists them on `save()` --
        # which is also what the in-place update path does for an existing
        # feature, and stays correct if lamindb ever accepts the arguments.
        if synonyms is not None:
            feature.synonyms = synonyms
        if self.unit is not None:
            feature.unit = self.unit
        return feature

    @property
    def uid(self) -> Promise[FeatureUID, Self]:
        """Get a promise for the UID of the materialized feature."""
        return Promise(
            self,
            lambda s, ctx: FeatureUID(s.materialize(ctx).uid),
            label=f"{type(self).__name__}({self.name!r}).uid",
        )
