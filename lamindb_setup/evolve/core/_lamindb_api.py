"""The lamindb internals this package reaches into, in one place.

lamindb's public API does not cover everything the schema engine needs.  Three
things come from modules under `lamindb.models` that carry no compatibility
promise:

* parsing a categorical dtype string into its component registries, and the
  filter payload of a component into queryset lookups,
* finding the link table that pairs two registries for a categorical feature,
* turning a sheet row's index value into the `Record.name` lamindb stores,
* listing branches by their status code,
* computing the content hash a schema save would store.

Each was previously reached for at its point of use, in three unrelated modules,
so a lamindb upgrade that changed any of them surfaced as an unrelated-looking
failure in whichever of `_drift_data`, `migrations` or `_sheet_exports`
happened to run first.  Collecting them here gives that coupling one documented
home: this file is the list of what a lamindb bump can break, and each method
says what shape this package assumes.

Every method degrades rather than raises when lamindb does not answer the way we
expect -- returning `None` so the caller can stay conservative -- because all
three feed decisions ("is this change safe?", "can this migration write link
rows itself?") whose safe answer is "assume not".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = [
    "CategoricalLink",
    "DEFAULT_MODELS",
    "LamindbModels",
]

# Stands for "the schema's own value" in :meth:`LamindbModels.schema_content_hash`.
_OWN: Any = object()


@dataclass(frozen=True)
class CategoricalLink:
    """The link table joining an annotated object to a categorical value.

    `model` is the link registry; `owner_field` and `value_field` name its
    foreign keys to the annotated object and to the value respectively (the
    `feature` key is implied and checked when this is built).
    """

    model: Any
    owner_field: str
    value_field: str


class LamindbModels:
    """Adapter over the private `lamindb.models` surface this package uses."""

    def parse_dtype(self, dtype_str: str) -> list[dict[str, Any]] | None:
        """Return the components of a categorical dtype string, or `None`.

        `None` means lamindb could not parse it, or named a registry that is
        not configured for this instance -- both of which leave the caller
        unable to say what the dtype validates against.
        """
        try:
            from lamindb.models.feature import parse_dtype

            return parse_dtype(dtype_str)
        except Exception:
            return None

    def itype_str(self, itype: Any) -> str | None:
        """`itype` -- a registry class or field -- as lamindb stores it on a schema.

        `None` when lamindb cannot serialize it.
        """
        try:
            from lamindb.models.feature import serialize_dtype

            return serialize_dtype(itype, is_itype=True)
        except Exception:
            return None

    def cat_filter_lookups(self, filter_str: str) -> dict[str, str] | None:
        """The `filter_str` of a parsed categorical as queryset lookups, or `None`.

        The `[...]` payload of a dtype component (`source__uid='x'`,
        `is_type=True`), split and unquoted the way lamindb's curator reads it,
        as `{lookup: value}`.  A relation lookup (`source__uid`) is left for
        the database to follow rather than resolved to the related record, so a
        filter naming a record this instance does not have matches nothing.
        `None` when lamindb cannot parse it.
        """
        try:
            from lamindb.models.feature import parse_filter_string

            parsed = parse_filter_string(filter_str)
        except Exception:
            return None
        return {key: value for key, (_relation, _field, value) in parsed.items()}

    def categorical_link(
        self, owner_model: Any, value_model: Any, *, instance: Any
    ) -> CategoricalLink | None:
        """Return the link table pairing `owner_model` with `value_model`.

        `None` when lamindb has no such table, or when it is not keyed the way
        this package assumes: a `<owner>_id` / `feature_id` / `<value>_id`
        triple.  A future link table of another shape must be handed back to
        `add_values` rather than mis-populated, so the shape check lives here
        with the import it guards.
        """
        try:
            from lamindb.models._feature_manager import get_categorical_link_info
        except ImportError:  # pragma: no cover - lamindb always ships this
            return None
        owner_field = owner_model.__name__.lower()
        try:
            link_model, value_field, _ = get_categorical_link_info(
                owner_model, value_model, instance=instance
            )
        except (AttributeError, TypeError, ValueError):
            return None
        if not all(
            hasattr(link_model, f"{name}_id")
            for name in (owner_field, "feature", value_field)
        ):
            return None
        return CategoricalLink(link_model, owner_field, value_field)

    def type_schema_index(self, record_type: Any) -> Any | None:
        """Return the schema index of a record type, or `None` if it has none.

        A feature that *is* a type's schema index is written into `Record.name`
        rather than into a link table, so a migration must not try to write link
        rows for it.
        """
        try:
            from lamindb.models.record import get_type_schema_index

            return get_type_schema_index(record_type)
        except ImportError:  # pragma: no cover - lamindb always ships this
            return None

    def index_value_as_record_name(self, value: Any, feature: Any) -> str | None:
        """The `Record.name` lamindb derives from a sheet row's index `value`.

        `None` for a null value.  Unlike the rest of this adapter, a value
        lamindb itself refuses as an index (anything but a string) *raises*
        `TypeError`: the caller is predicting what a schema save will do, and
        that save would fail on it too.
        """
        from lamindb.models.record import coerce_index_value_to_record_name

        return coerce_index_value_to_record_name(value, feature)

    def branch_names(
        self, *, prefix: str, exclude_status_codes: Sequence[int]
    ) -> set[str]:
        """Names of branches starting with `prefix`, excluding those statuses.

        Unlike the rest of this adapter, a failure here *propagates*: the caller
        uses the result to decide whether a re-export is still owed, and an empty
        set would incorrectly claim that none is.
        """
        from lamindb.models import Branch

        return set(
            Branch.objects.filter(name__startswith=prefix)
            .exclude(_status_code__in=tuple(exclude_status_codes))
            .values_list("name", flat=True)
        )

    def schema_content_hash(
        self,
        schema: Any,
        *,
        features: Sequence[Any],
        index: Any | None,
        optionals: Sequence[Any],
        slots: dict[str, Any],
        flexible: Any,
        coerce: Any,
        minimal_set: bool,
        ordered_set: bool,
        maximal_set: bool,
        itype: Any = _OWN,
        dtype: Any = _OWN,
    ) -> str | None:
        """The content hash `schema.save()` would store with these members and flags.

        Mirrors the arguments `Schema.save` hands lamindb's hash computation,
        swapping in the post-change members (`features` excludes `index`, in
        link order), optionals, slots and set flags -- and `itype` / `dtype`
        when given, both hashed by lamindb; every other input is read off
        `schema` as save does.  It runs no query given loaded records, so a
        PLAN-mode preview can afford it per schema.  `None` when lamindb cannot
        compute it, which callers read as "unknown".
        """
        import warnings

        try:
            with warnings.catch_warnings():
                # `Schema.save` passes a composite's legacy `itype='Composite'`
                # through as well, and lamindb warns about it every time.
                warnings.simplefilter("ignore")
                _, validated, *_ = schema._validate_kwargs_calculate_hash(
                    features=list(features),
                    index=index,
                    slots=slots,
                    name=schema.name,
                    description=schema.description,
                    itype=schema.itype if itype is _OWN else itype,
                    flexible=flexible,
                    type=None,
                    is_type=schema.is_type,
                    otype=schema.otype,
                    suffix=schema.suffix,
                    dtype=schema.dtype if dtype is _OWN else dtype,
                    minimal_set=minimal_set,
                    ordered_set=ordered_set,
                    maximal_set=maximal_set,
                    coerce=coerce,
                    # Only read when there are no features; passing the old count
                    # alongside new features merely makes lamindb log the update.
                    n_features=None if features or index else schema.n_members,
                    optional_features_manual=list(optionals),
                )
        except Exception:
            return None
        value = validated.get("hash")
        return value if isinstance(value, str) else None


#: The adapter used when a caller does not supply one.
DEFAULT_MODELS = LamindbModels()
