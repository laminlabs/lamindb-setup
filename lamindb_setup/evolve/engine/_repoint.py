"""Moving every reference from one record to another.

Two records that are the same thing -- the same schema created twice, an object
re-created under another uid -- are merged by pointing everything that names one
at the other.  The references are found through every foreign key the instance
defines, so a link a future lamindb adds moves too, rather than being cascaded
away with the record left behind.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..core import LaminSchemasError

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping

__all__ = ["referencing_fields", "repoint_references"]

# Bookkeeping fields two link rows may differ in and still record the same link.
_AUDIT_FIELDS: frozenset[str] = frozenset(
    {"id", "created_at", "updated_at", "created_by", "run", "branch", "space"}
)


def referencing_fields(model: Any) -> list[tuple[Any, Any]]:
    """Every `(model, foreign key)` in the instance that points at `model`."""
    from django.apps import apps

    found: list[tuple[Any, Any]] = []
    for candidate in apps.get_models(include_auto_created=True):
        for fk in candidate._meta.concrete_fields:
            if not fk.is_relation or fk.remote_field is None:
                continue
            if fk.remote_field.model is model:
                found.append((candidate, fk))
    return found


def _unique_sets(model: Any, fk: Any) -> list[tuple[str, ...]]:
    """The unconditional unique field sets of `model` that include `fk`."""
    from django.db.models import UniqueConstraint

    by_name = {f.name: f.attname for f in model._meta.concrete_fields}
    sets: list[tuple[str, ...]] = []
    for names in model._meta.unique_together:
        sets.append(tuple(by_name.get(n, n) for n in names))
    for constraint in model._meta.constraints:
        if isinstance(constraint, UniqueConstraint) and constraint.condition is None:
            if constraint.fields:
                sets.append(tuple(by_name.get(n, n) for n in constraint.fields))
    return [s for s in sets if fk.attname in s]


def _same_link(model: Any, row: Any, other: Any, fk: Any) -> bool:
    """Whether two rows of `model` say the same thing once `fk` is repointed."""
    for f in model._meta.concrete_fields:
        if f.name in _AUDIT_FIELDS or f.primary_key or f.attname == fk.attname:
            continue
        if getattr(row, f.attname) != getattr(other, f.attname):
            return False
    return True


def repoint_references(
    model: Any,
    from_pk: Any,
    to_pk: Any,
    *,
    error: type[LaminSchemasError] = LaminSchemasError,
    keep: Collection[Any] = (),
    only: Collection[Any] | None = None,
    exclude: Mapping[Any, Collection[Any]] | None = None,
) -> tuple[int, int]:
    """Move every reference to `from_pk` over to `to_pk`.

    A row that would become an exact duplicate of one `to_pk` already has --
    the same link recorded on both copies -- is dropped instead.  Any other
    clash is a real conflict and raises `error`, for the caller's transaction
    to roll back.  The rows of a foreign key in `keep` are left with
    `from_pk`: a merge that trashes the record rather than erasing it keeps
    the links that are its own content.  `only` restricts the move to the
    foreign keys it lists, and `exclude` maps a referencing model to the
    primary keys of its rows to leave with `from_pk`.  Returns how many rows
    were moved and how many dropped.
    """
    from django.db import DatabaseError, transaction

    moved = dropped = 0
    for related, fk in referencing_fields(model):
        if fk in keep or (only is not None and fk not in only):
            continue
        rows = list(
            related.objects.filter(**{fk.attname: from_pk}).exclude(
                pk__in=list((exclude or {}).get(related, ()))
            )
        )
        if not rows:
            continue
        unique_sets = _unique_sets(related, fk)
        for row in rows:
            duplicate = None
            for fields in unique_sets:
                lookup = {f: getattr(row, f) for f in fields if f != fk.attname}
                lookup[fk.attname] = to_pk
                duplicate = related.objects.filter(**lookup).exclude(pk=row.pk).first()
                if duplicate is not None:
                    break
            if duplicate is not None:
                if not _same_link(related, row, duplicate, fk):
                    raise error(
                        f"cannot merge: {related.__name__} row {row.pk} would clash "
                        f"with row {duplicate.pk} on {fk.name}, and the two record "
                        "different things"
                    )
                related.objects.filter(pk=row.pk).delete(permanent=True)
                dropped += 1
                continue
            try:
                with transaction.atomic():
                    related.objects.filter(pk=row.pk).update(**{fk.attname: to_pk})
            except DatabaseError as exc:
                raise error(
                    f"cannot merge: moving {related.__name__} row {row.pk} on "
                    f"{fk.name} was refused by the database: {exc}"
                ) from exc
            moved += 1
    return moved, dropped
