"""Inferred artifact schemas still listing a feature the plan deletes.

Curating a `DataFrame` artifact links, next to the schema it was validated
against, an unnamed schema of the columns it actually carries (`ArtifactSchema`,
slot `columns`; see :mod:`._schema_collisions`).  Every validated sheet export
creates one.  Such a schema belongs to no plan and validates nothing, yet it lists
every matched column as a member.  If it counted like any other schema still
declaring the feature, a feature that was ever part of an exported sheet could
never be deleted.

So the member is removed from it instead.  With `maximal_set=False` the schema
tolerates columns it does not list, so dropping one only lets it accept more:
`minimal_set` then asks for the remaining members alone, and an `ordered_set`
only compares the order of the listed columns that are present.  The schema is
saved afterwards, which recomputes `hash` and `n_members`.  Leaving them stale
would make lamindb create a duplicate for the next artifact with the same columns
instead of reusing this one.

Two outcomes need more than that:

* the remaining content is already held by another live schema, and lamindb
  keeps one per hash.  The artifact links move to that schema and this one is
  trashed, which is what curating the artifact today would have produced.  The
  same happens when several inferred schemas end up with the same content: the
  oldest keeps it.
* no member is left.  The artifact links are dropped and the schema is trashed.

Two cannot be settled, so the deletion stays refused: a `maximal_set` schema
would start rejecting the artifacts that still carry the column, and an index
cannot be dropped from the schema keyed by it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ..core import DEFAULT_MODELS
from ._lock import registry_for_kind
from ._schema_collisions import (
    _INFERRED_LINK_MODEL,
    _member_link_model,
    _reverse_references,
    classify,
)
from ._sheet_exports import COLUMNS_SLOT
from ._trash import delete_object

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

__all__ = [
    "InferredMemberRemoval",
    "InferredSchema",
    "live_member_holders",
    "removal_blockers",
]

# How many artifacts a refusal names before it only counts the rest.
_NAMED_ARTIFACTS = 10


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}{'' if count == 1 else 's'}"


def _label(schema: Any) -> str:
    name = getattr(schema, "name", None)
    uid = getattr(schema, "uid", "?")
    return f"schema {name!r} ({uid!r})" if name else f"unnamed schema {uid!r}"


def _inferred_link_model(model: Any) -> Any:
    """The link table recording a schema as an artifact's inferred schema."""
    return next(
        relation.related_model
        for relation in _reverse_references(model)
        if relation.related_model.__name__ == _INFERRED_LINK_MODEL
    )


def _artifact_links(schema: Any) -> Any:
    """`schema`'s `ArtifactSchema` rows, unfiltered by branch."""
    links = _inferred_link_model(type(schema))
    return links._base_manager.using(schema._state.db).filter(schema_id=schema.pk)


def _members(schema: Any) -> list[Any]:
    """`schema`'s member features in link order, which is what `save` hashes.

    Read through the link table rather than `Schema.members`, which follows
    the display order in `_aux` when one is stored.  A trashed member is
    included, as it is in lamindb's own hash.
    """
    links = _member_link_model(type(schema))
    relation = next(
        f
        for f in links._meta.fields
        if f.is_relation and f.related_model is not type(schema)
    )
    using = schema._state.db
    ids = list(
        links._base_manager.using(using)
        .filter(schema_id=schema.pk)
        .order_by("id")
        .values_list(relation.attname, flat=True)
    )
    model = relation.related_model
    by_id = {f.pk: f for f in model._base_manager.using(using).filter(pk__in=ids)}
    return [by_id[pk] for pk in ids if pk in by_id]


def live_member_holders(feature: Any) -> list[Any]:
    """The live schemas listing `feature` as a member, oldest first."""
    return list(
        registry_for_kind("Schema")
        .filter(features=feature)
        .exclude(branch_id=-1)
        .distinct()
        .order_by("pk")
    )


def _named_artifacts(schema: Any) -> str:
    """The artifacts recording `schema`, by key (or uid) -- for a refusal."""
    rows = sorted(
        {
            f"{key!r} ({uid})" if key else repr(uid)
            for key, uid in _artifact_links(schema).values_list(
                "artifact__key", "artifact__uid"
            )
        }
    )
    named = ", ".join(rows[:_NAMED_ARTIFACTS])
    if len(rows) > _NAMED_ARTIFACTS:
        named += f" and {len(rows) - _NAMED_ARTIFACTS} more"
    return named


def removal_blockers(
    schema: Any, feature: Any, *, managed: bool
) -> tuple[list[str], int]:
    """Why `feature` cannot be removed from `schema` automatically.

    Returns the reasons, empty when `schema` is an inferred artifact schema
    the member may be removed from, and how many artifact links record it.  It
    is one when :func:`~._schema_collisions.classify` would let it be absorbed:
    unnamed, untyped, not a type, not managed by a plan, a feature schema,
    recorded as at least one artifact's inferred schema, and referenced by
    nothing else -- no artifact or collection validates against it, no sheet
    uses it, no composite slots it.  On top of that its `maximal_set` must be
    off and `feature` must not be its index.

    A named schema is never an inferred one; it is refused without reasons,
    since the name is all the operator needs to act on it.
    """
    if getattr(schema, "name", None) is not None:
        return ["it is named"], 0
    collision = classify(schema, managed=managed)
    blockers = list(collision.blockers)
    if blockers:
        return blockers, collision.inferred_links
    if getattr(schema, "maximal_set", False):
        blockers.append(
            "it forbids columns it does not list (maximal_set=True), so it would "
            "reject the artifacts that still carry this one: "
            + _named_artifacts(schema)
        )
    index = getattr(schema, "index", None)
    if index is not None and index.pk == feature.pk:
        blockers.append("the feature is its index")
    return blockers, collision.inferred_links


@dataclass
class InferredSchema:
    """An inferred artifact schema losing members to the plan's deletions.

    `features` are the deleted features it lists, `artifacts` how many
    artifact links record it.  :meth:`InferredMemberRemoval.settle` fills in the
    rest: `content_hash` is the hash it takes without them (`None` when
    lamindb cannot compute it), `merge_into` the live schema already holding
    that content, and `emptied` whether no member is left at all.
    """

    schema: Any
    artifacts: int
    features: list[Any] = field(default_factory=list)
    content_hash: str | None = None
    merge_into: Any = None
    emptied: bool = False

    @property
    def uid(self) -> str:
        return str(getattr(self.schema, "uid", "?"))

    def describe(self) -> str:
        """One entry of the change detail, e.g. `'1LWK...' (2 artifacts)`."""
        artifacts = _plural(self.artifacts, "artifact")
        if self.emptied:
            return (
                f"{self.uid!r} ({artifacts}; no member left, so its links are "
                "dropped and it is trashed)"
            )
        if self.merge_into is not None:
            return (
                f"{self.uid!r} ({artifacts}; then the same as "
                f"{_label(self.merge_into)}, so its links move there and it is "
                "trashed)"
            )
        return f"{self.uid!r} ({artifacts})"


class InferredMemberRemoval:
    """The inferred schemas a removal pass takes deleted features out of.

    Built up feature by feature (:meth:`add`), then settled as a whole
    (:meth:`settle`): a schema listing several deleted features loses them in a
    single save, and inferred schemas ending up with the same content are merged
    rather than refused.  :meth:`apply` makes the edits.
    """

    def __init__(self) -> None:
        self.schemas: dict[Any, InferredSchema] = {}
        self._by_feature: dict[Any, list[InferredSchema]] = {}

    def __bool__(self) -> bool:
        return bool(self.schemas)

    def add(self, feature: Any, schema: Any, artifacts: int) -> None:
        """Record that `feature` is to be removed from inferred `schema`."""
        item = self.schemas.setdefault(
            schema.pk, InferredSchema(schema=schema, artifacts=artifacts)
        )
        item.features.append(feature)
        self._by_feature.setdefault(feature.pk, []).append(item)

    def settle(self, *, planned: Mapping[str, Any], excluded: Iterable[Any]) -> None:
        """Predict each schema's content without the features, and its fate.

        `planned` maps a content hash to the plan schema a PLAN-mode preview
        expects to hold it once applied, which takes precedence over a live
        holder.  `excluded` are primary keys of live schemas that will not be
        holding their current content by the time the edits run -- a plan schema
        the preview expects to rehash, or one the same removal pass deletes.
        """
        from ._schema_collisions import find_collisions

        if not self.schemas:
            return
        hashes: dict[Any, str] = {}
        for pk, item in self.schemas.items():
            dropped = {feature.pk for feature in item.features}
            remaining = [m for m in _members(item.schema) if m.pk not in dropped]
            if not remaining:
                item.emptied = True
                continue
            item.content_hash = _content_hash(item.schema, remaining, dropped)
            if item.content_hash is not None:
                hashes[pk] = item.content_hash
        if not hashes:
            return
        first = next(iter(self.schemas.values())).schema
        live = find_collisions(type(first), hashes, using=first._state.db)
        skip = set(self.schemas) | set(excluded)
        groups: dict[str, list[InferredSchema]] = {}
        for pk, value in hashes.items():
            groups.setdefault(value, []).append(self.schemas[pk])
        for value, items in groups.items():
            items.sort(key=lambda item: item.schema.pk)
            holders = [s for s in live.get(items[0].schema.pk, []) if s.pk not in skip]
            target = planned.get(value)
            if target is None:
                target = min(holders, key=lambda s: s.pk, default=None)
            if target is None:
                target, items = items[0].schema, items[1:]
            for item in items:
                item.merge_into = target

    def details_for(self, feature: Any) -> list[str]:
        """The change detail of deleting `feature`, if any inferred schema lists it."""
        items = self._by_feature.get(feature.pk)
        if not items:
            return []
        return [
            f"removed from {_plural(len(items), 'inferred artifact schema')}: "
            + ", ".join(item.describe() for item in items)
        ]

    def apply(self) -> dict[Any, set[Any]]:
        """Make the settled edits.

        Returns the columns each affected artifact recorded *before* them
        (feature ids, keyed by the artifact's primary key): the sheet-export
        sweep that follows judges the file, which still has the column, and must
        read it exactly as the preview did.

        The schemas that keep their content are saved first, so one merged into
        another finds it already holding that content.  A merged or emptied
        schema is trashed unchanged, as it was recorded.
        """
        from lamindb.errors import ValidationError

        recorded: dict[Any, set[Any]] = {}
        for item in self.schemas.values():
            columns = {m.pk for m in _members(item.schema)}
            for artifact_id, slot in _artifact_links(item.schema).values_list(
                "artifact_id", "slot"
            ):
                if slot == COLUMNS_SLOT:
                    recorded[artifact_id] = set(columns)
        settled = sorted(
            self.schemas.values(),
            key=lambda item: (item.emptied, item.merge_into is not None),
        )
        for item in settled:
            schema = item.schema
            if item.emptied:
                _artifact_links(schema).delete()
                delete_object(schema)
            elif item.merge_into is not None:
                _artifact_links(schema).update(schema_id=item.merge_into.pk)
                delete_object(schema)
            else:
                _forget_optionals(schema, item.features)
                schema.features.remove(*item.features)
                try:
                    schema.save(print_hash_mutation_warning=False)
                except ValidationError as exc:
                    exc.add_note(
                        f"while removing {len(item.features)} deleted feature(s) "
                        f"from inferred artifact schema {item.uid!r}"
                    )
                    raise
        return recorded


def _forget_optionals(schema: Any, features: list[Any]) -> None:
    """Drop `features` from `schema`'s optional uid list, if listed there."""
    listed = set(schema.optionals.get_uids())
    stale = [f for f in features if str(f.uid) in listed]
    if stale:
        schema.optionals.remove(stale)


def _content_hash(
    schema: Any,
    remaining: list[Any],
    dropped: set[Any],
    *,
    slots: dict[str, Any] | None = None,
) -> str | None:
    """The hash `schema` takes once saved with only the `remaining` members.

    `slots` are its components; an inferred schema slots nothing, since a
    composite is not one (see :func:`~._schema_collisions.classify`).
    """
    index = getattr(schema, "index", None)
    optionals = [f for f in schema.optionals.get() if f.pk not in dropped]
    return DEFAULT_MODELS.schema_content_hash(
        schema,
        features=[m for m in remaining if index is None or m.pk != index.pk],
        index=index,
        optionals=optionals,
        slots=slots or {},
        flexible=getattr(schema, "flexible", None),
        coerce=getattr(schema, "coerce", None),
        minimal_set=schema.minimal_set,
        ordered_set=schema.ordered_set,
        maximal_set=schema.maximal_set,
    )
