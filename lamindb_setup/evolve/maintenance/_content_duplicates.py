"""Live schemas holding one content hash, settled around the one to keep.

lamindb addresses a schema by a hash of its content and keeps one live schema
per hash: `Schema(...)` hands out the live schema holding the content it is
built with -- a second holder makes that lookup fail -- and since 2.12
`save()` refuses to rehash a schema onto a hash another live one holds.  An
instance written before then can still hold two: typically a plan schema
edited onto the content of the inferred `columns` schema of artifacts curated
while the two still differed (see
:mod:`lamindb_setup.evolve.engine._schema_collisions`).

Such a group is settled around a *keeper*, the schema everything ends up
pointing at:

* an inferred schema nothing else references is *absorbed*, exactly as
  `apply` absorbs one: its artifact links move to the keeper and it is
  deleted;
* any other is *merged* only when asked to, and only into the one schema of
  the group a plan manages: everything referencing it moves to the keeper and
  it is trashed with its own members and slots -- what `merge_schema` does
  in `apply`;
* otherwise the group is left as it is, for a decision by hand.

The keeper is the one schema that cannot be absorbed or, when every one could,
the oldest: the one lamindb would have handed out for the content.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from ..core import LaminSchemasError
    from ..engine import SchemaCollision

__all__ = [
    "ContentGroup",
    "schema_label",
    "settle_content_group",
    "settle_schemas",
]


@dataclass
class ContentGroup:
    """Live schemas sharing one content hash, and how they are settled.

    `keeper` is the schema the others are settled into; `absorbed` and
    `merged` are settled into it.  `blocked` lists the schemas standing in
    the way, with the reasons on each: the group is then left as it is.  A
    blocked group has a keeper when merging would settle it (the one schema a
    plan manages, the others not), and none when it would not.
    """

    content_hash: str
    schemas: list[Any]
    keeper: Any | None
    absorbed: list[SchemaCollision] = field(default_factory=list)
    merged: list[SchemaCollision] = field(default_factory=list)
    blocked: list[SchemaCollision] = field(default_factory=list)
    managed: int = 0

    @property
    def ok(self) -> bool:
        """Whether the group can be (or was) settled."""
        return not self.blocked

    @property
    def mergeable(self) -> bool:
        """Whether merging the blocked schemas into the keeper would settle it."""
        return bool(self.blocked) and self.keeper is not None


def schema_label(schema: Any) -> str:
    """`schema 'Name' (uid X)`, or `unnamed schema 'X'` for an unnamed one."""
    name = getattr(schema, "name", None)
    uid = getattr(schema, "uid", "?")
    return f"schema {name!r} (uid {uid})" if name else f"unnamed schema {uid!r}"


def settle_content_group(
    schemas: Iterable[Any],
    content_hash: str,
    *,
    managed: Callable[[Any], bool],
    merge: bool = False,
) -> ContentGroup:
    """Decide, read-only, how the live `schemas` sharing one content are settled.

    `managed` says whether a plan manages a schema: one that does is never
    absorbed or merged away.  `merge` allows merging a schema that cannot be
    absorbed into the one schema of the group a plan manages; a schema type is
    never merged.
    """
    from ..engine import classify_schema_collision

    ordered = sorted(schemas, key=lambda schema: schema.pk)
    planned = {schema.pk for schema in ordered if managed(schema)}
    classified = [
        classify_schema_collision(schema, managed=schema.pk in planned)
        for schema in ordered
    ]
    absorbable = [c for c in classified if c.absorbable]
    kept = [c for c in classified if not c.absorbable]
    group = ContentGroup(
        content_hash=content_hash,
        schemas=ordered,
        keeper=None,
        managed=len(planned),
    )
    if len(kept) <= 1:
        group.keeper = (kept or absorbable)[0].schema
        group.absorbed = [c for c in absorbable if c.schema.pk != group.keeper.pk]
        return group
    owners = [c for c in kept if c.schema.pk in planned]
    others = [c for c in kept if c.schema.pk not in planned]
    if len(owners) != 1 or any(getattr(c.schema, "is_type", False) for c in others):
        group.blocked = kept
        return group
    group.keeper = owners[0].schema
    if merge:
        group.absorbed = absorbable
        group.merged = others
    else:
        group.blocked = others
    return group


def settle_schemas(
    group: ContentGroup, *, error: type[LaminSchemasError]
) -> tuple[int, int]:
    """Settle `group` into its keeper, inside the caller's transaction.

    `group` must be one that can be settled (:attr:`ContentGroup.ok`).  The
    absorbed schemas are deleted, the merged ones trashed.  Returns how many
    references the merges moved and how many duplicate links they dropped; a
    merge that would clash raises `error`.
    """
    from ..engine import absorb_inferred_schema, merge_schema_into

    for collision in group.absorbed:
        absorb_inferred_schema(group.keeper, collision)
    moved = dropped = 0
    for collision in group.merged:
        count, duplicates = merge_schema_into(
            group.keeper, collision.schema, error=error
        )
        moved += count
        dropped += duplicates
    return moved, dropped
