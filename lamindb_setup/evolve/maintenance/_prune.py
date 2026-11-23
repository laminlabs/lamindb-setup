"""Find and remove live objects that still reference deleted (trashed) objects.

When an object is moved to the trash (`branch_id == -1`) LaminDB keeps the
objects that referenced it intact, leaving them pointing at a target that is no
longer live.  `prune-dangling` finds every live feature, schema or record that
still references a gone object — through its `type`, a schema's members,
index feature or slots, or a record's schema — and trashes them too.  A target
counts as gone when it was trashed *or* when it no longer exists anywhere in the
instance (permanently deleted); a target that is still live but merely lies
outside the prune scope (e.g. in an excluded space) is left alone.

Because trashing a dependent can leave further objects dangling, the analysis
cascades: an object that references a *to-be-pruned* object is pruned as well,
repeated until no live object references a gone (trashed, permanently deleted or
pruned) object.

:func:`compute_prune` is read-only and operates on the lamindb-free dataclasses
read by :mod:`.introspect`, so it can be unit-tested without an instance.
:func:`execute_prune` performs the deletion against the connected instance.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ..engine import delete_object

if TYPE_CHECKING:
    from collections.abc import Iterator

    from ..introspect import InstanceContents

__all__ = [
    "DanglingObject",
    "PrunePlan",
    "PruneRef",
    "compute_prune",
    "execute_prune",
    "print_prune",
    "render_prune",
]


@dataclass
class PruneRef:
    """A single reference from a live object to a gone (trashed or pruned) object."""

    relation: str
    target_uid: str
    target_deleted: bool
    """`True` if the target was originally trashed; `False` if it is itself
    only being pruned (i.e. this reference was discovered by cascade)."""
    target_missing: bool = False
    """`True` if the target does not exist anywhere in the instance — neither
    live (in any space) nor trashed — i.e. it was permanently deleted."""
    target_name: str | None = None
    """The referenced object's name, if known (its uid otherwise)."""


@dataclass
class DanglingObject:
    """A live object that references at least one gone object and will be pruned."""

    uid: str
    kind: str
    registry: str
    name: str | None
    refs: list[PruneRef] = field(default_factory=list)
    type_name: str | None = None
    created_by: str | None = None
    space_uid: str | None = None


@dataclass
class _ObjectRef:
    """A live object read from the instance with its outbound references."""

    uid: str
    kind: str
    registry: str
    name: str | None
    refs: list[tuple[str, str]]
    """`(relation, target_uid)` pairs; targets that are `None` are dropped."""
    type_name: str | None = None
    created_by: str | None = None
    space_uid: str | None = None


@dataclass
class PrunePlan:
    """The resolved cleanup: every live object that references a gone object."""

    objects: list[DanglingObject] = field(default_factory=list)
    deleted_count: int = 0
    missing_count: int = 0
    """Number of distinct referenced objects that no longer exist anywhere
    (permanently deleted), as opposed to merely trashed."""

    @property
    def is_empty(self) -> bool:
        """Whether there is nothing to prune."""
        return not self.objects


def _clean_refs(pairs: list[tuple[str, str | None]]) -> list[tuple[str, str]]:
    """Drop `(relation, target)` pairs whose target uid is `None`."""
    return [(relation, target) for relation, target in pairs if target is not None]


def _iter_object_refs(contents: InstanceContents) -> Iterator[_ObjectRef]:
    """Yield every read object with its declarative outbound references.

    Covers the relations a plan can declare: a feature/schema/record's `type`,
    a schema's members (`features`), index feature and slot components, and a
    record's schema.  The relation label is the field name used to declare it.
    """
    for ft in contents.feature_types:
        yield _ObjectRef(
            uid=ft.uid,
            kind="feature type",
            registry="Feature",
            name=ft.name,
            refs=_clean_refs([("type", ft.type_uid)]),
            type_name=ft.type_name,
            created_by=ft.created_by,
            space_uid=ft.space_uid,
        )
    for f in contents.features:
        yield _ObjectRef(
            uid=f.uid,
            kind="feature",
            registry="Feature",
            name=f.name,
            refs=_clean_refs([("type", f.type_uid)]),
            type_name=f.type_name,
            created_by=f.created_by,
            space_uid=f.space_uid,
        )
    for st in contents.schema_types:
        yield _ObjectRef(
            uid=st.uid,
            kind="schema type",
            registry="Schema",
            name=st.name,
            refs=_clean_refs([("type", st.type_uid)]),
            type_name=st.type_name,
            created_by=st.created_by,
            space_uid=st.space_uid,
        )
    for s in contents.schemas:
        pairs: list[tuple[str, str | None]] = [("type", s.type_uid)]
        pairs.extend(("features", uid) for uid in s.feature_uids)
        pairs.append(("index", s.index_feature_uid))
        pairs.extend(("slots", uid) for uid in s.slots.values())
        yield _ObjectRef(
            uid=s.uid,
            kind="schema",
            registry="Schema",
            name=s.name,
            refs=_clean_refs(pairs),
            type_name=s.type_name,
            created_by=s.created_by,
            space_uid=s.space_uid,
        )
    for rt in contents.record_types:
        yield _ObjectRef(
            uid=rt.uid,
            kind="sheet" if rt.schema_uid is not None else "record type",
            registry="Record",
            name=rt.name,
            refs=_clean_refs([("type", rt.type_uid), ("schema", rt.schema_uid)]),
            type_name=rt.type_name,
            created_by=rt.created_by,
            space_uid=rt.space_uid,
        )


def compute_prune(contents: InstanceContents) -> PrunePlan:
    """Find every live object that references a gone object, cascading.

    Read-only: nothing is deleted.  An object is dangling when any of its
    declarative references points at a *gone* object — one that was originally
    trashed (`contents.deleted_uids`), one that no longer exists anywhere in
    the instance (permanently deleted), or one that an earlier pass marked for
    pruning.  A reference to an object that is still live but merely lies outside
    the prune scope (e.g. in an excluded space) is *not* dangling.  The scan
    repeats until a full pass marks nothing new, so a chain of references to a
    gone object is pruned in full.  Objects are returned in discovery order
    (those closest to an originally-gone object first).
    """
    deleted = set(contents.deleted_uids)
    objects = list(_iter_object_refs(contents))

    # Every uid that exists as a live object anywhere in the instance, whether or
    # not it lies within the prune scope.  A reference whose target is neither
    # live (here) nor trashed (`deleted`) points at a permanently-deleted
    # object and is treated like a reference to a trashed one.
    known: set[str] = set(contents.object_meta)
    known.update(obj.uid for obj in objects)

    missing: set[str] = set()
    for obj in objects:
        for _relation, target_uid in obj.refs:
            if target_uid not in known and target_uid not in deleted:
                missing.add(target_uid)

    # uid -> name lookup so references can name their target, not just its uid.
    names: dict[str, str] = dict(contents.deleted_names)
    for obj in objects:
        if obj.name is not None:
            names[obj.uid] = obj.name

    dangling: dict[str, DanglingObject] = {}

    origin = deleted | missing
    while True:
        gone = origin | dangling.keys()
        found = False
        for obj in objects:
            if obj.uid in gone:
                continue
            hits = [
                PruneRef(
                    relation=relation,
                    target_uid=target_uid,
                    target_deleted=target_uid in deleted,
                    target_missing=target_uid in missing,
                    target_name=names.get(target_uid),
                )
                for relation, target_uid in obj.refs
                if target_uid in gone
            ]
            if hits:
                dangling[obj.uid] = DanglingObject(
                    uid=obj.uid,
                    kind=obj.kind,
                    registry=obj.registry,
                    name=obj.name,
                    refs=hits,
                    type_name=obj.type_name,
                    created_by=obj.created_by,
                    space_uid=obj.space_uid,
                )
                found = True
        if not found:
            break

    return PrunePlan(
        objects=list(dangling.values()),
        deleted_count=len(deleted),
        missing_count=len(missing),
    )


_REGISTRY_GETTERS = {
    "Feature": lambda ln: ln.Feature,
    "Schema": lambda ln: ln.Schema,
    "Record": lambda ln: ln.Record,
}


def execute_prune(prune_plan: PrunePlan, *, permanent: bool = False) -> int:
    """Delete every dangling object against the connected instance.

    By default the objects are soft-deleted (moved to the instance's trash, so
    the removal can be undone).  Pass `permanent=True` to delete them
    irreversibly.  Objects are deleted in reverse discovery order — the deepest
    dependents first — so that, when deleting permanently, an object is removed
    before the object it depends on.  Returns the number of objects deleted.

    Runs in one transaction, so a failure part-way leaves the instance as it
    was.  A repair is decided from a plan computed against a snapshot of the
    instance, and half of that plan is not a state anyone chose: the remaining
    dangling objects would still be dangling, and the report the caller attaches
    to the run would describe work that was only partly done.
    """
    import lamindb as ln
    from django.db import transaction

    deleted = 0
    with transaction.atomic():
        for obj in reversed(prune_plan.objects):
            registry = _REGISTRY_GETTERS[obj.registry](ln)
            record = registry.filter(uid=obj.uid).one_or_none()
            if record is None:
                continue
            delete_object(record, permanent=permanent)
            deleted += 1
    return deleted


# === Rendering ================================================================


def _build_renderable(prune_plan: PrunePlan, title: str) -> Any:
    from rich.console import Group
    from rich.text import Text

    lines: list[Any] = []
    for obj in prune_plan.objects:
        line = Text("  - ", style="bold red")
        line.append(obj.kind, style="bold cyan")
        if obj.type_name is not None:
            line.append(f" {obj.type_name!r}", style="magenta")
            line.append(" > ", style="dim")
        if obj.name is not None:
            line.append(f"{obj.name!r}", style="green")
        line.append("  ")
        line.append(obj.uid, style="red")
        if obj.created_by is not None:
            line.append(f" [{obj.created_by}]", style="dim")
        if obj.space_uid is not None:
            line.append(f" [space: {obj.space_uid}]", style="dim")
        lines.append(line)
        for ref in obj.refs:
            if ref.target_missing:
                note = "permanently deleted"
            elif ref.target_deleted:
                note = "deleted"
            else:
                note = "pruned"
            target = ref.target_name if ref.target_name is not None else ref.target_uid
            detail = Text("        ")
            detail.append(
                f"references via {ref.relation} -> {target} ({note})",
                style="yellow",
            )
            lines.append(detail)

    summary = Text()
    summary.append("Prune: ", style="bold")
    summary.append(f"{len(prune_plan.objects)} dangling object(s)", style="red")
    summary.append(" referencing ")
    summary.append(f"{prune_plan.deleted_count} deleted", style="dim")
    if prune_plan.missing_count:
        summary.append(" and ")
        summary.append(f"{prune_plan.missing_count} permanently-deleted", style="dim")
    summary.append(" object(s)", style="dim")
    if prune_plan.is_empty:
        summary = Text("Prune: nothing references a deleted object.", style="dim")

    header = Text(title, style="bold white")
    return Group(header, Text(), *lines, Text(), summary)


def render_prune(prune_plan: PrunePlan, *, title: str, color: bool = False) -> str:
    """Render the prune plan as a terraform-like string."""
    from rich.console import Console

    buffer = io.StringIO()
    console = Console(
        width=None if color else 200,
        file=buffer,
        force_terminal=color or None,
        color_system="auto" if color else None,
    )
    console.print(_build_renderable(prune_plan, title))
    return buffer.getvalue()


def print_prune(prune_plan: PrunePlan, *, title: str) -> None:
    """Print the prune plan to stdout with color."""
    from rich.console import Console

    Console().print(_build_renderable(prune_plan, title))
