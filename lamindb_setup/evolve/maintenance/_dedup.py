"""Find and remove duplicate features and schemas.

Two features (or two schemas) that share the same `type` parent — or that both
have no parent — *and* the same name are duplicates: LaminDB addresses such an
object by its (name, type) natural key, so a second one with the same key is an
accident that should not exist.  `repair duplicates` groups every live feature
and schema by (kind, parent, name), and for any group with more than one member
keeps the most recently created object and marks the older ones for deletion.

Only the `Feature` and `Schema` registries are considered (feature types,
features, schema types and named schemas); records are left untouched.  Unnamed
schemas (which LaminDB addresses by content hash, not by name) are skipped by
that pass.

A second pass finds live schemas sharing one content *hash*, which LaminDB no
longer allows either -- its `Schema(...)` lookup by content fails on a second
holder -- but an instance written before LaminDB 2.12 can still hold, typically a
plan schema edited onto the content of an inferred `columns` schema.  An
inferred schema nothing else references is absorbed into the schema sharing its
content, as `apply` absorbs one (see :mod:`._content_duplicates`); any other
group is listed for a decision by hand.

:func:`compute_duplicates` is read-only and operates on the lamindb-free
dataclasses read by :mod:`.introspect`, so it can be unit-tested without an
instance; :func:`compute_content_duplicates` reads the connected instance.
:func:`execute_dedup` performs both against the connected instance.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ..core import LaminSchemasError
from ..engine import delete_object
from ._content_duplicates import schema_label, settle_content_group, settle_schemas

if TYPE_CHECKING:
    from collections.abc import Collection

    from ..introspect import InstanceContents
    from ._content_duplicates import ContentGroup

__all__ = [
    "CONTENT_ABSORB",
    "CONTENT_KEEP",
    "CONTENT_MANUAL",
    "ContentDuplicateGroup",
    "ContentMember",
    "DuplicateGroup",
    "DuplicateMember",
    "DuplicatePlan",
    "compute_content_duplicates",
    "compute_duplicates",
    "execute_dedup",
    "print_dedup",
    "render_dedup",
]

CONTENT_KEEP = "keep"
CONTENT_ABSORB = "absorb"
CONTENT_MANUAL = "manual"


@dataclass
class DuplicateMember:
    """One object participating in a duplicate group."""

    uid: str
    kind: str
    registry: str
    name: str
    type_uid: str | None
    type_name: str | None = None
    created_by: str | None = None
    created_at: str | None = None
    space_uid: str | None = None


@dataclass
class DuplicateGroup:
    """A set of objects that share the same kind, parent and name.

    `keep` is the most recently created member; `remove` lists the older
    members (oldest first) that will be deleted.
    """

    kind: str
    registry: str
    name: str
    type_uid: str | None
    type_name: str | None
    keep: DuplicateMember
    remove: list[DuplicateMember] = field(default_factory=list)


@dataclass
class ContentMember:
    """One schema of a group sharing their content, and what becomes of it.

    `action` is :data:`CONTENT_KEEP`, :data:`CONTENT_ABSORB` or
    :data:`CONTENT_MANUAL`; `detail` says what it is recorded as, or why it
    cannot be absorbed.
    """

    uid: str
    name: str | None
    action: str
    detail: str = ""


@dataclass
class ContentDuplicateGroup:
    """Live schemas sharing one content hash.

    `reason` is set when the group needs a decision by hand, and nothing in
    it is changed; otherwise its absorbed members are settled into the kept one.
    """

    content_hash: str
    members: list[ContentMember]
    reason: str = ""
    settled: ContentGroup | None = field(default=None, repr=False, compare=False)

    @property
    def manual(self) -> bool:
        """Whether the group is left for a decision by hand."""
        return bool(self.reason)

    @property
    def absorbed(self) -> list[ContentMember]:
        """The members absorbed into the kept one."""
        return [m for m in self.members if m.action == CONTENT_ABSORB]


@dataclass
class DuplicatePlan:
    """Every duplicate group found, with the members to remove.

    `groups` share a kind, parent and name; `content_groups` share a content
    hash (see :func:`compute_content_duplicates`).
    """

    groups: list[DuplicateGroup] = field(default_factory=list)
    content_groups: list[ContentDuplicateGroup] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        """Whether there are no duplicates at all."""
        return not self.groups and not self.content_groups

    @property
    def remove_count(self) -> int:
        """Total number of objects that would be deleted across all groups."""
        return sum(len(group.remove) for group in self.groups)

    @property
    def absorb_count(self) -> int:
        """Total number of inferred schemas that would be absorbed."""
        return sum(len(g.absorbed) for g in self.content_groups if not g.manual)

    @property
    def manual_groups(self) -> list[ContentDuplicateGroup]:
        """The groups sharing a content that need a decision by hand."""
        return [g for g in self.content_groups if g.manual]

    @property
    def has_changes(self) -> bool:
        """Whether executing the plan would change anything."""
        return bool(self.remove_count or self.absorb_count)


@dataclass
class _Candidate:
    """A live feature/schema considered for duplicate detection."""

    uid: str
    kind: str
    registry: str
    name: str
    type_uid: str | None
    type_name: str | None
    created_by: str | None
    created_at: str | None
    space_uid: str | None


def _iter_candidates(contents: InstanceContents) -> list[_Candidate]:
    """Yield every named feature and schema as a duplicate-detection candidate.

    Unnamed schemas (`name is None`) are skipped: they are content-addressed,
    not name-addressed, so two of them are not duplicates by name.
    """
    candidates: list[_Candidate] = []
    for ft in contents.feature_types:
        candidates.append(
            _Candidate(
                uid=ft.uid,
                kind="feature type",
                registry="Feature",
                name=ft.name,
                type_uid=ft.type_uid,
                type_name=ft.type_name,
                created_by=ft.created_by,
                created_at=ft.created_at,
                space_uid=ft.space_uid,
            )
        )
    for f in contents.features:
        candidates.append(
            _Candidate(
                uid=f.uid,
                kind="feature",
                registry="Feature",
                name=f.name,
                type_uid=f.type_uid,
                type_name=f.type_name,
                created_by=f.created_by,
                created_at=f.created_at,
                space_uid=f.space_uid,
            )
        )
    for st in contents.schema_types:
        candidates.append(
            _Candidate(
                uid=st.uid,
                kind="schema type",
                registry="Schema",
                name=st.name,
                type_uid=st.type_uid,
                type_name=st.type_name,
                created_by=st.created_by,
                created_at=st.created_at,
                space_uid=st.space_uid,
            )
        )
    for s in contents.schemas:
        if s.name is None:
            continue
        candidates.append(
            _Candidate(
                uid=s.uid,
                kind="schema",
                registry="Schema",
                name=s.name,
                type_uid=s.type_uid,
                type_name=s.type_name,
                created_by=s.created_by,
                created_at=s.created_at,
                space_uid=s.space_uid,
            )
        )
    return candidates


def _to_member(candidate: _Candidate) -> DuplicateMember:
    return DuplicateMember(
        uid=candidate.uid,
        kind=candidate.kind,
        registry=candidate.registry,
        name=candidate.name,
        type_uid=candidate.type_uid,
        type_name=candidate.type_name,
        created_by=candidate.created_by,
        created_at=candidate.created_at,
        space_uid=candidate.space_uid,
    )


def _age_key(candidate: _Candidate) -> tuple[str, str]:
    """Sort key ordering candidates oldest first.

    `created_at` is an ISO-8601 timestamp, which sorts chronologically as a
    string; a missing timestamp sorts before any real one (treated as oldest).
    `uid` breaks ties deterministically.
    """
    return (candidate.created_at or "", candidate.uid)


def compute_duplicates(contents: InstanceContents) -> DuplicatePlan:
    """Group features and schemas by (kind, parent, name) and find duplicates.

    Read-only: nothing is deleted.  A group of two or more objects that share the
    same kind, `type` parent (or both lack one) and name is a duplicate set;
    the most recently created member is kept and the older ones are returned for
    removal (oldest first).  Groups are returned ordered by registry, name and
    parent for a stable report.
    """
    candidates = _iter_candidates(contents)

    grouped: dict[tuple[str, str | None, str], list[_Candidate]] = {}
    for candidate in candidates:
        key = (candidate.kind, candidate.type_uid, candidate.name)
        grouped.setdefault(key, []).append(candidate)

    groups: list[DuplicateGroup] = []
    for (kind, type_uid, name), members in grouped.items():
        if len(members) < 2:
            continue
        ordered = sorted(members, key=_age_key)
        newest = ordered[-1]
        older = ordered[:-1]
        groups.append(
            DuplicateGroup(
                kind=kind,
                registry=newest.registry,
                name=name,
                type_uid=type_uid,
                type_name=newest.type_name,
                keep=_to_member(newest),
                remove=[_to_member(c) for c in older],
            )
        )

    groups.sort(key=lambda g: (g.registry, g.kind, g.name, g.type_uid or ""))
    return DuplicatePlan(groups=groups)


def compute_content_duplicates(
    *,
    space_uids: Collection[str] | None = None,
    exclude_space_uids: Collection[str] | None = None,
    managed_uids: Collection[str] = (),
    skip_uids: Collection[str] = (),
) -> list[ContentDuplicateGroup]:
    """Find the live schemas sharing one content hash, connected, read-only.

    Each group is settled as :mod:`._content_duplicates` describes: an inferred
    schema nothing else references is absorbed into the one kept, and a group
    with more than one schema that cannot be absorbed is left for a decision by
    hand.  `managed_uids` are the schemas plans manage -- never absorbed, and
    kept over any other -- and `skip_uids` those the name pass removes anyway.
    `space_uids` and `exclude_space_uids` restrict the scan by space uid, as
    for `read_instance`.  Groups come ordered by content hash.
    """
    import lamindb as ln
    from django.db.models import Count

    live = ln.Schema.objects.exclude(branch_id=-1).filter(
        is_type=False, hash__isnull=False
    )
    if space_uids:
        live = live.filter(space__uid__in=list(space_uids))
    if exclude_space_uids:
        live = live.exclude(space__uid__in=list(exclude_space_uids))
    if skip_uids:
        live = live.exclude(uid__in=list(skip_uids))
    shared = list(
        live.order_by()
        .values("hash")
        .annotate(holders=Count("pk"))
        .filter(holders__gt=1)
        .values_list("hash", flat=True)
    )
    if not shared:
        return []
    by_hash: dict[str, list[Any]] = {}
    for schema in live.filter(hash__in=shared).order_by("pk"):
        by_hash.setdefault(schema.hash, []).append(schema)
    managed = set(managed_uids)
    return [
        _content_group(
            settle_content_group(
                by_hash[content_hash],
                content_hash,
                managed=lambda schema: str(schema.uid) in managed,
            )
        )
        for content_hash in sorted(by_hash)
    ]


def _content_group(group: ContentGroup) -> ContentDuplicateGroup:
    """The report of a settled group, holding on to it for :func:`execute_dedup`."""
    keeper = group.keeper.pk if group.keeper is not None else None
    absorbed = {c.schema.pk: c for c in group.absorbed}
    blocked = {c.schema.pk: c for c in group.blocked}
    members: list[ContentMember] = []
    for schema in group.schemas:
        if schema.pk == keeper:
            action, detail = CONTENT_KEEP, ""
        elif schema.pk in absorbed:
            action = CONTENT_ABSORB
            detail = (
                f"inferred: recorded columns of {absorbed[schema.pk].inferred_links} "
                "artifact(s)"
            )
        elif schema.pk in blocked:
            action, detail = CONTENT_MANUAL, "; ".join(blocked[schema.pk].blockers)
        else:
            action = CONTENT_MANUAL
            detail = "an inferred schema, absorbed once the others are resolved"
        members.append(
            ContentMember(
                uid=str(schema.uid), name=schema.name, action=action, detail=detail
            )
        )
    return ContentDuplicateGroup(
        content_hash=group.content_hash,
        members=members,
        reason=_manual_reason(group),
        settled=group,
    )


def _manual_reason(group: ContentGroup) -> str:
    """Why `group` needs a decision by hand; empty when it does not."""
    if group.ok:
        return ""
    if group.mergeable:
        return (
            f"a plan manages {schema_label(group.keeper)}: point what references "
            "the others at it and move them to the trash by hand"
        )
    if group.managed > 1:
        return "plans manage more than one of them: change the plans so they differ"
    return "none of them can be absorbed into another: decide by hand which to keep"


_REGISTRY_GETTERS = {
    "Feature": lambda ln: ln.Feature,
    "Schema": lambda ln: ln.Schema,
}


def execute_dedup(plan: DuplicatePlan, *, permanent: bool = False) -> int:
    """Delete every duplicate marked for removal against the connected instance.

    By default the objects are soft-deleted (moved to the instance's trash, so
    the removal can be undone).  Pass `permanent=True` to delete them
    irreversibly.  The kept member of each group is never touched.  Every
    inferred schema of a content group not left for a decision by hand is then
    absorbed into the kept schema -- its links moved over, the schema deleted,
    as `apply` absorbs one; :attr:`DuplicatePlan.absorb_count` counts them.
    Returns the number of objects deleted by name.

    Runs in one transaction: a half-deduplicated group is not a state anyone
    chose, and with `permanent=True` there is no undo for the members already
    gone.
    """
    import lamindb as ln
    from django.db import transaction

    deleted = 0
    with transaction.atomic():
        for group in plan.groups:
            registry = _REGISTRY_GETTERS[group.registry](ln)
            for member in group.remove:
                record = registry.filter(uid=member.uid).one_or_none()
                if record is None:
                    continue
                delete_object(record, permanent=permanent)
                deleted += 1
        for content in plan.content_groups:
            if not content.manual and content.settled is not None:
                settle_schemas(content.settled, error=LaminSchemasError)
    return deleted


# === Rendering ================================================================


def _member_line(member: DuplicateMember, *, prefix: str, style: str) -> Any:
    from rich.text import Text

    line = Text(prefix, style=style)
    line.append(member.uid, style=style)
    if member.created_at is not None:
        line.append(f" (created {member.created_at})", style="dim")
    if member.created_by is not None:
        line.append(f" [{member.created_by}]", style="dim")
    if member.space_uid is not None:
        line.append(f" [space: {member.space_uid}]", style="dim")
    return line


def _content_lines(group: ContentDuplicateGroup) -> list[Any]:
    from rich.text import Text

    header = Text("  - ", style="bold red")
    header.append("schema content", style="bold cyan")
    header.append(f" {group.content_hash!r}", style="magenta")
    header.append(f"  ({len(group.members)} schemas share it)", style="dim")
    lines: list[Any] = [header]
    styles = {CONTENT_KEEP: "green", CONTENT_ABSORB: "red", CONTENT_MANUAL: "yellow"}
    for member in group.members:
        style = styles[member.action]
        line = Text(f"      {member.action:<6} ", style=style)
        line.append(member.uid, style=style)
        if member.name is not None:
            line.append(f" {member.name!r}", style="green")
        if member.detail:
            line.append(f" ({member.detail})", style="dim")
        lines.append(line)
    if group.manual:
        lines.append(
            Text(f"      needs a decision by hand: {group.reason}", style="bold red")
        )
    return lines


def _build_renderable(plan: DuplicatePlan, title: str) -> Any:
    from rich.console import Group
    from rich.text import Text

    lines: list[Any] = []
    for group in plan.groups:
        header = Text("  - ", style="bold red")
        header.append(group.kind, style="bold cyan")
        if group.type_name is not None:
            header.append(f" {group.type_name!r}", style="magenta")
            header.append(" > ", style="dim")
        header.append(f"{group.name!r}", style="green")
        header.append(f"  ({len(group.remove) + 1} duplicates)", style="dim")
        lines.append(header)
        lines.append(_member_line(group.keep, prefix="      keep   ", style="green"))
        for member in group.remove:
            lines.append(_member_line(member, prefix="      remove ", style="red"))
    for content in plan.content_groups:
        lines.extend(_content_lines(content))

    summary = Text()
    if plan.is_empty:
        summary = Text(
            "Remove duplicates: no duplicate features or schemas found.", style="dim"
        )
    else:
        summary.append("Remove duplicates: ", style="bold")
        summary.append(f"{plan.remove_count} object(s)", style="red")
        summary.append(" to delete across ")
        summary.append(f"{len(plan.groups)} group(s)", style="red")
        if plan.content_groups:
            summary.append("; ")
            summary.append(f"{plan.absorb_count} inferred schema(s)", style="red")
            summary.append(" to absorb into the schema sharing their content")
        if plan.manual_groups:
            summary.append("; ")
            summary.append(f"{len(plan.manual_groups)} group(s)", style="bold red")
            summary.append(" of schemas sharing a content need a decision by hand")
        summary.append(".", style="dim")

    header = Text(title, style="bold white")
    return Group(header, Text(), *lines, Text(), summary)


def render_dedup(plan: DuplicatePlan, *, title: str, color: bool = False) -> str:
    """Render the duplicate plan as a terraform-like string."""
    from rich.console import Console

    buffer = io.StringIO()
    console = Console(
        width=None if color else 200,
        file=buffer,
        force_terminal=color or None,
        color_system="auto" if color else None,
    )
    console.print(_build_renderable(plan, title))
    return buffer.getvalue()


def print_dedup(plan: DuplicatePlan, *, title: str) -> None:
    """Print the duplicate plan to stdout with color."""
    from rich.console import Console

    Console().print(_build_renderable(plan, title))
