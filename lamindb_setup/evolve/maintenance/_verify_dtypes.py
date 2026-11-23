"""Find and repair features whose stored dtype string is broken.

Two broken categorical dtype forms are recognised:

* a bare `cat` (no registry) — lamindb's modern form is `cat[ULabel]`, so the
  feature is rewritten to `cat[ULabel]`;
* `cat[Record[<record-type-name>]]` — a record dtype must address its record
  type by *uid*, not name, so the name is resolved to the record type's uid and
  the feature is rewritten to `cat[Record[<record-type-uid>]]`.

When a `cat[Record[...]]` name cannot be resolved to a live record type, the
feature cannot be repaired; it is flagged for soft deletion (moved to the trash)
as the suggested remediation.

:func:`compute_verify_dtypes` is read-only and operates on the lamindb-free
dataclasses read by :mod:`.introspect`, so it can be unit-tested without an
instance.  :func:`execute_verify_dtypes` performs the fixes and deletions
against the connected instance.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ..engine import delete_object

if TYPE_CHECKING:
    from ..introspect import InstanceContents

__all__ = [
    "DtypeIssue",
    "VerifyDtypesPlan",
    "compute_verify_dtypes",
    "execute_verify_dtypes",
    "print_verify_dtypes",
    "render_verify_dtypes",
]

_RECORD_DTYPE_RE = re.compile(r"^cat\[Record\[(?P<token>[^\]]+)\]\]$")


@dataclass
class DtypeIssue:
    """A feature whose dtype is broken, with the repair (or deletion) to apply."""

    uid: str
    name: str | None
    current_dtype: str
    fixable: bool
    reason: str
    proposed_dtype: str | None = None
    recordtype_name: str | None = None
    recordtype_uid: str | None = None
    recordtype_deleted: bool = False
    type_name: str | None = None
    created_by: str | None = None
    space_uid: str | None = None
    requires_manual_resolution: bool = False


@dataclass
class VerifyDtypesPlan:
    """The resolved repair: every feature with a broken dtype to fix or delete."""

    issues: list[DtypeIssue] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        """Whether there is nothing to verify or repair."""
        return not self.issues

    @property
    def fixable(self) -> list[DtypeIssue]:
        """Issues whose dtype can be rewritten in place."""
        return [issue for issue in self.issues if issue.fixable]

    @property
    def unfixable(self) -> list[DtypeIssue]:
        """Issues that cannot be rewritten automatically."""
        return [issue for issue in self.issues if not issue.fixable]

    @property
    def deletable(self) -> list[DtypeIssue]:
        """Unfixable issues whose suggested remediation is soft deletion."""
        return [
            issue
            for issue in self.issues
            if not issue.fixable and not issue.requires_manual_resolution
        ]

    @property
    def manual(self) -> list[DtypeIssue]:
        """Issues that need a human to select the intended record type."""
        return [issue for issue in self.issues if issue.requires_manual_resolution]


def compute_verify_dtypes(contents: InstanceContents) -> VerifyDtypesPlan:
    """Find every feature with a broken dtype, resolving the repair to apply.

    Read-only: nothing is changed.  A feature is flagged when its dtype string is
    a bare `cat` (rewritten to `cat[ULabel]`) or `cat[Record[<name>]]` whose
    name resolves to a record type uid (rewritten to `cat[Record[<uid>]]`).  The
    record type is addressed by the leading comma-separated component of the
    `cat[Record[...]]` token; any trailing categorical filters (e.g.
    `is_type='True'`, `schema__uid='...'`) are preserved on a rewrite.  A
    token whose leading component is already a live record type uid is correct and
    skipped; one that resolves to neither a live uid nor name is flagged as
    unfixable (suggested for soft deletion).  When such a component instead names
    a *trashed* record type (by uid or name), the issue records that the record
    type exists but is deleted, rather than that it does not exist.
    """
    record_uids = {rt.uid for rt in contents.record_types}
    name_to_uids: dict[str, set[str]] = {}
    for rt in contents.record_types:
        if rt.name is not None:
            name_to_uids.setdefault(rt.name, set()).add(rt.uid)

    deleted_record_uids = contents.deleted_record_uids
    deleted_name_to_uids: dict[str, set[str]] = {}
    for uid, name in contents.deleted_record_names.items():
        deleted_name_to_uids.setdefault(name, set()).add(uid)

    issues: list[DtypeIssue] = []
    for f in contents.features:
        dtype_str = f.dtype_str
        if not dtype_str:
            continue

        if dtype_str == "cat":
            issues.append(
                DtypeIssue(
                    uid=f.uid,
                    name=f.name,
                    current_dtype=dtype_str,
                    fixable=True,
                    reason="bare 'cat' has no registry",
                    proposed_dtype="cat[ULabel]",
                    type_name=f.type_name,
                    created_by=f.created_by,
                    space_uid=f.space_uid,
                )
            )
            continue

        match = _RECORD_DTYPE_RE.match(dtype_str)
        if match is None:
            continue
        token = match.group("token")

        # The token may carry categorical filters (e.g.
        # `uid, is_type='True', schema__uid='...'`); the record type is
        # addressed by the leading comma-separated component, the rest are
        # filters that are preserved on any rewrite.
        parts = token.split(",", 1)
        ident = parts[0].strip()
        suffix = f",{parts[1]}" if len(parts) > 1 else ""

        if ident in record_uids:
            # Already addresses a live record type by uid; nothing to repair.
            continue

        resolved_uids = sorted(name_to_uids.get(ident, ()))
        if len(resolved_uids) > 1:
            issues.append(
                DtypeIssue(
                    uid=f.uid,
                    name=f.name,
                    current_dtype=dtype_str,
                    fixable=False,
                    reason=(
                        f"multiple live record types are named {ident!r} "
                        f"(uids {', '.join(resolved_uids)}); choose the intended "
                        "uid manually"
                    ),
                    recordtype_name=ident,
                    type_name=f.type_name,
                    created_by=f.created_by,
                    space_uid=f.space_uid,
                    requires_manual_resolution=True,
                )
            )
            continue
        if resolved_uids:
            resolved_uid = resolved_uids[0]
            issues.append(
                DtypeIssue(
                    uid=f.uid,
                    name=f.name,
                    current_dtype=dtype_str,
                    fixable=True,
                    reason="record type addressed by name instead of uid",
                    proposed_dtype=f"cat[Record[{resolved_uid}{suffix}]]",
                    recordtype_name=ident,
                    recordtype_uid=resolved_uid,
                    type_name=f.type_name,
                    created_by=f.created_by,
                    space_uid=f.space_uid,
                )
            )
            continue

        # Not a live record type: distinguish a reference to a trashed record
        # type (exists but deleted) from one that does not exist at all.
        deleted_uid: str | None = None
        deleted_name: str | None = None
        if ident in deleted_record_uids:
            deleted_uid = ident
            deleted_name = contents.deleted_record_names.get(ident)
        elif len(deleted_name_to_uids.get(ident, ())) == 1:
            deleted_name = ident
            deleted_uid = next(iter(deleted_name_to_uids[ident]))
        elif ident in deleted_name_to_uids:
            deleted_uids = sorted(deleted_name_to_uids[ident])
            issues.append(
                DtypeIssue(
                    uid=f.uid,
                    name=f.name,
                    current_dtype=dtype_str,
                    fixable=False,
                    reason=(
                        f"multiple deleted record types are named {ident!r} "
                        f"(uids {', '.join(deleted_uids)}); choose the intended "
                        "uid manually"
                    ),
                    recordtype_name=ident,
                    recordtype_deleted=True,
                    type_name=f.type_name,
                    created_by=f.created_by,
                    space_uid=f.space_uid,
                    requires_manual_resolution=True,
                )
            )
            continue

        if deleted_uid is not None or deleted_name is not None:
            label = deleted_name if deleted_name is not None else deleted_uid
            issues.append(
                DtypeIssue(
                    uid=f.uid,
                    name=f.name,
                    current_dtype=dtype_str,
                    fixable=False,
                    reason=(
                        f"record type {label!r} exists but is deleted (trashed); "
                        "suggest soft deleting the feature"
                    ),
                    recordtype_name=deleted_name,
                    recordtype_uid=deleted_uid,
                    recordtype_deleted=True,
                    type_name=f.type_name,
                    created_by=f.created_by,
                    space_uid=f.space_uid,
                )
            )
        else:
            issues.append(
                DtypeIssue(
                    uid=f.uid,
                    name=f.name,
                    current_dtype=dtype_str,
                    fixable=False,
                    reason=(
                        f"no record type named {ident!r}; "
                        "suggest soft deleting the feature"
                    ),
                    recordtype_name=ident,
                    type_name=f.type_name,
                    created_by=f.created_by,
                    space_uid=f.space_uid,
                )
            )

    return VerifyDtypesPlan(issues=issues)


def execute_verify_dtypes(plan: VerifyDtypesPlan) -> tuple[int, int]:
    """Apply the plan against the connected instance.

    Fixable issues have their feature's stored dtype string rewritten in place.
    Unfixable issues have their feature soft-deleted (moved to the instance's
    trash, so the removal can be undone).  Returns `(fixed, deleted)` counts.

    Runs in one transaction, so a failure part-way cannot leave some features
    repaired and others still holding a broken dtype string -- a mixture the
    report would misdescribe and a re-run would have to rediscover.
    """
    import lamindb as ln
    from django.db import transaction

    fixed = 0
    deleted = 0
    with transaction.atomic():
        for issue in plan.issues:
            record = ln.Feature.filter(uid=issue.uid).one_or_none()
            if record is None:
                continue
            if issue.fixable:
                setattr(record, "_dtype_str", issue.proposed_dtype)  # noqa: B010
                record.save()
                fixed += 1
            elif not issue.requires_manual_resolution:
                delete_object(record)
                deleted += 1
    return fixed, deleted


# === Rendering ================================================================


def _build_renderable(plan: VerifyDtypesPlan, title: str) -> Any:
    from rich.console import Group
    from rich.text import Text

    lines: list[Any] = []
    for issue in plan.issues:
        if issue.fixable:
            marker, style = "  ~ ", "bold yellow"
        elif issue.requires_manual_resolution:
            marker, style = "  ! ", "bold magenta"
        else:
            marker, style = "  - ", "bold red"
        line = Text(marker, style=style)
        line.append("feature", style="bold cyan")
        if issue.type_name is not None:
            line.append(f" {issue.type_name!r}", style="magenta")
            line.append(" > ", style="dim")
        if issue.name is not None:
            line.append(f"{issue.name!r}", style="green")
        line.append("  ")
        line.append(issue.uid, style=style)
        if issue.created_by is not None:
            line.append(f" [{issue.created_by}]", style="dim")
        if issue.space_uid is not None:
            line.append(f" [space: {issue.space_uid}]", style="dim")
        lines.append(line)

        detail = Text("        ")
        detail.append(f"dtype {issue.current_dtype!r}", style="yellow")
        if issue.fixable:
            detail.append(" -> ", style="dim")
            detail.append(f"{issue.proposed_dtype!r}", style="green")
        elif issue.requires_manual_resolution:
            detail.append(" needs manual resolution", style="magenta")
        else:
            detail.append(" cannot be fixed", style="red")
        lines.append(detail)

        if issue.recordtype_name is not None or issue.recordtype_uid is not None:
            rt = Text("        ")
            if issue.fixable and issue.recordtype_uid is not None:
                rt.append(
                    f"record type {issue.recordtype_name!r} -> {issue.recordtype_uid}",
                    style="dim",
                )
            elif issue.recordtype_deleted:
                if issue.recordtype_name is not None:
                    target = repr(issue.recordtype_name)
                    if issue.recordtype_uid is not None:
                        target += f" ({issue.recordtype_uid})"
                else:
                    target = str(issue.recordtype_uid)
                rt.append(
                    f"record type {target} exists but is deleted (trashed)",
                    style="dim",
                )
            elif issue.requires_manual_resolution:
                rt.append(
                    f"record type name {issue.recordtype_name!r} is ambiguous",
                    style="dim",
                )
            else:
                rt.append(
                    f"record type {issue.recordtype_name!r} does not exist",
                    style="dim",
                )
            lines.append(rt)

        note = Text("        ")
        note.append(issue.reason, style="dim")
        lines.append(note)

    summary = Text()
    summary.append("Verify dtypes: ", style="bold")
    summary.append(f"{len(plan.fixable)} fixable", style="yellow")
    summary.append(", ")
    summary.append(f"{len(plan.deletable)} to soft delete", style="red")
    summary.append(", ")
    summary.append(f"{len(plan.manual)} need manual resolution", style="magenta")
    if plan.is_empty:
        summary = Text("Verify dtypes: no broken dtype found.", style="dim")

    header = Text(title, style="bold white")
    return Group(header, Text(), *lines, Text(), summary)


def render_verify_dtypes(
    plan: VerifyDtypesPlan, *, title: str, color: bool = False
) -> str:
    """Render the repair dtypes plan as a terraform-like string."""
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


def print_verify_dtypes(plan: VerifyDtypesPlan, *, title: str) -> None:
    """Print the repair dtypes plan to stdout with color."""
    from rich.console import Console

    Console().print(_build_renderable(plan, title))
