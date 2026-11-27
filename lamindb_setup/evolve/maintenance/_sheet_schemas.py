"""Find and repair record types whose schema is a schema *type*.

lamindb's `Record.schema` foreign key accepts any :class:`~lamindb.Schema`,
including one with `is_type=True`.  A schema type only groups schemas: it
declares no features, so a record type pointed at one is a sheet that enforces
nothing.  The plan model has no way to express it either — `Sheet` takes a
`Schema` — so :mod:`~lamindb_setup.evolve.introspect` excludes such a record
type and the instance cannot be fully managed until it is repaired.

The repair depends on what the schema type carries:

* an **empty** schema type (no members, no slots, no `itype`) enforces nothing,
  so the record type's `schema` is cleared and it becomes a plain record type;
* a schema type that *does* carry content is a schema mislabeled as a type — or a
  real type that something was wrongly pointed at.  Clearing `is_type` would
  rehash the schema and orphan whatever types under it, so this is reported for a
  human decision rather than repaired.

:func:`compute_sheet_schemas` is read-only and operates on the lamindb-free
dataclasses read by :mod:`.introspect`, so it can be unit-tested without an
instance.  :func:`execute_sheet_schemas` applies the detachments against the
connected instance.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..introspect import InstanceContents

__all__ = [
    "SheetSchemaIssue",
    "SheetSchemasPlan",
    "compute_sheet_schemas",
    "execute_sheet_schemas",
    "print_sheet_schemas",
    "render_sheet_schemas",
]


@dataclass
class SheetSchemaIssue:
    """A record type whose `schema` is a schema type, with the repair to apply."""

    uid: str
    name: str | None
    schema_uid: str
    schema_name: str | None
    fixable: bool
    reason: str
    type_name: str | None = None
    created_by: str | None = None
    space_uid: str | None = None


@dataclass
class SheetSchemasPlan:
    """The resolved repair: every record type pointed at a schema type."""

    issues: list[SheetSchemaIssue] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        """Whether there is nothing to repair."""
        return not self.issues

    @property
    def fixable(self) -> list[SheetSchemaIssue]:
        """Issues whose record type can be detached from the schema type."""
        return [issue for issue in self.issues if issue.fixable]

    @property
    def unfixable(self) -> list[SheetSchemaIssue]:
        """Issues that need a human decision."""
        return [issue for issue in self.issues if not issue.fixable]


def compute_sheet_schemas(contents: InstanceContents) -> SheetSchemasPlan:
    """Find every record type whose `schema` points at a schema type.

    Read-only: nothing is changed.  An issue is fixable when the schema type is
    empty — no members, no slots and no `itype` — because then it enforces
    nothing and detaching it changes no validation.  A schema type carrying
    content is reported as unfixable: it is a schema mislabeled as a type, and
    clearing that flag is a decision with consequences for anything that types
    under it.
    """
    schema_types = {st.uid: st for st in contents.schema_types}

    issues: list[SheetSchemaIssue] = []
    for rt in contents.record_types:
        if rt.schema_uid is None:
            continue
        schema_type = schema_types.get(rt.schema_uid)
        if schema_type is None:
            continue

        empty = not (
            schema_type.feature_uids
            or schema_type.slots
            or schema_type.itype not in (None, "Feature", "Composite")
        )
        if empty:
            reason = (
                f"schema {schema_type.name!r} is a schema type ('is_type=True') "
                "with no members; it enforces nothing, so the record type is "
                "detached from it"
            )
        else:
            reason = (
                f"schema {schema_type.name!r} is a schema type ('is_type=True') "
                "but carries members; clear its 'is_type' flag to turn it into a "
                "schema, or point the record type at a real schema"
            )
        issues.append(
            SheetSchemaIssue(
                uid=rt.uid,
                name=rt.name,
                schema_uid=schema_type.uid,
                schema_name=schema_type.name,
                fixable=empty,
                reason=reason,
                type_name=rt.type_name,
                created_by=rt.created_by,
                space_uid=rt.space_uid,
            )
        )

    return SheetSchemasPlan(issues=issues)


def execute_sheet_schemas(plan: SheetSchemasPlan) -> int:
    """Clear `schema` on every fixable record type, returning how many changed.

    Runs in one transaction, so a failure part-way leaves every record type
    pointed where it was rather than detaching an arbitrary prefix of them.
    """
    import lamindb as ln
    from django.db import transaction

    detached = 0
    with transaction.atomic():
        for issue in plan.fixable:
            record = ln.Record.filter(uid=issue.uid).one_or_none()
            if record is None:
                continue
            setattr(record, "schema", None)  # noqa: B010
            record.save()
            detached += 1
    return detached


# === Rendering ================================================================


def _build_renderable(plan: SheetSchemasPlan, title: str) -> Any:
    from rich.console import Group
    from rich.text import Text

    lines: list[Any] = []
    for issue in plan.issues:
        marker = "  ~ " if issue.fixable else "  ! "
        style = "bold yellow" if issue.fixable else "bold red"
        line = Text(marker, style=style)
        line.append("record type", style="bold cyan")
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
        detail.append(
            f"schema {issue.schema_name!r} ({issue.schema_uid})", style="yellow"
        )
        if issue.fixable:
            detail.append(" -> ", style="dim")
            detail.append("none", style="green")
        else:
            detail.append(" cannot be detached safely", style="red")
        lines.append(detail)

        note = Text("        ")
        note.append(issue.reason, style="dim")
        lines.append(note)

    summary = Text()
    summary.append("Sheet schemas: ", style="bold")
    summary.append(f"{len(plan.fixable)} to detach", style="yellow")
    summary.append(", ")
    summary.append(f"{len(plan.unfixable)} to resolve by hand", style="red")
    if plan.is_empty:
        summary = Text(
            "Sheet schemas: no record type is pointed at a schema type.", style="dim"
        )

    header = Text(title, style="bold white")
    return Group(header, Text(), *lines, Text(), summary)


def render_sheet_schemas(
    plan: SheetSchemasPlan, *, title: str, color: bool = False
) -> str:
    """Render the sheet-schemas plan as a terraform-like string."""
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


def print_sheet_schemas(plan: SheetSchemasPlan, *, title: str) -> None:
    """Print the sheet-schemas plan to stdout with color."""
    from rich.console import Console

    Console().print(_build_renderable(plan, title))
