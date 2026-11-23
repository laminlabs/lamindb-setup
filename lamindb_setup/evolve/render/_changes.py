"""Render a computed set of changes for a human or a machine to read.

The terraform-like plan output, its Markdown and JSON counterparts, and the
advisory lines that accompany them.  All of it reads
:attr:`~lamindb_setup.evolve.engine._apply.ApplyContext.changes` and writes a string:
none of it touches the instance.

Split out of :mod:`lamindb_setup.evolve.engine._apply`, which had grown to five
thousand lines by fusing the engine with its own presentation layer -- so the
CLI and the CI reporter had to import the apply engine to format a diff they
had already computed.
"""

from __future__ import annotations

import io
from collections import Counter
from typing import TYPE_CHECKING, Any

from ..console import stderr_console
from ..core import Action, WarningCategory, warning_category

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from ..core import (
        ChangeItem,
        ChangeSet,
        MissingPrerequisite,
        MoveOutcome,
        PrerequisitePlan,
    )

__all__ = [
    "is_up_to_date",
    "missing_prerequisite_message",
    "print_changes",
    "render_changes",
    "render_changes_json",
    "render_changes_markdown",
    "render_move_outcome",
    "render_uid_misalignments",
    "uid_misalignment_message",
    "unmoved_rename_message",
    "unmoved_rename_refusal",
]


# The banner summarising each category of warning, by the number of changes
# carrying one.  A `CAVEAT` has none: its WARNING line says all there is.
_WARNING_BANNERS: dict[WarningCategory, str] = {
    WarningCategory.DRIFT: (
        "{count} object(s) changed outside the plan and will be auto-corrected "
        "to the plan; review the WARNING lines above."
    ),
    WarningCategory.RENAME: (
        "{count} change(s) look like a rename not declared with ctx.moved; "
        "review the WARNING lines above."
    ),
    WarningCategory.PREREQUISITE: (
        "{count} object(s) are owned by a prerequisite plan that is not applied "
        "to this instance."
    ),
}


def _warning_banners(changes: Iterable[ChangeItem]) -> list[str]:
    """One sentence per category of warning among `changes` that has a banner.

    Each counts the changes carrying a warning of its category, so remote drift
    is announced only for warnings that are drift (see
    :class:`~lamindb_setup.evolve.core.WarningCategory`).
    """
    counts: Counter[WarningCategory] = Counter()
    for change in changes:
        counts.update({warning_category(warning) for warning in change.warnings})
    return [
        banner.format(count=counts[category])
        for category, banner in _WARNING_BANNERS.items()
        if counts[category]
    ]


def _borrowed_part(borrowed: Sequence[ChangeItem]) -> str:
    """The summary's count of the borrowed objects a prerequisite has yet to create.

    A borrowed object the instance lacks is its prerequisite plan's to create,
    so it is counted apart from the plan's own creates (see
    :attr:`~lamindb_setup.evolve.core.ChangeItem.borrowed`).
    """
    return f"{len(borrowed)} to be created by prerequisite plans"


_UP_TO_DATE = "No changes. Instance is up to date."


def _is_noteworthy(change: ChangeItem) -> bool:
    """Whether the terminal diff lists `change` while it hides unchanged objects.

    Anything but an unchanged object is listed, and so is an unchanged one whose
    uid is misaligned or that carries a warning.  A borrowed object the instance
    already has is unchanged too: its `managed by a prerequisite plan` detail
    alone does not list it.
    """
    return (
        change.action is not Action.NO_CHANGE
        or change.uid_misalignment is not None
        or bool(change.warnings)
    )


def is_up_to_date(ctx: ChangeSet) -> bool:
    """Whether every change of `ctx` is an unchanged object with nothing to flag.

    The terminal diff then closes with `No changes. Instance is up to date.`
    (see :func:`print_changes`): nothing is created, updated, deleted, moved or
    transferred, and no unchanged object has a misaligned uid or a warning.
    """
    return not any(_is_noteworthy(change) for change in ctx.changes)


def _build_renderable(ctx: ChangeSet, title: str, *, show_unchanged: bool) -> Any:
    from rich.console import Group
    from rich.text import Text

    creates = [c for c in ctx.changes if c.action is Action.CREATE and not c.borrowed]
    borrowed = [c for c in ctx.changes if c.is_borrowed_create]
    updates = [c for c in ctx.changes if c.action is Action.UPDATE]
    deletes = [c for c in ctx.changes if c.action is Action.DELETE]
    moves = [c for c in ctx.changes if c.action is Action.MOVE]
    transfers = [c for c in ctx.changes if c.action is Action.TRANSFER]
    unchanged = [c for c in ctx.changes if c.action is Action.NO_CHANGE]
    banners = _warning_banners(ctx.changes)

    lines: list[Any] = []
    for change in ctx.changes:
        if not show_unchanged and not _is_noteworthy(change):
            continue
        if change.is_borrowed_create:
            # Comes from a prerequisite plan, as a transfer goes to a sibling.
            symbol = Text("  \u2190 ", style="bold blue")
            uid_style = "blue"
        elif change.action is Action.CREATE:
            symbol = Text("  + ", style="bold green")
            uid_style = "yellow"
        elif change.action is Action.UPDATE:
            symbol = Text("  ~ ", style="bold yellow")
            uid_style = "yellow"
        elif change.action is Action.DELETE:
            symbol = Text("  - ", style="bold red")
            uid_style = "red"
        elif change.action is Action.MOVE:
            symbol = Text("  \u21aa ", style="bold magenta")
            uid_style = "magenta"
        elif change.action is Action.TRANSFER:
            symbol = Text("  \u2192 ", style="bold blue")
            uid_style = "blue"
        else:
            symbol = Text("    ", style="dim")
            uid_style = "dim"
        line = Text()
        line.append_text(symbol)
        line.append(change.kind, style="bold cyan")
        if change.name is not None:
            line.append(f" {change.name!r}", style="green")
        line.append("  ")
        line.append(change.uid, style=uid_style)
        lines.append(line)
        if change.uid_misalignment is not None:
            uid_line = Text("      \u2260 ", style="bold magenta")
            uid_line.append(
                f"UID: {change.uid_misalignment.describe()}", style="bold magenta"
            )
            lines.append(uid_line)
        for warning in change.warnings:
            warning_line = Text("      ! ", style="bold red")
            warning_line.append(f"WARNING: {warning}", style="bold red")
            lines.append(warning_line)
        for detail in change.details:
            detail_line = Text("        ")
            detail_line.append(detail, style="yellow")
            lines.append(detail_line)

    summary = Text()
    summary.append("Plan: ", style="bold")
    summary.append(f"{len(creates)} to create", style="green")
    summary.append(", ")
    summary.append(f"{len(updates)} to update", style="yellow")
    summary.append(", ")
    summary.append(f"{len(deletes)} to delete", style="red")
    if moves:
        summary.append(", ")
        summary.append(f"{len(moves)} to move", style="magenta")
    if transfers:
        summary.append(", ")
        summary.append(f"{len(transfers)} to transfer", style="blue")
    summary.append(", ")
    summary.append(f"{len(unchanged)} unchanged", style="dim")
    if borrowed:
        summary.append(", ")
        summary.append(_borrowed_part(borrowed), style="blue")
    misaligned = [c for c in ctx.changes if c.uid_misalignment is not None]
    if misaligned:
        summary.append(", ")
        summary.append(f"{len(misaligned)} uid(s) misaligned", style="magenta")

    parts: list[Any] = [Text(title, style="bold white"), Text()]
    if lines:
        parts.extend([*lines, Text()])
    if banners:
        parts.extend(Text(f"! {banner}", style="bold red") for banner in banners)
        parts.append(Text())
    parts.append(summary)
    if is_up_to_date(ctx):
        parts.append(Text(_UP_TO_DATE))
    return Group(*parts)


def render_changes(
    ctx: ChangeSet, *, title: str, color: bool = False, show_unchanged: bool = False
) -> str:
    """Render the computed changes as a terraform-like plan string.

    As :func:`print_changes` prints them, unchanged objects included only with
    `show_unchanged`.
    """
    from rich.console import Console

    buffer = io.StringIO()
    console = Console(
        width=None if color else 200,
        file=buffer,
        force_terminal=color or None,
        color_system="auto" if color else None,
    )
    console.print(_build_renderable(ctx, title, show_unchanged=show_unchanged))
    return buffer.getvalue()


def print_changes(
    ctx: ChangeSet, *, title: str, err: bool = False, show_unchanged: bool = False
) -> None:
    """Print the computed changes with color to stdout, or to stderr with `err=True`.

    The stderr side goes through the shared
    :func:`~lamindb_setup.evolve.console.stderr_console`, for a caller whose
    stdout carries a Markdown or JSON document.

    An unchanged object is left out unless `show_unchanged` -- or its uid is
    misaligned or it carries a warning -- so the changes do not drown in a large
    plan; the summary line still counts it.  When nothing changes (see
    :func:`is_up_to_date`) the diff closes with `No changes. Instance is up to
    date.`, as the Markdown one does.
    """
    from rich.console import Console

    console = stderr_console() if err else Console()
    console.print(_build_renderable(ctx, title, show_unchanged=show_unchanged))


def render_move_outcome(
    outcome: MoveOutcome | None, *, instance_slug: str
) -> list[str]:
    """Return advisory lines about declared `ctx.moved` outcomes for `instance_slug`.

    Reports renames applied this run, notes when every declaration is already
    satisfied (so the `ctx.moved` calls can be removed once *all* instances are
    migrated), and warns about declarations that matched nothing (a typo, or a
    move already applied on this instance).  Returns an empty list when there
    are no declared moves.
    """
    if outcome is None or not (outcome.applied or outcome.already or outcome.unmatched):
        return []
    lines: list[str] = []
    if outcome.applied:
        lines.append(
            f"Renamed {len(outcome.applied)} object(s) in place on "
            f"{instance_slug}; keep the ctx.moved(...) declarations until every "
            "instance is migrated."
        )
    for old, new in outcome.unmatched:
        lines.append(
            f"! ctx.moved matched nothing on {instance_slug}: {old!r} -> {new!r} "
            "(already applied here, moved to a sibling plan, or a typo)."
        )
    if outcome.already and not outcome.applied and not outcome.unmatched:
        lines.append(
            f"All ctx.moved(...) declarations are already satisfied on "
            f"{instance_slug}; safe to remove them once every instance is migrated."
        )
    return lines


def _misaligned(changes: Iterable[ChangeItem]) -> list[ChangeItem]:
    return [c for c in changes if c.uid_misalignment is not None]


def _misalignment_line(change: ChangeItem) -> str:
    name = f" {change.name!r}" if change.name is not None else ""
    described = change.uid_misalignment.describe() if change.uid_misalignment else ""
    return f"  {change.kind}{name}: {described}"


def render_uid_misalignments(ctx: ChangeSet, *, instance_slug: str) -> list[str]:
    """Return advisory lines listing the objects whose uid is misaligned.

    An object is misaligned when its uid on `instance_slug` is not the one
    the plan's lock files for its other instances record (see
    :attr:`~lamindb_setup.evolve.core.ChangeItem.uid_misalignment`).  Returns an
    empty list when every uid agrees.
    """
    misaligned = _misaligned(ctx.changes)
    if not misaligned:
        return []
    return [
        f"! {len(misaligned)} object(s) on {instance_slug} do not have the uid "
        "this plan's other instances record; align them with 'lamin evolve "
        f"repair align-uids --source <instance> -i {instance_slug}':",
        *(_misalignment_line(change) for change in misaligned),
    ]


def uid_misalignment_message(misaligned: Sequence[ChangeItem]) -> str:
    """Return the error message for misaligned uids (`--strict-uids`)."""
    lines = "\n".join(_misalignment_line(change) for change in misaligned)
    return (
        f"{len(misaligned)} object(s) do not have the uid the plan's lock files "
        "for its other instances record, so the instances would no longer agree "
        f"on what is the same object:\n{lines}\n"
        "Align them first with 'lamin evolve repair align-uids --source "
        "<instance> -i <instance>', or pass --no-strict-uids to apply anyway."
    )


def unmoved_rename_message(suspected: Sequence[tuple[str, str, str]]) -> str:
    """Return the error message for suspected undeclared renames (strict mode).

    `suspected` is a list of `(kind, old_name, new_name)` triples recorded on
    :attr:`ApplyContext.suspected_renames`.
    """
    pairs = "\n".join(f"  {kind} {old!r} -> {new!r}" for kind, old, new in suspected)
    return (
        f"{len(suspected)} change(s) look like a rename but were not declared "
        "with ctx.moved, so applying would delete the old object and create a "
        "new one (losing its uid and everything validated against it):\n"
        f"{pairs}\n"
        'Declare ctx.moved(<object>, "<old name>") to rename in place, or pass '
        "--no-strict-renames if these are genuinely distinct objects."
    )


def unmoved_rename_refusal(
    plan_name: str, suspected: Sequence[tuple[str, str, str]]
) -> str:
    """Return the error refusing to apply `plan_name` over undeclared renames.

    What a strict `apply` raises, and what `plan` and `ci plan` report so
    they fail exactly when it would; `suspected` is as for
    :func:`unmoved_rename_message`.
    """
    return f"refusing to apply {plan_name!r}: {unmoved_rename_message(suspected)}"


def missing_prerequisite_message(
    plan_name: str,
    missing: Sequence[MissingPrerequisite],
    *,
    instance: str,
) -> str:
    """Return the error refusing to apply a plan before its prerequisites.

    `missing` are the borrowed objects the preview did not find on
    `instance` (see
    :attr:`~lamindb_setup.evolve.engine._apply.ApplyContext.missing_prerequisites`),
    grouped here by the prerequisite plan declaring them.  `instance` is named
    as the command was given it, so the command the message suggests runs as is.
    """
    groups: dict[PrerequisitePlan | None, list[MissingPrerequisite]] = {}
    for item in missing:
        groups.setdefault(item.owner, []).append(item)
    count = f"{len(missing)} object(s) it borrows from"
    exist = f"do not exist on {instance}"
    if len(groups) == 1:
        ((owner, items),) = groups.items()
        listing = "  " + ", ".join(item.describe() for item in items)
        head = f"{count} {_prerequisite_label(owner, single=True)} {exist}"
        that, together = "that plan", "list both plans in one apply"
    else:
        listing = "\n".join(
            f"  from {_prerequisite_label(owner)}: "
            + ", ".join(item.describe() for item in items)
            for owner, items in groups.items()
        )
        head = f"{count} prerequisite plans {exist}"
        that, together = "those plans", "list them in one apply with this one"
    targets = [owner.target for owner in groups if owner is not None and owner.target]
    command = (
        f" (lamin evolve apply {' '.join(targets)} -i {instance})"
        if len(targets) == len(groups)
        else ""
    )
    return (
        f"refusing to apply {plan_name!r}: {head}:\n{listing}\n"
        f"Apply {that} first{command}, or {together} -- they are applied "
        "dependencies-first."
    )


def _prerequisite_label(owner: PrerequisitePlan | None, *, single: bool = False) -> str:
    """Name a prerequisite plan: `'base' (plans/plan_base.py)`."""
    if owner is None:
        return "a prerequisite plan" if single else "an unknown prerequisite plan"
    label = f"{owner.name!r}" + (f" ({owner.target})" if owner.target else "")
    return f"prerequisite plan {label}" if single else label


# Diff symbols recognized by GitHub's `diff` syntax highlighting.
_MARKDOWN_ACTION_SYMBOL: dict[Action, str] = {
    Action.CREATE: "+",
    Action.UPDATE: "~",
    Action.DELETE: "-",
    Action.MOVE: "!",
    Action.TRANSFER: ">",
    Action.NO_CHANGE: " ",
}


def render_changes_json(
    ctx: ChangeSet, *, title: str, notes: list[str] | None = None
) -> str:
    """Render the computed changes as a machine-readable JSON document.

    The counterpart of :func:`render_changes_markdown` for callers that consume
    a diff rather than read it -- a CI step gating on what would change, say.
    `counts` is derived rather than left to the caller so that the summary a
    script asserts on is the same one the rendered views report.

    `notes` are the advisory lines the other views append as prose (declared
    moves, sheet exports); they are carried as data here rather than pasted into
    the document, so a consumer can ignore or surface them as it likes.

    `warning_categories` gives each of a change's `warnings` its
    :class:`~lamindb_setup.evolve.core.WarningCategory`, index by index, so a
    consumer can tell remote drift from a rename hint or a missing prerequisite
    while `warnings` stays the list of texts it always was.

    `borrowed` marks an object borrowed from a prerequisite plan.  A borrowed
    `create` is that plan's to make, so `counts` tallies it under
    `borrowed_create` rather than `create`, which counts the plan's own.

    `obj_id` is an `id()` value, stable only within one process, so it is
    dropped: `key` is the natural key and is what identifies an object across
    runs.
    """
    import json

    counts: dict[str, int] = {action.value: 0 for action in Action}
    counts["borrowed_create"] = 0
    changes: list[dict[str, Any]] = []
    for change in ctx.changes:
        entry = {
            "key": change.key,
            "kind": change.kind,
            "name": change.name,
            "action": change.action.value,
            "uid": change.uid,
            "details": list(change.details),
            "warnings": list(change.warnings),
            "warning_categories": [
                warning_category(warning).value for warning in change.warnings
            ],
            "borrowed": change.borrowed,
        }
        if change.uid_misalignment is not None:
            entry["uid_misalignment"] = {  # type: ignore[assignment]
                "uid": change.uid_misalignment.uid,
                "others": dict(change.uid_misalignment.others),
                "reason": change.uid_misalignment.reason,
            }
        changes.append(entry)
        counts[
            "borrowed_create" if change.is_borrowed_create else change.action.value
        ] += 1
    counts["uid_misaligned"] = len(_misaligned(ctx.changes))
    return json.dumps(
        {
            "title": title,
            "counts": counts,
            "changes": changes,
            "notes": list(notes or []),
        },
        indent=2,
    )


def render_changes_markdown(ctx: ChangeSet, *, title: str) -> str:
    """Render the computed changes as a GitHub-flavored Markdown diff.

    The output is a self-contained Markdown fragment suitable for posting as a
    pull request comment: a heading, a one-line summary, and a fenced `diff`
    block listing the create / update / delete changes.  No-change objects are
    omitted from the diff so the comment stays focused on what would change.
    A borrowed object a prerequisite plan has yet to create is listed as a
    context line, and counted apart from the plan's own creates.
    """
    creates = [c for c in ctx.changes if c.action is Action.CREATE and not c.borrowed]
    borrowed = [c for c in ctx.changes if c.is_borrowed_create]
    updates = [c for c in ctx.changes if c.action is Action.UPDATE]
    deletes = [c for c in ctx.changes if c.action is Action.DELETE]
    moves = [c for c in ctx.changes if c.action is Action.MOVE]
    transfers = [c for c in ctx.changes if c.action is Action.TRANSFER]
    unchanged = [c for c in ctx.changes if c.action is Action.NO_CHANGE]
    banners = _warning_banners(ctx.changes)
    misaligned = _misaligned(ctx.changes)
    actionable = creates + updates + deletes + moves + transfers + borrowed

    lines: list[str] = [f"### {title}", ""]
    move_part = f"{len(moves)} to move, " if moves else ""
    transfer_part = f"{len(transfers)} to transfer, " if transfers else ""
    borrowed_part = f", {_borrowed_part(borrowed)}" if borrowed else ""
    misaligned_part = f" {len(misaligned)} uid(s) misaligned." if misaligned else ""
    lines.append(
        f"**Plan:** {len(creates)} to create, {len(updates)} to update, "
        f"{len(deletes)} to delete, {move_part}{transfer_part}"
        f"{len(unchanged)} unchanged{borrowed_part}.{misaligned_part}"
    )
    lines.append("")

    if not actionable and not misaligned:
        lines.append(_UP_TO_DATE)
        return "\n".join(lines).rstrip() + "\n"

    lines.append("```diff")
    for change in ctx.changes:
        if change.action is Action.NO_CHANGE and change.uid_misalignment is None:
            continue
        # A context line, which GitHub's diff highlighting leaves uncoloured.
        symbol = " " if change.borrowed else _MARKDOWN_ACTION_SYMBOL[change.action]
        name = f" {change.name!r}" if change.name is not None else ""
        lines.append(f"{symbol} {change.kind}{name}  {change.uid}")
        if change.uid_misalignment is not None:
            lines.append(f"  ! UID: {change.uid_misalignment.describe()}")
        for warning in change.warnings:
            lines.append(f"  ! WARNING: {warning}")
        for detail in change.details:
            lines.append(f"    {detail}")
    lines.append("```")

    if banners:
        lines.append("")
        lines.append("> [!WARNING]\n" + "\n>\n".join(f"> {b}" for b in banners))
    if misaligned:
        lines.append("")
        lines.append(
            f"> [!CAUTION]\n> {len(misaligned)} object(s) do not have the uid this "
            "plan's other instances record; review the UID lines above and align "
            "them with `lamin evolve repair align-uids`."
        )

    return "\n".join(lines).rstrip() + "\n"
