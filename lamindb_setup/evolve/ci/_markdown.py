"""Render a :class:`~lamindb_setup.evolve.ci._pr.PRReport` as Markdown.

The output is a self-contained Markdown document suitable for posting as a pull
request comment: a summary line, a validation section, and a collapsible diff per
deployment.  Long diffs are truncated so the comment stays within GitHub's size
limit, and the result is deterministic so a workflow can update one comment in
place.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..render import code, object_label, object_ref, table_cell

if TYPE_CHECKING:
    from ..render import ObjectInfo, SummaryModel
    from ._drift import DriftResult, InstanceDriftResult
    from ._missing_exports import InstanceMissingExports, MissingExportsResult
    from ._pr import DeploymentDiff, PRReport
    from ._verify import VerifyResult

__all__ = [
    "render_drift_report",
    "render_missing_exports_report",
    "render_report",
    "render_verify_report",
]

# A conservative per-deployment cap; GitHub comments are limited to ~65k chars.
_MAX_CHANGES_PER_DIFF = 50

# Cap on objects rendered per group in the untracked-objects hierarchy so the
# drift comment stays well within GitHub's size limit even for large instances.
_MAX_OBJECTS_PER_GROUP = 25

# Cap on objects listed per plan in the export suggestions block.
_MAX_EXPORT_OBJECTS = 25

_MARKER = "<!-- lamin-schemas-ci -->"

_ACTION_SYMBOL = {
    "create": "+",
    "update": "~",
    "delete": "-",
    "move": "!",
    "transfer": ">",
    "no-change": " ",
}


def _diff_counts(diff: DeploymentDiff) -> tuple[int, int, int, int, int]:
    # A borrowed object is its prerequisite plan's to create, not this plan's.
    creates = sum(1 for c in diff.changes if c.action == "create" and not c.borrowed)
    updates = sum(1 for c in diff.changes if c.action == "update")
    deletes = sum(1 for c in diff.changes if c.action == "delete")
    moves = sum(1 for c in diff.changes if c.action == "move")
    transfers = sum(1 for c in diff.changes if c.action == "transfer")
    return creates, updates, deletes, moves, transfers


def _render_diff(diff: DeploymentDiff, *, strict_uids: bool = True) -> str:
    title = f"`{diff.plan_target}` → `{diff.instance_slug}`"
    if diff.ok:
        return "\n".join(_render_changes(diff, label=title, strict_uids=strict_uids))
    lines = [f"#### \u274c {title}", ""]
    if diff.changes:
        # The diff ran, but the apply would refuse it: show what it refuses.
        lines.extend(_render_changes(diff, label="Diff", strict_uids=strict_uids))
        lines.append("")
    lines.append("```")
    lines.append(diff.error or "diff failed")
    lines.append("```")
    return "\n".join(lines)


def _render_changes(
    diff: DeploymentDiff, *, label: str, strict_uids: bool
) -> list[str]:
    """The collapsible block listing `diff`'s changes, summarised after `label`."""
    lines: list[str] = []
    creates, updates, deletes, moves, transfers = _diff_counts(diff)
    summary = f"{creates} to create, {updates} to update, {deletes} to delete"
    if moves:
        summary += f", {moves} to move"
    if transfers:
        summary += f", {transfers} to transfer"
    if borrowed := diff.borrowed:
        summary += f", {len(borrowed)} to be created by prerequisite plans"
    misaligned = diff.misaligned
    if misaligned:
        summary += f", {len(misaligned)} uid(s) misaligned"
    actionable = diff.actionable
    # A misalignment apply goes ahead with is reported collapsed: with
    # strict_uids off it can cover most of a plan, and nothing waits on it.
    # Likewise a borrowed object a configured prerequisite creates; one no
    # configured plan creates is refused by apply, so it is expanded.
    expanded = actionable or diff.missing_prerequisites or (misaligned and strict_uids)
    open_attr = " open" if expanded else ""
    lines.append(f"<details{open_attr}>")
    lines.append(f"<summary>{label} \u2014 {summary}</summary>")
    lines.append("")
    if diff.lock_missing:
        lines.append(
            f"> **Note:** No lock file for `{diff.plan_target}` on "
            f"`{diff.instance_slug}`. Objects already present in the instance are "
            "shown as unchanged and will be adopted into this plan's lock on apply."
        )
        lines.append("")
    if misaligned:
        lines.extend(_render_misaligned(diff, strict_uids=strict_uids))
    # The borrowed objects are listed among the plan's own changes, as context
    # lines rather than as creates.
    listed = [c for c in diff.changes if c.action != "no-change"]
    if not listed:
        if not misaligned:
            lines.append("No changes. Instance is up to date.")
            lines.append("")
        lines.append("</details>")
        return lines

    lines.append("```diff")
    shown = listed[:_MAX_CHANGES_PER_DIFF]
    for change in shown:
        symbol = " " if change.borrowed else _ACTION_SYMBOL.get(change.action, " ")
        name = f" {change.name!r}" if change.name else ""
        lines.append(f"{symbol} {change.kind}{name}  {change.uid}")
        for detail in change.details:
            lines.append(f"    {detail}")
        for warning in change.warnings:
            lines.append(f"  ! WARNING: {warning}")
    if len(listed) > _MAX_CHANGES_PER_DIFF:
        lines.append(f"... {len(listed) - _MAX_CHANGES_PER_DIFF} more change(s)")
    lines.append("```")
    lines.append("")
    lines.append("</details>")
    return lines


def _render_misaligned(diff: DeploymentDiff, *, strict_uids: bool) -> list[str]:
    """The block listing the objects whose uid disagrees across instances."""
    misaligned = diff.misaligned
    align = (
        f"`lamin evolve repair align-uids --source <instance> -i {diff.instance_slug}`"
    )
    found = (
        f"{len(misaligned)} object(s) on `{diff.instance_slug}` do not have the uid "
        "this plan's lock files for its other instances record"
    )
    if strict_uids:
        callout = (
            f"> [!CAUTION]\n> {found}, so `apply` refuses them. Align them with "
            f"{align}."
        )
    else:
        callout = (
            f"> [!NOTE]\n> {found}. `strict_uids` is off, so `apply` goes ahead "
            f"regardless; align them with {align} to make them consistent."
        )
    lines = [
        callout,
        "",
        "| Kind | Name | Uids |",
        "| --- | --- | --- |",
    ]
    for change in misaligned[:_MAX_CHANGES_PER_DIFF]:
        lines.append(
            f"| {change.kind} | {table_cell(change.name or '')} | "
            f"{table_cell(change.uid_misalignment or '')} |"
        )
    if len(misaligned) > _MAX_CHANGES_PER_DIFF:
        lines.append(f"| ... | {len(misaligned) - _MAX_CHANGES_PER_DIFF} more | |")
    lines.append("")
    return lines


def render_report(report: PRReport) -> str:
    """Render `report` as a Markdown pull-request comment."""
    lines: list[str] = [_MARKER, "## lamin evolve CI"]

    if report.has_errors:
        lines.append("\u274c **Validation failed.** See details below.")
    elif report.has_changes:
        lines.append(
            "\u26a0\ufe0f **Plan shows pending changes.** Review the diffs below."
        )
    else:
        lines.append("\u2705 **All plans validated; no changes against any instance.**")
    lines.append("")
    if report.preview_migrations:
        lines.append(
            "> [!NOTE]\n> Migrations rehearsed: each instance's plans were applied "
            "in a transaction that was rolled back, so the outcomes below are "
            "what `ci apply` would do to the data."
        )
        lines.append("")

    # Validation section.
    lines.append("### Validation")
    if not report.validations:
        lines.append("_No plans selected._")
    else:
        for validation in report.validations:
            if validation.ok:
                lines.append(f"- \u2705 `{validation.target}`")
            else:
                lines.append(f"- \u274c `{validation.target}`")
                lines.append(f"  ```\n  {validation.error}\n  ```")
    lines.append("")

    # Diff section.
    if report.diffs:
        lines.append("### Plan diffs")
        for diff in report.diffs:
            lines.append(_render_diff(diff, strict_uids=report.strict_uids))
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


_DRIFT_MARKER = "<!-- lamin-schemas-drift -->"


# The plan-summary renderer shows an object exactly this way; sharing the two
# functions is what stops the drift report and the plan summary describing the
# same object differently.
_object_label = object_label
_object_ref = object_ref


def _object_lines(
    obj: ObjectInfo, model: SummaryModel, depth: int, budget: list[int]
) -> list[str]:
    """Render `obj` and its nested children, decrementing the shared `budget`.

    `budget` is a single-element list used as a mutable counter of how many more
    objects may be rendered in the current group; descent stops once it reaches 0
    so a group with many objects is truncated rather than blowing the size cap.
    """
    if budget[0] <= 0:
        return []
    budget[0] -= 1
    indent = "  " * depth
    lines = [f"{indent}- {_object_label(obj)}"]
    for req in model.requirements_of(obj):
        lines.append(f"{indent}  - _requires_ {_object_ref(req)}")
    for child in model.children_of(obj):
        if budget[0] <= 0:
            break
        lines.extend(_object_lines(child, model, depth + 1, budget))
    return lines


def _render_untracked_summary(summary: SummaryModel) -> list[str]:
    """Render the untracked objects as a grouped, type-nested, capped hierarchy.

    Mirrors the `validate` summary layout (objects grouped into Features /
    Schemas / Records and nested under their `type` parent) inside a collapsed
    `<details>` block, capping each group at :data:`_MAX_OBJECTS_PER_GROUP`.
    """
    lines = ["<details>", "<summary>Untracked objects by type</summary>", ""]
    for group_name, objects in summary.grouped_objects:
        lines.append(f"#### {group_name} ({len(objects)})")
        budget = [_MAX_OBJECTS_PER_GROUP]
        for root in summary.roots_of(objects):
            if budget[0] <= 0:
                break
            lines.extend(_object_lines(root, summary, 0, budget))
        remaining = len(objects) - (_MAX_OBJECTS_PER_GROUP - budget[0])
        if remaining > 0:
            lines.append(f"- \u2026 {remaining} more")
        lines.append("")
    lines.append("</details>")
    return lines


def _render_export_suggestions(inst: InstanceDriftResult) -> list[str]:
    """Render the per-plan suggestions to export tracked-but-unexported objects.

    Each suggestion names the owning plan and lists the tracked objects that an
    untracked object referenced but could not reach, capped per plan at
    :data:`_MAX_EXPORT_OBJECTS`.
    """
    lines = ["<details open>", "<summary>Export suggestions</summary>", ""]
    for suggestion in inst.export_suggestions:
        lines.append(
            f"- \U0001f4a1 Export **{len(suggestion.objects)}** object(s) from "
            f"`{suggestion.plan_target}` (plan `{suggestion.plan_var}`):"
        )
        for obj in suggestion.objects[:_MAX_EXPORT_OBJECTS]:
            kind = obj.kind or "object"
            name = f" {code(obj.name)}" if obj.name else ""
            lines.append(
                f"  - **{kind}**{name} (uid `{obj.uid}`) "
                f"— referenced by {obj.references} untracked object(s)"
            )
        remaining = len(suggestion.objects) - _MAX_EXPORT_OBJECTS
        if remaining > 0:
            lines.append(f"  - \u2026 {remaining} more")
    lines.append("")
    lines.append("</details>")
    return lines


def render_drift_report(result: DriftResult, *, max_chars: int | None = None) -> str:
    """Render a :class:`~lamindb_setup.evolve.ci._drift.DriftResult` as Markdown.

    When `max_chars` is set and the full report would exceed it, the verbose
    collapsible blocks (untracked-objects hierarchy and per-plan diffs) are
    dropped so the headline, per-instance counts, and export suggestions survive
    truncation.
    """
    full = _render_drift_report(result, verbose=True)
    if max_chars is None or len(full) <= max_chars:
        return full
    return _render_drift_report(result, verbose=False, truncated=True)


def _render_drift_report(
    result: DriftResult, *, verbose: bool, truncated: bool = False
) -> str:
    lines: list[str] = [_DRIFT_MARKER, "## lamin evolve drift"]

    if result.has_drift:
        lines.append("\u26a0\ufe0f **Drift detected.** See per-instance details below.")
    else:
        lines.append("\u2705 **No drift detected in any configured instance.**")
    if result.has_export_suggestions:
        lines.append("")
        lines.append(
            "\U0001f4a1 **Some referenced objects are tracked but not exported.** "
            "Export them from their owning plan so dependent objects can be "
            "reconstructed (see per-instance details below)."
        )
    if truncated:
        lines.append("")
        lines.append(
            "_Report condensed to fit GitHub's character limit: untracked-object "
            "and plan-diff details omitted. Run `ci drift` locally for the full "
            "report._"
        )
    lines.append("")

    for inst in result.instances:
        lines.append(f"### `{inst.slug}`")
        if (
            not inst.has_drift
            and not inst.skipped_plans
            and not inst.errors
            and not inst.warnings
            and not inst.export_suggestions
        ):
            lines.append("No drift. All objects are tracked and unchanged.")
            lines.append("")
            continue

        lines.append(
            f"- Untracked objects: **{inst.untracked_objects}**"
            + (f" → `{inst.output.name}`" if inst.output else "")
        )
        lines.append(
            f"- Modified/deleted tracked objects: **{inst.modified_or_deleted}**"
        )
        if inst.skipped_plans:
            joined = ", ".join(f"`{t}`" for t in inst.skipped_plans)
            lines.append(f"- \u26a0\ufe0f No lock file for: {joined}")
        for err in inst.errors:
            lines.append(f"- \u274c {err}")
        for warning in inst.warnings:
            lines.append(f"- \u26a0\ufe0f {warning}")
        lines.append("")

        if inst.export_suggestions:
            lines.extend(_render_export_suggestions(inst))
            lines.append("")

        if not verbose:
            continue

        if inst.summary is not None and inst.summary.objects:
            lines.extend(_render_untracked_summary(inst.summary))
            lines.append("")

        actionable_diffs = [d for d in inst.diffs if not d.ok or d.actionable]
        if actionable_diffs:
            lines.append("<details>")
            lines.append("<summary>Plan diffs</summary>")
            lines.append("")
            for diff in actionable_diffs:
                lines.append(_render_diff(diff))
                lines.append("")
            lines.append("</details>")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


_VERIFY_MARKER = "<!-- lamin-schemas-verify -->"


def render_verify_report(result: VerifyResult) -> str:
    """Render a :class:`~lamindb_setup.evolve.ci._verify.VerifyResult` as Markdown."""
    lines: list[str] = [_VERIFY_MARKER, "## lamin evolve config check"]
    if result.ok:
        lines.append(
            "\u2705 **Every committed plan is referenced or explicitly ignored.**"
        )
    else:
        lines.append("\u274c **Configuration check failed.** See details below.")
    lines.append("")

    if result.orphans:
        lines.append("### Orphan plans")
        lines.append(
            "These plans are committed but referenced by no instance and not "
            "listed in `ignored_plans`. Add each to an instance's `plans` or to "
            "`ignored_plans`:"
        )
        for plan in result.orphans:
            lines.append(f"- \u274c `{plan.name}`")
        lines.append("")

    if result.ignored_with_locks:
        lines.append("### Ignored plans with lock files")
        lines.append(
            "Ignored plans must not be applied, so they may not carry lock files. "
            "Delete the lock file(s) or stop ignoring the plan:"
        )
        for plan, locks in result.ignored_with_locks.items():
            joined = ", ".join(f"`{lock.name}`" for lock in locks)
            lines.append(f"- \u274c `{plan.name}`: {joined}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


_MISSING_EXPORTS_MARKER = "<!-- lamin-schemas-missing-exports -->"

# Condensed how-to so an agent can act on the report in one pass.
_MISSING_EXPORTS_HOWTO = [
    "> Export each object below from its **owning plan** so drift / dependent plans",
    "> can reference it. In that plan: add a field to its `PlanExports` subclass and",
    "> assign it in the body (`exports.<field> = <var>`). Find `<var>` by the",
    "> **natural key** \u2014 grep the object's name; the type-path disambiguates",
    "> same-named objects. Unassigned loose `Feature(...)` / `Schema(...)`: capture",
    "> into a variable first. Declarative `SchemaModel`: use `<Class>.schema` /",
    "> `<Class>.<field>`. Re-run `ci missing-exports` until it reports 0.",
]


def render_missing_exports_report(result: MissingExportsResult) -> str:
    """Render a :class:`~lamindb_setup.evolve.ci._missing_exports.MissingExportsResult`.

    Lists, per instance and per owning plan, the full closure of tracked objects
    still to export (uncapped) as a table keyed by natural key, plus a collapsed
    list of untracked objects that exporting cannot make reconstructable.
    """
    lines: list[str] = [_MISSING_EXPORTS_MARKER, "## lamin evolve missing-exports"]
    noteworthy = any(
        inst.has_missing or inst.blocked or inst.errors or inst.skipped_plans
        for inst in result.instances
    )
    if not noteworthy:
        lines.append("")
        lines.append(
            "\u2705 **Nothing to export.** Every reconstructable untracked object "
            "is already reachable through a plan's exports."
        )
        return "\n".join(lines) + "\n"

    lines.append("")
    lines.extend(_MISSING_EXPORTS_HOWTO)
    lines.append("")
    for inst in result.instances:
        lines.extend(_render_missing_instance(inst))
    return "\n".join(lines).rstrip() + "\n"


def _render_missing_instance(inst: InstanceMissingExports) -> list[str]:
    lines = [f"### `{inst.slug}`"]
    for err in inst.errors:
        lines.append(f"- \u274c {err}")
    for target in inst.skipped_plans:
        lines.append(f"- \u2139\ufe0f No lock file for `{target}` (skipped).")
    lines.append("")

    if inst.has_missing:
        lines.append(
            f"**Export {inst.to_export} object(s)** across {len(inst.groups)} "
            f"plan(s) to make **{inst.reconstructable} untracked object(s)** "
            "reconstructable."
        )
        lines.append("")
        for group in inst.groups:
            lines.append(
                f"#### `{group.plan_target}` (plan `{group.plan_var}`) \u2014 "
                f"export {len(group.objects)}"
            )
            lines.append("")
            lines.append("| kind | name | natural key | uid | refs |")
            lines.append("|---|---|---|---|---|")
            for obj in group.objects:
                name = code(obj.name) if obj.name else ""
                lines.append(
                    f"| {table_cell(obj.kind or '')} | {table_cell(name)} | "
                    f"{table_cell(code(obj.natural_key))} | "
                    f"{table_cell(code(obj.uid))} | {obj.references} |"
                )
            lines.append("")
    elif not inst.errors:
        lines.append("\u2705 Nothing to export for this instance.")
        lines.append("")

    if inst.blocked:
        roots = [b for b in inst.blocked if b.root_cause]
        cascades = [b for b in inst.blocked if not b.root_cause]
        lines.append("<details>")
        lines.append(
            f"<summary>Blocked \u2014 exporting can't fix ({len(roots)} root "
            f"cause(s), {len(cascades)} cascade(s))</summary>"
        )
        lines.append("")
        for blocked in roots:
            lines.append(f"- \u274c {blocked.message}")
        for blocked in cascades:
            lines.append(f"- \u21b3 {blocked.message}")
        lines.append("")
        lines.append("</details>")
        lines.append("")
    return lines
