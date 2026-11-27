"""GitHub-flavored Markdown summaries for posting on pull requests."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._base import PlanSummary
from ._mdtext import code

if TYPE_CHECKING:
    from ._model import DeclarationInfo, ObjectInfo, PlanRef, SummaryModel

__all__ = ["MarkdownPlanSummary", "declaration_label", "object_label", "object_ref"]


def object_ref(obj: ObjectInfo) -> str:
    """Return the bolded kind and code-spanned name of `obj`.

    Shared with the CI report renderer, which showed objects the same way in
    its own copy of this until the two drifted.
    """
    text = f"**{obj.kind}**"
    if obj.name is not None:
        text += f" {code(obj.name)}"
    return text


def object_label(obj: ObjectInfo) -> str:
    """Return :func:`object_ref` plus `obj`'s parenthesized details."""
    text = object_ref(obj)
    if obj.details:
        text += " (" + ", ".join(obj.details) + ")"
    return text


def declaration_label(declaration: DeclarationInfo) -> str:
    """`migrate **Feature** `a` ← `map_values()` (reason)`."""
    if declaration.key is not None:
        target = code(declaration.key)
    else:
        target = f"**{declaration.kind or 'object'}**"
        if declaration.name is not None:
            target += f" {code(declaration.name)}"
    text = f"{declaration.verb} {target} ← {code(declaration.source)}"
    if declaration.reason:
        text += f" ({declaration.reason})"
    return text


class MarkdownPlanSummary(PlanSummary):
    """Render a plan summary as GitHub-flavored Markdown.

    The output is a self-contained Markdown fragment suitable for posting as a
    pull request comment: a heading, the parent-plan dependency tree, and the
    objects the plan materializes grouped into nested bullet lists.
    """

    def _object_label(self, obj: ObjectInfo) -> str:
        return object_label(obj)

    def _object_ref(self, obj: ObjectInfo) -> str:
        return object_ref(obj)

    def _object_lines(
        self, obj: ObjectInfo, model: SummaryModel, depth: int
    ) -> list[str]:
        indent = "  " * depth
        lines = [f"{indent}- {self._object_label(obj)}"]
        for req in model.requirements_of(obj):
            lines.append(f"{indent}  - _requires_ {self._object_ref(req)}")
        for child in model.children_of(obj):
            lines.extend(self._object_lines(child, model, depth + 1))
        return lines

    def _plan_lines(self, plan: PlanRef, model: SummaryModel, depth: int) -> list[str]:
        indent = "  " * depth
        label = f"**{plan.name}**"
        if plan.default_space is not None:
            label += f" _(space {plan.default_space})_"
        lines = [f"{indent}- {label}"]
        for dep in model.dependents_of_plan(plan):
            lines.extend(self._plan_lines(dep, model, depth + 1))
        return lines

    def _render(self, model: SummaryModel) -> str:
        lines: list[str] = [f"## {model.heading}", "", "### Plans"]
        for base in model.base_plans:
            lines.extend(self._plan_lines(base, model, 0))

        lines.append("")
        lines.append(f"### Objects ({len(model.objects)})")
        for group_name, objects in model.grouped_objects:
            lines.append("")
            lines.append(f"#### {group_name} ({len(objects)})")
            for root in model.roots_of(objects):
                lines.extend(self._object_lines(root, model, 0))

        if model.declarations:
            lines.append("")
            lines.append(f"### Declarations ({len(model.declarations)})")
            lines.extend(f"- {declaration_label(d)}" for d in model.declarations)

        return "\n".join(lines).rstrip() + "\n"
