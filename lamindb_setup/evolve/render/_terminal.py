"""Pretty terminal summaries using :mod:`rich`.

`rich` is imported where it is used rather than at module level: this module
is reached through the :mod:`~lamindb_setup.evolve.render` front door, so an
import of it would make every caller that only wants a Markdown helper pay for
the terminal renderer.  The annotations keep the real types -- `from __future__
import annotations` means they are never evaluated.
"""

from __future__ import annotations

from typing_extensions import TypeVar

E = TypeVar("E", bound="PlanExports")

import io
from typing import TYPE_CHECKING

from ._base import PlanSummary

if TYPE_CHECKING:
    from rich.console import Console, Group, RenderableType
    from rich.text import Text
    from rich.tree import Tree

    from ..authoring import Plan, PlanExports
    from ._model import DeclarationInfo, ObjectInfo, PlanRef, SummaryModel

__all__ = ["TerminalPlanSummary"]


class TerminalPlanSummary(PlanSummary):
    """Render a plan summary as pretty, optionally colored terminal output."""

    def __init__(self, *, title: str = "Plan summary", width: int = 200) -> None:
        super().__init__(title=title)
        self.width = width

    def _object_label(self, obj: ObjectInfo) -> Text:
        from rich.text import Text

        text = Text()
        text.append(obj.kind, style="bold cyan")
        if obj.name is not None:
            text.append(f" {obj.name!r}", style="green")
        if obj.details:
            text.append(f"  ({', '.join(obj.details)})", style="dim")
        return text

    def _object_ref(self, obj: ObjectInfo) -> Text:
        from rich.text import Text

        text = Text()
        text.append(obj.kind, style="cyan")
        if obj.name is not None:
            text.append(f" {obj.name!r}", style="green")
        return text

    def _add_object_node(
        self, tree: Tree, obj: ObjectInfo, model: SummaryModel
    ) -> None:
        from rich.text import Text

        branch = tree.add(self._object_label(obj))
        requirements = model.requirements_of(obj)
        if requirements:
            req_branch = branch.add(Text("requires", style="dim italic"))
            for req in requirements:
                req_branch.add(self._object_ref(req))
        for child in model.children_of(obj):
            self._add_object_node(branch, child, model)

    def _objects_section(self, model: SummaryModel) -> Group:
        from rich.console import Group
        from rich.text import Text
        from rich.tree import Tree

        parts: list[RenderableType] = [
            Text(f"Objects ({len(model.objects)})", style="bold")
        ]
        for group_name, objects in model.grouped_objects:
            parts.append(Text())
            tree = Tree(
                Text(f"{group_name} ({len(objects)})", style="bold yellow"),
                guide_style="dim",
            )
            for root in model.roots_of(objects):
                self._add_object_node(tree, root, model)
            parts.append(tree)
        return Group(*parts)

    def _declaration_label(self, declaration: DeclarationInfo) -> Text:
        from rich.text import Text

        text = Text()
        text.append(declaration.verb, style="bold")
        text.append(" ")
        if declaration.key is not None:
            text.append(repr(declaration.key), style="green")
        else:
            text.append(declaration.kind or "object", style="cyan")
            if declaration.name is not None:
                text.append(f" {declaration.name!r}", style="green")
        text.append(" ← ")
        text.append(declaration.source, style="magenta")
        if declaration.reason:
            text.append(f"  ({declaration.reason})", style="dim")
        return text

    def _declarations_section(self, model: SummaryModel) -> Tree:
        from rich.text import Text
        from rich.tree import Tree

        tree = Tree(
            Text(f"Declarations ({len(model.declarations)})", style="bold"),
            guide_style="dim",
        )
        for declaration in model.declarations:
            tree.add(self._declaration_label(declaration))
        return tree

    def _add_plan(self, tree: Tree, plan: PlanRef, model: SummaryModel) -> None:
        from rich.text import Text

        for dep in model.dependents_of_plan(plan):
            label = Text(dep.name, style="magenta")
            if dep.default_space is not None:
                label.append(f"  (space {dep.default_space})", style="dim")
            branch = tree.add(label)
            self._add_plan(branch, dep, model)

    def _plans_tree(self, model: SummaryModel) -> Tree:
        from rich.text import Text
        from rich.tree import Tree

        tree = Tree(Text("Plans", style="bold"), guide_style="dim")
        for base in model.base_plans:
            label = Text(base.name, style="bold magenta")
            if base.default_space is not None:
                label.append(f"  (space {base.default_space})", style="dim")
            branch = tree.add(label)
            self._add_plan(branch, base, model)
        return tree

    def _build_renderable(self, model: SummaryModel) -> RenderableType:
        from rich.console import Group
        from rich.panel import Panel
        from rich.text import Text

        parts: list[RenderableType] = [self._plans_tree(model), Text()]
        parts.append(self._objects_section(model))
        if model.declarations:
            parts.extend([Text(), self._declarations_section(model)])
        return Panel(
            Group(*parts),
            title=Text(model.heading, style="bold white"),
            border_style="blue",
            padding=(1, 2),
        )

    def _make_console(self, *, color: bool, file: io.StringIO | None = None) -> Console:
        from rich.console import Console

        return Console(
            width=self.width if file is not None else None,
            file=file,
            force_terminal=color or None,
            color_system="auto" if color else None,
        )

    def _render(self, model: SummaryModel) -> str:
        buffer = io.StringIO()
        console = self._make_console(color=False, file=buffer)
        console.print(self._build_renderable(model))
        return buffer.getvalue()

    def print_summary(self, plan: Plan[E]) -> None:
        """Print a colored summary of `plan` to stdout."""
        from ._model import build_summary_model

        model = build_summary_model(plan, self.title)
        console = self._make_console(color=True)
        console.print(self._build_renderable(model))
