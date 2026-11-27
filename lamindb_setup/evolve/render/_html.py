"""Static, single-file HTML summaries with embedded CSS."""

from __future__ import annotations

import html
from typing import TYPE_CHECKING

from ._base import PlanSummary

if TYPE_CHECKING:
    from ._model import ObjectInfo, PlanRef, SummaryModel

__all__ = ["HtmlPlanSummary"]


_CSS = """\
:root {
  --bg: #0f172a;
  --panel: #1e293b;
  --border: #334155;
  --text: #e2e8f0;
  --muted: #94a3b8;
  --kind: #38bdf8;
  --name: #4ade80;
  --plan: #c084fc;
  --accent: #60a5fa;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  padding: 2rem;
  background: var(--bg);
  color: var(--text);
  font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
  line-height: 1.5;
}
.summary {
  max-width: 960px;
  margin: 0 auto;
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 1.5rem 2rem 2rem;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
}
h1 {
  margin: 0 0 1.5rem;
  font-size: 1.4rem;
  border-bottom: 1px solid var(--border);
  padding-bottom: 0.75rem;
}
h2 {
  font-size: 1rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--muted);
  margin: 1.5rem 0 0.75rem;
}
ul.tree {
  list-style: none;
  margin: 0;
  padding-left: 0;
}
ul.tree ul {
  list-style: none;
  margin: 0;
  padding-left: 1.25rem;
  border-left: 1px solid var(--border);
  margin-left: 0.4rem;
}
ul.tree li {
  position: relative;
  padding: 0.15rem 0 0.15rem 1rem;
}
ul.tree li::before {
  content: "";
  position: absolute;
  top: 0.9rem;
  left: 0;
  width: 0.8rem;
  height: 1px;
  background: var(--border);
}
ul.tree > li::before { display: none; }
ul.tree > li { padding-left: 0; }
.kind { color: var(--kind); font-weight: 600; }
.name { color: var(--name); }
.details { color: var(--muted); font-size: 0.85em; }
.plan { color: var(--plan); font-weight: 600; }
.space { color: var(--muted); font-size: 0.85em; }
.cycle { color: #f87171; font-size: 0.85em; }
h3.group {
  font-size: 0.95rem;
  color: var(--accent);
  margin: 1.25rem 0 0.5rem;
}
h3.group .count { color: var(--muted); font-weight: 400; font-size: 0.85em; }
.requires-label {
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  font-size: 0.8em;
}
code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
"""


class HtmlPlanSummary(PlanSummary):
    """Render a plan summary as a self-contained HTML page with embedded CSS."""

    def _esc(self, text: str) -> str:
        return html.escape(text)

    def _object_html(self, obj: ObjectInfo) -> str:
        parts = [f'<span class="kind">{self._esc(obj.kind)}</span>']
        if obj.name is not None:
            parts.append(f'<span class="name">{self._esc(repr(obj.name))}</span>')
        if obj.details:
            details = self._esc(", ".join(obj.details))
            parts.append(f'<span class="details">({details})</span>')
        return " ".join(parts)

    def _object_ref_html(self, obj: ObjectInfo) -> str:
        parts = [f'<span class="kind">{self._esc(obj.kind)}</span>']
        if obj.name is not None:
            parts.append(f'<span class="name">{self._esc(repr(obj.name))}</span>')
        return " ".join(parts)

    def _requires_items(self, obj: ObjectInfo, model: SummaryModel) -> str:
        requirements = model.requirements_of(obj)
        if not requirements:
            return ""
        items = "".join(
            f"<li>{self._object_ref_html(req)}</li>" for req in requirements
        )
        return f'<li><span class="requires-label">requires</span><ul>{items}</ul></li>'

    def _object_li(self, obj: ObjectInfo, model: SummaryModel) -> str:
        inner = self._requires_items(obj, model)
        inner += "".join(
            self._object_li(child, model) for child in model.children_of(obj)
        )
        children_html = f"<ul>{inner}</ul>" if inner else ""
        return f"<li>{self._object_html(obj)}{children_html}</li>"

    def _objects_section(self, model: SummaryModel) -> str:
        groups: list[str] = []
        for group_name, objects in model.grouped_objects:
            items = "".join(
                self._object_li(root, model) for root in model.roots_of(objects)
            )
            groups.append(
                f'<h3 class="group">{self._esc(group_name)} '
                f'<span class="count">({len(objects)})</span></h3>'
                f'<ul class="tree">{items}</ul>'
            )
        return "".join(groups)

    def _declarations_section(self, model: SummaryModel) -> str:
        if not model.declarations:
            return ""
        items = []
        for declaration in model.declarations:
            if declaration.key is not None:
                target = f'<span class="name">{self._esc(repr(declaration.key))}</span>'
            else:
                target = (
                    f'<span class="kind">{self._esc(declaration.kind or "object")}'
                    "</span>"
                )
                if declaration.name is not None:
                    name = self._esc(repr(declaration.name))
                    target += f' <span class="name">{name}</span>'
            text = (
                f"{self._esc(declaration.verb)} {target} &larr; "
                f"<code>{self._esc(declaration.source)}</code>"
            )
            if declaration.reason:
                reason = self._esc(declaration.reason)
                text += f' <span class="details">({reason})</span>'
            items.append(f"<li>{text}</li>")
        return (
            f"<h2>Declarations ({len(model.declarations)})</h2>\n"
            f'<ul class="tree">{"".join(items)}</ul>\n'
        )

    def _plan_html(self, plan: PlanRef) -> str:
        text = f'<span class="plan">{self._esc(plan.name)}</span>'
        if plan.default_space is not None:
            space = self._esc(plan.default_space)
            text += f' <span class="space">(space {space})</span>'
        return text

    def _plan_items(self, plan: PlanRef, model: SummaryModel) -> str:
        dependents = model.dependents_of_plan(plan)
        if not dependents:
            return ""
        items = [
            f"<li>{self._plan_html(dep)}{self._plan_items(dep, model)}</li>"
            for dep in dependents
        ]
        return "<ul>" + "".join(items) + "</ul>"

    def _plans_tree(self, model: SummaryModel) -> str:
        items = [
            f"<li>{self._plan_html(base)}{self._plan_items(base, model)}</li>"
            for base in model.base_plans
        ]
        return '<ul class="tree">' + "".join(items) + "</ul>"

    def _render(self, model: SummaryModel) -> str:
        heading = self._esc(model.heading)
        plans = self._plans_tree(model)
        objects = self._objects_section(model)
        return (
            "<!DOCTYPE html>\n"
            '<html lang="en">\n'
            "<head>\n"
            '<meta charset="utf-8">\n'
            '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
            f"<title>{heading}</title>\n"
            f"<style>\n{_CSS}</style>\n"
            "</head>\n"
            "<body>\n"
            '<div class="summary">\n'
            f"<h1>{heading}</h1>\n"
            "<h2>Plans</h2>\n"
            f"{plans}\n"
            f"<h2>Objects ({len(model.objects)})</h2>\n"
            f"{objects}\n"
            f"{self._declarations_section(model)}"
            "</div>\n"
            "</body>\n"
            "</html>\n"
        )
