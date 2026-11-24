# ruff: noqa: TC001
"""JSON rendering of a plan summary.

The machine-readable counterpart of the terminal, Markdown and HTML views: the
same :class:`~lamindb_setup.evolve.render._model.SummaryModel`, serialized
rather than laid out.  It exists so that `validate --format json` can be piped
into `jq` and asserted on, which is what a caller reaching for `--format`
in a script wants and what the terminal view -- box-drawing characters and ANSI
colour -- cannot give them.

The shape follows the model, so it stays in step with what the other renderers
show instead of being a separate description of a plan that could drift from it.
"""

from __future__ import annotations

import dataclasses
import json
from typing import Any

from ._base import PlanSummary
from ._model import SummaryModel

__all__ = ["JsonPlanSummary"]


class JsonPlanSummary(PlanSummary):
    """Render a plan summary as an indented JSON document."""

    def _render(self, model: SummaryModel) -> str:
        return json.dumps(_as_payload(model), indent=2, sort_keys=False)


def _as_payload(model: SummaryModel) -> dict[str, Any]:
    """Return the JSON-serializable payload for `model`.

    `obj_id`/`requires`/`parent_id` are `id()` values -- stable only
    within one process -- so they are renumbered densely from zero.  A caller
    diffing two runs of the same plan should see the same document, which raw
    ids would not give them.
    """
    numbering = {obj.obj_id: index for index, obj in enumerate(model.objects)}
    objects = []
    for obj in model.objects:
        entry = dataclasses.asdict(obj)
        entry["obj_id"] = numbering[obj.obj_id]
        entry["requires"] = sorted(
            numbering[req] for req in obj.requires if req in numbering
        )
        entry["parent_id"] = numbering.get(obj.parent_id) if obj.parent_id else None
        entry["label"] = obj.label
        objects.append(entry)
    declarations = [
        {**dataclasses.asdict(declaration), "label": declaration.label}
        for declaration in model.declarations
    ]
    return {
        "title": model.title,
        "heading": model.heading,
        "plan": dataclasses.asdict(model.plan),
        "objects": objects,
        "declarations": declarations,
    }
