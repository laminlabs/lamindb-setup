"""Turning what a run computed into something to read.

Two kinds of document, and a single front door for both.  The modules
underneath are implementation detail -- import from
`lamindb_setup.evolve.render` rather than reaching into them.

Plan summaries render a :class:`~lamindb_setup.evolve.Plan`,
including the parent-plan dependency tree and the full object dependency DAG:

- :class:`PlanSummary` is the abstract base class.
- :class:`TerminalPlanSummary` renders pretty terminal output using `rich`.
- :class:`HtmlPlanSummary` renders a self-contained HTML page.
- :class:`MarkdownPlanSummary` renders a GitHub-flavored Markdown fragment.
- :class:`JsonPlanSummary` renders a machine-readable JSON document.

Change renderers turn a computed diff into the terraform-like plan output and
its Markdown and JSON counterparts (:func:`render_changes` and friends).

:func:`code`, :func:`table_cell` and :func:`append_notes` are the Markdown text
helpers both sides share with the CI report renderer.
"""

from __future__ import annotations

from ._base import PlanSummary
from ._changes import (
    is_up_to_date,
    missing_prerequisite_message,
    print_changes,
    render_changes,
    render_changes_json,
    render_changes_markdown,
    render_move_outcome,
    render_uid_misalignments,
    uid_misalignment_message,
    unmoved_rename_message,
    unmoved_rename_refusal,
)
from ._html import HtmlPlanSummary
from ._json import JsonPlanSummary
from ._markdown import MarkdownPlanSummary, object_label, object_ref
from ._mdtext import append_notes, code, table_cell
from ._model import (
    DeclarationInfo,
    ObjectInfo,
    PlanRef,
    SummaryModel,
    build_summary_model,
)
from ._terminal import TerminalPlanSummary

__all__ = [
    "DeclarationInfo",
    "HtmlPlanSummary",
    "JsonPlanSummary",
    "MarkdownPlanSummary",
    "ObjectInfo",
    "PlanRef",
    "PlanSummary",
    "SummaryModel",
    "TerminalPlanSummary",
    "append_notes",
    "build_summary_model",
    "code",
    "is_up_to_date",
    "missing_prerequisite_message",
    "object_label",
    "object_ref",
    "print_changes",
    "render_changes",
    "render_changes_json",
    "render_changes_markdown",
    "render_move_outcome",
    "render_uid_misalignments",
    "table_cell",
    "uid_misalignment_message",
    "unmoved_rename_message",
    "unmoved_rename_refusal",
]
