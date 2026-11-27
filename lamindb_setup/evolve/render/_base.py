"""Abstract base class for plan summaries."""

from __future__ import annotations

from typing_extensions import TypeVar

E = TypeVar("E", bound="PlanExports")

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

from ._model import SummaryModel, build_summary_model

if TYPE_CHECKING:
    from ..authoring import Plan, PlanExports

__all__ = ["PlanSummary"]


class PlanSummary(ABC):
    """Render a human-readable summary of a :class:`~lamindb_setup.evolve.Plan`.

    Subclasses implement :meth:`_render` to turn the render-agnostic
    :class:`~lamindb_setup.evolve.render._model.SummaryModel` into a concrete
    representation (e.g. rich terminal output or an HTML page).
    """

    def __init__(self, *, title: str = "Plan summary") -> None:
        self.title = title

    def render(self, plan: Plan[E]) -> str:
        """Return the rendered summary of `plan` as a string."""
        model = build_summary_model(plan, self.title)
        return self._render(model)

    @abstractmethod
    def _render(self, model: SummaryModel) -> str:
        """Render `model` into a concrete string representation."""
        raise NotImplementedError("Subclasses must implement _render()")

    def print_summary(self, plan: Plan[E]) -> None:
        """Print the rendered summary of `plan` to stdout."""
        print(self.render(plan))

    def write(self, plan: Plan[E], path: str | Path) -> None:
        """Write the rendered summary of `plan` to `path`."""
        Path(path).write_text(self.render(plan), encoding="utf-8")
