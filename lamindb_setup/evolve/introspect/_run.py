"""Orchestrate reading, reconstructing, rendering and verifying a plan.

:func:`introspect_instance` ties the introspection steps together: it connects to
the instance and reads it, reconstructs and renders a plan file, then verifies in
a subprocess that applying the plan would make no changes.  It returns an
:class:`IntrospectionResult`; the CLI turns that into output and an exit code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from ..render import SummaryModel

__all__ = ["IntrospectionResult", "introspect_instance"]


@dataclass
class IntrospectionResult:
    """The outcome of introspecting an instance into a plan file."""

    output: Path
    func: str
    stem_uid: str
    reconstructed: int
    skipped_unnamed_schemas: int
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    verification: dict[str, Any] | None = None
    summary: SummaryModel | None = None
    out_of_scope_uids: dict[str, int] = field(default_factory=dict)

    @property
    def actionable_changes(self) -> int:
        """The number of create / update / delete actions verification found."""
        if not self.verification or "counts" not in self.verification:
            return 0
        counts = self.verification["counts"]
        return (
            counts.get("create", 0) + counts.get("update", 0) + counts.get("delete", 0)
        )

    @property
    def verification_failed(self) -> bool:
        """Whether verification itself could not be run."""
        return bool(self.verification and "error" in self.verification)

    @property
    def ok(self) -> bool:
        """Whether the plan reconstructs the instance faithfully and completely."""
        return (
            not self.errors
            and not self.verification_failed
            and self.actionable_changes == 0
        )


def _plan_func_name(name: str | None, output: Path) -> str:
    from ._template import plan_slug

    slug = plan_slug(name if name else output.stem)
    return slug if not slug[0].isdigit() else f"plan_{slug}"


def _generate_stem_uid() -> str:
    """Generate a fresh stem uid, avoiding collisions on the connected instance."""
    from ..engine import generate_stem_uid, taken_stem_uids

    return generate_stem_uid(taken_stem_uids().__contains__)


def introspect_instance(
    instance: str,
    output: str | Path,
    *,
    space_uids: Sequence[str] | None = None,
    exclude_space_uids: Sequence[str] | None = None,
    exclude_uids: set[str] | None = None,
    external_refs: dict[str, Any] | None = None,
    name: str | None = None,
    overwrite: bool = False,
    verify: bool = True,
    reporter: Callable[[str], None] | None = None,
    read_tracker: Callable[[Sequence[Any], str], Iterable[Any]] | None = None,
) -> IntrospectionResult:
    """Read `instance` and write a plan file to `output` that adopts its objects.

    Raises :class:`FileExistsError` if `output` exists and `overwrite` is
    `False`.  The returned result reports how many objects were reconstructed,
    any objects that could not be (`errors`), and the verification outcome.

    `exclude_uids` drops objects with those uids from the read (used by drift
    detection to skip objects already managed by other plans); `external_refs`
    maps an excluded object's uid to the owning plan's export, so a reference to
    it renders as a cross-plan import instead of failing.

    `reporter`, if given, is called with a short human-readable message at the
    start of each step so callers (e.g. the CLI) can show progress.

    `read_tracker`, if given, wraps each read loop so callers can render a
    per-object-kind progress bar (see :func:`..introspect._read.read_instance`).
    """
    report = reporter if reporter is not None else (lambda _message: None)

    output = Path(output)
    if output.exists() and not overwrite:
        raise FileExistsError(
            f"{output} already exists; pass overwrite=True to replace"
        )

    func = _plan_func_name(name, output)

    report(f"Connecting to instance {instance!r}")
    from ..engine import safe_connect

    safe_connect(instance)

    from ._read import read_instance
    from ._render import render_source
    from ._reverse import build_plan

    report("Reading feature types, features, schemas, record types and sheets")
    contents = read_instance(
        space_uids=space_uids,
        exclude_space_uids=exclude_space_uids,
        exclude_uids=exclude_uids,
        exclude_child_record_types=True,
        track=read_tracker,
    )

    candidates = (
        len(contents.feature_types)
        + len(contents.features)
        + len(contents.schema_types)
        + len(contents.schemas)
        + len(contents.record_types)
    )
    report(f"Reconstructing {candidates} object(s)")
    plan = build_plan(contents, external_refs)

    from ._summary import build_reconstructed_summary

    summary = build_reconstructed_summary(plan, title="Untracked objects", name=func)

    report("Allocating a transform stem uid")
    stem_uid = _generate_stem_uid()

    report("Rendering plan source")
    description = f"Plan introspected from instance {instance}"
    source = render_source(plan, description=description, func=func, stem_uid=stem_uid)

    report(f"Writing {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(source, encoding="utf-8")

    verification = None
    if verify:
        report("Verifying the plan round-trips (applying in plan mode)")
        # Deferred: _verify is also the `python -m` child entry point, and
        # importing it here would leave it in the child's sys.modules before
        # runpy executes it, which warns on the child's stderr.
        from ._verify import run_verification

        verification = run_verification(output, func, instance)

    return IntrospectionResult(
        output=output,
        func=func,
        stem_uid=stem_uid,
        reconstructed=len(plan.specs),
        skipped_unnamed_schemas=plan.skipped_unnamed_schemas,
        errors=plan.errors,
        warnings=plan.warnings,
        verification=verification,
        summary=summary,
        out_of_scope_uids=plan.out_of_scope_uids,
    )
