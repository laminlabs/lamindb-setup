"""Apply one plan against a connected instance, end to end.

This is the layer between the apply engine (:mod:`~lamindb_setup.evolve.engine._apply`,
which computes and executes changes) and the two entry points that drive it:
the `lamin evolve apply` command and the
configuration-driven `ci apply` in :mod:`~lamindb_setup.evolve.ci._apply`.

:func:`apply_one` is what makes those two the *same* apply: the preview, the
confirmation prompt, the tracked run, the atomic transaction, the lock file and
the sheet re-exports all happen here once, so a plan applied from CI lands
exactly as it would from a terminal.  It lives in its own module rather than
with the command it started in, because `ci apply` calling it used to mean the
`ci` package importing the CLI.

It does not decide whether to ask before applying, nor how to end the process:
the caller passes the prompt in (`confirm`), and every way an apply stops
short is raised as a :class:`~lamindb_setup.evolve.core.LaminSchemasError` for
the caller to report -- which the CLI does as `Error: <message>`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..console import OutputFormat, echo
from ..core import LaminSchemasError
from ..render import append_notes
from ._lock import lock_path

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from ..authoring import Plan
    from ._apply import ExistenceCache, SheetExportOutcome
    from ._track import TrackConfig

__all__ = [
    "ApplyAbortedError",
    "ApplyFailedError",
    "ApplyRefusedError",
    "SheetExportError",
    "apply_one",
    "render_sheet_export_outcome",
]


class ApplyRefusedError(LaminSchemasError):
    """The preview found a reason not to apply.

    An object borrowed from a prerequisite plan is missing from the instance, a
    uid is misaligned, or a rename was not declared.

    Raised before anything is written, so the instance and the lock are unchanged.
    """


class ApplyAbortedError(LaminSchemasError):
    """The `confirm` callable declined the changes; nothing was applied."""


class ApplyFailedError(LaminSchemasError):
    """The apply raised and its transaction was rolled back.

    No change was made and no lock file was written; the run carries an attached
    failure report.  The original exception is chained as `__cause__`.
    """


class SheetExportError(Exception):
    """A declared sheet re-export failed *after* the apply had already committed.

    Carries the rendered `section` so a Markdown run still reports what did
    happen -- the schema change landed and its lock was written; only the exports
    are unfinished, parked on the unmerged branch the outcome names.  The command
    exits non-zero once the output is written.
    """

    def __init__(self, outcome: SheetExportOutcome, *, section: str | None) -> None:
        super().__init__(str(outcome.error))
        self.outcome = outcome
        self.section = section
        # Filled in by a caller applying several plans, so the command can still
        # write out everything that ran before failing.
        self.sections: list[str] = []


def render_sheet_export_outcome(outcome: SheetExportOutcome) -> list[str]:
    """Advisory lines describing what happened to the invalidated sheet exports."""
    if outcome.branch is None and not outcome.notes and outcome.ok:
        return []
    notes = list(outcome.notes)
    if not outcome.ok and outcome.partial_promotions:
        # Distinct from the branch case below: the work is already merged and
        # visible, so re-running fixes nothing -- the next apply sees a schema
        # that matches the plan and owes no re-export. Say what is left instead.
        notes.append(
            f"Sheet re-export failed: {outcome.error}. The schema change is "
            "applied, its lock file written, and the new export version(s) "
            f"merged into the instance, but only {outcome.promoted} of "
            f"{len(outcome.promotions)} version family(ies) were re-pointed at "
            "the version describing the new schema. Re-running apply will not "
            "redo this -- the schema already matches the plan. Re-point the "
            "families below by hand (the first is the one that failed), or "
            "re-export those sheets from the hub."
        )
        # The uids are the whole point of the note: nothing else records which
        # families are outstanding once the run is over.
        notes.extend(
            f"  not re-pointed -- {described}"
            for described in outcome.describe_pending_promotions()
        )
    elif not outcome.ok:
        notes.append(
            f"Sheet re-export failed: {outcome.error}. The schema change is "
            "applied and its lock file written, but "
            + (
                f"the new export version(s) are parked on the unmerged branch "
                f"{outcome.branch_name!r}"
                if outcome.branch is not None
                else "no export version was written"
            )
            + ". Re-run apply once the cause is fixed: the unmerged branch "
            "records that these exports are still owed, so the next run redoes "
            "them onto its own branch and closes this one."
        )
    elif outcome.written or outcome.promotions:
        did = []
        if outcome.written:
            did.append(f"re-exported {outcome.written} sheet snapshot(s)")
        if outcome.promotions:
            did.append(
                f"restored {len(outcome.promotions)} to a version the family "
                "already held"
            )
        notes.append(
            f"Sheet exports: {' and '.join(did)} on branch "
            f"{outcome.branch_name!r} and merged it into the instance."
            + (
                " Closed the superseded branch(es) "
                + ", ".join(repr(name) for name in outcome.closed)
                + "."
                if outcome.closed
                else ""
            )
        )
    elif outcome.branch is not None:
        notes.append(
            "No sheet export needed rewriting: the invalidated snapshots still "
            f"hash to the data already stored. Merged branch "
            f"{outcome.branch_name!r}."
        )
    return notes


def _warn_legacy_lock(plan_file: Path, lock_file: Path) -> None:
    """Warn if a pre-per-instance `<plan>.lock` exists but the new one does not.

    Such a legacy lock has no recorded instance and is ignored; the new
    per-instance lock starts empty and re-adopts existing objects by natural key.
    """
    legacy = plan_file.with_suffix(".lock")
    if legacy.exists() and legacy != lock_file and not lock_file.exists():
        echo(
            f"Warning: ignoring legacy lock file {legacy} (no instance binding); "
            f"writing a per-instance lock to {lock_file}.",
            err=True,
        )


def apply_one(
    plan_obj: Plan,
    plan_file: Path,
    stem_uid: str,
    instance: str,
    instance_slug: str,
    *,
    claimed_keys: set[str],
    confirm: Callable[[str], bool] | None,
    output_format: OutputFormat,
    strict_renames: bool = True,
    allow_data_validated_changes: bool = False,
    strict_uids: bool = True,
    track_config: TrackConfig | None = None,
    existing_cache: ExistenceCache | None = None,
    members_cache: dict[int, list[Any]] | None = None,
    exports_cache: dict[str, Any] | None = None,
    track: Callable[..., Any] | None = None,
    show_unchanged: bool = False,
) -> str | None:
    """Apply one plan against the connected instance and write its lock file.

    `claimed_keys` are natural keys owned by sibling plans applied alongside
    this one, so an object moved to a sibling is not deleted.  `track_config`
    carries the key prefix and space the run is recorded under (see
    :func:`~lamindb_setup.evolve.engine._track.track_plan`); it is resolved once per
    command and passed down.
    `exports_cache` likewise shares the instance-wide sheet-export index, so a
    run applying several plans reads it once.
    `existing_cache` and `members_cache` optionally carry the instance state
    already read on behalf of every plan applied together (see
    :func:`~lamindb_setup.evolve.engine._apply.prefetch_plans`), so this plan reuses that
    one read instead of issuing its own; the caller owns them and is responsible
    for calling :func:`~lamindb_setup.evolve.engine._apply.forget_missing` between plans.
    `track` is the run-tracking context manager, defaulting to
    :func:`~lamindb_setup.evolve.engine._track.track_plan`; it is a parameter so a
    measurement run can leave the Transform/Run writes out of what it counts.
    `strict_uids` refuses the apply while any object's uid disagrees with the
    plan's lock files for other instances (see :mod:`._alignment`); off, the
    disagreement is only reported and an object that cannot be pinned is
    created with a fresh uid.
    `confirm` is asked `"Apply <n> change(s)?"` once the preview found
    something to do, and the apply proceeds only if it returns `True`; `None`
    applies without asking.  The CLI passes a terminal prompt unless `--yes`;
    `ci apply` never asks.  With a text format the preview and its notes are
    printed to stderr just before `confirm` is asked, so the prompt is not
    answered blind while stdout stays the document; the caller asks it on stderr
    too.
    Returns the rendered section for a text format (Markdown or JSON, for the
    caller to write out), otherwise prints to the terminal and returns `None`.
    `show_unchanged` lists every object in the terminal diffs -- the preview,
    the one confirmed on stderr and the applied one -- not only those that
    change (see :func:`~lamindb_setup.evolve.render.print_changes`); the failure
    report attached to a run lists them all regardless.

    Raises :class:`ApplyRefusedError` when the preview finds an object borrowed
    from a prerequisite plan missing from the instance, a misaligned uid
    (`strict_uids`) or an undeclared rename (`strict_renames`),
    :class:`ApplyAbortedError` when `confirm` declines, :class:`ApplyFailedError`
    when the apply raised and was rolled back, and
    :class:`~lamindb_setup.evolve.engine._track.TrackError` when the run cannot be
    tracked.
    """
    import traceback

    from django.db import transaction

    from ..console import progress_reporter
    from ..core import Action, ApplyMode
    from ..render import (
        is_up_to_date,
        missing_prerequisite_message,
        print_changes,
        render_changes,
        render_changes_json,
        render_changes_markdown,
        render_move_outcome,
        render_uid_misalignments,
        uid_misalignment_message,
        unmoved_rename_refusal,
    )
    from ._alignment import read_sibling_uids
    from ._apply import (
        ExistenceCache,
        compute_changes,
        run_sheet_export_migrations,
        write_lock,
    )
    from ._lock import LockFile
    from ._track import TrackConfig, attach_operation_report, track_plan

    track_run = track_plan if track is None else track
    track_config = TrackConfig.resolve() if track_config is None else track_config
    text_output = output_format.is_text

    def _render(ctx: Any, *, title: str, notes: list[str]) -> str:
        """Render one applied plan in whichever text format was asked for."""
        if output_format is OutputFormat.json:
            return render_changes_json(ctx, title=title, notes=notes)
        return append_notes(render_changes_markdown(ctx, title=title), notes)

    lock_file = lock_path(plan_file, instance_slug)
    lock = LockFile.read(lock_file)
    _warn_legacy_lock(plan_file, lock_file)
    # Read fresh for every plan, so an instance applied earlier in the same run
    # has already written the uids this one is created with.
    sibling_uids = read_sibling_uids(plan_file, plan_obj, instance_slug)

    # Existence lookups are shared between the preview and apply passes so the
    # apply pass reuses the preview's queries instead of re-hitting the instance.
    # A caller applying several plans against one instance passes its own caches
    # in, which shares that read across the plans as well.
    if existing_cache is None:
        existing_cache = ExistenceCache()
    with progress_reporter() as progress:
        preview = compute_changes(
            plan_obj,
            mode=ApplyMode.PLAN,
            lock=lock,
            existing_cache=existing_cache,
            members_cache=members_cache,
            exports_cache=exports_cache,
            claimed_keys=claimed_keys,
            report=progress.report,
            track=progress.track,
            allow_data_validated_changes=allow_data_validated_changes,
            sibling_uids=sibling_uids,
            strict_uids=strict_uids,
        )
    if not text_output:
        print_changes(
            preview, title=f"Plan: {plan_obj.name}", show_unchanged=show_unchanged
        )

    move_notes = render_move_outcome(preview.move_outcome, instance_slug=instance_slug)
    uid_notes = render_uid_misalignments(preview, instance_slug=instance_slug)
    if not text_output:
        for note in [*move_notes, *uid_notes]:
            echo(note)

    if preview.missing_prerequisites:
        # The apply pass would raise `MissingPrerequisiteError` on the first of
        # these -- after a run was tracked.  Refusing here leaves no trace.
        raise ApplyRefusedError(
            missing_prerequisite_message(
                plan_obj.name,
                preview.missing_prerequisites,
                instance=instance,
            )
        )

    if strict_uids and preview.uid_misalignments:
        raise ApplyRefusedError(
            f"refusing to apply {plan_obj.name!r} to {instance_slug}: "
            f"{uid_misalignment_message(preview.uid_misalignments)}"
        )
    move_notes = [*move_notes, *uid_notes]

    if strict_renames and preview.suspected_renames:
        raise ApplyRefusedError(
            unmoved_rename_refusal(plan_obj.name, preview.suspected_renames)
        )

    # A borrowed object is its prerequisite plan's to create, never this one's.
    actionable = [
        c
        for c in preview.changes
        if c.action in (Action.CREATE, Action.UPDATE, Action.DELETE, Action.MOVE)
        and not c.borrowed
    ]
    if not actionable:
        write_lock(preview, plan_obj, lock_file)
        if text_output:
            return _render(preview, title=f"Applied: {plan_obj.name}", notes=move_notes)
        if not is_up_to_date(preview):
            # Only transfers or misaligned uids: the diff did not say it already.
            echo("No changes. Instance is up to date.")
        return None

    if confirm is not None and text_output:
        # A text format renders its document only once the apply ran, so show
        # what is being confirmed on stderr: stdout and --output stay the document.
        print_changes(
            preview,
            title=f"Plan: {plan_obj.name}",
            err=True,
            show_unchanged=show_unchanged,
        )
        for note in move_notes:
            echo(note, err=True)
    if confirm is not None and not confirm(f"Apply {len(actionable)} change(s)?"):
        raise ApplyAbortedError(
            f"did not apply {len(actionable)} change(s) of {plan_obj.name!r}: "
            "the confirmation was declined"
        )

    # Track the application as a transform run before creating objects.  The
    # context manager guarantees `ln.finish()` runs whether the apply succeeds
    # or raises; on failure it records the run as errored.  The object mutations
    # run inside a single database transaction, so a mid-apply failure rolls
    # every change back (atomic apply), the run keeps an attached failure report,
    # and no lock file is written.
    with track_run(plan_file, stem_uid=stem_uid, config=track_config) as (
        key,
        run,
    ):
        if not text_output:
            echo(f"Tracking run as transform: {key}")
        try:
            with transaction.atomic():
                with progress_reporter() as progress:
                    ctx = compute_changes(
                        plan_obj,
                        mode=ApplyMode.APPLY,
                        lock=lock,
                        existing_cache=existing_cache,
                        members_cache=members_cache,
                        exports_cache=exports_cache,
                        claimed_keys=claimed_keys,
                        prefetch=False,
                        report=progress.report,
                        track=progress.track,
                        allow_data_validated_changes=allow_data_validated_changes,
                        sibling_uids=sibling_uids,
                        strict_uids=strict_uids,
                    )
        except Exception as exc:
            # The report is the run's record of what it attempted: list it all.
            rendered = render_changes(
                preview, title=f"Failed: {plan_obj.name}", show_unchanged=True
            )
            note = "\n\nApply failed and was rolled back; no changes were made:\n"
            report_text = rendered + note + traceback.format_exc()
            attach_operation_report(
                run,
                report_text,
                instance=instance,
                prefix=track_config.prefix,
                operation="apply",
                filename="apply-failure-report.txt",
                folder="apply-failure-reports",
            )
            raise ApplyFailedError(
                "apply failed and was rolled back; no changes were made "
                f"and no lock file was written: {exc}"
            ) from exc
        if not text_output:
            print_changes(
                ctx, title=f"Applied: {plan_obj.name}", show_unchanged=show_unchanged
            )
        write_lock(ctx, plan_obj, lock_file)
        if not text_output:
            echo(f"Wrote lock file: {lock_file}")
        # Sheet exports are written *after* the transaction committed: the
        # file cannot be rolled back with it, so it is isolated on a branch
        # merged only once every declared re-export succeeded.  The lock is
        # already written, because the schema change it records did land --
        # a failure here leaves the new versions behind on the unmerged
        # branch, which the raised error names so the run can be repeated.
        export_outcome = run_sheet_export_migrations(
            ctx.pending_sheet_exports,
            plan_name=plan_obj.name,
            run_uid=str(getattr(run, "uid", "")),
        )
        export_notes = render_sheet_export_outcome(export_outcome)
        section = (
            _render(
                ctx,
                title=f"Applied: {plan_obj.name}",
                notes=[*move_notes, *export_notes],
            )
            if text_output
            else None
        )
        if not text_output:
            for note in export_notes:
                echo(note, err=not export_outcome.ok)
        if not export_outcome.ok:
            raise SheetExportError(export_outcome, section=section)
        return section
