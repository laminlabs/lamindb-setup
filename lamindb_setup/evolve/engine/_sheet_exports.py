"""Sheet exports: the CSV snapshots a schema change can leave behind.

A sheet is a record type constrained by a schema, and lamindb lets it be
exported -- `Record.to_artifact()`, "Export to Artifact" in the hub -- as a
`DataFrame` artifact whose columns are the schema's members and whose
`schema` foreign key points back at the very schema the plan manages::

    Artifact.from_dataframe(sheet.to_dataframe(), key="sheet_exports/<name>.csv",
                            description="Export of sheet <sheet uid>",
                            schema=sheet.schema)

That artifact is a *snapshot*.  Every other value store the apply engine touches
is reconciled in place, but an export is a file already written: renaming a
member changes the column's header, retyping it changes how its values must
read, adding a required member adds a column it does not have.  None of that
reaches the file, so after such a change the artifact claims -- through its own
`schema` foreign key -- to validate against a schema its content no longer
satisfies.

The resolution is a **new version** of the export written from the migrated
rows, which is what :func:`reexport` does.  Two properties make that safe:
lamindb versions an artifact by its `key`, so a re-export supersedes the
previous one rather than duplicating it, and it deduplicates by content hash, so
a re-export of data that did not actually change creates no new version at all.

Because writing a file cannot be rolled back with the database transaction the
rest of the apply runs in, re-exports are written **after** that transaction
commits and onto a dedicated :class:`~lamindb.Branch`, which is merged into the
instance's branch only once every one of them succeeded (see
:func:`open_branch` and :func:`merge_branch`).  A failed run therefore leaves
the half-written exports parked on an unmerged branch instead of in the
instance.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..core import DEFAULT_MODELS, LamindbModels

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "SheetExport",
    "branch_name_for",
    "branch_prefix_for",
    "close_branches",
    "current_branch",
    "export_column_ids",
    "export_marker",
    "merge_branch",
    "open_branch",
    "promote_head",
    "reexport",
    "schema_sheet_exports",
    "sheet_export_index",
    "stale_exports",
    "unmerged_reexport_branches",
]

# lamindb's `Record.to_artifact` marks every export it writes with this
# description prefix followed by the sheet's uid, and keys it under this folder.
# The description is the exact marker (it names the sheet); the key prefix is the
# fallback for an export whose description was edited on the instance.
# lamindb 2.11 renamed both; exports written before keep the old markers.
EXPORT_DESCRIPTION_PREFIXES = ("Export of record type ", "Export of sheet ")
EXPORT_KEY_PREFIXES = ("lamindb_record_exports/", "sheet_exports/")

# The `ArtifactSchema` slot under which lamindb records the columns a curated
# `DataFrame` artifact actually carries.  Its schema's members are the live
# `Feature` records of the exported columns, which is what lets an export be
# compared against the current schema without reading the file back.
COLUMNS_SLOT = "columns"

# lamindb's `Branch._status_code` values this module cares about (see
# `lamindb.base.types.BRANCH_STATUS_TO_CODE`): a merge stamps the source
# branch `merged`, and `closed` is what retires an attempt a later run
# superseded.  Everything else is left `standalone`, which is what "this run
# has unfinished work" reads as.
_MERGED = -1
_CLOSED = -2


@dataclass(frozen=True)
class SheetExport:
    """One exported snapshot of a sheet, and why it no longer adheres.

    `sheet` is the live record type the export was written from, `artifact`
    the latest version of the exported artifact, and `reasons` the
    validation-affecting changes that invalidated it.
    """

    sheet: Any
    artifact: Any
    reasons: tuple[str, ...] = ()

    @property
    def label(self) -> str:
        """A short human identifier, e.g. `'BioSample' -> 'sheet_exports/x.csv'`."""
        sheet_name = getattr(self.sheet, "name", None) or getattr(
            self.sheet, "uid", "?"
        )
        key = getattr(self.artifact, "key", None) or getattr(self.artifact, "uid", "?")
        return f"{sheet_name!r} \u2192 {key!r}"

    def describe(self) -> str:
        """The label plus the reasons, for a refusal message or a diff detail."""
        return (
            f"{self.label} ({'; '.join(self.reasons)})" if self.reasons else self.label
        )


def export_marker() -> Any:
    """The `Artifact` filter matching every sheet export, any version.

    Either marker lamindb stamps an export with: the description naming the
    sheet, or the folder it keys the file under (see
    :data:`EXPORT_DESCRIPTION_PREFIXES` / :data:`EXPORT_KEY_PREFIXES`).
    """
    from django.db.models import Q

    marker = Q()
    for prefix in EXPORT_KEY_PREFIXES:
        marker |= Q(key__startswith=prefix)
    for prefix in EXPORT_DESCRIPTION_PREFIXES:
        marker |= Q(description__startswith=prefix)
    return marker


def sheet_export_index() -> dict[Any, list[Any]] | None:
    """`schema id -> [artifact id]` for every live, latest sheet export.

    One deliberately lightweight query over the whole instance: it selects two
    integer columns, filtered by the folder lamindb keys every export under, so
    the common answer (no sheet was ever exported, or none belongs to a schema
    this apply touches) costs a single indexed read and nothing is loaded.

    Scoped to the branch the instance is on.  `is_latest` is per-branch and
    only reconciled on merge, so an export version parked on an unmerged
    re-export branch also carries it -- and taking that one for the head would
    judge the change against a file the instance cannot see.

    Read once per instance rather than per plan -- a run applying a dozen plans
    would otherwise repeat it a dozen times -- and cached by the caller.  Returns
    `None` when the instance could not be introspected, which callers read as
    "unknown" rather than "none".
    """
    try:
        index: dict[Any, list[Any]] = {}
        rows = _live_exports().values_list("schema_id", "pk")
        for schema_id, artifact_id in rows:
            index.setdefault(schema_id, []).append(artifact_id)
        return index
    except Exception:
        # Cannot introspect the instance -> stay conservative.
        return None


def _live_exports() -> Any:
    """Every latest sheet export on the instance's branch, as a queryset.

    What :func:`sheet_export_index` and :func:`sheet_exports` read: an export
    carrying a schema, on the branch the instance is on.
    """
    from lamindb.models import Artifact

    return Artifact.objects.filter(
        is_latest=True,
        schema_id__isnull=False,
        branch_id=current_branch().id,
    ).filter(export_marker())


def sheet_exports(sheets: list[Any]) -> dict[Any, list[tuple[Any, Any]]] | None:
    """`(sheet, artifact)` per sheet, for its live exports, whatever schema they name.

    :func:`schema_sheet_exports` pairs an export with the sheets its schema
    constrains *now*, which loses the export the moment its sheet is pointed at
    another schema: the artifact keeps naming the one it was written against.
    This finds them from the sheet's side instead -- by the markers lamindb
    stamps an export with (see :func:`_sheet_for`) -- in one query, scoped like
    :func:`sheet_export_index`.

    Keyed by the sheet's primary key.  `None` when the instance could not be
    introspected.
    """
    from django.db.models import Q

    if not sheets:
        return {}
    marker = Q()
    for sheet in sheets:
        for prefix in EXPORT_DESCRIPTION_PREFIXES:
            marker |= Q(description__startswith=f"{prefix}{sheet.uid}")
        name = sheet.name or sheet.uid
        marker |= Q(key__contains=f"/{name}.")
    try:
        found: dict[Any, list[tuple[Any, Any]]] = {}
        for artifact in _live_exports().filter(marker).order_by("pk"):
            sheet = _sheet_for(artifact, sheets)
            if sheet is not None:
                found.setdefault(sheet.pk, []).append((sheet, artifact))
        return found
    except Exception:
        return None


def current_branch() -> Any:
    """The branch the instance is on, which re-exports are merged back into.

    Mirrors how lamindb decides the branch a new record lands on: the run
    context's, when :func:`lamindb.track` was given one, else the instance
    setting.
    """
    import lamindb as ln

    return getattr(ln.context, "branch", None) or ln.setup.settings.branch


def _sheets_of(schema_ids: list[Any]) -> dict[Any, list[Any]]:
    """The live sheet definitions (record types) constrained by each schema."""
    from lamindb.models import Record

    sheets: dict[Any, list[Any]] = {}
    for sheet in Record.objects.filter(is_type=True, schema_id__in=schema_ids).exclude(
        branch_id=-1
    ):
        sheets.setdefault(sheet.schema_id, []).append(sheet)
    return sheets


def _sheet_for(artifact: Any, sheets: list[Any]) -> Any | None:
    """Which of `sheets` an export artifact was written from.

    Matched on the description lamindb stamps every export with, which names the
    sheet by uid and is therefore exact.  An export whose description was edited
    on the instance falls back to the default key, which names it by sheet name.
    """
    description = getattr(artifact, "description", None) or ""
    for prefix in EXPORT_DESCRIPTION_PREFIXES:
        if description.startswith(prefix):
            uid = description[len(prefix) :].split(":", 1)[0].strip()
            for sheet in sheets:
                if str(sheet.uid) == uid:
                    return sheet
    key = getattr(artifact, "key", None) or ""
    if not key.startswith(EXPORT_KEY_PREFIXES):
        return None
    for sheet in sheets:
        name = sheet.name or sheet.uid
        filename = f"{name}{artifact.suffix}"
        if any(key == prefix + filename for prefix in EXPORT_KEY_PREFIXES):
            return sheet
        if key.startswith("lamindb_record_exports/") and key.endswith("/" + filename):
            # LaminDB includes parent types to distinguish equally named sheets.
            chain = [*reversed(sheet.query_types()), sheet]
            path = "/".join(node.name or node.uid for node in chain)
            if key == f"lamindb_record_exports/{path}{artifact.suffix}":
                return sheet
    return None


def schema_sheet_exports(
    schemas: list[Any], index: dict[Any, list[Any]]
) -> dict[Any, list[tuple[Any, Any]]]:
    """`(sheet, artifact)` per schema, for the exports `index` located.

    Resolves only the schemas that :func:`sheet_export_index` actually found an
    export for, so a plan whose schemas were never exported -- the overwhelmingly
    common case -- reaches the instance not at all here.

    Only the *latest* version of each export family is returned: an older version
    is history and is meant to keep describing the schema as it was, so a
    re-export supersedes the head rather than rewriting the past.
    """
    from lamindb.models import Artifact

    artifact_ids = [
        artifact_id for schema in schemas for artifact_id in index.get(schema.pk, ())
    ]
    if not artifact_ids:
        return {}
    schema_ids = [schema.pk for schema in schemas if schema.pk in index]
    sheets = _sheets_of(schema_ids)
    found: dict[Any, list[tuple[Any, Any]]] = {}
    for artifact in Artifact.objects.filter(pk__in=artifact_ids):
        sheet = _sheet_for(artifact, sheets.get(artifact.schema_id, []))
        if sheet is not None:
            found.setdefault(artifact.schema_id, []).append((sheet, artifact))
    return found


def export_columns(artifact: Any, schema: Any) -> tuple[set[Any] | None, bool]:
    """`(column feature ids, whether they are evidence about the file)`.

    Read from the `columns` feature set lamindb links when it curates a
    `DataFrame` against a schema, so the exported shape is known without
    fetching the file back out of storage.

    The second element is the catch.  A `Schema` is content-addressed, so when
    an export's columns were exactly its schema's members -- the usual case for a
    schema with no optionals -- lamindb deduplicates the feature set onto *that
    very schema row*.  Reconciling the schema in place then rewrites it, and it
    describes the schema as it is now rather than the file as it was written.
    `False` says so, and the caller falls back to the members the schema had
    before the change.

    The ids are `None` when the artifact carries no such link at all (an export
    written before lamindb recorded it, or one whose column count exceeded the
    annotation limit), which callers read as "unknown".
    """
    link = artifact._links_schema.filter(slot=COLUMNS_SLOT).first()
    if link is None:
        return None, False
    columns = link.schema
    members = getattr(columns, "members", None)
    if members is None:
        return None, False
    ids = {member.pk for member in members.all()}
    index = getattr(columns, "index", None)
    if index is not None:
        ids.add(index.pk)
    return ids, columns.pk != getattr(schema, "pk", None)


def export_column_ids(artifact: Any) -> set[Any] | None:
    """The ids of the `Feature` records the exported file has columns for.

    The plain form of :func:`export_columns`, without the "is this evidence"
    question -- for a caller that only wants the recorded shape.
    """
    ids, _ = export_columns(artifact, None)
    return ids


def stale_exports(
    schema: Any,
    exports: list[tuple[Any, Any]],
    *,
    member_ids: set[Any],
    previous_ids: set[Any],
    required_ids: set[Any],
    added_required: list[str],
    newly_required: list[str],
    maximal: bool,
    changed_features: dict[Any, str],
    index_change: str | None = None,
    recorded_columns: Mapping[Any, set[Any]] | None = None,
) -> list[SheetExport]:
    """Which of `exports` no longer adhere to `schema`.

    `exports` are the `(sheet, artifact)` pairs :func:`schema_sheet_exports`
    found for this schema.  Each is judged against the columns it actually
    carries (see :func:`export_columns`) and the shape the schema has *after* the
    change -- `member_ids` and `required_ids` are the ids of its members and
    of the subset a validated dataset must carry, `previous_ids` the members it
    had before, `added_required` and `newly_required` name the members that
    become required, which have no id to compare against (the first is brand new,
    the second was there but optional).  Five things make an export stale:

    * a **required member it has no column for** -- the schema gained one and the
      file cannot satisfy it;
    * a **member that stops being optional** -- the file was written while it was
      allowed to be blank, so its column, if any, may be;
    * a **column the schema no longer declares**, when `maximal` says the schema
      forbids rather than tolerates it;
    * a column whose feature this apply **renamed or retyped**
      (`changed_features` maps a live feature's id to what changed), which
      leaves the header or the values reading against the old definition; and
    * an **index change** (`index_change` describes it): the file's layout is
      keyed by the index -- it is written with the index as its first column, or
      without one (see :func:`reexport`) -- so every export written before the
      change has the old layout.

    A change that only widens a dtype, adds an *optional* member or edits a
    description is deliberately absent: the file still validates against the new
    schema, so there is nothing to rewrite.

    `recorded_columns` overrides the recorded shape per artifact (by primary
    key) for an export whose columns link this apply re-pointed or rewrote: an
    inferred schema absorbed into `schema` (see :mod:`._schema_collisions`) is
    still evidence about the file, though its link now names `schema` itself,
    and one a deleted feature was removed from (see :mod:`._inferred_schemas`)
    no longer lists a column the file still has.
    """
    stale: list[SheetExport] = []
    for sheet, artifact in exports:
        if recorded_columns and artifact.pk in recorded_columns:
            recorded, independent = recorded_columns[artifact.pk], True
        else:
            recorded, independent = export_columns(artifact, schema)
        reasons: list[str] = []
        if recorded is None:
            # The exported shape is unrecorded, so it can be neither compared nor
            # proven current.
            reasons.append("the columns it carries are not recorded")
        # Either unrecorded, or recorded as the schema's own members and
        # therefore rewritten in place by this apply.  What the file holds is
        # then what the schema had before the change.
        columns = recorded if recorded is not None and independent else previous_ids
        if missing := len(required_ids - columns):
            reasons.append(f"no column for {missing} required member(s)")
        if maximal and (extra := len(columns - member_ids)):
            reasons.append(
                f"{extra} column(s) the maximal_set schema no longer declares"
            )
        if added_required:
            reasons.append(
                "no column for new required "
                + ", ".join(repr(name) for name in added_required)
            )
        if newly_required:
            verb = "was" if len(newly_required) == 1 else "were"
            reasons.append(
                f"written while {', '.join(repr(n) for n in newly_required)} "
                f"{verb} still optional"
            )
        reasons.extend(
            sorted(changed_features[pk] for pk in columns if pk in changed_features)
        )
        if index_change is not None:
            reasons.append(f"written before the {index_change}")
        if reasons:
            stale.append(
                SheetExport(sheet=sheet, artifact=artifact, reasons=tuple(reasons))
            )
    return stale


# === writing a new version ====================================================


def branch_prefix_for(plan_name: str) -> str:
    """The prefix every re-export branch of `plan_name` shares."""
    slug = "".join(c if c.isalnum() or c in "-_" else "-" for c in plan_name)
    return f"reexport-{slug}-"


def branch_name_for(plan_name: str, run_uid: str) -> str:
    """The name of the branch this run's re-exports are written onto.

    One branch per run, like a pull request per attempt: lamindb keeps branches
    forever precisely so the provenance of a change stays traceable, and a run
    that failed is a different event from the run that later fixed it.
    Recoverability does not need the name to be reused -- an unmerged branch
    under :func:`branch_prefix_for` is what records that a plan still owes an
    export, whichever run left it (see :func:`unmerged_reexport_branches`).
    """
    return f"{branch_prefix_for(plan_name)}{run_uid}"


def open_branch(name: str, *, description: str) -> Any:
    """Create -- or adopt, on a retry within one run -- the re-export branch."""
    import lamindb as ln

    # Typed loosely: lamindb's `Branch` derives from `BaseSQLRecord` rather
    # than the `SQLRecord` its own signatures are bound to.
    existing: Any = ln.Branch.filter(name=name).one_or_none()
    if existing is not None:
        return existing
    branch: Any = ln.Branch(name=name, description=description)
    branch.save()
    return branch


def unmerged_reexport_branches(
    prefix: str, *, models: LamindbModels | None = None
) -> set[str]:
    """The names of `prefix` branches that were neither merged nor closed.

    An unmerged one is a run that wrote export versions and then failed before
    they could be made live -- so the plan that owns it still owes those exports,
    and the next apply has to redo them even though the schema now matches the
    plan.  Without this the failure would be announced exactly once and then be
    invisible: nothing else in the instance remembers that an export is owed.

    `closed` is excluded because that is what :func:`close_branches` marks an
    attempt a later run superseded -- lamindb's own vocabulary for an abandoned
    change request, and the honest alternative to rewriting a branch's history.

    One query per instance, cached by the caller.  Query failures propagate:
    returning an empty set would incorrectly claim that no retry is owed.

    `models` is the lamindb adapter the branch listing goes through (see
    :class:`~lamindb_setup.evolve.core.LamindbModels`).
    """
    models = DEFAULT_MODELS if models is None else models
    return models.branch_names(prefix=prefix, exclude_status_codes=(_MERGED, _CLOSED))


def close_branches(prefix: str, *, keep: str) -> list[str]:
    """Close the abandoned re-export branches of one plan, except `keep`.

    Called once a run has merged its own branch: any older unmerged sibling is a
    failed attempt this run has just superseded, and its parked versions were
    either adopted into this run's branch (same content) or are simply stale.
    Marking them `closed` retires them without rewriting what happened -- the
    branch, and whatever is still on it, stays inspectable forever.

    Returns the names closed, for the run's report.
    """
    from lamindb.models import Branch

    stale = list(
        Branch.objects.filter(name__startswith=prefix)
        .exclude(name=keep)
        .exclude(_status_code__in=(_MERGED, _CLOSED))
    )
    for branch in stale:
        branch._status_code = _CLOSED
        branch.save(update_fields=["_status_code"])
    return [branch.name for branch in stale]


def merge_branch(branch: Any, *, target: Any) -> None:
    """Merge the re-export branch into `target`, making the new versions live.

    Deliberately not run inside a `transaction.atomic()`: lamindb's merge
    issues its own `BEGIN`/`COMMIT` on PostgreSQL, which would commit an
    enclosing transaction out from under the caller.
    """
    import lamindb as ln

    ln.setup.merge(branch, target=target)


def reexport(export: SheetExport, *, branch: Any) -> Any:
    """Write a new version of `export.artifact` from the sheet's current rows.

    Mirrors what :meth:`lamindb.models.Record.to_artifact` does, with three
    deliberate differences: the previous export's `key` and `description` are
    reused rather than re-derived (so a custom key keeps its version family),
    `revises` names that export explicitly instead of being inferred from the
    key (the new version lives on another branch, where the inference would not
    find it), and `branch` parks the result until the merge.

    Returns the saved artifact -- the same one when the rows hash to the content
    already stored, since lamindb deduplicates by hash and creates no version for
    an export that did not actually change.
    """
    import lamindb as ln

    sheet = export.sheet
    previous = export.artifact
    schema = sheet.schema
    frame = sheet.to_dataframe(use_export_run=True)
    return ln.Artifact.from_dataframe(
        frame,
        key=previous.key,
        description=previous.description,
        schema=schema,
        csv_kwargs={"index": schema is not None and schema.index is not None},
        revises=previous,
        run=getattr(sheet, "_export_run", None),
        branch=branch,
        space=sheet.space,
    ).save()


def promote_head(previous: Any, artifact: Any) -> None:
    """Make `artifact` the head of its version family again, demoting `previous`.

    For the one case a re-export cannot express as a new version: the rows render
    exactly what a *superseded* version of the family holds, so lamindb's
    deduplication hands that row back.  A duplicate is not an option either --
    `(hash, key, storage)` is a database unique constraint, and
    `skip_hash_lookup` only defers to the `IntegrityError` handler that
    returns the same row -- so the family already contains the right content and
    what is wrong is only which member it points at.

    Stable once done: :func:`lamindb.models._is_versioned.reconcile_is_latest_within_branch`,
    which every merge runs, picks its winner from the records that are *already*
    `is_latest` and only ever demotes, so it never promotes the newer sibling
    back over this.

    Written with a queryset update rather than `save()`: this only re-points a
    flag, and `save()` on a versioned record re-runs the revises/demote logic.

    The demote and the promote are one transaction: they are two statements over
    the same invariant -- exactly one member of a family is the head -- and an
    interruption between them is the one state no reader can interpret, a family
    with no head at all.  This is the phase that runs *after* the apply
    committed, so nothing else would roll it back.
    """
    from django.db import transaction

    model = type(artifact)
    with transaction.atomic():
        model.objects.filter(pk=previous.pk).update(is_latest=False)
        model.objects.filter(pk=artifact.pk).update(is_latest=True)
    artifact.is_latest = True
