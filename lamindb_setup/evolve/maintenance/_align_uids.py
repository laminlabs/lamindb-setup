"""Align the uids of plan-managed objects across the instances a plan is applied to.

A plan applied to several instances is meant to give each object one identity
everywhere: `apply` creates an object with the uid the plan's lock files for
its other instances record (see :mod:`lamindb_setup.evolve.engine._alignment`).
Instances provisioned before that rule existed, or out of band, still disagree.
This is the repair: one instance is named the *source*, and every *target* has
its objects' uids rewritten to the source's, along with its lock files.

What a uid rename has to carry along on a target, all in one transaction:

* the `uid` column itself -- in two phases through temporary uids, so a set
  of renames that swaps uids between objects never collides with itself;
* every reference to the old uid held as a *string* rather than a foreign key:
  dtype strings such as `cat[Record[<uid>]]` and `schema__uid` filters (on
  every feature, managed or not), a schema's `itype`, and the feature uids in
  a schema's or feature's `_aux` (optionals, index, value features);
* the content hash of every schema whose members changed uid, which lamindb
  derives from the member uids, and of every composite embedding one.

Foreign keys address the internal id, which does not change, so they need
nothing.  Afterwards every lock file of the target that records a changed uid
is rewritten -- the aligned plans', and those of plans built on them -- so the
next `apply` finds the state it expects.

When the uid an object needs is already taken on the target, the holder decides
what happens.  A trashed (or archived) holder is moved to a fresh uid of its
own, nothing else about it touched.  A live holder is refused by default; with
`merge_duplicates` an *equivalent* holder (same kind, name, parent and shape --
typically a stub `lamin io sync` created) is merged into the plan's object,
and with `reassign_colliding` it keeps living under a fresh uid, its
references following it.

A rehash can leave two live schemas on one content, which lamindb does not
allow -- most often because they already shared it before the alignment, as a
plan schema edited onto the content of an inferred schema did before lamindb
refused that.  They are settled as :mod:`._content_duplicates` describes: an
inferred schema is absorbed into the other, exactly as `apply` absorbs one;
with `merge_duplicates` any other schema no plan manages is merged into the
one a plan does; anything else refuses the target, saying why.

:func:`read_source_identity` and :func:`compute_alignment` are read-only.
:func:`execute_alignment` writes, or with `dry_run` proves what it would write
inside a transaction it rolls back.  :func:`rewrite_target_locks` writes the
lock files once the transaction has committed.
"""

from __future__ import annotations

from typing import Any

from typing_extensions import TypeVar

T = TypeVar("T")

import io
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from ..core import SEED_ONLY_KINDS, LaminSchemasError
from ._content_duplicates import schema_label, settle_content_group, settle_schemas

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterable, Sequence

    from ..authoring import Plan
    from ._content_duplicates import ContentGroup

    Report = Callable[[str], None]
    Track = Callable[[Sequence[Any], str], Iterable[Any]]

__all__ = [
    "AlignError",
    "HOLDER_CONFLICT",
    "HOLDER_FREE",
    "HOLDER_MERGE",
    "HOLDER_REASSIGN",
    "SourceIdentity",
    "TargetAlignment",
    "UidHolder",
    "UidRename",
    "collect_instance_locks",
    "compute_alignment",
    "execute_alignment",
    "plan_file_of",
    "print_alignment",
    "read_source_identity",
    "render_alignment",
    "rewrite_target_locks",
    "with_prerequisites",
]

HOLDER_FREE = "free"
HOLDER_MERGE = "merge"
HOLDER_REASSIGN = "reassign"
HOLDER_CONFLICT = "conflict"


# Fields a duplicate link row may differ in and still be the same link: who
# wrote it, when, and in which run -- not what it says.
class AlignError(LaminSchemasError):
    """The uids cannot be aligned as asked; nothing was changed."""


class _DryRun(Exception):
    """Rolls the dry-run transaction of :func:`execute_alignment` back."""


# === Batching ===================================================================
#
# A target is usually a remote Postgres instance, where every statement costs a
# round trip.  So the work below is done a registry at a time -- one statement
# for many rows -- rather than a row at a time.

#: How many rows (or uids) one batched statement covers.
_BATCH = 500


def _silent(_message: str) -> None:
    """A `report` callback that prints nothing."""


def _untracked(sequence: Sequence[Any], _description: str) -> Iterable[Any]:
    """A `track` callback that draws no progress bar."""
    return sequence


def _chunks(items: Sequence[T], size: int = _BATCH) -> Iterable[Sequence[T]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _by_model(items: Iterable[tuple[Any, T]]) -> dict[Any, list[T]]:
    """Group `(model, item)` pairs by model, keeping their order."""
    grouped: dict[Any, list[T]] = {}
    for model, item in items:
        grouped.setdefault(model, []).append(item)
    return grouped


def _records_by_uid(keys: Iterable[tuple[Any, str]]) -> dict[tuple[Any, str], Any]:
    """Load the record holding each `(model, uid)`, on any branch, in bulk."""
    found: dict[tuple[Any, str], Any] = {}
    for model, uids in _by_model(keys).items():
        for chunk in _chunks(sorted(set(uids))):
            for record in model.objects.filter(uid__in=list(chunk)):
                found[(model, str(record.uid))] = record
    return found


def _set_uids(model: Any, uids: dict[Any, str]) -> None:
    """Set the `uid` of each row of `model` (by primary key), in bulk.

    Each batch is one `UPDATE ... SET uid = CASE pk WHEN ... END`.  The new
    uids must be free and distinct: a unique index is checked row by row while
    the statement runs, which is why :func:`_execute` moves uids in two phases.
    """
    from django.db.models import Case, CharField, Value, When

    for chunk in _chunks(list(uids.items())):
        model.objects.filter(pk__in=[pk for pk, _uid in chunk]).update(
            uid=Case(
                *(When(pk=pk, then=Value(uid)) for pk, uid in chunk),
                output_field=CharField(),
            )
        )


# === Plans ======================================================================


def plan_file_of(plan: Plan[Any]) -> Path:
    """Return the file that defines `plan`, from the module it was declared in."""
    module = sys.modules.get(plan.defining_module or "")
    path = getattr(module, "__file__", None)
    if path is None:
        raise AlignError(f"cannot tell which file defines plan {plan.name!r}")
    return Path(path).resolve()


def with_prerequisites(
    plans: Sequence[tuple[Plan[Any], Path]],
) -> list[tuple[Plan[Any], Path]]:
    """Return `plans` plus every plan they depend on, prerequisites first.

    A plan only borrows its prerequisites' objects -- their locks record them --
    so aligning a plan without its prerequisites would leave the very objects
    its schemas are built from on the old uids.
    """
    ordered: list[tuple[Plan[Any], Path]] = []
    seen: set[int] = set()
    files = {id(plan): path for plan, path in plans}

    def visit(plan: Plan[Any]) -> None:
        if id(plan) in seen:
            return
        seen.add(id(plan))
        for prerequisite in plan.depends_on:
            visit(prerequisite)
        ordered.append((plan, files.get(id(plan)) or plan_file_of(plan)))

    for plan, _path in plans:
        visit(plan)
    return ordered


# === Source =====================================================================


@dataclass
class SourceIdentity:
    """The uids the source instance holds for each plan's objects.

    `tracked` and `seeded` map a plan's name to `{natural key: uid}`, read
    from the source's lock files and verified against the live source instance.
    `looked_up` counts the seeded objects whose uid was read from the instance
    itself, when the source lock has no entry for that seed.
    """

    slug: str
    tracked: dict[str, dict[str, str]] = field(default_factory=dict)
    seeded: dict[str, dict[str, str]] = field(default_factory=dict)
    lock_files: list[Path] = field(default_factory=list)
    missing_locks: list[str] = field(default_factory=list)
    looked_up: int = 0

    def uid_for(self, plan_name: str, key: str, *, seeded: bool) -> str | None:
        """The source uid of `key` in plan `plan_name`, if it has one."""
        return (self.seeded if seeded else self.tracked).get(plan_name, {}).get(key)


def read_source_identity(
    plans: Sequence[tuple[Plan[Any], Path]], source_slug: str
) -> SourceIdentity:
    """Read the source's uids for `plans` and verify them, connected to the source.

    The lock files say which uids win, so a stale one -- a uid the source no
    longer has, or only in its trash -- would push a uid onto the targets that
    names nothing on the source.  Every recorded uid is therefore checked
    against the live source first, and any mismatch refuses the whole run.
    """
    from ..engine import (
        LockFile,
        is_live,
        lock_path,
        lock_with_moves,
        natural_key,
        registry_for_kind,
        resolve_existing,
    )

    identity = SourceIdentity(slug=source_slug)
    expected: dict[int, tuple[Any, dict[str, str]]] = {}
    for plan, plan_file in plans:
        path = lock_path(plan_file, source_slug)
        lock = lock_with_moves(plan, LockFile.read(path))
        if lock is None:
            identity.missing_locks.append(plan.name)
            continue
        identity.lock_files.append(path)
        tracked = identity.tracked.setdefault(plan.name, {})
        seeded = identity.seeded.setdefault(plan.name, {})
        unknown_seeded: list[Any] = []
        for dependency in plan.own_dependencies:
            obj = dependency.obj
            key = natural_key(obj)
            kind = type(obj).__name__
            if kind in SEED_ONLY_KINDS:
                uid = lock.seeded.get(key)
                if uid is None:
                    unknown_seeded.append(obj)
                    continue
                seeded[key] = uid
            else:
                uid = lock.uid_for(key)
                if uid is None:
                    continue
                tracked[key] = uid
            registry = registry_for_kind(kind)
            expected.setdefault(id(registry), (registry, {}))[1][uid] = key
        if unknown_seeded:
            existing = resolve_existing(plan, lock)
            for obj in unknown_seeded:
                record = existing.get(id(obj))
                if record is not None and is_live(record):
                    seeded[natural_key(obj)] = str(record.uid)
                    identity.looked_up += 1

    stale: list[str] = []
    for registry, uids in expected.values():
        found = dict(
            registry.objects.filter(uid__in=list(uids)).values_list("uid", "branch_id")
        )
        for uid, key in sorted(uids.items(), key=lambda item: item[1]):
            branch_id = found.get(uid)
            if branch_id is None:
                stale.append(f"{key}: uid {uid} does not exist on {source_slug}")
            elif branch_id <= 0:
                stale.append(f"{key}: uid {uid} is in the trash of {source_slug}")
    if stale:
        raise AlignError(
            f"the lock files of {source_slug} do not match the instance, so they "
            "cannot say which uids win:\n  "
            + "\n  ".join(stale)
            + f"\nRe-apply the plan(s) to {source_slug} first, or pick another "
            "--source."
        )
    return identity


# === Target =====================================================================


@dataclass
class UidRename:
    """One object on the target whose uid changes to the source's."""

    kind: str
    key: str
    name: str | None
    plan: str
    old_uid: str
    new_uid: str
    model: Any = field(default=None, repr=False, compare=False)
    pk: Any = field(default=None, repr=False, compare=False)


@dataclass
class UidHolder:
    """A record on the target that already holds a uid a rename needs.

    `action` is one of :data:`HOLDER_FREE` (trashed or archived: moved to a
    fresh uid), :data:`HOLDER_MERGE`, :data:`HOLDER_REASSIGN` (live: kept under a
    fresh uid, references following) or :data:`HOLDER_CONFLICT` (refused).
    `new_uid` is the uid it ended up with, once executed.
    """

    uid: str
    label: str
    live: bool
    action: str
    rename: UidRename
    reason: str = ""
    references: list[str] = field(default_factory=list)
    new_uid: str | None = None
    model: Any = field(default=None, repr=False, compare=False)
    pk: Any = field(default=None, repr=False, compare=False)


@dataclass
class TargetAlignment:
    """What aligning one target instance to the source changes (or changed).

    `settled` describes each schema the rehash absorbed or merged into another
    holding the same content; `schema_pks` are the schemas the plans resolve to
    on the target, which are never settled away.
    """

    source_slug: str
    instance: str
    slug: str
    plans: list[str] = field(default_factory=list)
    renames: list[UidRename] = field(default_factory=list)
    holders: list[UidHolder] = field(default_factory=list)
    aligned: int = 0
    missing: list[str] = field(default_factory=list)
    unpinned: list[str] = field(default_factory=list)
    lock_files: list[Path] = field(default_factory=list)
    rewritten_dtypes: int = 0
    rewritten_aux: int = 0
    rehashed: list[str] = field(default_factory=list)
    rehashed_uids: set[str] = field(default_factory=set)
    moved_references: int = 0
    dropped_duplicates: int = 0
    settled: list[str] = field(default_factory=list)
    merge_duplicates: bool = False
    schema_pks: set[Any] = field(default_factory=set, repr=False, compare=False)
    error: str | None = None
    executed: bool = False

    @property
    def conflicts(self) -> list[UidHolder]:
        """Holders that block the alignment of this target."""
        return [h for h in self.holders if h.action == HOLDER_CONFLICT]

    @property
    def is_empty(self) -> bool:
        """Whether every uid already agrees with the source."""
        return not self.renames

    @property
    def ok(self) -> bool:
        """Whether the alignment can (or did) go through."""
        return not self.conflicts and self.error is None

    @property
    def affected_uids(self) -> set[str]:
        """The uids a lock of this instance may still record that no longer hold.

        Known before anything runs (unlike :attr:`uid_map`, which needs the
        fresh uids of reassigned holders), so it can select the lock files to
        check and rewrite up front.
        """
        affected = {r.old_uid for r in self.renames}
        affected |= {h.uid for h in self.holders if h.action == HOLDER_REASSIGN}
        return affected

    @property
    def uid_map(self) -> dict[str, str]:
        """Old uid to new uid, for every rename and live reassignment."""
        mapping = {r.old_uid: r.new_uid for r in self.renames}
        for holder in self.holders:
            if holder.action == HOLDER_REASSIGN and holder.new_uid is not None:
                mapping[holder.uid] = holder.new_uid
        return mapping


def _describe(record: Any) -> str:
    kind = type(record).__name__
    if getattr(record, "is_type", False):
        kind += " type"
    name = getattr(record, "name", None)
    label = f" {name!r}" if name is not None else ""
    return f"{kind}{label} (uid {record.uid})"


def compute_alignment(
    plans: Sequence[tuple[Plan[Any], Path]],
    source: SourceIdentity,
    *,
    instance: str,
    slug: str,
    merge_duplicates: bool = False,
    reassign_colliding: bool = False,
    report: Report | None = None,
    track: Track | None = None,
) -> TargetAlignment:
    """Work out, read-only, which uids change on the connected target.

    Each object is resolved the way this instance's own state resolves it -- the
    uid its lock pins, else by name, kind and parent -- and renamed to the uid
    the source records for it when the two differ.  Objects the source never
    recorded are left alone (`unpinned`), as are objects the target does not
    have (`missing`): the next `apply` creates those with the source's uid.

    `report` and `track` are optional progress callbacks (see
    :func:`lamindb_setup.evolve.console.progress_reporter`).
    """
    from ..engine import (
        LockFile,
        is_live,
        lock_path,
        natural_key,
        resolve_existing,
    )

    report = report or _silent
    track = track or _untracked
    alignment = TargetAlignment(
        source_slug=source.slug,
        instance=instance,
        slug=slug,
        merge_duplicates=merge_duplicates,
    )
    by_record: dict[tuple[Any, Any], UidRename] = {}
    wanted: dict[tuple[Any, str], UidRename] = {}
    for plan, plan_file in track(list(plans), f"Resolving plan objects on {slug}"):
        alignment.plans.append(plan.name)
        path = lock_path(plan_file, slug)
        lock = LockFile.read(path)
        if lock is not None:
            alignment.lock_files.append(path)
        existing = resolve_existing(plan, lock)
        for dependency in plan.own_dependencies:
            obj = dependency.obj
            key = natural_key(obj)
            kind = type(obj).__name__
            record = existing.get(id(obj))
            live = record is not None and is_live(record)
            if live and type(record).__name__ == "Schema":
                alignment.schema_pks.add(record.pk)
            want = source.uid_for(plan.name, key, seeded=kind in SEED_ONLY_KINDS)
            if want is None:
                alignment.unpinned.append(key)
                continue
            if not live:
                alignment.missing.append(key)
                continue
            if str(record.uid) == want:
                alignment.aligned += 1
                continue
            model = type(record)
            if (model, record.pk) in by_record:
                continue
            other = wanted.get((model, want))
            if other is not None:
                raise AlignError(
                    f"{key!r} and {other.key!r} resolve to two records on {slug} "
                    f"that would both take uid {want}; deduplicate them first "
                    "(e.g. with 'lamin evolve repair duplicates')."
                )
            rename = UidRename(
                kind=kind,
                key=key,
                name=getattr(obj, "name", None),
                plan=plan.name,
                old_uid=str(record.uid),
                new_uid=want,
                model=model,
                pk=record.pk,
            )
            by_record[(model, record.pk)] = rename
            wanted[(model, want)] = rename
            alignment.renames.append(rename)

    final = _final_uid_fn(by_record)
    if wanted:
        report(f"Looking up who holds the {len(wanted)} uid(s) to take on {slug}")
    held = _records_by_uid(wanted)
    for (model, want), rename in wanted.items():
        holder = held.get((model, want))
        if holder is None or (model, holder.pk) in by_record:
            continue
        entry = UidHolder(
            uid=want,
            label=_describe(holder),
            live=is_live(holder),
            action=HOLDER_FREE,
            rename=rename,
            model=model,
            pk=holder.pk,
        )
        if entry.live:
            entry.references = _reference_summary(model, holder.pk)
            target = model.objects.get(pk=rename.pk)
            blocker = _merge_blocker(holder, target, final)
            if merge_duplicates and blocker is None:
                entry.action = HOLDER_MERGE
            elif reassign_colliding:
                entry.action = HOLDER_REASSIGN
            else:
                entry.action = HOLDER_CONFLICT
                if merge_duplicates:
                    entry.reason = f"cannot merge: it {blocker}"
                elif blocker is None:
                    entry.reason = (
                        "it duplicates the plan's object; pass --merge-duplicates "
                        "to merge it, or --reassign-colliding to keep it under a "
                        "fresh uid"
                    )
                else:
                    entry.reason = (
                        f"it is a different object ({blocker}); pass "
                        "--reassign-colliding to keep it under a fresh uid"
                    )
        alignment.holders.append(entry)
    return alignment


def _final_uid_fn(
    by_record: dict[tuple[Any, Any], UidRename],
) -> Callable[[Any], str | None]:
    """The uid a record has once the renames are applied (`None` for none)."""

    def final(record: Any) -> str | None:
        if record is None:
            return None
        rename = by_record.get((type(record), record.pk))
        return rename.new_uid if rename is not None else str(record.uid)

    return final


def _merge_blocker(
    holder: Any, target: Any, final: Callable[[Any], str | None]
) -> str | None:
    """Why `holder` is not the same object as `target`, or `None` if it is.

    Compared as they will be once aligned: references are taken through
    `final`, so a stub whose parent is itself a stub of the plan's parent
    still counts as sitting under the same parent.
    """
    if bool(getattr(holder, "is_type", False)) != bool(
        getattr(target, "is_type", False)
    ):
        return "is a type where the plan's object is not, or the reverse"
    if getattr(holder, "name", None) != getattr(target, "name", None):
        return f"is named {holder.name!r}, not {target.name!r}"
    if final(getattr(holder, "type", None)) != final(getattr(target, "type", None)):
        return "sits under a different parent type"
    model = type(target).__name__
    if model == "Feature" and not target.is_type:
        if holder._dtype_str != target._dtype_str:
            return f"has dtype {holder._dtype_str!r}, not {target._dtype_str!r}"
    if model == "Schema" and not target.is_type:
        for attr in ("itype", "otype"):
            if getattr(holder, attr, None) != getattr(target, attr, None):
                return f"has a different {attr}"
        if {final(m) for m in holder.members.all()} != {
            final(m) for m in target.members.all()
        }:
            return "declares different members"
    if (
        model == "Record"
        and target.is_type
        and final(getattr(holder, "schema", None))
        != final(getattr(target, "schema", None))
    ):
        return "is validated by a different schema"
    return None


# === References =================================================================


def _reference_summary(model: Any, pk: Any) -> list[str]:
    """Describe what references the record `pk`: `"3 x Record.type"`, ..."""
    from ..engine import referencing_fields

    lines: list[str] = []
    for related, fk in referencing_fields(model):
        count = related.objects.filter(**{fk.attname: pk}).count()
        if count:
            lines.append(f"{count} x {related.__name__}.{fk.name}")
    return lines


def _token_pattern(mapping: dict[str, str]) -> re.Pattern[str]:
    """Match any old uid as a whole token, never inside a longer identifier."""
    alternatives = "|".join(
        re.escape(uid) for uid in sorted(mapping, key=len, reverse=True)
    )
    return re.compile(rf"(?<![A-Za-z0-9])(?:{alternatives})(?![A-Za-z0-9])")


def _sub_tokens(value: str, pattern: re.Pattern[str], mapping: dict[str, str]) -> str:
    return pattern.sub(lambda match: mapping[match.group(0)], value)


def _map_json(value: Any, transform: Callable[[str], str]) -> Any:
    if isinstance(value, str):
        return transform(value)
    if isinstance(value, list):
        return [_map_json(item, transform) for item in value]
    if isinstance(value, dict):
        return {key: _map_json(item, transform) for key, item in value.items()}
    return value


def _rows_mentioning(
    model: Any, name: str, uids: Iterable[str], *, as_text: bool = False
) -> dict[Any, Any]:
    """Map the primary key of every row whose `name` mentions a uid to its value.

    The database does the filtering, so only rows that may need rewriting come
    back.  `as_text` matches a JSON column by its text; the caller still
    decides on the decoded value, the match only narrows the rows.
    """
    from django.db.models import Q, TextField
    from django.db.models.functions import Cast

    queryset = model.objects.all()
    lookup = name
    if as_text:
        queryset = queryset.annotate(mentions_text=Cast(name, TextField()))
        lookup = "mentions_text"
    rows: dict[Any, Any] = {}
    for chunk in _chunks(sorted(uids)):
        condition = Q()
        for uid in chunk:
            condition |= Q(**{f"{lookup}__contains": uid})
        rows.update(queryset.filter(condition).values_list("pk", name))
    return rows


def _rewrite_string_references(mapping: dict[str, str]) -> tuple[int, int, set[Any]]:
    """Rewrite every uid-string reference to an old uid, instance-wide.

    Returns the number of dtype strings and `_aux` blobs rewritten, and the
    primary keys of the schemas touched (their hash may depend on it).
    """
    import lamindb as ln

    if not mapping:
        return 0, 0, set()
    pattern = _token_pattern(mapping)
    dtypes = auxes = 0
    touched_schemas: set[Any] = set()
    for model, fields in (
        (ln.Feature, ("_dtype_str",)),
        (ln.Schema, ("itype", "_dtype_str")),
    ):
        for name in fields:
            for pk, value in _rows_mentioning(model, name, mapping).items():
                new = _sub_tokens(value, pattern, mapping)
                if new != value:
                    model.objects.filter(pk=pk).update(**{name: new})
                    dtypes += 1
                    if model is ln.Schema:
                        touched_schemas.add(pk)
    # Exact matches only: an `_aux` value is either a uid or something else.
    for model in (ln.Feature, ln.Schema):
        rows = _rows_mentioning(model, "_aux", mapping, as_text=True)
        for pk, aux in rows.items():
            new_aux = _map_json(aux, lambda s: mapping.get(s, s))
            if new_aux != aux:
                model.objects.filter(pk=pk).update(_aux=new_aux)
                auxes += 1
                if model is ln.Schema:
                    touched_schemas.add(pk)
    return dtypes, auxes, touched_schemas


def _rehash(
    feature_pks: set[Any],
    schema_pks: set[Any],
    *,
    track: Track | None = None,
    shared: _SharedContent | None = None,
) -> dict[str, str]:
    """Recompute the hash of every schema the renames reach, and its composites.

    lamindb hashes a schema from its member and index uids, and a composite
    from its components' hashes, so both go stale when a member is renamed.
    The reach is wide: every schema a renamed feature is a member of, which
    includes the inferred schema of each artifact curated with it.  So rather
    than re-save them one by one -- a dozen statements each -- the schemas,
    their links and members are loaded in bulk, each hash is computed the way
    `Schema.save` computes it (components before the composites slotting
    them), and the new hashes are written in one batched update.  A schema
    whose members live in another registry is still re-saved by lamindb.
    Schemas left on one content are settled by `shared` (see
    :func:`_write_hashes`).  Returns the uid of every schema whose hash changed,
    mapped to its label -- leaving out any absorbed into another.
    """
    from lamindb.models import SchemaFeature

    from ..engine import is_live

    track = track or _untracked
    shared = shared if shared is not None else _SharedContent()
    pks = set(schema_pks)
    for chunk in _chunks(sorted(feature_pks)):
        pks.update(
            SchemaFeature.objects.filter(feature_id__in=list(chunk)).values_list(
                "schema_id", flat=True
            )
        )
    order = _composites_last(pks)
    if not order:
        return {}
    schemas = _schemas_by_pk(order)
    shared.before.update((pk, schema.hash) for pk, schema in schemas.items())
    members = _members_by_schema(order)
    slots = _slots_by_schema(order, schemas)
    pending: dict[Any, str] = {}
    unsettled = set(order)
    rehashed: dict[Any, tuple[str, str]] = {}
    for pk in track(order, f"Rehashing {len(order)} schema(s)"):
        unsettled.discard(pk)
        schema = schemas.get(pk)
        if schema is None or schema.is_type or not is_live(schema):
            continue
        before = schema.hash
        content_hash = _content_hash(schema, members.get(pk, []), slots.get(pk, {}))
        if content_hash is None:
            # lamindb's save reads the components' hashes from the database.
            _write_hashes(pending, schemas, shared, unsettled=unsettled | {pk})
            pending.clear()
            schema.save(print_hash_mutation_warning=False)
        elif content_hash != before:
            schema.hash = content_hash
            pending[pk] = content_hash
        if schema.hash != before:
            rehashed[pk] = (str(schema.uid), schema.name or str(schema.uid))
    _write_hashes(pending, schemas, shared)
    return {
        uid: label for pk, (uid, label) in rehashed.items() if pk not in shared.absorbed
    }


def _composites_last(pks: set[Any]) -> list[Any]:
    """`pks` and every composite slotting one, transitively, components first.

    Read a level of composites at a time, and ordered so each composite comes
    after every component of it that is rehashed too: one pass then reaches
    the hash each schema settles on.
    """
    from lamindb.models import SchemaComponent

    components_of: dict[Any, set[Any]] = {}
    closure: set[Any] = set()
    frontier = set(pks)
    while frontier:
        closure |= frontier
        composites: set[Any] = set()
        for chunk in _chunks(sorted(frontier)):
            rows = SchemaComponent.objects.filter(
                component_id__in=list(chunk)
            ).values_list("component_id", "composite_id")
            for component, composite in rows:
                components_of.setdefault(composite, set()).add(component)
                composites.add(composite)
        frontier = composites - closure
    ordered: list[Any] = []
    placed: set[Any] = set()
    remaining = sorted(closure)
    while remaining:
        ready = [pk for pk in remaining if components_of.get(pk, set()) <= placed]
        # A cycle cannot be built in lamindb; never loop on one regardless.
        ready = ready or remaining
        ordered.extend(ready)
        placed.update(ready)
        remaining = [pk for pk in remaining if pk not in placed]
    return ordered


def _schemas_by_pk(pks: Sequence[Any]) -> dict[Any, Any]:
    import lamindb as ln

    found: dict[Any, Any] = {}
    for chunk in _chunks(list(pks)):
        found.update((s.pk, s) for s in ln.Schema.objects.filter(pk__in=list(chunk)))
    return found


def _members_by_schema(pks: Sequence[Any]) -> dict[Any, list[Any]]:
    """Each schema's feature members, in link order -- the order lamindb hashes."""
    import lamindb as ln
    from lamindb.models import SchemaFeature

    links: dict[Any, list[Any]] = {}
    for chunk in _chunks(list(pks)):
        rows = (
            SchemaFeature.objects.filter(schema_id__in=list(chunk))
            .order_by("id")
            .values_list("schema_id", "feature_id")
        )
        for schema_id, feature_id in rows:
            links.setdefault(schema_id, []).append(feature_id)
    feature_pks = sorted({pk for ids in links.values() for pk in ids})
    features: dict[Any, Any] = {}
    for chunk in _chunks(feature_pks):
        features.update(
            (f.pk, f) for f in ln.Feature.objects.filter(pk__in=list(chunk))
        )
    return {
        schema_id: [features[pk] for pk in ids if pk in features]
        for schema_id, ids in links.items()
    }


def _slots_by_schema(
    pks: Sequence[Any], schemas: dict[Any, Any]
) -> dict[Any, dict[str, Any]]:
    """Each composite's slots, a rehashed component as the object rehashed."""
    from lamindb.models import SchemaComponent

    rows: list[tuple[Any, Any, str]] = []
    for chunk in _chunks(list(pks)):
        rows.extend(
            SchemaComponent.objects.filter(composite_id__in=list(chunk)).values_list(
                "composite_id", "component_id", "slot"
            )
        )
    others = _schemas_by_pk(sorted({c for _, c, _ in rows if c not in schemas}))
    slots: dict[Any, dict[str, Any]] = {}
    for composite, component, slot in rows:
        loaded = schemas.get(component)
        if loaded is None:
            loaded = others.get(component)
        slots.setdefault(composite, {})[slot] = loaded
    return slots


def _content_hash(schema: Any, members: list[Any], slots: dict[str, Any]) -> str | None:
    """The hash `schema.save()` would store, from members and slots in hand.

    `None` when it cannot be told without lamindb's own save: the members
    live in another registry, the index is not among them, or lamindb cannot
    hash the schema at all.
    """
    from ..core import DEFAULT_MODELS

    itype = schema.itype
    if itype == "Composite":
        members = []
    elif itype is not None and not str(itype).startswith("Feature"):
        return None
    if any(component is None for component in slots.values()):
        return None
    index_uid = getattr(schema, "_index_feature_uid", None)
    index = None
    if index_uid is not None:
        index = next((m for m in members if str(m.uid) == index_uid), None)
        if index is None:
            return None
    optional_uids = {str(uid) for uid in schema.optionals.get_uids()}
    return DEFAULT_MODELS.schema_content_hash(
        schema,
        features=[m for m in members if index is None or m.pk != index.pk],
        index=index,
        optionals=[m for m in members if str(m.uid) in optional_uids],
        slots=slots,
        flexible=schema.flexible,
        coerce=schema.coerce,
        minimal_set=schema.minimal_set,
        ordered_set=schema.ordered_set,
        maximal_set=schema.maximal_set,
    )


def _unmanaged(_schema: Any) -> bool:
    """A `managed` predicate under which no plan manages any schema."""
    return False


@dataclass
class _SharedContent:
    """How a rehash settles schemas it leaves on one content, and what it did.

    `managed` tells the schemas a plan manages; `merge` is
    `--merge-duplicates`; `before` holds each loaded schema's hash from
    before the alignment, by primary key.  The rest records the outcome: one
    line per schema absorbed or merged, the absorbed (deleted) and merged
    (trashed) primary keys, and the references the merges moved and dropped.
    """

    managed: Callable[[Any], bool] = _unmanaged
    merge: bool = False
    before: dict[Any, str | None] = field(default_factory=dict)
    settled: list[str] = field(default_factory=list)
    absorbed: set[Any] = field(default_factory=set)
    merged: set[Any] = field(default_factory=set)
    moved: int = 0
    dropped: int = 0

    def record(self, group: ContentGroup) -> None:
        """Note what settling `group` does -- before it does it.

        Deleting a schema clears its primary key, so the absorbed ones are
        noted while they still have theirs.
        """
        keeper = schema_label(group.keeper)
        for collision in group.absorbed:
            self.absorbed.add(collision.schema.pk)
            self.settled.append(f"{keeper} {collision.detail()}")
        for collision in group.merged:
            self.merged.add(collision.schema.pk)
            self.settled.append(
                f"{keeper} merges {schema_label(collision.schema)}, which has the "
                "same content: what referenced it now references the plan's "
                "schema, and it is in the trash"
            )


def _write_hashes(
    hashes: dict[Any, str],
    schemas: dict[Any, Any],
    shared: _SharedContent | None = None,
    *,
    unsettled: Collection[Any] = (),
) -> None:
    """Store the new `hashes` (by schema primary key), settling a shared content.

    lamindb keeps one schema per content hash outside the trash, and its save
    refuses one that would take another's.  So does this, judged on the hashes
    every schema will have: another rehashed one's new hash, not its old one --
    the loaded schemas whose new hash is not known yet (`unsettled`) are left
    out until it is.  Live schemas left on one content are settled as
    :mod:`._content_duplicates` describes, by `shared`: an inferred schema is
    absorbed into the other, and with `--merge-duplicates` any other schema no
    plan manages is merged into the one a plan does.  Anything else is refused,
    saying whether the schemas already shared their content or only would once
    aligned.
    """
    import lamindb as ln
    from django.db.models import Case, CharField, Value, When

    shared = shared if shared is not None else _SharedContent()
    if not hashes:
        return
    gone = shared.absorbed | shared.merged
    holders: dict[str, dict[Any, Any]] = {}
    for pk, schema in schemas.items():
        if pk in unsettled or pk in gone:
            continue
        if schema.branch_id != -1 and schema.hash is not None:
            holders.setdefault(schema.hash, {})[pk] = schema
    chunk: Sequence[Any]
    for chunk in _chunks(sorted(set(hashes.values()))):
        others = ln.Schema.objects.filter(hash__in=list(chunk)).exclude(branch_id=-1)
        for other in others:
            if other.pk not in schemas:
                holders.setdefault(other.hash, {})[other.pk] = other
    groups: list[ContentGroup] = []
    for content_hash in dict.fromkeys(hashes.values()):
        sharing = holders.get(content_hash, {})
        if len(sharing) < 2:
            continue
        group = settle_content_group(
            sharing.values(), content_hash, managed=shared.managed, merge=shared.merge
        )
        if not group.ok:
            raise AlignError(_shared_content_refusal(group, hashes, shared))
        groups.append(group)
    for group in groups:
        shared.record(group)
        moved, dropped = settle_schemas(group, error=AlignError)
        shared.moved += moved
        shared.dropped += dropped
    writes = [(pk, value) for pk, value in hashes.items() if pk not in shared.absorbed]
    for chunk in _chunks(writes):
        ln.Schema.objects.filter(pk__in=[pk for pk, _hash in chunk]).update(
            hash=Case(
                *(When(pk=pk, then=Value(value)) for pk, value in chunk),
                output_field=CharField(),
            )
        )


def _shared_content_refusal(
    group: ContentGroup, hashes: dict[Any, str], shared: _SharedContent
) -> str:
    """Why the alignment cannot leave `group` on one content, and what to do.

    Told from the plan's schema when the group has one.  The schemas *already*
    had the same content when they all held one hash before the alignment -- a
    duplicate it only carries along -- and otherwise *would* have it once
    aligned.
    """
    subject = group.keeper or next(
        (s for s in group.schemas if shared.managed(s)), group.schemas[0]
    )
    others = [s for s in group.schemas if s.pk != subject.pk]

    def old(schema: Any) -> str | None:
        if schema.pk in shared.before:
            return shared.before[schema.pk]
        return None if schema.pk in hashes else schema.hash

    names = ", ".join(schema_label(s) for s in others)
    if old(subject) is not None and all(old(s) == old(subject) for s in others):
        head = (
            f"{schema_label(subject)} already has the same content as {names}, "
            "which lamindb no longer allows, and the alignment rehashes them "
            "together"
        )
    else:
        head = (
            f"once aligned, {schema_label(subject)} would have the same content "
            f"as {names}, and lamindb keeps one schema per content"
        )
    reasons = [
        f"{schema_label(c.schema)} cannot be absorbed: {'; '.join(c.blockers)}"
        for c in group.blocked
        if c.schema.pk != subject.pk
    ]
    if group.mergeable:
        many = len(group.blocked) > 1
        them, they = ("them", "they are") if many else ("it", "it is")
        hint = (
            f"Pass --merge-duplicates to merge {them} into {schema_label(subject)}: "
            f"everything referencing {them} is re-pointed to it, and {they} moved "
            "to the trash."
        )
    elif group.managed > 1:
        hint = (
            "Plans manage more than one of them: change the plans so their schemas "
            "differ, or resolve the duplicate by hand first."
        )
    else:
        hint = (
            "Which one to keep is a decision to make by hand: resolve the duplicate "
            "first ('lamin evolve repair duplicates' lists schemas sharing their "
            "content)."
        )
    return head + ":\n  " + "\n  ".join(reasons) + "\n" + hint


# === Execution ==================================================================


def execute_alignment(
    alignment: TargetAlignment,
    *,
    dry_run: bool = False,
    report: Report | None = None,
    track: Track | None = None,
) -> None:
    """Align the connected target's uids, in one transaction.

    With `dry_run` the work is done exactly as for real and then rolled back,
    so the report (and any refusal a merge runs into) is the one a real run
    would produce.  A refusal is recorded on :attr:`TargetAlignment.error`.
    `report` and `track` are optional progress callbacks (see
    :func:`lamindb_setup.evolve.console.progress_reporter`).
    """
    from django.db import transaction

    if not alignment.ok:
        return
    try:
        with transaction.atomic():
            _execute(alignment, report=report or _silent, track=track or _untracked)
            if dry_run:
                raise _DryRun
    except _DryRun:
        pass
    except AlignError as exc:
        alignment.error = str(exc)
        return
    alignment.executed = not dry_run


def _execute(alignment: TargetAlignment, *, report: Report, track: Track) -> None:
    import lamindb as ln

    from ..engine import fresh_uids, repoint_references

    alignment.moved_references = alignment.dropped_duplicates = 0
    alignment.settled = []
    freed = _by_model(
        (h.model, h) for h in alignment.holders if h.action == HOLDER_FREE
    )
    if freed:
        report(f"Freeing {sum(map(len, freed.values()))} uid(s) held in the trash")
    for model, holders in freed.items():
        uids = dict(
            zip((h.pk for h in holders), fresh_uids(model, len(holders)), strict=False)
        )
        for holder in holders:
            holder.new_uid = uids[holder.pk]
        _set_uids(model, uids)

    # Children before parents, so a merged parent no longer has the merged
    # children's rows pointing at it by the time its own references move.
    merges = sorted(
        (h for h in alignment.holders if h.action == HOLDER_MERGE),
        key=lambda h: h.rename.key.count("/"),
        reverse=True,
    )
    for holder in (
        track(merges, f"Merging {len(merges)} duplicate(s)") if merges else ()
    ):
        moved, dropped = repoint_references(
            holder.model, holder.pk, holder.rename.pk, error=AlignError
        )
        alignment.moved_references += moved
        alignment.dropped_duplicates += dropped
        holder.model.objects.filter(pk=holder.pk).delete(permanent=True)

    # The uid columns, in two phases: everything that moves first steps aside
    # to a temporary uid, so a rename can take a uid another one is vacating.
    # Each phase is one batched update per registry.
    reassigned = [h for h in alignment.holders if h.action == HOLDER_REASSIGN]
    moves = _by_model(
        [(r.model, (r.pk, r.new_uid)) for r in alignment.renames]
        + [(h.model, (h.pk, None)) for h in reassigned]
    )
    report(f"Rewriting {len(alignment.renames)} uid(s)")
    final: dict[Any, dict[Any, str]] = {}
    for model, rows in moves.items():
        fresh = iter(fresh_uids(model, len(rows) + sum(u is None for _, u in rows)))
        aside = {pk: next(fresh) for pk, _uid in rows}
        final[model] = {pk: uid or next(fresh) for pk, uid in rows}
        _set_uids(model, aside)
    for model, uids in final.items():
        _set_uids(model, uids)
    for holder in reassigned:
        holder.new_uid = final[holder.model][holder.pk]

    report("Rewriting uid references")
    dtypes, auxes, schemas = _rewrite_string_references(alignment.uid_map)
    alignment.rewritten_dtypes = dtypes
    alignment.rewritten_aux = auxes
    # Every feature whose uid changed hashes into the schemas it is a member
    # of: the renamed ones, the merge targets that took over a stub's links,
    # and the live holders moved aside to a fresh uid.
    feature_pks = {r.pk for r in alignment.renames if r.model is ln.Feature}
    feature_pks |= {h.rename.pk for h in merges if h.model is ln.Feature}
    feature_pks |= {h.pk for h in reassigned if h.model is ln.Feature}
    schemas |= {h.rename.pk for h in merges if h.model is ln.Schema}
    shared = _SharedContent(
        managed=_managed_schemas(alignment), merge=alignment.merge_duplicates
    )
    rehashed = _rehash(feature_pks, schemas, track=track, shared=shared)
    alignment.rehashed = list(rehashed.values())
    alignment.rehashed_uids = set(rehashed)
    alignment.settled = shared.settled
    alignment.moved_references += shared.moved
    alignment.dropped_duplicates += shared.dropped


def _managed_schemas(alignment: TargetAlignment) -> Callable[[Any], bool]:
    """Whether a plan manages a schema of the target, once the uids are rewritten.

    A plan manages the schemas the aligned plans resolve to, and every schema a
    lock of the target records -- the aligned plans' and those of plans built on
    them (see :func:`collect_instance_locks`).  A lock records the uids from
    before the alignment, so they are mapped first.  Read on first use: only a
    shared content needs to know.
    """
    managed: set[Any] | None = None

    def is_managed(schema: Any) -> bool:
        nonlocal managed
        if managed is None:
            managed = set(alignment.schema_pks) | _locked_schemas(alignment)
        return schema.pk in managed

    return is_managed


def _locked_schemas(alignment: TargetAlignment) -> set[Any]:
    """The primary keys of the schemas the target's lock files record."""
    import lamindb as ln

    from ..engine import LockFile

    mapping = alignment.uid_map
    locks = (LockFile.read(path) for path in alignment.lock_files)
    uids = sorted(
        {
            mapping.get(entry.uid, entry.uid)
            for lock in locks
            if lock is not None
            for entry in lock.objects.values()
            if entry.uid is not None and entry.kind in ("Schema", "SchemaType")
        }
    )
    pks: set[Any] = set()
    for chunk in _chunks(uids):
        pks.update(
            ln.Schema.objects.filter(uid__in=list(chunk)).values_list("pk", flat=True)
        )
    return pks


# === Lock files =================================================================


def collect_instance_locks(
    alignment: TargetAlignment, roots: Iterable[Path]
) -> list[Path]:
    """Add every lock of the target under `roots` that records a changed uid.

    The renames reach further than the plans being aligned: a plan built on
    top of them records their uids too -- as a parent `type`, in a
    `cat[Record[<uid>]]` dtype, as a schema member -- and so may a lock
    recording a live holder that is moved aside.  Every such lock has to be
    rewritten with the rest, or the next apply of that plan reads the
    alignment as drift.  Returns the lock files added to
    :attr:`TargetAlignment.lock_files`.
    """
    from ..engine import instance_lock_files

    affected = alignment.affected_uids
    if not affected:
        return []
    pattern = _token_pattern(dict.fromkeys(affected, ""))
    known = {path.resolve() for path in alignment.lock_files}
    added: list[Path] = []
    for path in instance_lock_files(roots, alignment.slug):
        if path in known:
            continue
        known.add(path)
        if pattern.search(path.read_text(encoding="utf-8")):
            alignment.lock_files.append(path)
            added.append(path)
    return added


def rewrite_target_locks(alignment: TargetAlignment) -> list[Path]:
    """Rewrite the target's lock files to the aligned uids; return those written.

    Run after the transaction committed.  Each entry's uid is mapped, and so is
    every uid reference in the snapshot of the instance it recorded; a schema
    the alignment rehashed gets its new content hash, and a schema's optionals
    are re-sorted the way a fresh snapshot sorts them, so the next `apply`
    reads no drift into the alignment.  A seeded object that was aligned is
    recorded in the identity-only `seeded` section.  A lock with nothing to
    change is left exactly as it is.
    """
    import lamindb as ln

    from ..engine import LockFile, remote_state_hash

    mapping = alignment.uid_map
    if not mapping or not alignment.executed:
        return []
    pattern = _token_pattern(mapping)
    hashes = dict(
        ln.Schema.objects.filter(uid__in=alignment.rehashed_uids).values_list(
            "uid", "hash"
        )
    )
    written: list[Path] = []
    for path in alignment.lock_files:
        lock = LockFile.read(path)
        if lock is None:
            continue
        before = json.dumps(lock.to_json(), sort_keys=True)
        for entry in lock.objects.values():
            if entry.uid is not None:
                entry.uid = mapping.get(entry.uid, entry.uid)
            if entry.remote_snapshot is None:
                continue
            snapshot = _map_json(
                entry.remote_snapshot, lambda s: _sub_tokens(s, pattern, mapping)
            )
            if entry.kind == "Schema":
                if "hash" in snapshot and entry.uid in hashes:
                    snapshot["hash"] = hashes[entry.uid]
                if isinstance(snapshot.get("optionals"), list):
                    snapshot["optionals"] = sorted(snapshot["optionals"])
            if snapshot != entry.remote_snapshot:
                entry.remote_snapshot = snapshot
                entry.remote_hash = remote_state_hash(snapshot)
        lock.seeded = {key: mapping.get(uid, uid) for key, uid in lock.seeded.items()}
        for rename in alignment.renames:
            if rename.kind in SEED_ONLY_KINDS and rename.plan == lock.plan:
                lock.seeded[rename.key] = rename.new_uid
        if json.dumps(lock.to_json(), sort_keys=True) == before:
            continue
        lock.write(path)
        written.append(path)
    return written


# === Rendering ==================================================================


def _build_renderable(alignments: Iterable[TargetAlignment], title: str) -> Any:
    from rich.console import Group
    from rich.text import Text

    parts: list[Any] = [Text(title, style="bold white"), Text()]
    for alignment in alignments:
        header = Text()
        header.append(alignment.slug, style="bold cyan")
        header.append(f"  <- {alignment.source_slug}", style="dim")
        parts.append(header)
        for rename in alignment.renames:
            line = Text("  ~ ", style="bold yellow")
            line.append(rename.kind, style="bold cyan")
            if rename.name is not None:
                line.append(f" {rename.name!r}", style="green")
            line.append("  ")
            line.append(rename.old_uid, style="red")
            line.append(" -> ", style="dim")
            line.append(rename.new_uid, style="green")
            parts.append(line)
        for holder in alignment.holders:
            style = "bold red" if holder.action == HOLDER_CONFLICT else "magenta"
            line = Text(f"    {holder.action}: ", style=style)
            line.append(f"{holder.label} holds {holder.uid}")
            if holder.new_uid is not None and holder.action != HOLDER_MERGE:
                line.append(f", now {holder.new_uid}", style="dim")
            parts.append(line)
            if holder.reason:
                parts.append(Text(f"      {holder.reason}", style="dim"))
            if holder.references and holder.action != HOLDER_FREE:
                parts.append(
                    Text(
                        "      referenced by " + ", ".join(holder.references),
                        style="dim",
                    )
                )
        summary = Text("  ")
        summary.append(f"{len(alignment.renames)} to rename", style="yellow")
        summary.append(f", {alignment.aligned} already aligned", style="dim")
        if alignment.missing:
            summary.append(
                f", {len(alignment.missing)} not on this instance (apply creates "
                "them with the source uid)",
                style="dim",
            )
        if alignment.unpinned:
            summary.append(
                f", {len(alignment.unpinned)} not recorded by the source", style="dim"
            )
        parts.append(summary)
        if alignment.rewritten_dtypes or alignment.rewritten_aux or alignment.rehashed:
            parts.append(
                Text(
                    f"  references: {alignment.rewritten_dtypes} dtype string(s), "
                    f"{alignment.rewritten_aux} _aux blob(s) rewritten; "
                    f"{len(alignment.rehashed)} schema(s) rehashed",
                    style="dim",
                )
            )
        if alignment.moved_references or alignment.dropped_duplicates:
            parts.append(
                Text(
                    f"  merged: {alignment.moved_references} reference(s) moved, "
                    f"{alignment.dropped_duplicates} duplicate link(s) dropped",
                    style="dim",
                )
            )
        for line in alignment.settled:  # type: ignore[assignment]
            parts.append(Text(f"  same content: {line}", style="magenta"))
        if alignment.error is not None:
            parts.append(Text(f"  refused: {alignment.error}", style="bold red"))
        elif alignment.conflicts:
            parts.append(
                Text(
                    f"  refused: {len(alignment.conflicts)} uid(s) are held by live "
                    "records; nothing on this instance is changed.",
                    style="bold red",
                )
            )
        parts.append(Text())
    return Group(*parts)


def render_alignment(
    alignments: Iterable[TargetAlignment], *, title: str, color: bool = False
) -> str:
    """Render the alignment of every target as text."""
    from rich.console import Console

    buffer = io.StringIO()
    console = Console(
        width=None if color else 200,
        file=buffer,
        force_terminal=color or None,
        color_system="auto" if color else None,
    )
    console.print(_build_renderable(alignments, title))
    return buffer.getvalue()


def print_alignment(alignments: Iterable[TargetAlignment], *, title: str) -> None:
    """Print the alignment of every target to stdout with color."""
    from rich.console import Console

    Console().print(_build_renderable(alignments, title))
