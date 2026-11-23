"""User-supplied narrowing data migrations that unblock a validation-affecting change.

The drift taxonomy refuses a validation-affecting dtype change whenever it *could*
invalidate an already-validated record, and `--allow-data-validated-changes` only
lifts that refusal when the live data is *already* safe (no stored values, or every
value already validates -- see :mod:`lamindb_setup.evolve.engine._drift_data`).  Neither
path can *transform* data.

A :class:`DataMigration` fills that gap.  The plan author attaches one to a feature
with :meth:`~lamindb_setup.evolve.PlanContext.migrate`; when apply reaches a
dtype change that the data would otherwise refuse, the engine flips the feature's
dtype and then runs the migration's `migrate` callable -- inside the same
`transaction.atomic()` as the rest of the apply -- to relocate / rewrite the
stored values so they validate against the new dtype.

Ordering (verified against a live instance for `str -> cat[Record]`):

* the dtype write must precede any categorical value write -- lamindb rejects a
  categorical value while the feature is still `str` -- so the *engine* flips the
  dtype first and the migration only fixes up values; and
* the stale scalar rows survive the flip and stay readable, so the migration can
  still read the old values (e.g. `feature.links_recordjson`) to build the new
  ones.

The value rewrite itself runs in its own phase, after every object in the plan has
been materialized.  A migration only ever maps values *onto* records -- it never
creates them -- so everything it might look up has to exist by then; running last
makes that true for anything the plan declares, in any order.

Nothing runs in PLAN mode: a preview only reports the intended migration.  A
migration is naturally once-only -- once the live dtype matches the plan, the dtype
change (and hence the migration) is skipped on the next apply.

One narrowing sits outside all of this: a **sheet export** (see
:mod:`lamindb_setup.evolve.engine._sheet_exports`) is a CSV already written to storage,
which no transaction can roll back.  Its migration therefore runs *after* the
apply commits, onto a branch merged into the instance only once every one of them
succeeded -- see :class:`SheetExportContext`.

Another rewrites no data at all: changing a schema's **index** only re-marks which
member identifies a row, and the data validated under the old index cannot be
checked against the new one in place.  Its declaration is the plan author vouching
for that data -- see :class:`IndexChangeContext`.

Turning a schema's **flexible** flag off is the last: it is refused only where it
could reject data (`maximal_set=True`), and whatever the flexible match
annotated while it was on can be kept or removed -- see
:class:`FlexibleChangeContext`.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from ..core import LaminSchemasError

if TYPE_CHECKING:
    from collections.abc import Callable

    from ..core import Materializable

__all__ = [
    "CHILDREN_REMOVAL",
    "DECLARATION_TARGETS",
    "DEFAULT",
    "DTYPE_CHANGE_TARGETS",
    "FEATURE_REMOVAL_TARGETS",
    "FLEXIBLE_CHANGE",
    "INDEX_CHANGE",
    "INDEX_NAME_CONFLICTS",
    "MEMBER_ADDED",
    "MEMBER_REMOVAL",
    "MEMBER_REQUIRED",
    "NULL",
    "NULLABILITY",
    "RELOCATIONS",
    "ROWS_REMOVAL",
    "STALE_EXPORTS",
    "VALIDATED_REMOVAL",
    "DataMigration",
    "DeclarationTarget",
    "DefaultFill",
    "FeatureRemovalContext",
    "FlexibleChangeContext",
    "IndexChangeContext",
    "IndexNameConflict",
    "MemberMigrationContext",
    "MemberRemovalContext",
    "MigrationContext",
    "MigrationError",
    "MigrationReport",
    "Migrated",
    "NullFill",
    "RecordTypeRemovalContext",
    "SchemaRemovalContext",
    "SheetExportContext",
    "WriteStrategy",
]


class NullFill:
    """Sentinel for "this value is null".

    Distinct from `None`, which every helper already reads as "do nothing with
    this row": a backfill that skipped a row leaves it invalid, whereas one that
    filled it with null answered for it.  Nothing is stored either way -- lamindb
    has no null annotation, so an un-annotated row *is* one, for every dtype
    alike -- but the two are reported apart and only the latter settles the row.

    Refused for a feature that is not nullable, which is what makes the
    distinction more than bookkeeping.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "NULL"


class DefaultFill:
    """Sentinel for "write the feature's `default_value`".

    Resolved per *feature*, not per migration, so one declaration covers members
    of different dtypes.  Falls back to :data:`NULL` for a nullable feature that
    declares no default, which is what lamindb's own
    `DataFrameCurator.standardize()` fills a missing column with.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "DEFAULT"


#: Backfill / rewrite a value as null -- refused for a non-nullable feature.
NULL = NullFill()

#: Backfill / rewrite a value as the feature's `default_value` (null if unset).
DEFAULT = DefaultFill()


class MigrationError(LaminSchemasError):
    """Raised when a data migration fails to relocate every value.

    A migration is expected to move *all* of a feature's stored values into the
    store the new dtype uses, as labels it admits; if any value is left stranded
    -- in the old store, or as a link to a label outside the new dtype's
    registries or filters (see
    :func:`lamindb_setup.evolve.engine._drift_data.feature_stranded_value_count`)
    -- the apply raises this so the enclosing `transaction.atomic()` rolls the
    half-migrated state back.
    """


class WriteStrategy(enum.Enum):
    """How a relocation wrote the replacement annotations.

    A `str -> cat[...]` relocation writes the link rows itself when the target
    store has the shape :func:`~lamindb_setup.evolve.engine._migrations._plan_link_writes`
    knows how to build, and otherwise falls back to lamindb's `add_values`,
    which re-resolves the feature and runs a curator per annotated object.  The
    choice is invisible in the resulting data -- both end with the same link
    rows -- so the migration records which one it took.
    """

    BATCHED_LINKS = "batched link rows"
    ADD_VALUES = "add_values"


@dataclass
class MigrationReport:
    """Per-value outcome counters a migration reports back to the caller.

    The engine threads one of these onto every :class:`MigrationContext` and
    renders :meth:`summary` into the change details, so a `plan` dry-run shows
    *what the migration did to the data* ("12 matched, 3 mapped, 2 dropped")
    rather than only whether it left values stranded.  A hand-written migration
    may ignore it entirely; the helpers in
    :mod:`lamindb_setup.evolve.engine._migrations` populate it.

    `kept` counts values left as they were because the new dtype already
    admits them, `matched` those that resolved against an existing target directly,
    `mapped` those redirected by an explicit mapping, `transformed` those
    rescued by a transform callable, `nulled` those written as the feature's
    null (see :data:`NULL`), and `dropped` those resolved to `None` and
    therefore de-annotated.  `updated` counts the values and attributes a seeded
    record took over from the plan, and `relocated` the annotations moved off a
    record the plan drops.  `notes` carries free-form lines appended with
    :meth:`note`.  `write_strategy` records which write path a relocation took
    (see :class:`WriteStrategy`), which the resulting data does not reveal.
    """

    kept: int = 0
    matched: int = 0
    mapped: int = 0
    transformed: int = 0
    nulled: int = 0
    dropped: int = 0
    updated: int = 0
    relocated: int = 0
    notes: list[str] = field(default_factory=list)
    write_strategy: WriteStrategy | None = None

    def note(self, message: str) -> None:
        """Append a free-form line to the report."""
        self.notes.append(message)

    def summary(self) -> str | None:
        """A one-line human summary, or `None` when nothing was recorded.

        Zero-valued counters are omitted so the line stays short; the notes are
        appended after the counters.
        """
        counts = [
            f"{value} {label}"
            for label, value in (
                ("kept", self.kept),
                ("matched", self.matched),
                ("mapped", self.mapped),
                ("transformed", self.transformed),
                ("nulled", self.nulled),
                ("dropped", self.dropped),
                ("updated", self.updated),
                ("relocated", self.relocated),
            )
            if value
        ]
        parts = [", ".join(counts)] if counts else []
        parts.extend(self.notes)
        if self.write_strategy is not None:
            parts.append(f"written via {self.write_strategy.value}")
        return "; ".join(parts) if parts else None


@dataclass
class MigrationContext:
    """What a :class:`DataMigration` callable receives when it runs.

    `feature` is the *live* LaminDB `Feature` record, already flipped to
    `desired_dtype` by the engine (the dtype write must precede any categorical
    value write; the stale scalar rows survive the flip and remain readable).  The
    callable relocates / rewrites the feature's stored values so they validate
    against `desired_dtype`, then returns; it runs inside the apply transaction,
    so raising rolls the whole apply back.

    `target_registry` and `cat_filters` are the *plan's* resolved target: the
    live registry class the new dtype points at (e.g. `ln.Record`,
    `bt.CellType`) and the filters narrowing it (e.g. `{"type__uid": ...}`).
    The engine resolves both from the plan feature's declared
    :class:`~lamindb_setup.evolve.LaminDType`, which is the only place they
    survive intact -- `desired_dtype` is a *string*, and no registry-name to
    class resolver exists.  Both are `None` for a scalar target.

    `report` collects per-value outcomes for the plan preview (see
    :class:`MigrationReport`).
    """

    feature: Any
    desired_dtype: str
    live_dtype: str | None
    target_registry: Any = None
    cat_filters: dict[str, Any] | None = None
    report: MigrationReport = field(default_factory=MigrationReport)


@dataclass
class MemberMigrationContext:
    """What a :class:`DataMigration` attached to a *schema* receives.

    The counterpart of :class:`MigrationContext` for the other narrowing a plan
    can declare a migration for: making an optional schema member required again.
    Nothing about a feature's dtype changes; what has to be fixed up is the rows
    that omitted the member, since those are what the block exists to protect.

    `schema` is the live LaminDB `Schema`, its optional set already narrowed
    by the engine, and `features` are the live members it no longer marks
    optional.  The callable annotates every row that lacks one of them --
    :meth:`missing` enumerates exactly those -- and returns; the engine then
    re-counts them and fails the apply if any is left, so a partial backfill
    rolls back rather than silently invalidating records.

    A row backfilled with null is the one exception.  lamindb has no stored null
    -- `add_values` skips a `None`, a categorical value is a foreign key, and
    both JSON stores are `NOT NULL` columns -- so an un-annotated row *is* a
    null one, whatever the member's dtype.  A migration that filled a row with
    null says so with :meth:`accept_null`, which exempts that row from the
    re-count; a row merely left alone is not exempted and still fails the apply.

    `report` collects per-row outcomes for the plan preview (see
    :class:`MigrationReport`).  `rows_omitting` is the engine's query behind
    :meth:`missing`, supplied by whoever constructs the context.
    """

    schema: Any
    features: list[Any]
    report: MigrationReport = field(default_factory=MigrationReport)
    accepted_nulls: dict[Any, dict[str, set[Any]]] = field(default_factory=dict)
    rows_omitting: Callable[[Any, Any], list[Any]] = field(
        kw_only=True, repr=False, compare=False
    )

    def accept_null(self, feature: Any, row: Any) -> None:
        """Declare that `row` deliberately holds no value for `feature`.

        Recorded per `(feature, row)` rather than per feature, because a
        backfill may fill some rows with a real value and leave only the rest
        null; only the rows named here are exempted from the engine's re-count.
        """
        by_model = self.accepted_nulls.setdefault(feature.pk, {})
        by_model.setdefault(type(row).__name__, set()).add(row.pk)

    def missing(self, feature: Any) -> list[Any]:
        """The rows validated by `schema` that carry no value for `feature`.

        Spans every registry a schema validates rows of (`Record`, `Artifact`,
        `Collection`) and every composite schema that embeds this one, matching
        what the engine counts when it checks the backfill succeeded.
        """
        return self.rows_omitting(self.schema, feature)


@dataclass
class MemberRemovalContext:
    """What a :class:`DataMigration` attached to a schema *losing* a member receives.

    The third narrowing a plan can declare a migration for.  Removing a member
    changes nothing about the rows themselves -- it is the schema that stops
    declaring the feature -- so what has to be answered for is the values already
    stored for it, which no longer belong to anything the schema describes.

    `schema` is the live LaminDB `Schema`, the member already unlinked by the
    engine, and `features` are the live features it no longer declares.  The
    callable clears the values those members hold on the rows the schema
    validates -- :meth:`carrying` enumerates exactly those -- and returns; the
    engine then re-counts them and fails the apply if any is left.

    A migration that deliberately *keeps* the values (they stay on the rows as
    undeclared extra data) is exempt from that re-count; it says so with
    `DataMigration(keeps_values=True)`, which
    :func:`~lamindb_setup.evolve.keep_member_values` sets.

    `report` collects per-row outcomes for the plan preview (see
    :class:`MigrationReport`).  `rows_carrying` is the engine's query behind
    :meth:`carrying`, supplied by whoever constructs the context.
    """

    schema: Any
    features: list[Any]
    report: MigrationReport = field(default_factory=MigrationReport)
    kept: list[Any] = field(default_factory=list)
    rows_carrying: Callable[[Any, Any], list[Any]] = field(
        kw_only=True, repr=False, compare=False
    )

    def keep(self, feature: Any) -> None:
        """Declare that `feature`'s stored values are deliberately left in place.

        Exempts it from the post-check that otherwise requires a removed member's
        values to be gone.  Stated per member rather than per migration because
        the declarations are not matched to members: a schema dropping two of
        them may keep one and delete the other.
        """
        self.kept.append(feature)

    def carrying(self, feature: Any) -> list[Any]:
        """The rows validated by `schema` that hold a value for `feature`.

        The exact complement of :meth:`MemberMigrationContext.missing`, over the
        same registries (`Record`, `Artifact`, `Collection`) and the same
        composite closure, and what the engine re-counts to check the drop
        succeeded.
        """
        return self.rows_carrying(self.schema, feature)


@dataclass
class FeatureRemovalContext:
    """What a :class:`DataMigration` attached to a *deleted* feature receives.

    The plan has stopped declaring the feature altogether, so the removal pass is
    about to move it to the trash.  Trashing detaches nothing: the values stay
    keyed to it, invisible to every query that goes through a live feature, while
    `get_values()` still reports them by name -- and a feature of the same name
    declared later is a different record, so the history splits in two.

    `feature` is the live LaminDB `Feature`, still on its branch: the
    migration runs before the deletion, so everything it reads is still there.
    Only the two attributes :func:`~lamindb_setup.evolve.clear_values`
    needs, which is what lets `drop_values()` serve this case unchanged.

    Declared by natural key, since there is no plan object left to name::

        ctx.migrate("FeatureType:ft/Feature:retired", drop_values())
    """

    feature: Any
    report: MigrationReport = field(default_factory=MigrationReport)


@dataclass
class RecordTypeRemovalContext:
    """What a :class:`DataMigration` attached to a *deleted* record type receives.

    The plan has stopped declaring a `RecordType` or `Sheet` that still has
    records -- a sheet's rows, a vocabulary's values -- and lamindb's trash
    cascades to every one of them.  The removal is refused until the plan says
    what should become of them, declared by natural key like a deleted feature::

        ctx.migrate("Sheet:Experiments", drop_rows())

    `record_type` is the live record type, still on its branch: the migration
    runs before the deletion.  :meth:`rows` lists its records that are not in
    the trash, sub-types left out.
    """

    record_type: Any
    report: MigrationReport = field(default_factory=MigrationReport)
    live_rows: Callable[[Any], Any] = field(kw_only=True, repr=False, compare=False)

    def rows(self) -> Any:
        """The records typed by `record_type` that are not in the trash."""
        return self.live_rows(self.record_type)


@dataclass
class SchemaRemovalContext:
    """What a :class:`DataMigration` attached to a *deleted* schema receives.

    The plan has stopped declaring a schema that artifacts or collections were
    validated against.  Trashing it leaves them pointing at a trashed schema,
    which no longer describes them for anyone querying live schemas, so the
    removal is refused until the plan vouches for that::

        ctx.migrate("Schema:Legacy", keep_validated(reason="superseded by v2"))

    `schema` is the live schema, still on its branch.
    """

    schema: Any
    report: MigrationReport = field(default_factory=MigrationReport)


@dataclass
class RecordUpdateContext:
    """What a :class:`DataMigration` attached to a *seeded* record receives.

    A plan seeds its records (see
    :data:`~lamindb_setup.evolve.core.SEED_ONLY_KINDS`): once one exists, the
    plan's edits to it are ignored.  Declaring an update opts one into them::

        ctx.migrate(record, update_record())

    `record` is the live record.  `values` maps each live `Feature` whose
    stored value differs from the one the plan declares to that declared value,
    and `fields` each descriptive attribute that differs to the plan's.  Only
    what the plan declares is compared: a feature it leaves out, or declares as
    `None`, keeps its stored value.
    """

    record: Any
    values: dict[Any, Any] = field(default_factory=dict)
    fields: dict[str, Any] = field(default_factory=dict)
    report: MigrationReport = field(default_factory=MigrationReport)


@dataclass
class RecordRemovalContext:
    """What a :class:`DataMigration` declared for a record the plan *drops* receives.

    A seeded record the plan stops declaring stays on the instance.  Declaring
    its removal by natural key moves it to the trash, which is refused while
    other objects still hold it as a categorical value::

        ctx.migrate("RecordType:Vocabulary/Record:obsolete", drop_record())

    `record` is the live record, still live: the migration runs before it is
    trashed.  `relocate_to` is the live record its annotations move onto, or
    `None`.  :meth:`links` lists the link rows that still name `record` as
    their value, one queryset per link table.
    """

    record: Any
    relocate_to: Any = None
    report: MigrationReport = field(default_factory=MigrationReport)
    value_links: Callable[[Any], list[Any]] = field(
        kw_only=True, repr=False, compare=False
    )

    def links(self) -> list[Any]:
        """The link rows naming `record` as their value, per link table."""
        return self.value_links(self.record)


@dataclass
class SheetExportContext:
    """What a :class:`DataMigration` attached to a schema with *stale exports* receives.

    The fourth narrowing a plan can declare a migration for, and the only one
    whose subject is not stored *in* the database.  A sheet export is a CSV
    already written to storage (see :mod:`lamindb_setup.evolve.engine._sheet_exports`);
    renaming a member, retyping one, or adding a required one leaves that file
    describing the schema as it was, while its `schema` foreign key claims it
    still validates.

    `schema` is the live LaminDB `Schema`, already reconciled to the plan,
    and `exports` the snapshots the change invalidated -- each naming its sheet,
    the artifact at the head of its version family, and why it no longer adheres.
    :meth:`write` produces the new version.

    Two things set this context apart from the other three, and a hand-written
    migration has to respect both:

    * **It runs after the apply transaction has committed.** A file write cannot
      be rolled back with the database, so raising here does not undo the schema
      change -- it leaves :attr:`branch` unmerged and fails the command.
    * **Everything it creates lands on :attr:`branch`**, a branch merged into the
      instance only once every declared migration succeeded.  Write through
      :meth:`write` (or pass `branch=self.branch` yourself), or the new version
      becomes visible before the run that produced it is known to be good.

    :attr:`branch` is created on first access rather than up front, so a
    migration that writes nothing -- `keep_stale_exports()`, or a hand-written
    one that decides against it -- leaves no empty branch behind (lamindb never
    deletes them).

    `report` collects per-export outcomes for the diff (see
    :class:`MigrationReport`).  `reexport` and `current_branch` are the
    engine operations behind :meth:`write`, supplied by whoever constructs the
    context.
    """

    schema: Any
    exports: list[Any]
    report: MigrationReport = field(default_factory=MigrationReport)
    written: list[Any] = field(default_factory=list)
    promotions: list[tuple[Any, Any]] = field(default_factory=list)
    branch_factory: Callable[[], Any] | None = None
    _branch: Any = None
    reexport: Callable[..., Any] = field(kw_only=True, repr=False, compare=False)
    current_branch: Callable[[], Any] = field(kw_only=True, repr=False, compare=False)

    @property
    def branch(self) -> Any:
        """The branch new export versions are written onto, created on demand."""
        if self._branch is None and self.branch_factory is not None:
            self._branch = self.branch_factory()
        return self._branch

    def write(self, export: Any) -> Any:
        """Write a new version of `export` from the sheet's current rows.

        Returns the saved artifact, which is the *existing* one when the rows
        hash to the content already stored -- lamindb deduplicates by hash, so an
        export that did not actually change gains no version, and nothing is
        recorded in :attr:`written`.  That is the property a declaration relies on
        to be safe to leave in a plan forever.

        The same deduplication has two failure modes, because its lookup is
        scoped by neither key, branch, nor `is_latest`:

        * it can resolve onto a version an earlier, *failed* run parked on
          another re-export branch -- same content, stranded where the merge
          cannot reach it.  That one is adopted onto this run's branch.
        * it can resolve onto a **superseded** version of the family, when the
          rows happen to render exactly what some older export holds (a schema
          change and its revert, say).  Nothing promotes that row, so the head
          would keep its stale content while this reported a re-export.  The
          family already holds the right bytes, so the fix is to re-point it --
          recorded in :attr:`promotions` and applied *after* the merge, since it
          touches the instance's own branch and must not become visible before
          the run is known to be good (see
          :func:`~lamindb_setup.evolve.engine._sheet_exports.promote_head`).

        An artifact already on the instance's own branch is otherwise left alone.
        Moving it here would take a live artifact *off* the branch every query
        sees, to hand it back at the merge -- and to hide it permanently if this
        run failed in between.  A file write that cannot be rolled back must
        never be able to remove something that was already there.
        """
        artifact = self.reexport(export, branch=self.branch)
        if getattr(artifact, "pk", None) == getattr(export.artifact, "pk", None):
            return artifact
        if not getattr(artifact, "is_latest", True):
            self.promotions.append((export.artifact, artifact))
            return artifact
        branch = self.branch
        if branch is not None:
            live = getattr(artifact, "branch_id", None)
            if live not in (branch.id, self.current_branch().id):
                artifact.branch = branch
                artifact.save()
        self.written.append(artifact)
        return artifact


@dataclass
class IndexChangeContext:
    """What a :class:`DataMigration` attached to a schema whose *index* changes receives.

    The fifth narrowing a plan can declare a migration for, and the only one with
    nothing to rewrite.  `Schema.index` marks which member identifies a row, so
    adding, changing or removing it changes how every dataset the schema already
    validated would be curated now -- and that cannot be re-checked in place.  The
    declaration is the plan author vouching that the data conforms (see
    :func:`~lamindb_setup.evolve.accept_index_change`).

    `schema` is the live LaminDB `Schema`, its index already switched by the
    engine.  `previous` and `index` are the live index features before and
    after, either of which is `None` when the schema had, respectively has, no
    index.  It runs inside the apply transaction, like the member narrowings.

    `report` collects notes for the diff (see :class:`MigrationReport`).
    """

    schema: Any
    previous: Any
    index: Any
    report: MigrationReport = field(default_factory=MigrationReport)


@dataclass
class FlexibleChangeContext:
    """What a :class:`DataMigration` attached to a schema turning `flexible` off receives.

    While `flexible` was on, curating a dataset against the schema also
    validated and annotated every column whose name matched a registered feature:
    each such column is listed among the columns the artifact records for the
    slot, and a categorical one carries label links.  Turning the flag off changes
    none of that.  Under `maximal_set=True` it is refused as well, since a
    dataset carrying such a column would no longer validate.

    Two declarations answer for it: :func:`~lamindb_setup.evolve.accept_flexible_change`
    keeps the annotations and, on a `maximal_set` schema, vouches for the data;
    :func:`~lamindb_setup.evolve.drop_nonmember_annotations` removes them.

    `schema` is the live LaminDB `Schema`, its flag already switched;
    `previous` and `flexible` are the values before and after.
    :meth:`nonmember_columns` lists what is left to answer for.  It runs inside the
    apply transaction, like the member narrowings.

    `report` collects notes for the diff (see :class:`MigrationReport`).
    `recorded_nonmembers` is the engine's query behind :meth:`nonmember_columns`,
    supplied by whoever constructs the context.
    """

    schema: Any
    previous: Any
    flexible: Any
    report: MigrationReport = field(default_factory=MigrationReport)
    recorded_nonmembers: Callable[[Any], list[Any]] = field(
        kw_only=True, repr=False, compare=False
    )

    def nonmember_columns(self) -> list[Any]:
        """Every artifact slot `schema` curated whose recorded columns it does not declare.

        One entry per artifact and slot -- `columns` for a dataframe the schema
        validated itself, else the composite's slot -- with the `artifact`, the
        `slot`, the `link` row recording its columns, the `recorded` schema
        that row points at and the non-member `features` it lists.  Trashed
        artifacts are left out.
        """
        return self.recorded_nonmembers(self.schema)


# The narrowings a migration can be declared to answer.  A schema takes
# declarations for several unrelated ones at once, and they are looked up by the
# object they are attached to, so a declaration has to say which of them it is
# for -- otherwise a member-removal cleanup reads as a backfill for a member the
# same schema makes required, and satisfies a block it does nothing about.
MEMBER_REQUIRED = "member_required"
MEMBER_ADDED = "member_added"
MEMBER_REMOVAL = "member_removal"
STALE_EXPORTS = "stale_exports"
INDEX_CHANGE = "index_change"
FLEXIBLE_CHANGE = "flexible_change"
ROWS_REMOVAL = "rows_removal"
VALIDATED_REMOVAL = "validated_removal"
CHILDREN_REMOVAL = "children_removal"
NULLABILITY = "nullability"
UNIT_CHANGE = "unit_change"
SHEET_SCHEMA = "sheet_schema"
SLOT_CHANGE = "slot_change"
MAXIMAL_SET = "maximal_set"
MINIMAL_SET = "minimal_set"
ORDERED_SET = "ordered_set"
MERGE_SCHEMA = "merge_schema"
DETACH_MEMBER = "detach_member"
RECORD_UPDATE = "record_update"
RECORD_REMOVAL = "record_removal"


@dataclass(frozen=True)
class DeclarationTarget:
    """One kind of target a :class:`DataMigration` can be declared for.

    A plan object of `kind` -- `"Feature"`, `"Schema"`, `"Sheet"` or
    `"Record"` -- or, with `by_key`, the natural key of one whose leaf is of
    that kind: the only way to address an object the plan no longer declares.
    A natural key may stand in for an object target too, since the engine looks
    every declaration up by key -- that is how a schema no plan manages, one a
    deleted feature is detached from, is answered for.  `answers` says what a
    declaration attached there answers for, which is what an error naming a
    misplaced declaration explains.
    """

    kind: str
    answers: str
    by_key: bool = False

    def describe(self) -> str:
        """`a Schema (...)` or `the natural key of a Feature (...)`."""
        what = f"the natural key of a {self.kind}" if self.by_key else f"a {self.kind}"
        return f"{what} ({self.answers})"


#: The targets each narrowing answers for, read by the offline check of a plan's
#: `ctx.migrate` declarations (see :meth:`PlanContext.migrate
#: <lamindb_setup.evolve.PlanContext.migrate>`).  A narrowing missing here -- the
#: empty one of a hand-written migration included -- is not checked.
DECLARATION_TARGETS: dict[str, tuple[DeclarationTarget, ...]] = {
    MEMBER_REQUIRED: (DeclarationTarget("Schema", "a member it makes required"),),
    MEMBER_ADDED: (DeclarationTarget("Schema", "a required member it gains"),),
    MEMBER_REMOVAL: (DeclarationTarget("Schema", "a member it removes"),),
    STALE_EXPORTS: (
        DeclarationTarget("Schema", "the sheet exports its change invalidates"),
        DeclarationTarget("Sheet", "the exports written against its old schema"),
    ),
    INDEX_CHANGE: (DeclarationTarget("Schema", "a change to its index"),),
    FLEXIBLE_CHANGE: (DeclarationTarget("Schema", "turning its flexible flag off"),),
    SLOT_CHANGE: (DeclarationTarget("Schema", "a change to its slots"),),
    MAXIMAL_SET: (DeclarationTarget("Schema", "turning maximal_set on"),),
    MINIMAL_SET: (DeclarationTarget("Schema", "turning minimal_set on"),),
    ORDERED_SET: (DeclarationTarget("Schema", "turning ordered_set on"),),
    MERGE_SCHEMA: (
        DeclarationTarget("Schema", "taking over another schema's content"),
    ),
    NULLABILITY: (DeclarationTarget("Feature", "turning its nullable off"),),
    UNIT_CHANGE: (DeclarationTarget("Feature", "a change to its unit"),),
    SHEET_SCHEMA: (DeclarationTarget("Sheet", "pointing it at another schema"),),
    RECORD_UPDATE: (DeclarationTarget("Record", "writing the plan's edits over it"),),
    ROWS_REMOVAL: (
        DeclarationTarget(
            "RecordType", "the records of one the plan deletes", by_key=True
        ),
        DeclarationTarget("Sheet", "the rows of one the plan deletes", by_key=True),
    ),
    VALIDATED_REMOVAL: (
        DeclarationTarget(
            "Schema", "the datasets one the plan deletes validated", by_key=True
        ),
    ),
    CHILDREN_REMOVAL: tuple(
        DeclarationTarget(kind, "the children of one the plan deletes", by_key=True)
        for kind in ("FeatureType", "SchemaType", "RecordType", "Sheet")
    ),
    DETACH_MEMBER: (
        DeclarationTarget(
            "Feature", "the schemas listing one the plan deletes", by_key=True
        ),
    ),
    RECORD_REMOVAL: (
        DeclarationTarget("Record", "trashing one the plan drops", by_key=True),
    ),
}

#: The targets of a dtype helper (`map_values`, `map_to_records`, ...), which
#: names no narrowing: a feature's dtype change is the one block an untagged
#: migration answers.
DTYPE_CHANGE_TARGETS: tuple[DeclarationTarget, ...] = (
    DeclarationTarget("Feature", "a change to its dtype"),
)

#: The targets of `keep_values`: the stored values of a feature the plan deletes.
FEATURE_REMOVAL_TARGETS: tuple[DeclarationTarget, ...] = (
    DeclarationTarget("Feature", "the values of one the plan deletes", by_key=True),
)

#: What :data:`RECORD_UPDATE` may write: a record's feature values, its
#: description, and its reference (with `reference_type`).
RECORD_ASPECTS: tuple[str, ...] = ("values", "description", "reference")

#: How a sheet row whose `Record.name` and new index value differ is resolved:
#: keep the name, or take the index feature's value.  lamindb's own vocabulary
#: (`Schema.save(index_name_conflict=...)`).
IndexNameConflict = Literal["keep_name", "keep_feature"]
INDEX_NAME_CONFLICTS: tuple[str, ...] = ("keep_name", "keep_feature")

#: Where a removal declaration moves what the deleted object leaves behind
#: instead of letting the trash take it: a deleted record type's rows
#: (`keep_rows`, `move_rows`), the datasets a deleted schema validated
#: (`revalidate_against`), the children typed under a deleted type
#: (`adopt_children`).  The engine performs each itself, onto
#: :attr:`DataMigration.relocates_to`.
RELOCATIONS: tuple[str, ...] = (
    "keep_rows",
    "move_rows",
    "revalidate_against",
    "adopt_children",
)


class DataMigration:
    """A narrowing data migration attached to a plan object.

    Declared with :meth:`~lamindb_setup.evolve.PlanContext.migrate`, it
    unblocks a validation-affecting change the live data would otherwise refuse.
    `migrate` receives a context shaped by what it unblocks -- a
    :class:`MigrationContext` for a feature's dtype narrowing (the live feature,
    already flipped to the new dtype), a :class:`MemberMigrationContext` for a
    schema making an optional member required -- and must fix the data up so
    nothing is left invalid; it runs inside the apply `transaction.atomic()`.

    `name` identifies the migration in previews and the change audit; `reason`
    is a short human-readable explanation of what it does (defaulting to `name`).

    `keeps_values` marks a migration that deliberately leaves the data where it
    is, which a member removal, a stale sheet export and a schema turning
    `flexible` off all have a meaning for: the values stay on the rows as
    undeclared extra data, the exported snapshot stays as it is, respectively the
    annotations the flexible match wrote stay on the artifacts.  It exempts the
    migration from the post-check that would otherwise require them to be gone.
    For a member removal it is refused on a `maximal_set` schema, which is
    precisely the kind that does not tolerate an undeclared member.

    `narrowing` names the block the migration answers (:data:`MEMBER_REQUIRED`,
    :data:`MEMBER_ADDED`, :data:`MEMBER_REMOVAL`, :data:`STALE_EXPORTS`,
    :data:`INDEX_CHANGE`, :data:`FLEXIBLE_CHANGE`, :data:`ROWS_REMOVAL`,
    :data:`VALIDATED_REMOVAL` or :data:`NULLABILITY`), so a declaration cannot
    unblock a narrowing it does nothing about -- a schema commonly takes several
    at once.  Empty (the default, and what a hand-written migration gets) means it
    answers whichever narrowing it is declared for.

    `index_name_conflict` only means something for an :data:`INDEX_CHANGE`: when
    a sheet using the schema has rows whose `Record.name` and value for the new
    index feature both exist and differ, lamindb has to pick one of them --
    `"keep_name"` or `"keep_feature"`.  Without a choice the apply refuses and
    names the rows rather than letting lamindb prompt for it on stdin.

    `merges` only means something for a :data:`MERGE_SCHEMA`: the uid or
    natural key of the live schema whose content the plan schema takes over.
    `detaches` likewise names the schemas a :data:`DETACH_MEMBER` removes a
    deleted feature from.  `updates` names the :data:`RECORD_ASPECTS` a
    :data:`RECORD_UPDATE` writes (empty: all of them), and `relocates_to` the
    record -- a plan `Record`, a natural key or a uid -- a
    :data:`RECORD_REMOVAL` moves the dropped record's annotations onto.

    `relocation` names what a removal declaration moves onto `relocates_to`
    -- a plan object, a natural key, a uid, or `None` for no type at all --
    instead of letting the trash take it (see :data:`RELOCATIONS`): a deleted
    record type's rows, a deleted schema's datasets, a deleted type's children.
    `narrower` lets a `revalidate_against` point the datasets at a schema
    that admits less than the deleted one did.

    `targets` are what the migration may be declared for (see
    :class:`DeclarationTarget`), which finalizing the plan checks every
    `ctx.migrate` against -- so `validate` refuses, say, a schema backfill
    attached to a feature.  It defaults to the narrowing's entry in
    :data:`DECLARATION_TARGETS`; the dtype helpers, which name no narrowing,
    pass :data:`DTYPE_CHANGE_TARGETS`.  `None` -- what a hand-written
    migration naming no narrowing gets -- leaves the declaration unchecked.

    A migration never *creates* the objects it maps into, so they must already
    exist when it runs.  The engine guarantees that by running every declared
    migration in its own phase, after the whole plan has been materialized -- so
    a target declared anywhere in the plan is there, whatever order the plan file
    lists it in.

    The `migrate` callable must not capture LaminDB class objects from an outer
    scope: a plan is re-finalized against each instance's freshly rebuilt classes,
    so import `lamindb` (and any registry) *inside* the callable.
    """

    def __init__(
        self,
        *,
        name: str,
        migrate: Callable[[Any], None],
        reason: str = "",
        keeps_values: bool = False,
        narrowing: str = "",
        index_name_conflict: IndexNameConflict | None = None,
        merges: str | None = None,
        detaches: tuple[str, ...] = (),
        updates: tuple[str, ...] = (),
        relocates_to: Any = None,
        relocation: str = "",
        narrower: bool = False,
        targets: tuple[DeclarationTarget, ...] | None = None,
    ) -> None:
        if (
            index_name_conflict is not None
            and index_name_conflict not in INDEX_NAME_CONFLICTS
        ):
            raise ValueError(
                f"DataMigration {name!r}: index_name_conflict must be one of "
                f"{', '.join(repr(c) for c in INDEX_NAME_CONFLICTS)}, got "
                f"{index_name_conflict!r}"
            )
        self.name = name
        self.reason = reason or name
        self.keeps_values = keeps_values
        self.narrowing = narrowing
        self.index_name_conflict = index_name_conflict
        unknown = sorted(set(updates) - set(RECORD_ASPECTS))
        if unknown:
            raise ValueError(
                f"DataMigration {name!r}: updates must be among "
                f"{', '.join(repr(a) for a in RECORD_ASPECTS)}, got "
                f"{', '.join(repr(a) for a in unknown)}"
            )
        if relocation and relocation not in RELOCATIONS:
            raise ValueError(
                f"DataMigration {name!r}: relocation must be one of "
                f"{', '.join(repr(r) for r in RELOCATIONS)}, got {relocation!r}"
            )
        self.merges = merges
        self.detaches = detaches
        self.updates = updates
        self.relocates_to = relocates_to
        self.relocation = relocation
        self.narrower = narrower
        self.targets = (
            targets if targets is not None else DECLARATION_TARGETS.get(narrowing)
        )
        self._migrate = migrate

    def run(
        self,
        context: MigrationContext
        | MemberMigrationContext
        | MemberRemovalContext
        | FeatureRemovalContext
        | SheetExportContext
        | IndexChangeContext
        | FlexibleChangeContext
        | RecordTypeRemovalContext
        | SchemaRemovalContext
        | RecordUpdateContext
        | RecordRemovalContext,
    ) -> None:
        """Execute the migration against `context`.

        Which context it gets is decided by the object the migration is attached
        to, not by the migration itself -- hence the deliberately loose annotation
        on `migrate`: a single class serves both narrowings, and the pairing is
        an invariant of :meth:`~lamindb_setup.evolve.PlanContext.migrate`
        rather than something the signature can express.
        """
        self._migrate(context)


@dataclass
class Migrated:
    """A migration declared for `obj` via :meth:`PlanContext.migrate`.

    `obj` is a plan object, or the natural key of one the plan no longer
    contains -- the only way to address a feature the plan is deleting, which by
    definition has no object left to point at.
    """

    obj: Materializable | str
    migration: DataMigration
