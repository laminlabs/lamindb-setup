"""Planning utilities to collect schema setup without materializing in LaminDB.

The primitives a plan is built out of -- :class:`~lamindb_setup.evolve.core.Promise`,
:class:`~lamindb_setup.evolve.core.Materializable` and the dependency types -- live in
:mod:`lamindb_setup.evolve.core`, which both this layer and the engine share.  They
are re-exported here because this is where they used to be and plan files import
them from here.
"""

from __future__ import annotations

from typing import Any, Generic

from typing_extensions import TypeVar

E = TypeVar("E", bound="PlanExports", default="PlanExports")

import inspect
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from types import FunctionType
from typing import TYPE_CHECKING, ClassVar, get_origin, get_type_hints, overload

from ..core import (
    SEED_ONLY_KINDS,
    Dependency,
    DependencyDAG,
    DependencyMeta,
    Materializable,
    Promise,
    active_collector,
    collecting,
)
from ._declarations import PlanDeclarationError, check_declarations, named_key

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from ..core import SpaceUID
    from ._migrations import DataMigration, Migrated


__all__ = [
    "Plan",
    "PlanContext",
    "PlanExports",
    "PlanNotFinalizedError",
    "Promise",
    "Materializable",
    "Moved",
    "Dependency",
    "DependencyDAG",
    "DependencyMeta",
    "create_plan",
    "plan_declaration_blocker",
]


def plan_declaration_blocker(
    modules: Mapping[str, object], *, instance_connected: bool
) -> str | None:
    """Return why a plan may not be declared in this state, or `None` if it may.

    The rule itself, separated from reading the process state it is normally
    asked about (see :func:`_ensure_lamindb_not_loaded`): plans must be defined
    lazily, before connecting to an instance, so they
    can be summarized and materialized in a controlled way.
    """
    if instance_connected:
        return (
            "a lamindb instance is already connected; plans must be defined "
            "before connecting to an instance."
        )
    return None


def _ensure_lamindb_not_loaded() -> None:
    """Raise unless a plan may be declared against the current process state."""
    # Declaration imports do not load lamindb. An actual connected instance
    # still blocks plan construction; the offline placeholder is allowed.
    from lamindb_setup import settings
    from lamindb_setup.core import django as _django

    reason = plan_declaration_blocker(
        sys.modules,
        instance_connected=bool(_django.IS_SETUP)
        and settings.instance.slug != "none/none",
    )
    if reason is not None:
        raise RuntimeError(reason)


# === Plan and related classes ================================================


@dataclass
class Moved:
    """A declared address change: `obj` was previously known as `from_`.

    `obj` is the object at its *new* address; `from_` is its previous name
    (a bare leaf name) or a full previous natural key.  See
    :meth:`PlanContext.moved`.
    """

    obj: Materializable
    from_: str


class PlanNotFinalizedError(RuntimeError):
    """Raised when plan results are accessed before the plan has been finalized."""


class PlanContext:
    """Mutable context passed to :meth:`Plan.run` for plan-wide settings."""

    def __init__(self) -> None:
        self.default_space_uid: SpaceUID | None = None
        self._moves: list[Moved] = []
        self._migrations: list[Migrated] = []

    def migrate(self, obj: Materializable | str, migration: DataMigration, /) -> None:
        """Attach a narrowing data migration to `obj` for the next apply.

        Unblocks a validation-affecting change on `obj` that the live data would
        otherwise refuse (a narrowing the data does not already prove safe).  The
        shapes, by the kind of `obj`:

        * a **Feature** whose dtype narrows: on apply the engine flips the dtype
          and then runs `migration` inside the same transaction to rewrite the
          stored values so they validate against it.  The callable receives a
          :class:`~lamindb_setup.evolve.authoring._migrations.MigrationContext`.
        * a **Feature** whose `nullable` the plan turns off: `migration`
          backfills the sheet rows that omit it, once per schema declaring it
          (see :func:`~lamindb_setup.evolve.fill_nulls`), or vouches for the
          datasets validated with it (see
          :func:`~lamindb_setup.evolve.accept_non_null`).  A backfill receives a
          :class:`~lamindb_setup.evolve.authoring._migrations.MemberMigrationContext`.
        * a **Schema** that makes an optional member required: the engine
          narrows the optional set and then runs `migration` to annotate the
          rows that omitted the member.  The callable receives a
          :class:`~lamindb_setup.evolve.authoring._migrations.MemberMigrationContext`, whose
          `missing()` enumerates exactly those rows.  A row still omitting it
          afterwards fails the apply, so a partial backfill rolls back.
        * a **Schema** that drops a member whose values are stored: the engine
          unlinks it and then runs `migration` to say what becomes of them (see
          :func:`~lamindb_setup.evolve.drop_member_values` and
          :func:`~lamindb_setup.evolve.keep_member_values`).  The
          callable receives a
          :class:`~lamindb_setup.evolve.authoring._migrations.MemberRemovalContext`.
        * a **Schema** whose change invalidates an exported sheet snapshot: the
          engine hands `migration` the stale exports so it can write a new
          version of each (see
          :func:`~lamindb_setup.evolve.reexport_sheets` and
          :func:`~lamindb_setup.evolve.keep_stale_exports`).  The
          callable receives a
          :class:`~lamindb_setup.evolve.authoring._migrations.SheetExportContext`, and --
          alone among these -- runs *after* the apply transaction committed, onto
          a branch merged into the instance only on success: an exported CSV is a
          file in storage, which no transaction can roll back.
        * a **Schema** whose `index` the plan adds, changes or removes while it
          has already validated data: the engine switches the index and then runs
          `migration`, which states that the data conforms (see
          :func:`~lamindb_setup.evolve.accept_index_change`).
          The callable receives a
          :class:`~lamindb_setup.evolve.authoring._migrations.IndexChangeContext`.
        * a **Schema** whose `flexible` the plan turns off: the engine switches
          the flag and then runs `migration`, which either keeps what the
          flexible match annotated -- and, on a `maximal_set` schema, vouches
          that the data still validates (see
          :func:`~lamindb_setup.evolve.accept_flexible_change`) -- or removes it
          (see :func:`~lamindb_setup.evolve.drop_nonmember_annotations`).  The
          callable receives a
          :class:`~lamindb_setup.evolve.authoring._migrations.FlexibleChangeContext`.
        * a **Sheet** the plan points at another schema while it has rows:
          :func:`~lamindb_setup.evolve.revalidate_rows` has the apply check them
          against the new schema, and
          :func:`~lamindb_setup.evolve.reexport_sheets` or
          :func:`~lamindb_setup.evolve.keep_stale_exports` say what becomes of
          the exports written against the old one, as for a schema.
        * a **natural key string** naming a feature the plan no longer declares,
          e.g. `"FeatureType:ft/Feature:retired"`: the removal pass runs
          `migration` before the feature is trashed (see
          :func:`~lamindb_setup.evolve.drop_values` and
          :func:`~lamindb_setup.evolve.keep_values`).  The callable
          receives a
          :class:`~lamindb_setup.evolve.authoring._migrations.FeatureRemovalContext`.  A
          string is the only way to address it: the plan deleting the feature is
          precisely the plan with no object left to point at.  A record type or
          sheet the plan deletes is addressed the same way, to say what becomes
          of its records (:func:`~lamindb_setup.evolve.drop_rows`, receiving a
          :class:`~lamindb_setup.evolve.authoring._migrations.RecordTypeRemovalContext`),
          and so is a deleted schema, to vouch for the datasets it validated
          (:func:`~lamindb_setup.evolve.keep_validated`, receiving a
          :class:`~lamindb_setup.evolve.authoring._migrations.SchemaRemovalContext`).
          Instead of letting the trash take them, the records can be kept
          (:func:`~lamindb_setup.evolve.keep_rows`) or moved to another record
          type or sheet (:func:`~lamindb_setup.evolve.move_rows`), the datasets
          pointed at another schema
          (:func:`~lamindb_setup.evolve.revalidate_against`), and the sub-types,
          features and schemas typed under a deleted type re-parented
          (:func:`~lamindb_setup.evolve.adopt_children`) -- each performed by the
          apply itself, before the deletion.
        * a seeded **Record**, which ignores the plan's edits once it exists:
          :func:`~lamindb_setup.evolve.update_record` writes the plan's feature
          values and description over the instance's, receiving a
          :class:`~lamindb_setup.evolve.authoring._migrations.RecordUpdateContext`.
          A record the plan stops declaring stays on the instance;
          :func:`~lamindb_setup.evolve.drop_record`, declared by its natural key,
          trashes it -- relocating the annotations that hold it as their value
          first -- receiving a
          :class:`~lamindb_setup.evolve.authoring._migrations.RecordRemovalContext`.

        Like :meth:`moved`, the declaration is idempotent: once the instance
        matches the plan the change -- and hence the migration -- is skipped, so it
        is safe to leave in place until every instance has been migrated and then
        remove it in a follow-up commit.

        A migration never *creates* the objects it maps into: they must be declared
        in the plan (or already exist in the instance).  Declaration order does not
        matter -- the apply runs every migration in its own phase, after it has
        materialized every object the plan declares.

        Finalizing the plan checks every declaration, so `validate` catches a
        misdeclared one offline with a
        :class:`~lamindb_setup.evolve.authoring._declarations.PlanDeclarationError`:
        a helper attached to a kind of target it does not answer for (the shapes
        above, see :attr:`DataMigration.targets
        <lamindb_setup.evolve.authoring._migrations.DataMigration.targets>`; a
        natural key may stand in for an object, as for a schema no plan
        manages), a string that is not a natural key, and a removal declared for
        the key of an object this plan or a prerequisite still declares.  A
        hand-written migration naming no narrowing answers whatever it is
        declared for, so only its target is checked to be a plan object or a
        natural key.
        """
        from ._migrations import Migrated

        self._migrations.append(Migrated(obj, migration))

    @property
    def migrations(self) -> tuple[Migrated, ...]:
        """Return the data migrations declared via :meth:`migrate`."""
        return tuple(self._migrations)

    def moved(self, obj: Materializable, from_: str, /) -> None:
        """Declare that `obj` was previously stored under a different address.

        Records a rename or a move to another parent so the next apply keeps
        the existing object's identity -- its `uid` and everything validated
        against it -- instead of deleting the old address and creating a new one.
        `from_` is either the object's previous *name* (a leaf rename under the
        same type chain) or a full previous natural key such as
        `"FeatureType:old/Feature:x"` for a move that also changes the type
        chain.  A record type's parent decides which `cat[Record[<type>]]`
        features admit its records, so moving one out from under a type is
        applied only when no such feature holds a value below it.

        The declaration is idempotent: once applied (the new address is recorded
        in the lock) it is a no-op, so it is safe to leave in place until every
        instance has been migrated and then remove it in a follow-up commit.
        Finalizing the plan refuses a `from_` naming the object's *current*
        name or natural key, and a `from_` holding a `:` or `/` that is not
        a natural key.

        Seed-only objects have no lock entry to re-address and so cannot be
        moved: renaming one on the instance is invisible to the plan and needs
        no declaration here.
        """
        kind = type(obj).__name__
        if kind in SEED_ONLY_KINDS:
            raise TypeError(
                f"ctx.moved() does not apply to {kind}: it is seeded, not tracked, "
                "so it has no recorded address to move. Rename it on the instance "
                f"and update the plan's {kind} name to match."
            )
        self._moves.append(Moved(obj, from_))

    @property
    def moves(self) -> tuple[Moved, ...]:
        """Return the address changes declared via :meth:`moved`."""
        return tuple(self._moves)


class PlanExports:
    """Base class for declaring the reusable outputs of a :class:`Plan`.

    Subclasses declare named, typed fields for the objects a plan produces so
    they can be exported and reused by other plans::

        class MyExports(PlanExports):
            feature_0: Feature
            schema_0: Schema

    Only the schema types used as plan building blocks may be declared as
    fields; any other annotation raises :class:`TypeError` when the subclass is
    defined.
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        def _allowed_export_types() -> set[type]:
            from ._features import Feature, FeatureType
            from ._records import RecordType, Sheet
            from ._schemas import Schema, SchemaType

            return {
                RecordType,
                SchemaType,
                FeatureType,
                Feature,
                Schema,
                Sheet,
            }

        allowed = _allowed_export_types()
        hints = get_type_hints(cls)
        for name, hint in hints.items():
            if hint not in allowed:
                hint_name = getattr(hint, "__name__", None) or repr(hint)
                allowed_names = ", ".join(sorted(t.__name__ for t in allowed))
                raise TypeError(
                    f"PlanExports field {name!r} has unsupported type "
                    f"{hint_name}; allowed export types are: {allowed_names}."
                )


class Plan(ABC, Generic[E]):
    """Collect metadata declarations for validation, preview, and apply.

    Usually created with :func:`create_plan`. Calling :meth:`finalize` runs
    the plan and collects its declarations and dependencies in memory;
    it does not save them to an instance.
    """

    _finalizing: ClassVar[set[int]] = set()

    @property
    @abstractmethod
    def _exports_cls(self) -> type[E]:
        """Return the :class:`PlanExports` subclass to use for this plan's exports."""
        raise NotImplementedError("Subclasses must implement _exports_cls()")

    @abstractmethod
    def run(self, ctx: PlanContext, exports: E, /) -> None:
        """Define the plan by creating schema objects.

        Subclasses implement this to declare the objects to materialize.  Use
        `ctx` to configure plan-wide settings such as the default space, and
        assign reusable outputs to `exports`.
        """
        raise NotImplementedError("Subclasses must implement run()")

    def __init__(self, name: str) -> None:
        _ensure_lamindb_not_loaded()
        self.name: str = name
        self._dependencies: list[Any] = []
        self._plan_dependencies: list[Plan[Any]] = []
        self._ctx: PlanContext | None = None
        self._exports: E | None = None
        self._finalized = False

    @classmethod
    def active(cls) -> Plan[Any] | None:
        """Return the active plan while it is running, or `None` otherwise."""
        collector = active_collector()
        return collector if isinstance(collector, Plan) else None

    @property
    def defining_module(self) -> str | None:
        """Return the name of the module that defines this plan, if known.

        Used to tell plans declared in a file apart from plans merely imported
        into it.  Defaults to the module of the plan's own class; subclasses
        such as :class:`_FunctionPlan` override this to point at the user code.
        """
        return type(self).__module__

    @property
    def ctx(self) -> PlanContext:
        """Return the :class:`PlanContext` produced while running the plan."""
        if not self._finalized or self._ctx is None:
            raise PlanNotFinalizedError(
                "plan must be finalized before accessing 'ctx'; call finalize() first."
            )
        return self._ctx

    @property
    def exports(self) -> E:
        """Return the :class:`PlanExports` populated while running the plan."""
        if not self._finalized:
            raise PlanNotFinalizedError(
                "plan must be finalized before accessing 'exports'; "
                "call finalize() first."
            )
        if self._exports is None:
            raise AttributeError(f"plan {self.name!r} does not declare exports.")
        return self._exports

    @property
    def dependencies(self) -> DependencyDAG:
        """Return the collected dependencies ordered for creation."""
        if not self._finalized:
            raise PlanNotFinalizedError(
                "plan must be finalized before accessing 'dependencies'; "
                "call finalize() first."
            )
        all_dependencies: list[Dependency] = []
        seen: set[int] = set()

        for plan in self.depends_on:
            for dep in plan.dependencies:
                key = id(dep.obj)
                if key in seen:
                    continue
                seen.add(key)
                all_dependencies.append(dep)

        for dep in self._dependencies:
            key = id(dep.obj)
            if key in seen:
                continue
            seen.add(key)
            all_dependencies.append(dep)

        return DependencyDAG(all_dependencies)

    @property
    def own_dependencies(self) -> DependencyDAG:
        """Return only the dependencies this plan declares itself.

        Unlike :attr:`dependencies`, the objects inherited from the plans in
        :attr:`depends_on` are excluded.  `destroy` uses this so tearing down a
        plan only deletes the objects it concretely creates, leaving the objects
        managed by upstream plans untouched.
        """
        if not self._finalized:
            raise PlanNotFinalizedError(
                "plan must be finalized before accessing 'own_dependencies'; "
                "call finalize() first."
            )
        return DependencyDAG(list(self._dependencies))

    @property
    def inherited_dependencies(self) -> DependencyDAG:
        """Return only the dependencies inherited from :attr:`depends_on`.

        The complement of :attr:`own_dependencies` within :attr:`dependencies`:
        the objects a prerequisite plan declares and this plan merely
        *references*.  They still have to be resolved -- a schema cannot link a
        borrowed feature without it -- but they are the prerequisite plan's to
        manage, so `apply` adopts them without recording them in this plan's
        lock (see :meth:`~lamindb_setup.evolve.engine._apply.ApplyContext._borrow`).
        """
        own = {id(dep.obj) for dep in self.own_dependencies}
        return DependencyDAG(
            [dep for dep in self.dependencies if id(dep.obj) not in own]
        )

    @property
    def inherited_moves(self) -> tuple[Moved, ...]:
        """Return the address changes declared by the plans in :attr:`depends_on`.

        A prerequisite plan renaming one of its objects changes that object's
        natural key, and this plan records the borrowed object by that key
        wherever it references it (a schema's member list, a sheet's schema).
        Surfacing the upstream declarations lets `apply` rebase those
        references instead of reading the rename as "the plan dropped a member".
        Collected transitively, since a prerequisite may itself depend on a plan
        that declares a move.
        """
        moves: list[Moved] = []
        for plan in self.depends_on:
            moves.extend(plan.ctx.moves)
            moves.extend(plan.inherited_moves)
        return tuple(moves)

    @property
    def depends_on(self) -> tuple[Plan[Any], ...]:
        """Return directly declared prerequisite plans."""
        return tuple(self._plan_dependencies)

    def add_dependency(self, obj: Dependency, /) -> None:
        """Record a dependency as a to-be-materialized object."""
        self._dependencies.append(obj)

    def add_plan_dependency(self, plan: Plan[Any], /) -> None:
        """Record another plan that must be finalized before this one runs."""
        if plan is self:
            raise ValueError("a plan cannot depend on itself")
        if plan in self._plan_dependencies:
            return
        self._plan_dependencies.append(plan)

    def finalize(self) -> None:
        """Run :meth:`run` once, collecting the planned objects."""
        if self._finalized:
            return
        key = id(self)
        if key in Plan._finalizing:
            raise RuntimeError("circular plan dependency detected")
        Plan._finalizing.add(key)
        try:
            for dependency_plan in self.depends_on:
                dependency_plan.finalize()

            if active_collector() is not None:
                raise RuntimeError(
                    "A plan is already active; nested plans are not supported."
                )
            ctx = PlanContext()
            exports = self._exports_cls()
            with collecting(self):
                self.run(ctx, exports)
            self._ctx = ctx
            self._exports = exports
            self._check_seed_only_named()
            self._check_declarations(ctx)
            self._finalized = True
        except BaseException:
            self._ctx = None
            self._exports = None
            self._dependencies = []
            self._finalized = False
            raise
        finally:
            Plan._finalizing.remove(key)

    def _check_seed_only_named(self) -> None:
        """Reject a declared seed-only object that has no name.

        A seed-only object is found by `(name, type)` until the lock remembers
        its uid (see
        :meth:`~lamindb_setup.evolve.engine._apply.ApplyContext._seed`), so an unnamed
        one has no identity to match against on its first apply, and every apply
        to a new instance would create another copy of it.  Raised here rather
        than at apply time so `validate` catches it offline.
        """
        for dependency in self._dependencies:
            obj = dependency.obj
            kind = type(obj).__name__
            if kind in SEED_ONLY_KINDS and getattr(obj, "name", None) is None:
                raise PlanDeclarationError(
                    f"plan {self.name!r} declares an unnamed {kind}: a {kind} is "
                    "seeded by name, so an unnamed one would be re-created on "
                    "every apply. Give it a name."
                )

    def _check_declarations(self, ctx: PlanContext) -> None:
        """Reject a `ctx.migrate` or `ctx.moved` that can never do what it says.

        A misplaced helper, a malformed natural key, a removal declared for an
        object this plan or a prerequisite still declares, a move from the
        object's current address -- see
        :func:`~lamindb_setup.evolve.authoring._declarations.check_declarations`.
        Raised here rather than at apply time so `validate` catches it offline.
        """
        declared: dict[str, str] = {}
        for dependency in self._dependencies:
            if (key := named_key(dependency.obj)) is not None:
                declared.setdefault(key, "the plan")
        for plan in self.depends_on:
            for dependency in plan.dependencies:
                if (key := named_key(dependency.obj)) is not None:
                    declared.setdefault(key, f"prerequisite plan {plan.name!r}")
        check_declarations(self.name, ctx.migrations, ctx.moves, declared)

    def reset(self) -> None:
        """Discard finalized state so the next :meth:`finalize` re-runs the plan.

        Applying the same plan against several instances in one process
        reconnects LaminDB between them; `lamindb_setup` tears Django down and
        rebuilds the model classes on that switch (`reset_django`), so a
        `lamindb` class captured while the plan was finalized against the
        previous instance (e.g. a `LaminDType` holding `ln.ULabel`) is stale
        for the next one and fails `isinstance`/`issubclass` checks.  Reset
        clears the cached objects so the plan is finalized afresh against each
        instance's live classes; it cascades to the prerequisite plans in
        :attr:`depends_on` since their objects go stale for the same reason.
        """
        self._finalized = False
        self._ctx = None
        self._exports = None
        self._dependencies = []
        for dependency_plan in self._plan_dependencies:
            dependency_plan.reset()


class _FunctionPlan(Plan[E], Generic[E]):
    """A :class:`Plan` defined by a decorated function (see :func:`create_plan`)."""

    def __init__(self, func: Callable[..., None], exports_cls: type[E]) -> None:
        if isinstance(func, FunctionType):
            name = func.__name__
        else:
            name = getattr(func, "__name__", repr(func))
        super().__init__(name)
        self._func = func
        self._func_exports_cls = exports_cls
        for plan in self.extract_plan_dependencies(func):
            self.add_plan_dependency(plan)

    @property
    def _exports_cls(self) -> type[E]:
        return self._func_exports_cls

    @property
    def defining_module(self) -> str | None:
        """Return the module that defines the decorated plan function."""
        return getattr(self._func, "__module__", None)

    @staticmethod
    def extract_exports_cls(func: Callable[..., None]) -> type[PlanExports]:
        """Resolve the :class:`PlanExports` subclass from a plan function signature.

        The plan function must accept a first positional `ctx` parameter and a
        second positional `exports` parameter annotated with
        :class:`PlanExports`, a subclass, or `None`.  Returns the annotated
        :class:`PlanExports` subclass, or the base :class:`PlanExports` when the
        annotation is `None`.
        """
        params = [
            p
            for p in inspect.signature(func).parameters.values()
            if p.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        if len(params) < 2:
            raise TypeError(
                f"plan function {getattr(func, '__name__', func)!r} must accept a "
                "second positional 'exports' parameter annotated with PlanExports, "
                "a subclass, or None."
            )
        exports_param = params[1]
        try:
            hints = get_type_hints(func)
        except Exception:
            hints = getattr(func, "__annotations__", {})
        if exports_param.name not in hints:
            raise TypeError(
                f"plan function {getattr(func, '__name__', func)!r} must have type "
                "annotations to resolve the second positional parameter annotation."
            )
        hint = hints.get(exports_param.name)
        if hint is None or hint is type(None):
            return PlanExports
        if isinstance(hint, type) and issubclass(hint, PlanExports):
            return hint
        raise TypeError(
            f"plan function {getattr(func, '__name__', func)!r} declares a second "
            f"parameter {exports_param.name!r} that is not annotated with "
            "PlanExports, a subclass, or None."
        )

    @staticmethod
    def extract_plan_dependencies(func: Callable[..., None]) -> list[Plan[Any]]:
        """Resolve prerequisite plans declared via typed default parameters.

        Parameters after `ctx` and `exports` can declare plan dependencies by
        using a `Plan[...]` type annotation and assigning a :class:`Plan`
        instance as the default value.
        """
        parameters = list(inspect.signature(func).parameters.values())
        try:
            hints = get_type_hints(func)
        except Exception:
            hints = getattr(func, "__annotations__", {})

        dependencies: list[Plan[Any]] = []
        for param in parameters[2:]:
            if param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                continue

            hint = hints.get(param.name)
            origin = get_origin(hint)
            is_plan_hint = (
                hint is Plan
                or origin is Plan
                or (isinstance(hint, type) and issubclass(hint, Plan))
            )

            if is_plan_hint:
                if param.default is inspect.Parameter.empty:
                    raise TypeError(
                        f"plan function {getattr(func, '__name__', func)!r} "
                        f"declares plan parameter {param.name!r} without a "
                        "default Plan value."
                    )
                if not isinstance(param.default, Plan):
                    raise TypeError(
                        f"plan function {getattr(func, '__name__', func)!r} "
                        f"declares plan parameter {param.name!r} with a "
                        "non-Plan default value."
                    )
                dependencies.append(param.default)
                continue

            if isinstance(param.default, Plan):
                raise TypeError(
                    f"plan function {getattr(func, '__name__', func)!r} uses a "
                    f"Plan default for parameter {param.name!r} but does not "
                    "annotate it as Plan[...]."
                )

        return dependencies

    def run(self, ctx: PlanContext, exports: E) -> None:
        self._func(ctx, exports)


@overload
def create_plan(
    func: Callable[[PlanContext, E], None],
) -> _FunctionPlan[E]: ...


@overload
def create_plan(
    func: Callable[[PlanContext, None], None],
) -> _FunctionPlan[PlanExports]: ...


def create_plan(
    func: Callable[..., None],
) -> _FunctionPlan[Any]:
    """Turn a declaration function into a :class:`Plan`.

    The function takes a :class:`PlanContext` and an `exports` argument
    annotated with a :class:`PlanExports` subclass, or `None` if it has no
    exports. Its body runs when the plan is finalized, not at decoration time.
    Creating declarations in that body registers them with the plan.

    Example::

        @create_plan
        def measurements(ctx: PlanContext, exports: None) -> None:
            Feature("measurement", dtype=LaminDType(float))

    Use `lamin evolve init` to create a plan file with the imports and stable
    transform UID required by the CLI. See :doc:`manage-schemas`.
    """
    exports_cls = _FunctionPlan.extract_exports_cls(func)
    return _FunctionPlan(func, exports_cls)
