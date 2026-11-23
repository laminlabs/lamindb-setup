"""The object graph a plan is built from: promises, dependencies, materializables.

Vocabulary rather than machinery: what a declared object *is*
(:class:`Materializable`), how declarations are collected into a graph
(:class:`Dependency`, :class:`DependencyDAG`), and how a reference to a
not-yet-created object is carried until it has a uid (:class:`Promise`).  Both
layers above speak it -- `authoring` builds the graph, `engine` walks it --
so it belongs here with the rest of the shared vocabulary rather than inside
either of them.

Neither collaborator is imported back.  The two things this module needs are
described as protocols and supplied by the caller: :class:`DependencyCollector`
is whatever records declarations while a plan runs (`Plan` registers itself as
that for the duration of its run), and :class:`MaterializeContext` is whatever
resolves them against an instance (the apply engine's context).  A context is
always passed in -- nothing here ever builds one -- so the module-level import
graph runs strictly downhill with no exceptions.
"""

from __future__ import annotations

from typing import Any, Generic

from typing_extensions import TypeVar

R = TypeVar("R")
T = TypeVar("T", default=Any)

from collections.abc import Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

    from ._uids import SpaceUID

__all__ = [
    "Dependency",
    "DependencyCollector",
    "DependencyDAG",
    "DependencyMeta",
    "Materializable",
    "MaterializeContext",
    "Promise",
    "SEED_ONLY_KINDS",
    "active_collector",
    "collecting",
]

# Plan object kinds that are *seeded* rather than tracked: they are created when
# missing and adopted when already present, but never drift-checked, updated or
# deleted.  The lock remembers only their uid, in an identity-only `seeded`
# section, so they keep one identity across instances (see
# :mod:`lamindb_setup.evolve.engine._alignment`).
#
# Non-type records are the vocabulary *values* behind a record type (the options
# in a sheet's dropdown).  Tracking them was write-only: nothing on the read
# side ever observed them (introspection reads `is_type=True` only, so
# `repair dangling` and the CI drift check never see them), while their `name`
# counted as structural -- so a value renamed on the instance turned every plan
# carrying it into a hard `DriftError`, repairable only by repeating
# `ctx.moved` in each dependent plan.  Seeding them keeps vocabularies in
# version control and keeps them available as `map_to_records` targets, while
# leaving their ongoing lifecycle to the instance.
SEED_ONLY_KINDS: frozenset[str] = frozenset({"Record"})


class MaterializeContext(Protocol):
    """What a declared object may ask of the run that is materializing it.

    The concrete implementation is
    :class:`~lamindb_setup.evolve.engine._apply.ApplyContext`, which is a few thousand
    lines of apply engine.  Declaring the handful of methods the building blocks
    actually call keeps the arrow pointing down: `authoring`'s `_features`,
    `_records` and `_schemas` describe what they need instead of naming the
    engine that provides it.
    """

    def find_typed(
        self, registry: Any, obj: Materializable, **filters: Any
    ) -> Any | None:
        """Read-only lookup of `obj`, constrained by its parent type."""
        ...

    def resolve_space(self, obj: Materializable) -> Any | None:
        """Look up the existing LaminDB `Space` for `obj`, or `None`."""
        ...

    def resolve_space_uid(self, uid: SpaceUID) -> Any:
        """Look up the existing LaminDB `Space` for an explicit `uid`."""
        ...

    def locked_uid_of(self, obj: Materializable) -> str | None:
        """Return the uid recorded in the lock for `obj` if any."""
        ...

    def pinned_uid_of(self, obj: Materializable) -> str | None:
        """Return the uid `obj` is pinned to when this instance's lock has none."""
        ...

    def creation_uid(self, obj: Materializable) -> str | None:
        """Return the uid to create `obj` with, or `None` for a fresh one."""
        ...

    def materialize_object(self, obj: Materializable) -> Any:
        """Resolve `obj`: reuse, create, or return a placeholder."""
        ...


class Promise(Generic[R, T]):
    """A deferred call."""

    __slots__ = ("_arg", "_func", "_value", "label")

    def __init__(
        self,
        arg: T,
        func: Callable[[T, MaterializeContext], R],
        *,
        label: str | None = None,
    ) -> None:
        self._arg = arg
        self._func = func
        self._value: R | None = None
        self.label: str | None = label

    def resolve(self, ctx: MaterializeContext) -> R:
        """Resolve to the concrete uid, materializing the underlying object.

        `ctx` threads the active :class:`MaterializeContext` so the underlying
        object is materialized once and shares the apply-wide cache and lock
        state.  Callers outside an apply run pass a context of their own
        (:meth:`~lamindb_setup.evolve.engine._apply.ApplyContext.standalone`).
        """
        if self._value is None:
            self._value = self._func(self._arg, ctx)
            if _provisional is not None:
                _provisional.append(self)
        return self._value

    def get_arg(self) -> T:
        """Get the underlying argument that will be materialized."""
        return self._arg

    def __repr__(self) -> str:
        if self.label is not None:
            return f"Promise({self.label})"
        return f"Promise({self._func!r})"


class Dependency:
    """Represents a dependency on another object that needs to be materialized first."""

    def __init__(
        self,
        obj: Materializable,
        *,
        requires: Iterable[Materializable] | None = None,
    ) -> None:
        self.obj = obj
        self.requires: list[Materializable] = list(requires) if requires else []


class DependencyDAG:
    """Dependencies ordered so that required objects are created first."""

    def __init__(self, dependencies: Iterable[Dependency]) -> None:
        self._dependencies = list(dependencies)

    def __iter__(self) -> Iterator[Dependency]:
        return iter(self.ordered())

    def __len__(self) -> int:
        return len(self._dependencies)

    def ordered(self) -> list[Dependency]:
        """Return the dependencies in a valid creation order (requirements first)."""
        index = {id(dep.obj): dep for dep in self._dependencies}
        ordered: list[Dependency] = []
        done: set[int] = set()
        visiting: set[int] = set()

        def visit(dep: Dependency) -> None:
            key = id(dep.obj)
            if key in done:
                return
            if key in visiting:
                raise ValueError("circular dependency detected")
            visiting.add(key)
            for required in dep.requires:
                required_dep = index.get(id(required))
                if required_dep is not None:
                    visit(required_dep)
            visiting.remove(key)
            done.add(key)
            ordered.append(dep)

        for dep in self._dependencies:
            visit(dep)
        return ordered


class DependencyCollector(Protocol):
    """Whatever is recording declarations while a plan runs.

    :class:`~lamindb_setup.evolve.Plan` is the only implementation; the
    protocol is what keeps this module from having to import it.
    """

    def add_dependency(self, obj: Dependency, /) -> None:
        """Record a dependency as a to-be-materialized object."""
        ...


_collector: DependencyCollector | None = None


def active_collector() -> DependencyCollector | None:
    """Return the collector recording declarations right now, or `None`."""
    return _collector


@contextmanager
def collecting(collector: DependencyCollector) -> Iterator[None]:
    """Record every :class:`Materializable` created in the block into `collector`."""
    global _collector
    previous = _collector
    _collector = collector
    try:
        yield
    finally:
        _collector = previous


_provisional: list[Promise[Any, Any]] | None = None


@contextmanager
def provisional_promises() -> Iterator[None]:
    """Forget, once the block exits, what each promise resolved in it resolved to.

    A promise memoizes its value beyond the pass that resolved it, so a later
    pass over the same plan reuses the uid.  A pass that is rolled back must not
    hand its uids on: they name records the rollback discarded, so a promise it
    resolved has to be resolved again by the next pass.
    """
    global _provisional
    previous = _provisional
    resolved: list[Promise[Any, Any]] = []
    _provisional = resolved
    try:
        yield
    finally:
        _provisional = previous
        for promise in resolved:
            promise._value = None


class DependencyMeta(type):
    """Metaclass that registers every created instance with the active collector as a dependency."""

    @classmethod
    def _iter_materializables(mcs, value: Any) -> Iterator[Materializable]:
        """Yield the :class:`Materializable` objects referenced by `value`.

        Resolves :class:`Promise` arguments to their underlying object and
        unpacks common containers (mappings, lists, tuples, and sets) so that
        objects nested inside collections such as a schema's `features` list
        or a composite schema's `slots` are recorded as dependencies.
        """
        if isinstance(value, Promise):
            value = value.get_arg()
        if isinstance(value, Materializable):
            yield value
            return
        if isinstance(value, (str, bytes)):
            return
        if isinstance(value, Mapping):
            for key, val in value.items():
                yield from mcs._iter_materializables(key)
                yield from mcs._iter_materializables(val)
            return
        if isinstance(value, (list, tuple, set, frozenset)):
            for item in value:
                yield from mcs._iter_materializables(item)

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        instance = super().__call__(*args, **kwargs)
        collector = active_collector()
        if collector is not None:
            requires: list[Materializable] = []
            seen: set[int] = set()
            for obj in (*args, *kwargs.values()):
                for required in cls._iter_materializables(obj):
                    if id(required) not in seen:
                        seen.add(id(required))
                        requires.append(required)
            collector.add_dependency(Dependency(instance, requires=requires))
        return instance


@contextmanager
def _without_name_suggestions() -> Iterator[None]:
    """Skip LaminDB's "did you mean" lookup while building a record.

    `SQLRecord.__init__` runs `suggest_records_with_similar_names` for every
    record it constructs: an exact-match query plus a fuzzy search, two
    statements per object.  On an exact hit it does not just log a hint -- it
    silently discards the record being built and returns the existing one.

    Both are redundant, and the second is unwanted.  :meth:`Materializable.commit`
    is only ever reached once the apply context has established that the object
    is *missing*, and that lookup is the stricter of the two: it matches on
    `is_type` and the parent type, which LaminDB's does not, so LaminDB can
    "find" a plain feature for a declared feature type of the same name and
    quietly adopt it.  Deciding existence in one place, with the plan's own
    rules, is the point of the apply context.

    Restores the previous value rather than assuming it was on, so a caller that
    already disabled the lookup keeps it disabled and nesting is safe.
    """
    import lamindb as ln

    creation = ln.settings.creation
    previous = creation.search_names
    creation.search_names = False
    try:
        yield
    finally:
        creation.search_names = previous


class Materializable(metaclass=DependencyMeta):
    """Interface for objects that can be checked against and created in LaminDB.

    Subclasses implement :meth:`stage` (build the unsaved LaminDB object) and
    :meth:`get` (a read-only existence lookup that never writes).  :meth:`commit`
    and :meth:`materialize` provide the shared create and get-or-create
    orchestration: existence is always checked first and `.save()` is only
    called when an object actually needs to be created.
    """

    def stage(self, ctx: MaterializeContext) -> Any:
        """Build the unsaved LaminDB object, resolving nested dependencies via `ctx`."""
        raise NotImplementedError("Subclasses must implement stage()")

    def get(self, ctx: MaterializeContext) -> Any | None:
        """Read-only lookup of an existing record, or `None`. Never saves."""
        raise NotImplementedError("Subclasses must implement get()")

    def commit(self, ctx: MaterializeContext) -> Any:
        """Create and persist the object (`stage` then `save`).

        The object is placed in the space resolved from its `space_uid` or the
        plan-wide default before it is saved.  It is created with the uid
        :meth:`MaterializeContext.creation_uid` names, when there is one, so it
        keeps the identity it has on the plan's other instances.
        :meth:`_post_save` then applies any settings that require an
        already-persisted record.
        """
        with _without_name_suggestions():
            record = self.stage(ctx)
        uid = ctx.creation_uid(self)
        # lamindb hands back an existing record for some inputs (a schema whose
        # content hash is already taken); its identity is not ours to change.
        if uid is not None and record._state.adding:
            record.uid = uid
        space = ctx.resolve_space(self)
        if space is not None:
            record.space = space
        record.save()
        self._post_save(ctx, record)
        return record

    def _post_save(self, ctx: MaterializeContext, record: Any) -> None:
        """Apply settings that require an already-saved record. No-op by default."""
        return None

    def materialize(self, ctx: MaterializeContext) -> Any:
        """Resolve the object: reuse it if it exists, otherwise create it.

        `ctx` carries the apply-wide cache and lock state.  Callers outside an
        apply run pass a context of their own
        (:meth:`~lamindb_setup.evolve.engine._apply.ApplyContext.standalone`).
        """
        return ctx.materialize_object(self)
