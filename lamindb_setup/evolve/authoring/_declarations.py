"""The offline check of a plan's `ctx.migrate` and `ctx.moved` declarations.

A declaration that can never do what it says used to pass `validate` and
surface at `plan`/`apply` time at best -- against a live instance -- and at
worst be a silent no-op, indistinguishable from the "idempotent once applied"
one a declaration is meant to become.  Everything checked here is decidable from
the plan alone, so :meth:`~lamindb_setup.evolve.Plan.finalize` runs it:

* a `ctx.migrate` target is a plan object or a natural key, and of a kind the
  migration answers for (:attr:`DataMigration.targets
  <lamindb_setup.evolve.authoring._migrations.DataMigration.targets>`);
* a natural key parses as `Kind:name` segments joined by `/`, with known
  kinds, each parent a type of its child;
* a removal declared by natural key does not name an object the plan -- or a
  prerequisite it loads -- still declares, which would leave it nothing to do;
* a `ctx.moved` does not name the object's current address as its previous one.

Natural keys are spelled as :func:`~lamindb_setup.evolve.engine._lock.natural_key`
spells them for named objects; this layer cannot import the engine, and an
unnamed object (keyed by a hash of its configuration) is never addressed by hand.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..core import LaminSchemasError, Materializable

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from ._migrations import DataMigration, DeclarationTarget, Migrated
    from ._plan import Moved

__all__ = [
    "NATURAL_KEY_KINDS",
    "PlanDeclarationError",
    "check_declarations",
    "describe_target",
    "migration_call",
    "named_key",
    "parse_natural_key",
]


class PlanDeclarationError(LaminSchemasError, ValueError):
    """A plan declares something that can never do what it says.

    Raised while the plan is finalized, so `validate` reports it offline as a
    one-line `Error:`.  A :class:`ValueError` too, which is what the checks it
    replaces raised.
    """


_FEATURE_TYPES = frozenset({"FeatureType"})
_SCHEMA_TYPES = frozenset({"SchemaType"})
_RECORD_TYPES = frozenset({"RecordType", "Sheet"})

#: Every kind a natural key segment can name, with the kinds its parent can be.
NATURAL_KEY_KINDS: dict[str, frozenset[str]] = {
    "Feature": _FEATURE_TYPES,
    "FeatureType": _FEATURE_TYPES,
    "Record": _RECORD_TYPES,
    "RecordType": _RECORD_TYPES,
    "Schema": _SCHEMA_TYPES,
    "SchemaType": _SCHEMA_TYPES,
    "Sheet": _RECORD_TYPES,
}

_KEY_SHAPE = (
    "expected 'Kind:name' segments joined by '/', e.g. 'FeatureType:ft/Feature:x', "
    "with Kind one of " + ", ".join(sorted(NATURAL_KEY_KINDS))
)


def parse_natural_key(key: str) -> tuple[tuple[str, str], ...]:
    """Split `key` into its `(kind, name)` segments, outermost type first.

    Raises :class:`ValueError` saying what is wrong with it.  A segment with no
    `:` continues the previous segment's name, so a name holding a `/` still
    parses.
    """
    segments: list[tuple[str, str]] = []
    for part in key.split("/"):
        kind, sep, name = part.partition(":")
        if not sep:
            if not segments:
                raise ValueError(f"it does not start with 'Kind:name'; {_KEY_SHAPE}")
            parent_kind, parent_name = segments[-1]
            segments[-1] = (parent_kind, f"{parent_name}/{part}")
            continue
        if kind not in NATURAL_KEY_KINDS:
            raise ValueError(f"{kind!r} is not a kind; {_KEY_SHAPE}")
        if not name:
            raise ValueError(f"its {kind} segment has no name; {_KEY_SHAPE}")
        if segments and segments[-1][0] not in NATURAL_KEY_KINDS[kind]:
            raise ValueError(
                f"a {kind} cannot sit below a {segments[-1][0]}: its type is "
                + " or ".join(f"a {k}" for k in sorted(NATURAL_KEY_KINDS[kind]))
            )
        segments.append((kind, name))
    return tuple(segments)


def named_key(obj: Any) -> str | None:
    """`obj`'s natural key, or `None` when an object in its chain has no name."""
    chain: list[str] = []
    node: Any = obj
    seen: set[int] = set()
    while node is not None and id(node) not in seen:
        seen.add(id(node))
        name = getattr(node, "name", None)
        if not isinstance(name, str):
            return None
        chain.append(f"{type(node).__name__}:{name}")
        node = getattr(node, "type", None)
    return "/".join(reversed(chain))


def describe_target(obj: Any) -> str:
    """`Feature 'x'` for a plan object, the quoted key for a natural key."""
    if isinstance(obj, str):
        return repr(obj)
    kind = type(obj).__name__
    name = getattr(obj, "name", None)
    return kind if name is None else f"{kind} {name!r}"


def migration_call(migration: DataMigration) -> str:
    """How a summary or an error names `migration`: `map_values()`.

    A name a helper qualified (`fill_missing[organism]`) is shown as it is.
    """
    name = migration.name
    return f"{name}()" if name.isidentifier() else name


def _or(parts: Sequence[str]) -> str:
    return parts[0] if len(parts) == 1 else ", ".join(parts[:-1]) + " or " + parts[-1]


def _check_target(
    plan: str, obj: Materializable | str, migration: DataMigration
) -> DeclarationTarget | None:
    """Refuse a target `migration` does not answer for; return the one it matched.

    `None` means the migration declares no targets, so only the shape of
    `obj` is checked.  A natural key may address any target of its leaf kind:
    the engine looks every declaration up by key, which is also how a schema no
    plan manages -- one a deleted feature is detached from -- is answered for.
    It matches a removal first, which is the only use of a key for an object
    the plan still declares.
    """
    call = migration_call(migration)
    if not isinstance(obj, (str, Materializable)):
        raise PlanDeclarationError(
            f"plan {plan!r}: ctx.migrate(..., {call}) is declared for a "
            f"{type(obj).__name__}; it takes a plan object (a Feature, Schema, "
            "Sheet or Record) or a natural key."
        )
    by_key = isinstance(obj, str)
    if by_key:
        try:
            kind = parse_natural_key(obj)[-1][0]  # type: ignore[arg-type]
        except ValueError as exc:
            raise PlanDeclarationError(
                f"plan {plan!r}: ctx.migrate({obj!r}, {call}) names no object: "
                f"{obj!r} is not a natural key ({exc})."
            ) from None
    else:
        kind = type(obj).__name__
    targets = getattr(migration, "targets", None)
    if not targets:
        return None
    matches = sorted(
        (t for t in targets if t.kind == kind and (by_key or not t.by_key)),
        key=lambda t: not t.by_key,
    )
    if matches:
        return matches[0]
    actual = f"the natural key {obj!r}" if by_key else describe_target(obj)
    hint = ""
    if not by_key and all(t.by_key for t in targets):
        hint = (
            " It answers for an object the plan no longer declares, so it is "
            "declared by that object's natural key."
        )
    raise PlanDeclarationError(
        f"plan {plan!r}: {call} answers "
        f"{_or([t.describe() for t in targets])}, not {actual}.{hint}"
    )


def check_declarations(
    plan: str,
    migrations: Iterable[Migrated],
    moves: Iterable[Moved],
    declared: dict[str, str],
) -> None:
    """Refuse a declaration of `plan` that can never do what it says.

    `declared` maps the natural key of every object the plan and its
    prerequisites declare to who declares it (`"the plan"`, `"prerequisite
    plan 'base'"`).  Raises :class:`PlanDeclarationError` for the first problem.
    """
    for migrated in migrations:
        obj, migration = migrated.obj, migrated.migration
        target = _check_target(plan, obj, migration)
        if target is None or not target.by_key:
            continue
        owner = declared.get(str(obj))
        if owner is None:
            continue
        hint = ""
        if any(t.kind == target.kind and not t.by_key for t in migration.targets or ()):
            hint = f" To answer a change to it, pass the plan's {target.kind} object."
        raise PlanDeclarationError(
            f"plan {plan!r}: {migration_call(migration)} is declared for {obj!r}, "
            f"which {owner} still declares. It answers for that object's removal, "
            "so it has nothing to do while the object is declared: stop declaring "
            f"the object, or remove the ctx.migrate.{hint}"
        )
    for move in moves:
        _check_move(plan, move)


def _check_move(plan: str, move: Moved) -> None:
    obj, from_ = move.obj, move.from_
    label = f"ctx.moved({describe_target(obj)}, {from_!r})"
    if ":" in from_ or "/" in from_:
        try:
            parse_natural_key(from_)
        except ValueError as exc:
            raise PlanDeclarationError(
                f"plan {plan!r}: {label} names no previous address: {from_!r} is "
                f"neither a bare previous name nor a natural key ({exc})."
            ) from None
    if from_ in (getattr(obj, "name", None), named_key(obj)):
        raise PlanDeclarationError(
            f"plan {plan!r}: {label} names the object's current address. The "
            "second argument is where it was before: its previous name, or its "
            "previous natural key."
        )
