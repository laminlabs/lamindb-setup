"""The schema-drift taxonomy, expressed as code.

When an object's recorded configuration diverges from the plan (or from the live
instance), each changed property has to be classified: is the change *safe* --
every record already validated against the object stays valid, so the change can
be applied in place as an `UPDATE` -- or does it *potentially require a data
change*, in which case it is refused as drift?

The rule tables below are the single source of truth for that decision.  They are
meant to read like the taxonomy itself:

===========================  ==========================  =====================
property                     safe transition             blocked transition
===========================  ==========================  =====================
Feature.nullable             `False -> True`           `True -> False`
Feature.coerce               any direction               --
Feature.default_value        any direction               --
Schema.flexible              `False -> True`;          `True -> False` with
                             `True -> False` with      `maximal_set=True`
                             `maximal_set=False`
Schema.coerce                any direction               --
Schema.maximal_set           `True -> False`           `False -> True`
Schema.minimal_set           `True -> False`           `False -> True`
Schema.ordered_set           `True -> False`           `False -> True`
Schema.optionals             mark a member optional      make a member required
Feature.dtype (scalar)       `int -> float`,           `float -> int`,
                             `int -> num`,             `str -> int`, ...
                             `float -> num`,
                             `<any> -> str`
Feature.dtype (cat -> cat)   add a registry              drop a registry
Feature.dtype (cat filters)  drop a filter               add or change a filter
Feature.cat_filters          drop a filter               add or change a filter
Feature.dtype (cat -> str)   any `cat[...] -> str`     --
Feature.dtype (scalar->cat)  --                          `<scalar> -> cat[...]`
Feature.dtype (list[...])    widen the element type      narrow the element type
Feature.dtype (list-ness)    --                          scalar <-> list (both ways)
Schema.dtype                 --                          any change
everything else              --                          any change
===========================  ==========================  =====================

Rationale for the `dtype` rules:

* Widening the numeric ladder (`int` -> `float` -> `num`) or rendering any
  value as a string never invalidates an existing value.  The reverse might
  (a float may not be an int, an arbitrary string may not parse as an int).
* A categorical dtype is a *union* of registries (`cat[A|B]`).  Adding a member
  only admits more labels, so existing labels stay valid; removing one can strand
  records that used it.
* A categorical value is stored as its label's name string, so widening a
  `cat[...]` feature to a free `str` never invalidates an existing value and
  is *revalidation*-safe.  (The apply layer holds such a change back when the
  feature still has categorical values: those live in per-registry link tables an
  in-place dtype flip does not touch, so they must first be relocated to the
  scalar store -- see :meth:`ApplyContext._dtype_change_needs_relocation`.  This
  taxonomy stays pure and data-independent, so it reports only the revalidation
  verdict.)  The reverse (`<scalar> -> cat[...]`) is refused here: an arbitrary
  scalar is not guaranteed to be a registered label.  That block is only lifted
  by the data-aware override (see :mod:`lamindb_setup.evolve.engine._drift_data`) once
  every stored value is shown to be a valid label in the target registry.
* A categorical is compared by its *registry set*, ignoring the module prefix and
  a registry's *default* name field.  So a bare `cat[bionty.Gene]` equals the
  fully-qualified `cat[bionty.Gene.symbol]` (`symbol` is Gene's default
  field).  A *non-default* name field (`cat[bionty.Gene.ensembl_gene_id]`)
  selects a different match and stays distinct.  See :func:`canonical_dtype_str`.
* The inline `[...]` filter payload lamindb bakes into a categorical's stored
  string (a Record `type__uid`, a bionty `source__uid`, `is_type=True`) is
  part of the dtype too, compared term by term (see :func:`dtype_filter_terms`).
  Each term only *narrows* the labels a value may be, so dropping one admits
  more labels and is safe, while adding or changing one can strand a value that
  satisfied the old filter: `cat[Record[A]] -> cat[Record[B]]` is blocked, even
  though both are `cat[Record]`.  A plan carries its filters next to the bare
  dtype, as `cat_filters`, under the same rule.
* `None` in a declarative annotation (`Feature[int | None]`) is shorthand for
  `nullable=True`; lamindb has no null dtype, so nullability is handled by the
  `nullable` field above rather than by a dtype union.
* Each of a schema's three set flags constrains a dataset more when `True`:
  `maximal_set` admits no column beyond the members, `minimal_set` requires
  every member that is not optional (an index stays required either way), and
  `ordered_set` requires the members in their declared order.  Dropping one only
  admits more datasets; raising one can reject a dataset that validated.
* `Schema.optionals` is the *set* of members a record may omit, so it follows
  the same widening logic as a categorical union: adding a member to it only
  admits more records, while removing one (making the member required again)
  invalidates every record that omitted it.
* Changing a dtype's *shape* -- scalar vs. `list[...]` vs. `cat[...]` -- is
  blocked, with the sole exception of `cat[...] -> str` above.  A list may hold
  several values (it cannot collapse to a scalar) and an existing scalar value is
  not itself a list, so either direction potentially requires a data change.
  Only a `list[...] -> list[...]` change keeps its shape and is classified by
  its element type.
* These are a *feature's* dtype rules.  A schema records a `dtype` too -- what
  the members of an `itype` schema must be -- but nothing changes it in place,
  so any change to it is blocked; only a respelling of the same dtype is not a
  change.

Rationale for the `Schema.flexible` rule, which -- alone in the table -- depends
on another property of the object:

* lamindb uses the two set flags for different things.  `flexible` decides which
  columns are *pulled into* validation: `True` validates and annotates every
  column whose name matches a registered feature, `False` only the members.
  Whether a column nothing validates *fails* is `maximal_set`'s business
  (pandera's `strict`, plus the column-name check).
* So under `maximal_set=False` turning `flexible` off only skips checks a
  dataset already passed, and is safe.  Under `maximal_set=True` it shrinks the
  admitted columns from "any registered feature" to "the members", and a dataset
  that carried a matched non-member column would now be rejected.
* Turning it on is never refused.  Under `maximal_set=True` every column of a
  dataset that passed was a member, so there is nothing new to match.  Under
  `maximal_set=False` the extra columns are now validated -- by name alone,
  against whichever registered feature carries that name -- which can fail data
  that passed; :func:`caveat` reports that as a warning instead.
* The rule reads `maximal_set` as it is *after* the change (`context`), so
  turning both flags off in one edit is safe.  A change to `maximal_set` itself
  is judged by its own rule; without the context the flexible rule stays
  conservative and blocks turning `flexible` off.

The classifier is pure and lamindb-independent: it operates on the serialized
values already captured in the lock / remote snapshots.  It tolerates every dtype
string encoding the codebase produces -- the canonical lamindb form
(`"int"` / `"cat[ULabel]"` / `"list[int]"`), the `builtins.int` /
quoted-`'num'` form emitted by
:func:`~lamindb_setup.evolve.engine._lock.serialize_config`, and the legacy
registry-class-path form (`bionty.models.Gene`) written by older versions --
by normalizing them through :func:`canonical_dtype_str` before comparing.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from ..authoring import dtype_filter_terms, dtypes_match, parse_dtype

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "Verdict",
    "caveat",
    "classify",
]


class Verdict(Enum):
    """Outcome of classifying a single property change."""

    SAFE = "safe"
    """No revalidation needed: apply the change in place as an `UPDATE`."""

    BLOCKED = "blocked"
    """May invalidate already-validated records: refuse as a `DriftError`."""


# Boolean flags where only *widening* (`False -> True`) keeps existing records
# valid; the reverse can strand records and is refused.
#
# * `Feature.nullable`: `True` admits `None`; reverting to `False` would
#   invalidate any record that stored a `None`.
#
# `Schema.flexible` looks like one but is not: see :func:`_classify_flexible`.
_WIDEN_FALSE_TO_TRUE: frozenset[tuple[str, str]] = frozenset(
    {
        ("Feature", "nullable"),
    }
)

# Boolean flags where `True` constrains a dataset more, so only *dropping* the
# constraint (`True -> False`) keeps existing records valid; raising it can
# reject a dataset that validated and is refused.  See the module docstring.
_WIDEN_TRUE_TO_FALSE: frozenset[tuple[str, str]] = frozenset(
    {
        ("Schema", "maximal_set"),
        ("Schema", "minimal_set"),
        ("Schema", "ordered_set"),
    }
)

# Fields that only influence how *future* values are ingested and never the
# validity of records already stored, so any change is safe in either direction.
_FUTURE_ONLY: frozenset[tuple[str, str]] = frozenset(
    {
        ("Feature", "coerce"),
        ("Feature", "default_value"),
        ("Schema", "coerce"),
    }
)

# Set-valued properties where only *growing* the set keeps existing records
# valid; shrinking it can strand records and is refused.
#
# * `Schema.optionals`: the members a record is allowed to omit.  Marking one
#   more member optional only admits more records, while dropping a member from
#   the set makes it required again and invalidates every record that omitted
#   it.  The members are addressed by whichever stable key the caller's snapshot
#   uses (natural keys in a plan/lock configuration, uids in a remote snapshot);
#   both sides of a comparison always use the same one.
_WIDEN_BY_SUPERSET: frozenset[tuple[str, str]] = frozenset(
    {
        ("Schema", "optionals"),
    }
)

# Directed "is safely widened to" graph over simple (non-categorical) dtypes.
# `<any> -> str` is handled separately (every value renders as a string).
_SCALAR_WIDENS_TO: dict[str, frozenset[str]] = {
    "int": frozenset({"float", "num"}),
    "float": frozenset({"num"}),
}


def classify(
    kind: str,
    field: str,
    old: object,
    new: object,
    *,
    context: Mapping[str, object] | None = None,
) -> Verdict:
    """Classify the change of `field` on a `kind` object from `old` to `new`.

    `old` / `new` are the serialized property values taken from a lock or
    remote snapshot.  Returns :attr:`Verdict.SAFE` when the change can be applied
    in place without revalidating existing records, or :attr:`Verdict.BLOCKED`
    when it potentially requires a data change.

    `context` holds the object's other properties as they are *after* the
    change -- the configuration or snapshot `new` comes from.  Only
    `Schema.flexible` reads it (its `maximal_set`); without it that rule stays
    conservative.
    """
    if old == new:
        return Verdict.SAFE
    if field == "dtype":
        if kind == "Feature":
            return _classify_dtype(old, new)
        # A schema's `dtype` (what an `itype` schema's members must be) is
        # not a feature's: nothing changes it in place, so only a respelling of
        # the same dtype is safe.
        return Verdict.SAFE if _same_dtype(old, new) else Verdict.BLOCKED
    if (kind, field) == ("Feature", "cat_filters"):
        return _classify_cat_filters(old, new)
    if (kind, field) == ("Schema", "slots"):
        # Dropping a slot stops checking that part of a dataset; adding one or
        # swapping a component judges datasets by something they never met.
        old_slots = dict(old) if isinstance(old, dict) else {}
        new_slots = dict(new) if isinstance(new, dict) else {}
        kept = all(old_slots.get(slot) == part for slot, part in new_slots.items())
        return Verdict.SAFE if kept else Verdict.BLOCKED
    if (kind, field) == ("RecordType", "schema"):
        # A record type without a schema validates nothing, so dropping one --
        # a sheet demoted to a plain record type -- only admits more.  Gaining
        # or swapping one starts validating rows that were never checked.
        return Verdict.SAFE if new is None else Verdict.BLOCKED
    if (kind, field) == ("Schema", "flexible"):
        return _classify_flexible(old, new, context)
    if (kind, field) in _FUTURE_ONLY:
        return Verdict.SAFE
    if (kind, field) in _WIDEN_FALSE_TO_TRUE:
        if not _as_bool(old) and _as_bool(new):
            return Verdict.SAFE
        return Verdict.BLOCKED
    if (kind, field) in _WIDEN_TRUE_TO_FALSE:
        if _as_bool(old) and not _as_bool(new):
            return Verdict.SAFE
        return Verdict.BLOCKED
    if (kind, field) in _WIDEN_BY_SUPERSET:
        if _as_set(new) >= _as_set(old):
            return Verdict.SAFE
        return Verdict.BLOCKED
    return Verdict.BLOCKED


def caveat(
    kind: str,
    field: str,
    old: object,
    new: object,
    *,
    context: Mapping[str, object] | None = None,
) -> str | None:
    """A warning for a change :func:`classify` lets through that can still bite.

    Only one such change exists: turning `Schema.flexible` on under
    `maximal_set=False`.  It is applied in place -- refusing it would make a
    schema impossible to open up -- but it starts validating columns nothing
    looked at before, matched to a feature by name alone (`Feature.name` is not
    unique), so it is reported.  `None` for every other change.
    """
    if (kind, field) != ("Schema", "flexible"):
        return None
    if _as_bool(old) or not _as_bool(new) or _maximal_set(context) is not False:
        return None
    return (
        "every column whose name matches a registered feature is now validated "
        "and annotated too, matched by name alone across all feature types, so a "
        "dataset with an extra column that such a feature rejects no longer "
        "validates -- including datasets already validated against this schema"
    )


def _maximal_set(context: Mapping[str, object] | None) -> bool | None:
    """`maximal_set` after the change, or `None` when `context` does not say."""
    if context is None or context.get("maximal_set") is None:
        return None
    return _as_bool(context.get("maximal_set"))


def _classify_flexible(
    old: object, new: object, context: Mapping[str, object] | None
) -> Verdict:
    """`Schema.flexible`: only turning it off under `maximal_set=True` is blocked.

    lamindb reads an unset flag as off, so `None` and `False` are the same.
    See the module docstring for why `maximal_set` decides.
    """
    if not _as_bool(old) or _as_bool(new):
        return Verdict.SAFE
    return Verdict.SAFE if _maximal_set(context) is False else Verdict.BLOCKED


def _as_bool(value: object) -> bool:
    """Coerce a serialized flag (possibly `None`/unset) to a plain bool."""
    return bool(value)


def _as_set(value: object) -> frozenset:
    """Coerce a serialized set-valued property (possibly `None`/unset) to a set."""
    if value is None:
        return frozenset()
    if isinstance(value, (list, tuple, set, frozenset)):
        return frozenset(value)
    return frozenset({value})


# --- dtype classification ---------------------------------------------------
#
# A parsed dtype is a small tagged tuple:
#   ("simple", "<token>")        e.g. ("simple", "int")
#   ("cat", frozenset({...}))    the set of pipe-separated cat[...] components
#   ("list", <parsed inner>)     a list[...] wrapper


def _same_dtype(old: object, new: object) -> bool:
    """Whether two serialized dtypes are spellings of one dtype, filters included."""
    return isinstance(old, str) and isinstance(new, str) and dtypes_match(old, new)


def _classify_dtype(old: object, new: object) -> Verdict:
    parsed_old = parse_dtype(old)
    parsed_new = parse_dtype(new)
    if parsed_old is None or parsed_new is None:
        # Unparseable / non-string: stay conservative.
        return Verdict.BLOCKED
    verdict = _classify_parsed(parsed_old, parsed_new)
    if verdict is Verdict.SAFE and not _filters_only_dropped(old, new):
        return Verdict.BLOCKED
    return verdict


def _filters_only_dropped(old: object, new: object) -> bool:
    """Whether every filter `new` puts on a categorical `old` already had.

    A component the change adds to a union is new territory -- no stored value
    can have been one of its labels -- so only the components on both sides are
    compared.
    """
    old_terms = dtype_filter_terms(old)
    return all(
        terms <= old_terms[component]
        for component, terms in dtype_filter_terms(new).items()
        if component in old_terms
    )


def _classify_cat_filters(old: object, new: object) -> Verdict:
    """A plan's `cat_filters`: only dropping a filter is safe.

    The serialized filters are a `{key: value}` mapping (a value may be a
    `{"ref": <natural key>}` reference); each entry narrows the labels a
    value may be, so a change is safe when it only removes entries.
    """
    old_filters = {} if old is None else old
    new_filters = {} if new is None else new
    if not isinstance(old_filters, dict) or not isinstance(new_filters, dict):
        return Verdict.BLOCKED
    if all(
        key in old_filters and old_filters[key] == value
        for key, value in new_filters.items()
    ):
        return Verdict.SAFE
    return Verdict.BLOCKED


def _classify_parsed(old: tuple, new: tuple) -> Verdict:
    if old == new:
        return Verdict.SAFE
    if old[0] != new[0]:
        # `cat[...] -> str` is safe: a categorical value is stored as its
        # label's name string, so rendering the feature as a free string never
        # invalidates an existing value.
        if old[0] == "cat" and new == ("simple", "str"):
            return Verdict.SAFE
        # Any other change of shape (scalar <-> list[...] <-> cat[...]) is
        # structural and never safe: a list may hold several values so it cannot
        # collapse to a scalar, an existing scalar value is not itself a list, and
        # an arbitrary scalar is not guaranteed to be a registered categorical
        # label.  `<scalar> -> cat[...]` is refused here and only lifted by the
        # data-aware override once every stored value validates as a label.
        return Verdict.BLOCKED
    tag = old[0]
    if tag == "list":
        # Same (list) shape: classify by the element type.
        return _classify_parsed(old[1], new[1])
    if tag == "cat":
        # Adding registries widens the union (existing labels stay valid);
        # removing any member can strand records.
        return Verdict.SAFE if old[1] <= new[1] else Verdict.BLOCKED
    return _classify_scalar(old[1], new[1])


def _classify_scalar(old: str, new: str) -> Verdict:
    if new == "str":
        # Every value renders as a string.
        return Verdict.SAFE
    if new in _SCALAR_WIDENS_TO.get(old, frozenset()):
        return Verdict.SAFE
    return Verdict.BLOCKED
