"""A plan schema edited onto the content of another live schema.

lamindb addresses a schema by a hash of its content -- members, optional markers,
index and set flags, but not its name or type -- and since 2.12 `Schema.save()`
refuses to rehash a schema onto a hash another live schema already holds.  An
in-place reconcile can do exactly that.

The usual culprit is an *inferred* schema.  Curating an artifact links, besides
the schema it was validated against, a schema of the columns it actually carries
(`ArtifactSchema`, slot `columns`).  lamindb builds it content-addressed: it
reuses the validating schema when the content matches, and otherwise creates an
unnamed, untyped one -- e.g. for a sheet export of a schema with optional
members, since the columns record carries no optional markers.  Making those
members required then gives the plan's schema exactly that content.

Such a schema records nothing the plan's schema does not say once the change is
saved -- had it looked that way at curation time, lamindb would have reused it --
so it is *absorbed*: its artifact links are re-pointed to the plan's schema and
it is deleted.  Anything else holding the content is refused with the reasons,
unless a declaration (or a repair's `--merge-duplicates`) says to *merge* it:
everything referencing it is re-pointed and it is trashed (:func:`merge_into`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ._sheet_exports import COLUMNS_SLOT

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from ..core import LaminSchemasError

__all__ = [
    "SchemaCollision",
    "absorb",
    "classify",
    "find_collisions",
    "merge_into",
    "own_content",
    "refusal",
]

_INFERRED_LINK_MODEL = "ArtifactSchema"

# What a reference to the colliding schema means, by `(registry, field)`.
_REFERENCES: dict[tuple[str, str], str] = {
    ("Artifact", "schema"): "{n} artifact(s) validate against it",
    ("Collection", "schema"): "{n} collection(s) validate against it",
    ("Record", "schema"): "it is the schema of {n} sheet(s)",
    ("SchemaComponent", "component"): "it is a slot of {n} composite schema(s)",
    ("SchemaComponent", "composite"): "it is a composite schema",
    ("SchemaProject", "schema"): "it is annotated with {n} project(s)",
    ("SchemaBlock", "schema"): "it carries {n} block(s)",
    ("Schema", "type"): "it types {n} schema(s)",
}


@dataclass(frozen=True)
class SchemaCollision:
    """A live schema already holding the content a plan schema is changed to.

    `blockers` say why it cannot be absorbed and are empty when it can;
    `inferred_links` is how many artifacts record it as an inferred schema.
    """

    schema: Any
    blockers: tuple[str, ...]
    inferred_links: int

    @property
    def absorbable(self) -> bool:
        return not self.blockers

    @property
    def label(self) -> str:
        name = getattr(self.schema, "name", None)
        uid = getattr(self.schema, "uid", "?")
        return f"schema {name!r} ({uid})" if name else f"unnamed schema {uid!r}"

    def detail(self) -> str:
        """The change detail recorded when it is absorbed."""
        return (
            f"absorbs inferred schema {getattr(self.schema, 'uid', '?')!r} with "
            f"the same content (recorded columns of {self.inferred_links} "
            "artifact(s))"
        )


def find_collisions(
    model: Any, hashes: Mapping[Any, str], *, using: str | None = None
) -> dict[Any, list[Any]]:
    """The live schemas already holding each hash, keyed like `hashes`.

    `hashes` maps a schema's primary key to the hash it is about to take; the
    schema itself is never reported against its own entry.  Trashed schemas do
    not count, exactly as in lamindb's own check.  One query for all of them.
    """
    from django.db.models import Q

    if not hashes:
        return {}
    holders: dict[str, list[Any]] = {}
    for schema in model.objects.using(using).filter(
        ~Q(branch_id=-1), hash__in=set(hashes.values())
    ):
        holders.setdefault(schema.hash, []).append(schema)
    return {
        pk: [schema for schema in holders.get(value, []) if schema.pk != pk]
        for pk, value in hashes.items()
    }


def _member_link_model(model: Any) -> Any:
    """The link table holding a schema's feature members (`SchemaFeature`)."""
    return model._meta.get_field("features").through


def _reverse_references(model: Any) -> Iterable[Any]:
    """Every foreign key elsewhere pointing at a schema, as Django relations."""
    for relation in model._meta.get_fields():
        if relation.one_to_many and relation.auto_created and not relation.concrete:
            yield relation


def classify(candidate: Any, *, managed: bool) -> SchemaCollision:
    """Whether `candidate` is an inferred schema that may be absorbed.

    It may when it is unnamed, untyped, not a type, not managed by a plan, a
    feature schema, linked as the inferred schema of at least one artifact, and
    referenced by nothing else.  The last test reads *every* reverse relation
    lamindb defines -- only its own member links and its inferred links are
    expected -- so a reference a future lamindb adds refuses the merge rather
    than being cascaded away with the schema.
    """
    blockers: list[str] = []
    name = getattr(candidate, "name", None)
    if name is not None:
        blockers.append(f"it is named {name!r}")
    if getattr(candidate, "type_id", None) is not None:
        blockers.append("it has a schema type")
    if getattr(candidate, "is_type", False):
        blockers.append("it is a schema type")
    if managed:
        blockers.append("it is managed by a plan")
    if not str(getattr(candidate, "itype", None) or "").startswith("Feature"):
        blockers.append("it is not a feature schema")
    model = type(candidate)
    using = candidate._state.db
    members = _member_link_model(model)
    inferred = 0
    for relation in _reverse_references(model):
        related = relation.related_model
        if related is members:
            continue
        count = (
            related._base_manager.using(using)
            .filter(**{relation.field.name: candidate.pk})
            .count()
        )
        if related.__name__ == _INFERRED_LINK_MODEL:
            inferred = count
            continue
        if count:
            template = _REFERENCES.get(
                (related.__name__, relation.field.name),
                f"{{n}} {related.__name__} row(s) reference it",
            )
            blockers.append(template.format(n=count))
    if not inferred:
        blockers.append("no artifact records it as an inferred schema")
    return SchemaCollision(
        schema=candidate, blockers=tuple(blockers), inferred_links=inferred
    )


def absorb(schema: Any, collision: SchemaCollision) -> dict[Any, set[Any]]:
    """Re-point `collision`'s inferred links to `schema` and delete it.

    Returns the columns it recorded per artifact (feature ids, keyed by the
    artifact's primary key): once re-pointed, an artifact's columns link is the
    plan's own schema, which a sheet-export check would otherwise read as no
    evidence about the file at all.  An artifact holds at most one link per slot,
    so moving the row cannot collide with an existing one.
    """
    candidate = collision.schema
    model = type(candidate)
    using = candidate._state.db
    members = _member_link_model(model)
    member_fk = next(
        f.attname
        for f in members._meta.fields
        if f.is_relation and f.related_model is not model
    )
    columns = set(
        members._base_manager.using(using)
        .filter(schema_id=candidate.pk)
        .values_list(member_fk, flat=True)
    )
    links = next(
        relation.related_model
        for relation in _reverse_references(model)
        if relation.related_model.__name__ == _INFERRED_LINK_MODEL
    )
    rows = links._base_manager.using(using).filter(schema_id=candidate.pk)
    recorded = {
        artifact_id: set(columns)
        for artifact_id, slot in rows.values_list("artifact_id", "slot")
        if slot == COLUMNS_SLOT
    }
    rows.update(schema_id=schema.pk)
    candidate.delete(permanent=True)
    return recorded


def own_content(model: Any) -> tuple[Any, Any]:
    """The foreign keys that make up a schema's own content: members and slots."""
    return (
        model.features.through._meta.get_field("schema"),
        model.components.through._meta.get_field("composite"),
    )


def merge_into(
    schema: Any, holder: Any, *, error: type[LaminSchemasError]
) -> tuple[int, int]:
    """Trash `holder` and point everything referencing it at `schema`.

    `schema` holds (or is about to hold) the same content.  `holder` goes to
    the trash first, so it no longer holds the content hash, and keeps its own
    member and slot links: in the trash it is the schema it was.  Everything
    else referencing it moves -- artifacts and collections it validates, sheets,
    composites slotting it, projects, blocks, inferred links -- with a link
    `schema` already has dropped.  Returns how many rows were moved and how
    many dropped; a clash raises `error` for the caller's transaction.
    """
    from ._repoint import repoint_references
    from ._trash import delete_object

    model = type(holder)
    delete_object(model.objects.get(pk=holder.pk))
    return repoint_references(
        model, holder.pk, schema.pk, error=error, keep=own_content(model)
    )


def refusal(key: str, reasons: list[str]) -> str:
    """The message refusing a change that would give `key` taken content."""
    return (
        f"cannot change {key!r} in place: the change gives it the same content as "
        "another live schema, and lamindb keeps a single schema per content "
        "hash:\n  "
        + "\n  ".join(reasons)
        + "\n  Change the plan so the two differ, resolve the other schema first, "
        "or declare ctx.migrate(schema, merge_schema('<its uid>')) to point "
        "everything referencing it at this one. Only an unnamed, untyped "
        "inferred schema that nothing else references is absorbed automatically."
    )
