"""Structured, render-agnostic model of a :class:`~lamindb_setup.evolve.Plan` summary."""

from __future__ import annotations

from typing import Any

from typing_extensions import TypeVar

E = TypeVar("E", bound="PlanExports")

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..authoring import Plan, PlanExports

__all__ = [
    "DeclarationInfo",
    "ObjectInfo",
    "PlanRef",
    "SummaryModel",
    "build_summary_model",
]


# Ordered grouping of object kinds into user-facing sections. Kinds within a
# group are listed in dependency (topological) order; the groups themselves are
# shown in the order below.
_OBJECT_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Features", ("FeatureType", "Feature")),
    ("Schemas", ("SchemaType", "Schema")),
    ("Records", ("RecordType", "Record", "Sheet")),
)
_OTHER_GROUP = "Other"


@dataclass
class ObjectInfo:
    """A single object that a plan will materialize, plus its requirements."""

    obj_id: int
    kind: str
    name: str | None
    details: list[str] = field(default_factory=list)
    requires: list[int] = field(default_factory=list)
    parent_id: int | None = None

    @property
    def label(self) -> str:
        """A one-line human-readable label for the object."""
        text = self.kind if self.name is None else f"{self.kind} {self.name!r}"
        if self.details:
            text += " (" + ", ".join(self.details) + ")"
        return text

    @property
    def ref(self) -> str:
        """A short reference (kind and name only) for cross-referencing."""
        return self.kind if self.name is None else f"{self.kind} {self.name!r}"


@dataclass
class DeclarationInfo:
    """A `ctx.migrate` or `ctx.moved` the plan declares.

    `verb` is `"migrate"` or `"moved"`.  The target is a plan object --
    `kind` and `name` -- or a natural `key`.  `source` is what the
    declaration attaches: the migration (`map_values()`) or the previous
    address (`'old_b'`); `reason` the migration's.
    """

    verb: str
    source: str
    kind: str | None = None
    name: str | None = None
    key: str | None = None
    reason: str | None = None

    @property
    def target(self) -> str:
        """`Feature 'a'` for a plan object, the quoted key for a natural key."""
        if self.key is not None:
            return repr(self.key)
        kind = self.kind or "object"
        return kind if self.name is None else f"{kind} {self.name!r}"

    @property
    def label(self) -> str:
        """`migrate Feature 'a' ← map_values()`."""
        return f"{self.verb} {self.target} ← {self.source}"


@dataclass
class PlanRef:
    """A node in the plan dependency tree (a plan and its prerequisite plans)."""

    name: str
    default_space: str | None
    depends_on: list[PlanRef] = field(default_factory=list)


@dataclass
class SummaryModel:
    """A fully resolved, render-agnostic summary of a plan."""

    title: str
    plan: PlanRef
    objects: list[ObjectInfo] = field(default_factory=list)
    declarations: list[DeclarationInfo] = field(default_factory=list)

    @property
    def heading(self) -> str:
        """The summary heading combining the title with the plan name."""
        return f"{self.title}: {self.plan.name}"

    @property
    def objects_by_id(self) -> dict[int, ObjectInfo]:
        """Map object ids to their :class:`ObjectInfo`."""
        return {obj.obj_id: obj for obj in self.objects}

    def requirements_of(
        self, obj: ObjectInfo, *, exclude_parent: bool = True
    ) -> list[ObjectInfo]:
        """Return the objects that `obj` directly requires.

        By default the object's `type` parent is omitted because it is shown
        through nesting rather than as a cross-reference.
        """
        by_id = self.objects_by_id
        return [
            by_id[req]
            for req in obj.requires
            if req in by_id and not (exclude_parent and req == obj.parent_id)
        ]

    def children_of(self, obj: ObjectInfo) -> list[ObjectInfo]:
        """Return the objects nested under `obj` via their `type` parent."""
        return [other for other in self.objects if other.parent_id == obj.obj_id]

    def roots_of(self, objects: list[ObjectInfo]) -> list[ObjectInfo]:
        """Return the top-level objects of `objects` (no parent within the set)."""
        ids = {obj.obj_id for obj in objects}
        return [
            obj for obj in objects if obj.parent_id is None or obj.parent_id not in ids
        ]

    @property
    def grouped_objects(self) -> list[tuple[str, list[ObjectInfo]]]:
        """Group objects by kind into ordered, user-facing sections.

        Each object appears exactly once, in dependency (topological) order
        within its group. Groups follow :data:`_OBJECT_GROUPS`; any unrecognized
        kind falls into an `Other` group rendered last.
        """
        kind_to_group = {kind: name for name, kinds in _OBJECT_GROUPS for kind in kinds}
        buckets: dict[str, list[ObjectInfo]] = {name: [] for name, _ in _OBJECT_GROUPS}
        for obj in self.objects:
            group = kind_to_group.get(obj.kind, _OTHER_GROUP)
            buckets.setdefault(group, []).append(obj)
        return [(name, objects) for name, objects in buckets.items() if objects]

    @property
    def plans_by_name(self) -> dict[str, PlanRef]:
        """Map plan names to their :class:`PlanRef`, deduplicated by name."""
        result: dict[str, PlanRef] = {}

        def visit(plan: PlanRef) -> None:
            if plan.name in result:
                return
            result[plan.name] = plan
            for dep in plan.depends_on:
                visit(dep)

        visit(self.plan)
        return result

    @property
    def base_plans(self) -> list[PlanRef]:
        """The plans with no prerequisite plans (zero dependencies)."""
        return [p for p in self.plans_by_name.values() if not p.depends_on]

    def dependents_of_plan(self, plan: PlanRef) -> list[PlanRef]:
        """Return the plans that directly depend on `plan`."""
        return [
            p
            for p in self.plans_by_name.values()
            if any(dep.name == plan.name for dep in p.depends_on)
        ]


def _describe_dtype(dtype: Any) -> str:
    inner = getattr(dtype, "dtype", dtype)
    return getattr(inner, "__name__", None) or repr(inner)


def _truncate(text: str, limit: int = 50) -> str:
    """Collapse whitespace and truncate `text` to `limit` characters."""
    collapsed = " ".join(text.split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: limit - 1].rstrip() + "…"


def _describe(obj: Any) -> tuple[str, str | None, list[str]]:
    """Describe an object as `(kind, name, details)` for the summary.

    `details` is an ordered list of compact `key=value` fragments describing
    the object's *configuration*. Relational attributes that are already
    conveyed by the rendered layout are intentionally omitted: the `type`
    parent (shown via nesting) and the bound `schema` (shown via `requires`).
    """
    kind = type(obj).__name__
    name = getattr(obj, "name", None)
    details: list[str] = []

    # Data type (Feature, dynamic Schema).
    dtype = getattr(obj, "dtype", None)
    if dtype is not None:
        details.append(f"dtype={_describe_dtype(dtype)}")

    # Feature configuration.
    unit = getattr(obj, "unit", None)
    if unit:
        details.append(f"unit={unit!r}")
    synonyms = getattr(obj, "synonyms", None)
    if synonyms:
        details.append("synonyms=" + ", ".join(repr(s) for s in synonyms))
    nullable = getattr(obj, "nullable", None)
    if nullable is not None:
        details.append(f"nullable={nullable}")
    default_value = getattr(obj, "default_value", None)
    if default_value is not None:
        details.append(f"default={default_value!r}")

    # Schema structure and container type.
    otype = getattr(obj, "otype", None)
    if otype:
        details.append(f"otype={otype!r}")
    features = getattr(obj, "features", None)
    if isinstance(features, (list, dict)):
        details.append(f"features={len(features)}")
    index = getattr(obj, "index", None)
    index_name = getattr(index, "name", None) if index is not None else None
    if index_name:
        details.append(f"index={index_name!r}")
    slots = getattr(obj, "slots", None)
    if isinstance(slots, dict) and slots:
        details.append(f"slots={len(slots)}")

    # Schema set semantics (only when set to a non-default value).
    if getattr(obj, "minimal_set", True) is False:
        details.append("minimal_set=False")
    if getattr(obj, "ordered_set", False) is True:
        details.append("ordered_set=True")
    if getattr(obj, "maximal_set", False) is True:
        details.append("maximal_set=True")
    flexible = getattr(obj, "flexible", None)
    if flexible is not None:
        details.append(f"flexible={flexible}")

    # Coercion (Feature and Schema).
    coerce = getattr(obj, "coerce", None)
    if coerce is not None:
        details.append(f"coerce={coerce}")

    # External references of a Record.
    reference = getattr(obj, "reference", None)
    if reference:
        details.append(f"reference={reference!r}")
    reference_type = getattr(obj, "reference_type", None)
    if reference_type:
        details.append(f"reference_type={reference_type!r}")

    # Free-text description, collapsed and truncated for readability.
    description = getattr(obj, "description", None)
    if isinstance(description, str) and description.strip():
        details.append(f"description={_truncate(description)!r}")

    # Explicit space override (None when defaulting to the plan-wide space).
    space_uid = getattr(obj, "space_uid", None)
    if space_uid is not None:
        details.append(f"space={str(space_uid)!r}")

    # Record-type space enforcement and experiment-sheet flag.
    single_space_uid = getattr(obj, "single_space_uid", None)
    if single_space_uid is not None:
        details.append(f"single_space={str(single_space_uid)!r}")
    if getattr(obj, "is_experiment_sheet", False):
        details.append("is_experiment_sheet=True")
    if getattr(obj, "exclude_children_from_plans", False):
        details.append("exclude_children_from_plans=True")

    return kind, name, details


def _build_plan_ref(plan: Plan[Any]) -> PlanRef:
    default_space = plan.ctx.default_space_uid
    return PlanRef(
        name=plan.name,
        default_space=repr(default_space) if default_space is not None else None,
        depends_on=[_build_plan_ref(p) for p in plan.depends_on],
    )


def _declaration(
    verb: str, obj: Any, source: str, reason: str | None = None
) -> DeclarationInfo:
    if isinstance(obj, str):
        return DeclarationInfo(verb=verb, source=source, key=obj, reason=reason)
    return DeclarationInfo(
        verb=verb,
        source=source,
        kind=type(obj).__name__,
        name=getattr(obj, "name", None),
        reason=reason,
    )


def _build_declarations(plan: Plan[Any]) -> list[DeclarationInfo]:
    """The plan's own `ctx.migrate` declarations, then its `ctx.moved` ones.

    A prerequisite's are its own to run, so they are not listed here.
    """
    from ..authoring import migration_call

    declarations = [
        _declaration(
            "migrate",
            migrated.obj,
            migration_call(migrated.migration),
            migrated.migration.reason,
        )
        for migrated in plan.ctx.migrations
    ]
    declarations.extend(
        _declaration("moved", move.obj, repr(move.from_)) for move in plan.ctx.moves
    )
    return declarations


def build_summary_model(plan: Plan[E], title: str) -> SummaryModel:
    """Finalize `plan` and build a render-agnostic :class:`SummaryModel`."""
    plan.finalize()

    dependencies = list(plan.dependencies)
    known_ids = {id(dep.obj) for dep in dependencies}

    objects: list[ObjectInfo] = []
    for dep in dependencies:
        kind, name, details = _describe(dep.obj)
        requires = [id(req) for req in dep.requires if id(req) in known_ids]
        parent = getattr(dep.obj, "type", None)
        parent_id = (
            id(parent) if parent is not None and id(parent) in known_ids else None
        )
        objects.append(
            ObjectInfo(
                obj_id=id(dep.obj),
                kind=kind,
                name=name,
                details=details,
                requires=requires,
                parent_id=parent_id,
            )
        )

    return SummaryModel(
        title=title,
        plan=_build_plan_ref(plan),
        objects=objects,
        declarations=_build_declarations(plan),
    )
